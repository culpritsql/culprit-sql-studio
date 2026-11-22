//! One owned fixture composition reuses the existing actor, command admission and cleanup owners.
use super::super::actor::Actor;
use super::*;
use culprit_core::{
    Result,
    telemetry::{CollectionStop, NativeActivityIdentity, SourceShutdown},
};

pub(super) const CASE_NAME: &str = "transaction-metadata";
const WRITING_CHILDREN: u32 = 66;
const MAX_ACTORS: usize = 4;
const MAX_FIXTURE_DISPATCHES: u32 = 64;
const MAX_FIXTURE_STATEMENTS: u32 = 512;
const MAX_RETIREMENT_OBSERVATIONS: u32 = 20;
const RETIREMENT_TIMEOUT: Duration = Duration::from_secs(3);
const RETIREMENT_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMetadataRole {
    Writer,
    Retainer,
    OtherOverflow,
    Replacement,
}
impl TransactionMetadataRole {
    fn index(self) -> usize {
        match self {
            Self::Writer => 0,
            Self::Retainer => 1,
            Self::OtherOverflow => 2,
            Self::Replacement => 3,
        }
    }
}
struct NativeActor {
    identity: NativeActivityIdentity,
    actor: Option<Actor>,
}
pub(super) struct TransactionMetadataState {
    actors: Vec<NativeActor>,
    actor_dispatch_charges: u32,
    actor_open_attempts: u32,
    statements: u32,
    retirement_observations: u32,
    retirement_unconfirmed: bool,
    pub(super) retirement_confirmed: bool,
}
impl TransactionMetadataState {
    pub(super) fn new() -> Self {
        Self {
            actors: vec![],
            actor_dispatch_charges: 0,
            actor_open_attempts: 0,
            statements: 0,
            retirement_observations: 0,
            retirement_unconfirmed: false,
            retirement_confirmed: false,
        }
    }
    pub(super) fn fence(&mut self) {
        self.retirement_unconfirmed = true;
        for slot in &mut self.actors {
            slot.actor.take();
        }
    }
}
/// Each child INSERT assigns a genuine writing subxid; neither SAVEPOINT count nor lock count is a verdict.
fn writing_children(schema: &str, actor: u32) -> (String, u32) {
    let table = format!("{}.items", quote_ident(schema));
    let mut sql = "BEGIN;SAVEPOINT retained_children".to_owned();
    for child in 0..WRITING_CHILDREN {
        sql.push_str(&format!(";SAVEPOINT child_{child};INSERT INTO {table}(actor,child) VALUES({actor},{child});RELEASE SAVEPOINT child_{child}"));
    }
    (sql, 2 + 3 * WRITING_CHILDREN)
}
impl MaintenanceFixture {
    pub async fn run_transaction_metadata(body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Box::pin(Self::run_owned(CASE_NAME, None, CASE_TIMEOUT, body)).await;
    }
    fn transaction_state(&self) -> &TransactionMetadataState {
        self.transaction_metadata.as_ref().expect("explicit transaction metadata fixture")
    }
    fn transaction_state_mut(&mut self) -> &mut TransactionMetadataState {
        self.transaction_metadata.as_mut().expect("explicit transaction metadata fixture")
    }
    pub fn transaction_identity(&self, role: TransactionMetadataRole) -> &NativeActivityIdentity {
        &self.transaction_state().actors[role.index()].identity
    }
    pub fn transaction_metadata_receipt(&self) -> serde_json::Value {
        let state = self.transaction_state();
        serde_json::json!({
            "actor_identities":state.actors.iter().map(|slot|&slot.identity).collect::<Vec<_>>(),
            "actor_open_attempts":state.actor_open_attempts,"actor_open_cap":MAX_ACTORS,
            "actor_dispatch_charges":state.actor_dispatch_charges,"admin_dispatches":self.fixture_queries,
            "fixture_dispatch_charges_including_ownership":2+state.actor_dispatch_charges+self.fixture_queries,
            "fixture_dispatch_cap_including_ownership":MAX_FIXTURE_DISPATCHES,
            "fixture_sql_statement_charges":state.statements,"fixture_sql_statement_cap":MAX_FIXTURE_STATEMENTS,
            "writing_children_per_writer":WRITING_CHILDREN,"retirement_observations":state.retirement_observations,
            "retirement_observation_cap":MAX_RETIREMENT_OBSERVATIONS,"retirement_confirmed":state.retirement_confirmed,
            "retirement_unconfirmed":state.retirement_unconfirmed,"source_queries_paid_separately":true,
            "body_deadline_seconds":CASE_TIMEOUT.as_secs(),"control_deadline_ms":CONTROL_TIMEOUT.as_millis().to_string(),
            "schema_identifier_bytes":self.schema.len(),"source_cost_qualified":false,
        })
    }
    fn admit_transaction_work(&self, dispatches: u32, statements: u32) -> Result<()> {
        case::require_active(&self.cancellation)?;
        let state = self.transaction_state();
        if self.actor_control_unconfirmed || self.admin_in_flight || state.retirement_unconfirmed {
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        if 2 + self.fixture_queries + state.actor_dispatch_charges + dispatches > MAX_FIXTURE_DISPATCHES
            || state.statements + statements > MAX_FIXTURE_STATEMENTS
        {
            return Err(CollectionStop::QueryLimit.error());
        }
        Ok(())
    }
    async fn transaction_execute(&mut self, sql: &str, statements: u32) {
        self.admit_transaction_work(1, statements).unwrap();
        self.transaction_state_mut().statements += statements;
        self.execute(sql).await;
    }
    async fn open_transaction_actor(&mut self, role: TransactionMetadataRole) {
        self.admit_transaction_work(2, 2).unwrap();
        assert_eq!(role.index(), self.transaction_state().actors.len(), "one finite native actor roster");
        assert!(self.transaction_state().actors.len() < MAX_ACTORS);
        self.connections_started += 1;
        let state = self.transaction_state_mut();
        state.actor_open_attempts += 1;
        state.actor_dispatch_charges += 2;
        state.statements += 2;
        self.actor_control_unconfirmed = true;
        let actor = Box::pin(Actor::open(
            &self.connector,
            self.specification.database.as_deref().unwrap(),
            &format!("culprit_owned_{}", owner()),
            "fixture-transaction-metadata",
            &self.cancellation,
        ))
        .await
        .unwrap();
        self.transaction_state_mut().actors.push(NativeActor { identity: actor.identity.clone(), actor: Some(actor) });
        self.actor_control_unconfirmed = false;
        self.transaction_actor_execute(role, "SET idle_in_transaction_session_timeout='35s'", 1).await;
    }
    async fn transaction_actor_execute(&mut self, role: TransactionMetadataRole, sql: &str, statements: u32) {
        self.try_transaction_actor_execute(role, sql, statements).await.unwrap();
    }
    async fn try_transaction_actor_execute(
        &mut self,
        role: TransactionMetadataRole,
        sql: &str,
        statements: u32,
    ) -> Result<()> {
        self.admit_transaction_work(1, statements)?;
        self.transaction_state_mut().actor_dispatch_charges += 1;
        self.transaction_state_mut().statements += statements;
        self.actor_control_unconfirmed = true;
        Box::pin(self.transaction_state_mut().actors[role.index()].actor.as_mut().unwrap().execute(sql)).await?;
        self.actor_control_unconfirmed = false;
        Ok(())
    }
    pub async fn prepare_transaction_metadata(&mut self) {
        assert!(self.transaction_state().actors.is_empty());
        self.transaction_execute("SET statement_timeout='1s';SET lock_timeout='1s'", 2).await;
        self.schema_created = true;
        self.transaction_execute(&format!("CREATE SCHEMA {}", quote_ident(&self.schema)), 1).await;
        self.transaction_execute(
            &format!("CREATE TABLE {}.items(actor integer,child integer)", quote_ident(&self.schema)),
            1,
        )
        .await;
        for role in
            [TransactionMetadataRole::Writer, TransactionMetadataRole::Retainer, TransactionMetadataRole::OtherOverflow]
        {
            Box::pin(self.open_transaction_actor(role)).await;
        }
        // This native snapshot installs an independent transaction-level backend_xmin; no application rows are read.
        self.transaction_actor_execute(
            TransactionMetadataRole::Retainer,
            "BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT pg_catalog.pg_current_snapshot()",
            2,
        )
        .await;
        for (role, actor) in [(TransactionMetadataRole::Writer, 1), (TransactionMetadataRole::OtherOverflow, 2)] {
            let (sql, statements) = writing_children(&self.schema, actor);
            self.transaction_actor_execute(role, &sql, statements).await;
        }
    }
    pub async fn rollback_writing_children(&mut self) {
        self.transaction_actor_execute(TransactionMetadataRole::Writer, "ROLLBACK TO SAVEPOINT retained_children", 1)
            .await;
    }
    pub async fn rollback_writer_transaction(&mut self) {
        self.transaction_actor_execute(TransactionMetadataRole::Writer, "ROLLBACK", 1).await;
    }
    async fn retire_transaction_actor(&mut self, role: TransactionMetadataRole) -> bool {
        if self.transaction_state().actors[role.index()].actor.is_none() {
            return true;
        }
        if self.try_transaction_actor_execute(role, "ROLLBACK", 1).await.is_err() {
            self.transaction_state_mut().retirement_unconfirmed = true;
            return false;
        }
        self.actor_control_unconfirmed = true;
        let mut actor = self.transaction_state_mut().actors[role.index()].actor.take().unwrap();
        let confirmed = Box::pin(actor.retire()).await;
        if confirmed {
            self.actor_control_unconfirmed = false;
        } else {
            self.transaction_state_mut().retirement_unconfirmed = true;
        }
        confirmed
    }
    async fn absent_transaction_actors(&mut self, identities: &[NativeActivityIdentity]) -> bool {
        let pids = identities.iter().map(|id| id.id.parse::<i32>().unwrap()).collect::<Vec<_>>();
        let deadline = tokio::time::Instant::now() + RETIREMENT_TIMEOUT;
        loop {
            if self.admit_transaction_work(1, 1).is_err() {
                return false;
            }
            if self.transaction_state().retirement_observations >= MAX_RETIREMENT_OBSERVATIONS {
                return false;
            }
            self.fixture_queries += 1;
            self.transaction_state_mut().statements += 1;
            self.transaction_state_mut().retirement_observations += 1;
            self.admin_in_flight = true;
            let result = tokio::time::timeout(
                CONTROL_TIMEOUT,
                self.admin.as_ref().unwrap().query(
                    &tagged("SELECT pid FROM pg_catalog.pg_stat_activity WHERE pid=ANY($1::integer[])"),
                    &[&pids],
                ),
            )
            .await;
            let rows = match result {
                Ok(Ok(rows)) => rows,
                _ => return false,
            };
            self.admin_in_flight = false;
            if rows.is_empty() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(RETIREMENT_INTERVAL).await;
        }
    }
    pub async fn replace_transaction_writer(&mut self) {
        let original = self.transaction_identity(TransactionMetadataRole::Writer).clone();
        assert!(Box::pin(self.retire_transaction_actor(TransactionMetadataRole::Writer)).await);
        let absent = self.absent_transaction_actors(std::slice::from_ref(&original)).await;
        if !absent {
            self.transaction_state_mut().retirement_unconfirmed = true;
        }
        assert!(absent, "the original backend must genuinely leave before opening a replacement");
        Box::pin(self.open_transaction_actor(TransactionMetadataRole::Replacement)).await;
        assert_ne!(self.transaction_identity(TransactionMetadataRole::Replacement), &original);
    }
    pub(super) fn admit_transaction_drop(&mut self) -> bool {
        if self.admit_transaction_work(1, 1).is_err() {
            return false;
        }
        self.transaction_state_mut().statements += 1;
        true
    }
    pub fn begin_transaction_probe(&mut self) {
        self.admit_transaction_work(0, 0).unwrap();
        self.actor_control_unconfirmed = true;
    }
    pub fn completed_transaction_probe(&mut self, shutdown: SourceShutdown) {
        assert_eq!(shutdown, SourceShutdown::Quiescent);
        assert!(self.actor_control_unconfirmed);
        self.actor_control_unconfirmed = false;
    }
    pub async fn retire_transaction_metadata(&mut self) -> bool {
        if self.transaction_state().retirement_confirmed {
            return true;
        }
        if self.transaction_state().retirement_unconfirmed || self.actor_control_unconfirmed || self.admin_in_flight {
            return false;
        }
        for role in [
            TransactionMetadataRole::Writer,
            TransactionMetadataRole::Retainer,
            TransactionMetadataRole::OtherOverflow,
            TransactionMetadataRole::Replacement,
        ] {
            if role.index() < self.transaction_state().actors.len()
                && !Box::pin(self.retire_transaction_actor(role)).await
            {
                return false;
            }
        }
        let identities = self.transaction_state().actors.iter().map(|slot| slot.identity.clone()).collect::<Vec<_>>();
        if !self.absent_transaction_actors(&identities).await {
            self.transaction_state_mut().retirement_unconfirmed = true;
            return false;
        }
        self.transaction_state_mut().retirement_confirmed = true;
        true
    }
    #[cfg(test)]
    pub(crate) fn unconnected_transaction_future_fixture() -> Self {
        let mut fixture = Self::unconnected_relation_future_fixture();
        fixture.relation_queue = None;
        fixture.transaction_metadata = Some(TransactionMetadataState::new());
        fixture
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_fixture_sql_charges_each_bounded_child_statement() {
        let (sql, statements) = writing_children("owned_schema", 1);
        assert_eq!(statements, 200);
        assert_eq!(sql.matches(&format!("INSERT INTO {}.items", quote_ident("owned_schema"))).count(), 66);
        assert_eq!(sql.matches("RELEASE SAVEPOINT child_").count(), 66);
        assert!(sql.starts_with("BEGIN;SAVEPOINT retained_children;SAVEPOINT child_0"));
        assert!(!sql.contains("pg_stat_reset") && !sql.contains("pg_locks") && !sql.contains("ALTER SYSTEM"));
    }
    #[tokio::test]
    async fn cancellation_admits_no_fixture_or_reader_phase_and_an_empty_actor_vector_does_not_heal_debt() {
        let mut fixture = MaintenanceFixture::unconnected_transaction_future_fixture();
        fixture.cancellation.cancel();
        assert!(std::panic::catch_unwind(AssertUnwindSafe(|| fixture.begin_transaction_probe())).is_err());
        assert!(!fixture.actor_control_unconfirmed);
        assert_eq!(fixture.fixture_queries, 0);
        fixture.transaction_state_mut().retirement_unconfirmed = true;
        assert!(!fixture.transaction_state().retirement_confirmed);
        assert!(fixture.transaction_state().actors.is_empty());
        assert!(!fixture.retire_transaction_metadata().await);
        assert!(!fixture.cleanup().await);
        assert!(fixture.transaction_state().retirement_unconfirmed);
        assert_eq!(fixture.fixture_queries, 0);
        assert_eq!(fixture.transaction_state().actor_dispatch_charges, 0);
    }
}
