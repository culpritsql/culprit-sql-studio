//! Fixture-only native locks, layered on the existing nonce, control, actor and cleanup owners.
use super::super::actor::Actor;
use super::*;
use culprit_core::telemetry::*;
use tokio_postgres::types::ToSql;

const QUEUE_CASE_TIMEOUT: Duration = Duration::from_secs(90);
const PARALLEL_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const PHASE_TIMEOUT: Duration = Duration::from_secs(3);
const PHASE_INTERVAL: Duration = Duration::from_millis(25);
const MAX_ACTORS: usize = 5;
const MAX_FIXTURE_DISPATCHES: u32 = MAX_PHASE_OBSERVATIONS + 32;
const MEDIUM_RELATIONS: u32 = 32;
const PARALLEL_ROWS: u32 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationQueueCase {
    Small,
    Medium,
    Parallel,
}
impl RelationQueueCase {
    pub fn name(self) -> &'static str {
        match self {
            Self::Small => "relation-queue-small",
            Self::Medium => "relation-queue-medium",
            Self::Parallel => "relation-queue-parallel",
        }
    }
    pub(super) fn from_name(name: &str) -> Option<Self> {
        [Self::Small, Self::Medium, Self::Parallel].into_iter().find(|case| case.name() == name)
    }
    fn relations(self) -> u32 {
        if self == Self::Medium { MEDIUM_RELATIONS } else { 1 }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationQueueRole {
    Holder,
    Gatekeeper,
    Affected,
}
impl RelationQueueRole {
    fn index(self) -> usize {
        match self {
            Self::Holder => 0,
            Self::Gatekeeper => 1,
            Self::Affected => 2,
        }
    }
}

pub(super) struct RelationQueueState {
    pub(super) case: RelationQueueCase,
    actors: Vec<Actor>,
    identities: Vec<NativeActivityIdentity>,
    pub(super) other_schema_created: bool,
    pub(super) statements: u32,
    retired_queries: u32,
    retired_cancel_attempts: u32,
    retirement_confirmed: bool,
    retirement_unconfirmed: bool,
    database_oid: Option<String>,
    relation_oid: Option<String>,
    competing_relation_oid: Option<String>,
}
impl RelationQueueState {
    pub(super) fn new(case: RelationQueueCase) -> Self {
        Self {
            case,
            actors: vec![],
            identities: vec![],
            other_schema_created: false,
            statements: 0,
            retired_queries: 0,
            retired_cancel_attempts: 0,
            retirement_confirmed: false,
            retirement_unconfirmed: false,
            database_oid: None,
            relation_oid: None,
            competing_relation_oid: None,
        }
    }
    pub(super) fn fence(&mut self) {
        self.retirement_unconfirmed = true;
        self.actors.clear();
    }
    pub(super) fn drop_sql(&self, schema: &str) -> String {
        let mut sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(schema));
        if self.other_schema_created {
            sql.push_str(&format!(";DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&format!("{schema}_other"))));
        }
        sql
    }
    pub(super) fn receipt(&self) -> serde_json::Value {
        serde_json::json!({"case":self.case.name(),"fixture_actor_queries":self.actor_queries(),"fixture_actor_cancel_attempts":self.retired_cancel_attempts+self.actors.iter().map(|actor|actor.cancellation_attempts).sum::<u32>(),
        "fixture_statements_upper_bound":self.statements,"actors_attempted_upper_bound":MAX_ACTORS,"actors_opened":self.identities.len(),"actual_native_actor_identities":self.identities,
        "retirement_confirmed":self.retirement_confirmed,"retirement_unconfirmed":self.retirement_unconfirmed,
        "database_oid":self.database_oid,"relation_oid":self.relation_oid,"competing_relation_oid":self.competing_relation_oid,
        "held_relation_fixture_count":self.case.relations(),"parallel_fixture_rows":if self.case==RelationQueueCase::Parallel {PARALLEL_ROWS}else{0},
        "case_timeout_seconds":QUEUE_CASE_TIMEOUT.as_secs(),"parallel_join_allowance_ms":PARALLEL_JOIN_TIMEOUT.as_millis().to_string(),
        "native_cost_qualified":false,"native_cpu_measured":false})
    }
    fn actor_queries(&self) -> u32 {
        self.retired_queries + self.actors.iter().map(|actor| actor.queries).sum::<u32>()
    }
}

impl MaintenanceFixture {
    pub async fn run_relation_queue(case: RelationQueueCase, body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Box::pin(Self::run_owned(case.name(), None, QUEUE_CASE_TIMEOUT, body)).await;
    }
    fn queue(&self) -> &RelationQueueState {
        self.relation_queue.as_ref().expect("explicit owned relation queue case")
    }
    fn queue_mut(&mut self) -> &mut RelationQueueState {
        self.relation_queue.as_mut().expect("explicit owned relation queue case")
    }
    pub fn relation_queue_case(&self) -> RelationQueueCase {
        self.queue().case
    }
    pub fn relation_identity(&self, role: RelationQueueRole) -> &NativeActivityIdentity {
        &self.queue().identities[role.index()]
    }
    pub fn relation_oid(&self) -> &str {
        self.queue().relation_oid.as_deref().unwrap()
    }
    pub fn relation_database_oid(&self) -> &str {
        self.queue().database_oid.as_deref().unwrap()
    }
    pub fn competing_relation_oid(&self) -> &str {
        self.queue().competing_relation_oid.as_deref().unwrap()
    }
    pub fn relation_receipt(&self) -> serde_json::Value {
        let mut value = self.queue().receipt();
        value["fixture_admin_dispatches"] = serde_json::json!(self.fixture_queries);
        value["fixture_dispatches_including_ownership"] =
            serde_json::json!(self.fixture_queries + self.queue().actor_queries() + 2);
        value["fixture_dispatch_cap_including_ownership"] = serde_json::json!(MAX_FIXTURE_DISPATCHES);
        value
    }
    fn admit_queue_dispatches(&self, additional: u32) {
        case::require_active(&self.cancellation).unwrap();
        assert!(self.fixture_queries + self.queue().actor_queries() + 2 + additional <= MAX_FIXTURE_DISPATCHES);
    }
    pub fn begin_relation_source_capture(&mut self) {
        case::require_active(&self.cancellation).unwrap();
        assert!(!self.admin_in_flight && !self.actor_control_unconfirmed);
        self.actor_control_unconfirmed = true;
    }
    /// Only the existing capture helper's returned manifest reaches here: it already checked actual close and metrics.
    pub fn completed_relation_source_capture(&mut self) {
        assert!(self.actor_control_unconfirmed);
        self.actor_control_unconfirmed = false;
    }
    async fn queue_execute(&mut self, sql: &str, statements: u32) {
        self.admit_queue_dispatches(1);
        self.queue_mut().statements += statements;
        self.execute(sql).await;
    }
    async fn queue_observe(&mut self, sql: &str, parameters: &[&(dyn ToSql + Sync)]) -> Vec<tokio_postgres::Row> {
        self.admit_queue_dispatches(1);
        assert!(!self.admin_in_flight && !self.actor_control_unconfirmed);
        assert!(self.phase_observations < MAX_PHASE_OBSERVATIONS);
        self.phase_observations += 1;
        self.fixture_queries += 1;
        self.queue_mut().statements += 1;
        self.admin_in_flight = true;
        let result =
            tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().query(&tagged(sql), parameters)).await;
        let rows = result.expect("owned relation observer deadline").expect("owned relation observer response");
        self.admin_in_flight = false;
        rows
    }
    async fn open_queue_actors(&mut self) {
        let marker = format!("culprit_owned_{}", owner());
        for role in 0..MAX_ACTORS {
            self.admit_queue_dispatches(2);
            self.connections_started += 1;
            self.queue_mut().statements += 2;
            self.actor_control_unconfirmed = true;
            let actor = Box::pin(Actor::open(
                &self.connector,
                self.specification.database.as_deref().unwrap(),
                &marker,
                "fixture-shared-relation-queue",
                &self.cancellation,
            ))
            .await
            .unwrap();
            self.queue_mut().identities.push(actor.identity.clone());
            self.queue_mut().actors.push(actor);
            self.actor_control_unconfirmed = false;
            self.actor_execute(role, "SET idle_in_transaction_session_timeout='35s'", 1).await;
        }
    }
    async fn actor_execute(&mut self, index: usize, sql: &str, statements: u32) {
        self.admit_queue_dispatches(1);
        assert!(!self.actor_control_unconfirmed && !self.admin_in_flight);
        self.queue_mut().statements += statements;
        self.actor_control_unconfirmed = true;
        Box::pin(self.queue_mut().actors[index].execute(sql)).await.unwrap();
        self.actor_control_unconfirmed = false;
    }
    fn actor_start(&mut self, index: usize, sql: String, statements: u32) {
        self.admit_queue_dispatches(1);
        assert!(!self.actor_control_unconfirmed && !self.admin_in_flight);
        self.queue_mut().statements += statements;
        self.actor_control_unconfirmed = true;
        self.queue_mut().actors[index].start(sql).unwrap();
        self.actor_control_unconfirmed = false;
    }
    async fn actor_settle(&mut self, index: usize, allowance: Duration) {
        case::require_active(&self.cancellation).unwrap();
        assert!(!self.actor_control_unconfirmed && !self.admin_in_flight);
        self.actor_control_unconfirmed = true;
        Box::pin(self.queue_mut().actors[index].settle_for(allowance)).await.unwrap();
        self.actor_control_unconfirmed = false;
    }
    pub async fn prepare_relation_queue(&mut self) {
        assert!(self.queue().identities.is_empty(), "fixture actors are admitted once");
        self.queue_execute("SET statement_timeout='1s';SET lock_timeout='1s'", 2).await;
        self.schema_created = true;
        self.queue_execute(&format!("CREATE SCHEMA {}", quote_ident(&self.schema)), 1).await;
        self.queue_mut().other_schema_created = true;
        self.queue_execute(&format!("CREATE SCHEMA {}", quote_ident(&format!("{}_other", self.schema))), 1).await;
        let (sql, statements) = self.relation_tables();
        self.queue_execute(&sql, statements).await;
        let primary = format!("{}.items", quote_ident(&self.schema));
        let other = format!("{}.items", quote_ident(&format!("{}_other", self.schema)));
        let rows=self.queue_observe("SELECT current_setting('server_version_num')::integer/10000 AS major,(SELECT proparallel::text FROM pg_catalog.pg_proc WHERE oid='pg_catalog.pg_sleep(double precision)'::regprocedure) AS sleep_parallel,current_database(),(SELECT oid::text FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_oid,$1::text::regclass::oid::text AS relation_oid,$2::text::regclass::oid::text AS other_oid",&[&primary,&other]).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 18, "this native safety acceptance requires actual PG18");
        assert_eq!(rows[0].get::<_, String>(1), "s");
        assert_eq!(rows[0].get::<_, String>(2), self.specification.database.as_ref().unwrap().as_str());
        self.queue_mut().database_oid = Some(rows[0].get(3));
        self.queue_mut().relation_oid = Some(rows[0].get(4));
        self.queue_mut().competing_relation_oid = Some(rows[0].get(5));
        assert_ne!(self.relation_oid(), self.competing_relation_oid());
        self.open_queue_actors().await;
        if self.relation_queue_case() == RelationQueueCase::Parallel {
            self.actor_execute(0,"SET max_parallel_workers_per_gather=2;SET min_parallel_table_scan_size=0;SET parallel_setup_cost=0;SET parallel_tuple_cost=0",4).await;
            self.actor_start(0, format!("SELECT count(*) FROM {primary} WHERE pg_catalog.pg_sleep(0.01) IS NULL"), 1);
            self.wait_relation_workers().await;
        } else {
            self.actor_execute(0, &format!("BEGIN;LOCK TABLE {} IN ACCESS SHARE MODE", self.held_relations()), 2).await;
        }
        self.actor_execute(3, &format!("BEGIN;LOCK TABLE {other} IN ACCESS SHARE MODE"), 2).await;
        self.actor_start(4, format!("BEGIN;LOCK TABLE {other} IN ACCESS EXCLUSIVE MODE;ROLLBACK"), 3);
        self.actor_start(1, format!("BEGIN;LOCK TABLE {primary} IN ACCESS EXCLUSIVE MODE;ROLLBACK"), 3);
        self.wait_relation_gate().await;
        self.actor_start(2, format!("BEGIN;LOCK TABLE {primary} IN ACCESS SHARE MODE;ROLLBACK"), 3);
        self.wait_relation_cascade().await;
    }
    fn relation_tables(&self) -> (String, u32) {
        let mut sql = format!(
            "CREATE TABLE {}.items(id integer,padding text);CREATE TABLE {}.items(id integer)",
            quote_ident(&self.schema),
            quote_ident(&format!("{}_other", self.schema))
        );
        let mut statements = 2;
        for index in 1..self.relation_queue_case().relations() {
            sql.push_str(&format!(";CREATE TABLE {}.load_{index}(id integer)", quote_ident(&self.schema)));
            statements += 1;
        }
        if self.relation_queue_case() == RelationQueueCase::Parallel {
            sql.push_str(&format!(";ALTER TABLE {}.items ALTER COLUMN padding SET STORAGE PLAIN;INSERT INTO {}.items SELECT value,repeat('x',2048) FROM generate_series(1,{PARALLEL_ROWS}) value;ANALYZE {}.items",quote_ident(&self.schema),quote_ident(&self.schema),quote_ident(&self.schema)));
            statements += 3;
        }
        (sql, statements)
    }
    fn held_relations(&self) -> String {
        std::iter::once(format!("{}.items", quote_ident(&self.schema)))
            .chain(
                (1..self.relation_queue_case().relations())
                    .map(|index| format!("{}.load_{index}", quote_ident(&self.schema))),
            )
            .collect::<Vec<_>>()
            .join(",")
    }
    async fn wait_relation_workers(&mut self) {
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        let holder = self.relation_identity(RelationQueueRole::Holder).id.parse::<i32>().unwrap();
        loop {
            let rows = self
                .queue_observe(
                    "SELECT count(*)::integer FROM pg_catalog.pg_stat_activity WHERE leader_pid=$1",
                    &[&holder],
                )
                .await;
            assert_eq!(rows.len(), 1);
            if rows[0].get::<_, i32>(0) > 0 {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "actual parallel workers were not established; no plan-only or skipped acceptance"
            );
            tokio::time::sleep(PHASE_INTERVAL).await;
        }
    }
    async fn wait_relation_gate(&mut self) {
        self.wait_relation_edges(false).await;
    }
    async fn wait_relation_cascade(&mut self) {
        self.wait_relation_edges(true).await;
    }
    async fn wait_relation_edges(&mut self, affected_required: bool) {
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        let holder = self.relation_identity(RelationQueueRole::Holder).id.parse::<i32>().unwrap();
        let gate = self.relation_identity(RelationQueueRole::Gatekeeper).id.parse::<i32>().unwrap();
        let affected = self.relation_identity(RelationQueueRole::Affected).id.parse::<i32>().unwrap();
        let relation = self.relation_oid().parse::<u32>().unwrap();
        let competing_holder = self.queue().identities[3].id.parse::<i32>().unwrap();
        let competing_gate = self.queue().identities[4].id.parse::<i32>().unwrap();
        let competing_relation = self.competing_relation_oid().parse::<u32>().unwrap();
        let pids = vec![holder, gate, affected, competing_holder, competing_gate];
        loop {
            let rows=self.queue_observe("WITH locks AS MATERIALIZED (SELECT pid,relation,mode,granted FROM pg_catalog.pg_locks WHERE pid=ANY($1::integer[]) LIMIT 129) SELECT pg_catalog.pg_blocking_pids($2),pg_catalog.pg_blocking_pids($3),(SELECT count(*) FROM locks WHERE pid=$4 AND relation=$5::oid AND granted AND mode='AccessShareLock'),(SELECT count(*) FROM locks WHERE pid=$2 AND relation=$5::oid AND NOT granted AND mode='AccessExclusiveLock'),(SELECT count(*) FROM locks WHERE pid=$3 AND relation=$5::oid AND NOT granted AND mode='AccessShareLock'),pg_catalog.pg_blocking_pids($6),(SELECT count(*) FROM locks WHERE pid=$7 AND relation=$8::oid AND granted AND mode='AccessShareLock'),(SELECT count(*) FROM locks WHERE pid=$6 AND relation=$8::oid AND NOT granted AND mode='AccessExclusiveLock')",&[&pids,&gate,&affected,&holder,&relation,&competing_gate,&competing_holder,&competing_relation]).await;
            assert_eq!(rows.len(), 1);
            let gate_owners: Vec<i32> = rows[0].get(0);
            let affected_owners: Vec<i32> = rows[0].get(1);
            let competing_owners: Vec<i32> = rows[0].get(5);
            if competing_owners.contains(&competing_holder)
                && rows[0].get::<_, i64>(6) > 0
                && rows[0].get::<_, i64>(7) > 0
                && gate_owners.contains(&holder)
                && rows[0].get::<_, i64>(2) > 0
                && rows[0].get::<_, i64>(3) > 0
                && (!affected_required || affected_owners.contains(&gate) && rows[0].get::<_, i64>(4) > 0)
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "same-resource native holder/pending gate/affected chain was not established"
            );
            tokio::time::sleep(PHASE_INTERVAL).await;
        }
    }
    pub async fn release_relation_queue(&mut self) {
        if self.relation_queue_case() == RelationQueueCase::Parallel {
            self.actor_settle(0, PARALLEL_JOIN_TIMEOUT).await;
        } else {
            self.actor_execute(0, "ROLLBACK", 1).await;
        }
        self.actor_settle(1, CONTROL_TIMEOUT).await;
        self.actor_settle(2, CONTROL_TIMEOUT).await;
        self.actor_execute(3, "ROLLBACK", 1).await;
        self.actor_settle(4, CONTROL_TIMEOUT).await;
    }
    pub async fn retire_relation_queue(&mut self) -> bool {
        if self.queue().retirement_confirmed {
            return true;
        }
        if self.queue().retirement_unconfirmed || self.admin_in_flight || self.actor_control_unconfirmed {
            return false;
        }
        let mut actors = std::mem::take(&mut self.queue_mut().actors);
        let mut settled = true;
        for actor in &mut actors {
            settled &= Box::pin(actor.retire()).await;
            self.queue_mut().retired_queries += actor.queries;
            self.queue_mut().retired_cancel_attempts += actor.cancellation_attempts;
        }
        if !settled {
            self.queue_mut().retirement_unconfirmed = true;
            return false;
        }
        let pids = self.queue().identities.iter().map(|id| id.id.parse::<i32>().unwrap()).collect::<Vec<_>>();
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            let rows = self
                .queue_observe("SELECT pid FROM pg_catalog.pg_stat_activity WHERE pid=ANY($1::integer[])", &[&pids])
                .await;
            if rows.is_empty() {
                self.queue_mut().retirement_confirmed = true;
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                self.queue_mut().retirement_unconfirmed = true;
                return false;
            }
            tokio::time::sleep(PHASE_INTERVAL).await;
        }
    }
}

#[cfg(test)]
impl MaintenanceFixture {
    /// A storage-only constructor: no client, runtime, native owner proof or database is created.
    pub(crate) fn unconnected_relation_future_fixture() -> Self {
        let specification = ConnectionSpec {
            engine: Some(culprit_core::Engine::Postgres),
            host: "never-contact.invalid".into(),
            port: Some(9),
            ..Default::default()
        };
        let connector = Connector::new(&specification).unwrap();
        Self {
            specification,
            cancellation: CollectionCancellation::default(),
            builder_pid: 0,
            holder_pid: 0,
            connector,
            schema: "never-create-or-drop".into(),
            admin: None,
            admin_in_flight: false,
            holder: None,
            build: None,
            cancel: None,
            idle_builder: None,
            schema_created: false,
            fixture_queries: 0,
            phase_observations: 0,
            connections_started: 0,
            cancellation_attempts: 0,
            actor_control_unconfirmed: false,
            automatic_case: None,
            relation_queue: Some(RelationQueueState::new(RelationQueueCase::Small)),
            transaction_metadata: None,
        }
    }
    pub(crate) fn relation_unpolled_work(&self) -> (u32, u32, u32, u32) {
        (self.fixture_queries, self.connections_started, self.phase_observations, self.queue().actor_queries())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn an_empty_actor_vector_cannot_heal_unconfirmed_retirement_or_admit_cleanup_sql() {
        let mut fixture = MaintenanceFixture::unconnected_relation_future_fixture();
        fixture.queue_mut().retirement_unconfirmed = true;
        assert!(!fixture.retire_relation_queue().await);
        assert_eq!(fixture.relation_unpolled_work(), (0, 0, 0, 0));
        fixture.actor_control_unconfirmed = true;
        fixture.schema_created = true;
        assert!(!fixture.cleanup().await);
        assert_eq!(fixture.relation_unpolled_work(), (0, 0, 0, 0));
        assert!(fixture.queue().retirement_unconfirmed);
    }
    #[test]
    fn cancellation_stops_the_new_source_phase_before_any_fixture_or_source_dispatch() {
        let mut fixture = MaintenanceFixture::unconnected_relation_future_fixture();
        fixture.cancellation.cancel();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture.begin_relation_source_capture())).is_err()
        );
        assert!(!fixture.actor_control_unconfirmed);
        assert_eq!(fixture.relation_unpolled_work(), (0, 0, 0, 0));
    }
}
