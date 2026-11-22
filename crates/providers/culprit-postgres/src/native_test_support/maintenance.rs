use super::{case, owner, specification};
use crate::{
    connect::Connector,
    rows::{quote_ident, tagged},
};
use culprit_core::ConnectionSpec;
use culprit_core::telemetry::{CollectionBudget, digest};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{panic::AssertUnwindSafe, time::Duration};
use tokio::task::JoinHandle;
use tokio_postgres::{CancelToken, Client};

mod transaction_metadata;
pub use transaction_metadata::TransactionMetadataRole;
use transaction_metadata::TransactionMetadataState;

mod relation_queue;
use relation_queue::RelationQueueState;
pub use relation_queue::{RelationQueueCase, RelationQueueRole};

const CASE_TIMEOUT: Duration = Duration::from_secs(25);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(3);
const AUTOMATIC_FOCUS_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_CADENCE_RECOVERY_TIMEOUT: Duration = Duration::from_secs(380);
const MAX_PHASE_OBSERVATIONS: u32 = 120;
const POSTGRES_IDENTIFIER_BYTES: usize = 63;
const COMPANION_SCHEMA_SUFFIX: &str = "_other";
const CASE_KEY_HEX_BYTES: usize = 12;

/// Keep the complete admitted nonce while reserving room for the fixture's companion schema.
fn owned_schema_name(owner: &str, case: &str) -> String {
    let case_key = digest(case.as_bytes());
    let schema = format!("culprit_maintenance_{owner}_{}", &case_key[..CASE_KEY_HEX_BYTES]);
    assert!(schema.len() + COMPANION_SCHEMA_SUFFIX.len() <= POSTGRES_IDENTIFIER_BYTES);
    schema
}

/// Test lifetimes pay the unchanged native resource cooldown and, separately, the 300-second standard cadence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutomaticCase {
    Focused,
    DefaultCadenceRecovery,
}
impl AutomaticCase {
    fn name(self) -> &'static str {
        match self {
            Self::Focused => "builtin-safety",
            Self::DefaultCadenceRecovery => "builtin-recovery",
        }
    }
    fn deadline(self) -> Duration {
        match self {
            Self::Focused => AUTOMATIC_FOCUS_TIMEOUT,
            Self::DefaultCadenceRecovery => DEFAULT_CADENCE_RECOVERY_TIMEOUT,
        }
    }
}
type BuildResult = (Client, std::result::Result<(), tokio_postgres::Error>);

pub struct MaintenanceFixture {
    pub specification: ConnectionSpec,
    pub cancellation: CollectionCancellation,
    pub builder_pid: i32,
    pub holder_pid: i32,
    connector: Connector,
    schema: String,
    admin: Option<Client>,
    admin_in_flight: bool,
    holder: Option<Client>,
    build: Option<JoinHandle<BuildResult>>,
    cancel: Option<CancelToken>,
    idle_builder: Option<Client>,
    schema_created: bool,
    fixture_queries: u32,
    phase_observations: u32,
    connections_started: u32,
    cancellation_attempts: u32,
    actor_control_unconfirmed: bool,
    automatic_case: Option<AutomaticCase>,
    relation_queue: Option<RelationQueueState>,
    transaction_metadata: Option<TransactionMetadataState>,
}

impl MaintenanceFixture {
    pub async fn run(case: &str, body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Self::run_owned(case, None, CASE_TIMEOUT, body).await;
    }

    pub async fn run_automatic(case: AutomaticCase, body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Self::run_owned(case.name(), Some(case), case.deadline(), body).await;
    }

    async fn run_owned(
        case: &str,
        automatic_case: Option<AutomaticCase>,
        deadline: Duration,
        body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>,
    ) {
        let mut fixture = Self::open(case, automatic_case).await;
        let cancellation = fixture.cancellation.clone();
        let completed =
            case::finish(deadline, &cancellation, AssertUnwindSafe(body(&mut fixture)).catch_unwind()).await;
        let settled = fixture.cleanup().await;
        eprintln!("owned maintenance fixture cleanup_confirmed={settled}; source cost remains unmeasured");
        let receipt = if let Some(case) = fixture.automatic_case {
            super::write_automatic_receipt(
                case.name(),
                "fixture-cleanup.json",
                &serde_json::json!({
                    "cleanup_confirmed": settled, "deadline_reached": completed.deadline_reached,
                    "fixture_attempted_dispatches": fixture.fixture_queries,
                    "fixture_native_sql_statements_upper_bound": 18 + fixture.phase_observations,
                    "fixture_dispatch_upper_bound": 11 + MAX_PHASE_OBSERVATIONS,
                    "phase_observations": fixture.phase_observations, "phase_observation_cap": MAX_PHASE_OBSERVATIONS,
                    "fixture_connections_started": fixture.connections_started,
                    "native_cancel_attempts": fixture.cancellation_attempts,
                    "ownership_admission_queries_including_setup": 2,
                    "body_deadline_seconds": deadline.as_secs(), "source_cost_measured": false,
                    "ownership_read_deadline_ms": 5_000, "admin_connect_deadline_ms": 3_000,
                    "cleanup_control_deadline_upper_bound_ms": 15_000,
                    "actor_control_unconfirmed": fixture.actor_control_unconfirmed,
                }),
            )
        } else if let Some(state) = &fixture.relation_queue {
            super::write_automatic_receipt(
                state.case.name(),
                "fixture-cleanup.json",
                &serde_json::json!({
                    "cleanup_confirmed":settled,"deadline_reached":completed.deadline_reached,
                    "fixture_admin_dispatches":fixture.fixture_queries,"fixture_phase_observations":fixture.phase_observations,
                    "phase_observation_cap":MAX_PHASE_OBSERVATIONS,"fixture_connections_started":fixture.connections_started,
                    "actor_control_unconfirmed":fixture.actor_control_unconfirmed,"admin_in_flight":fixture.admin_in_flight,
                    "ownership_admission_queries_including_setup":2,"source_queries_paid_separately":true,
                    "fixture":state.receipt(),"source_cost_qualified":false,
                }),
            )
        } else if let Some(state) = &fixture.transaction_metadata {
            super::write_automatic_receipt(
                transaction_metadata::CASE_NAME,
                "fixture-cleanup.json",
                &serde_json::json!({
                    "cleanup_confirmed":settled,"deadline_reached":completed.deadline_reached,
                    "fixture":fixture.transaction_metadata_receipt(),"source_cost_qualified":false,
                    "admin_in_flight":fixture.admin_in_flight,"actor_control_unconfirmed":fixture.actor_control_unconfirmed,
                    "retirement_confirmed":state.retirement_confirmed,
                }),
            )
        } else {
            Ok(())
        };
        Self::finish(completed, settled, receipt);
    }

    fn finish(completed: case::Completion<std::thread::Result<()>>, settled: bool, receipt: std::io::Result<()>) {
        if let Err(panic) = completed.value {
            std::panic::resume_unwind(panic);
        }
        assert!(
            !completed.deadline_reached,
            "owned maintenance fixture exceeded its absolute deadline; cleanup_confirmed={settled}"
        );
        assert!(settled, "owned maintenance fixture cleanup must be confirmed");
        receipt.expect("owned native receipt must be stored after original settlement");
    }

    async fn open(case: &str, automatic_case: Option<AutomaticCase>) -> Self {
        assert!(matches!(
            case,
            "waiting"
                | "released"
                | "builtin-safety"
                | "builtin-recovery"
                | "relation-queue-small"
                | "relation-queue-medium"
                | "relation-queue-parallel"
                | transaction_metadata::CASE_NAME
        ));
        let database = std::env::var("CULPRIT_PREVIEW_PG_DATABASE").unwrap();
        let budget = if automatic_case.is_some()
            || RelationQueueCase::from_name(case).is_some()
            || case == transaction_metadata::CASE_NAME
        {
            CollectionBudget {
                page_rows: 1,
                max_snapshot_rows: 1,
                page_bytes: 64 * 1024,
                total_bytes: 64 * 1024,
                max_queries: 2,
                statement_timeout_ms: 1_000,
                total_timeout_ms: 5_000,
                ..CollectionBudget::default()
            }
        } else {
            CollectionBudget::default()
        };
        let specification = specification(&database, budget).await;
        let connector = Connector::new(&specification).unwrap();
        let admin =
            tokio::time::timeout(CONTROL_TIMEOUT, connector.connect_unconfigured()).await.unwrap().unwrap().client;
        Self {
            specification,
            cancellation: CollectionCancellation::default(),
            connector,
            schema: owned_schema_name(&owner(), case),
            builder_pid: 0,
            holder_pid: 0,
            admin: Some(admin),
            admin_in_flight: false,
            holder: None,
            build: None,
            cancel: None,
            idle_builder: None,
            schema_created: false,
            fixture_queries: 0,
            phase_observations: 0,
            connections_started: 1,
            cancellation_attempts: 0,
            actor_control_unconfirmed: false,
            automatic_case,
            relation_queue: RelationQueueCase::from_name(case).map(RelationQueueState::new),
            transaction_metadata: (case == transaction_metadata::CASE_NAME).then(TransactionMetadataState::new),
        }
    }

    async fn execute(&mut self, sql: &str) {
        case::require_active(&self.cancellation).unwrap();
        assert!(!self.admin_in_flight && !self.actor_control_unconfirmed);
        self.fixture_queries += 1;
        self.admin_in_flight = true;
        let result =
            tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().batch_execute(&tagged(sql))).await;
        let sqlstate = result
            .as_ref()
            .ok()
            .and_then(|result| result.as_ref().err())
            .and_then(tokio_postgres::Error::as_db_error)
            .map(|error| error.code().code());
        assert!(
            matches!(&result, Ok(Ok(()))),
            "owned fixture control operation {} did not acknowledge completion; native_sqlstate={sqlstate:?}",
            self.fixture_queries
        );
        self.admin_in_flight = false;
    }

    pub async fn prepare(&mut self) {
        self.execute("SET statement_timeout='1s';SET lock_timeout='1s'").await;
        self.schema_created = true; // Retain cleanup ownership before the first mutation can be dispatched.
        self.execute(&format!("CREATE SCHEMA {}", quote_ident(&self.schema))).await;
        self.execute(&format!(
            "CREATE TABLE {}.items(id integer PRIMARY KEY,value integer NOT NULL);INSERT INTO {}.items VALUES(1,0)",
            quote_ident(&self.schema),
            quote_ident(&self.schema)
        ))
        .await;
        case::require_active(&self.cancellation).unwrap();
        self.connections_started += 1;
        let holder =
            tokio::time::timeout(CONTROL_TIMEOUT, self.connector.connect_unconfigured()).await.unwrap().unwrap().client;
        self.holder = Some(holder);
        let holder = self.holder.as_ref().unwrap();
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        let setup = if self.automatic_case.is_some() {
            "SET statement_timeout='1s';SET lock_timeout='1s';SET idle_in_transaction_session_timeout='210s'"
        } else {
            "SET statement_timeout='1s';SET lock_timeout='1s';SET idle_in_transaction_session_timeout='35s'"
        };
        let result = tokio::time::timeout(CONTROL_TIMEOUT, holder.batch_execute(&tagged(setup))).await;
        assert!(matches!(result, Ok(Ok(()))));
        self.actor_control_unconfirmed = false;
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        self.holder_pid =
            tokio::time::timeout(CONTROL_TIMEOUT, holder.query_one(&tagged("SELECT pg_backend_pid()"), &[]))
                .await
                .unwrap()
                .unwrap()
                .get(0);
        self.actor_control_unconfirmed = false;
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        let result = tokio::time::timeout(
            CONTROL_TIMEOUT,
            holder.batch_execute(&tagged(&format!(
                "BEGIN;UPDATE {}.items SET value=value+1 WHERE id=1",
                quote_ident(&self.schema)
            ))),
        )
        .await;
        assert!(matches!(result, Ok(Ok(()))));
        self.actor_control_unconfirmed = false;
        case::require_active(&self.cancellation).unwrap();
        self.connections_started += 1;
        let builder =
            tokio::time::timeout(CONTROL_TIMEOUT, self.connector.connect_unconfigured()).await.unwrap().unwrap().client;
        self.idle_builder = Some(builder);
        let builder = self.idle_builder.as_ref().unwrap();
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        let setup = if self.automatic_case.is_some() {
            "SET statement_timeout='180s';SET lock_timeout='0';SET application_name='fixture-concurrent-index'"
        } else {
            "SET statement_timeout='30s';SET lock_timeout='0';SET application_name='fixture-concurrent-index'"
        };
        let result = tokio::time::timeout(CONTROL_TIMEOUT, builder.batch_execute(&tagged(setup))).await;
        assert!(matches!(result, Ok(Ok(()))));
        self.actor_control_unconfirmed = false;
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        self.builder_pid =
            tokio::time::timeout(CONTROL_TIMEOUT, builder.query_one(&tagged("SELECT pg_backend_pid()"), &[]))
                .await
                .unwrap()
                .unwrap()
                .get(0);
        self.actor_control_unconfirmed = false;
        self.cancel = Some(builder.cancel_token());
        let sql = format!("CREATE INDEX CONCURRENTLY maintenance_value ON {}.items(value)", quote_ident(&self.schema));
        case::require_active(&self.cancellation).unwrap();
        self.fixture_queries += 1;
        let builder = self.idle_builder.take().unwrap();
        self.build = Some(tokio::spawn(async move {
            let result = builder.batch_execute(&sql).await;
            (builder, result)
        }));
    }

    pub async fn wait_for_writer(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            case::require_active(&self.cancellation).unwrap();
            assert!(self.phase_observations < MAX_PHASE_OBSERVATIONS);
            self.phase_observations += 1;
            self.fixture_queries += 1;
            assert!(!self.admin_in_flight && !self.actor_control_unconfirmed);
            self.admin_in_flight = true;
            let result = tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().query_opt(
                &tagged("SELECT phase,current_locker_pid FROM pg_catalog.pg_stat_progress_create_index WHERE pid=$1 AND datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())"),
                &[&self.builder_pid],
            )).await;
            let row = result.expect("owned fixture phase deadline").expect("owned fixture phase observation");
            self.admin_in_flight = false;
            if row.is_some_and(|row| {
                row.get::<_, String>(0) == "waiting for writers before build"
                    && row.get::<_, i64>(1) == i64::from(self.holder_pid)
            }) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "owned UPDATE must hold the concurrent index in the documented native wait"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn release(&mut self) -> bool {
        let Some(holder) = self.holder.take() else { return true };
        self.fixture_queries += 1;
        self.actor_control_unconfirmed = true;
        let confirmed = matches!(
            tokio::time::timeout(CONTROL_TIMEOUT, holder.batch_execute(&tagged("ROLLBACK"))).await,
            Ok(Ok(()))
        );
        self.actor_control_unconfirmed = !confirmed;
        confirmed
    }

    async fn settle_build(&mut self) -> Option<bool> {
        let build = self.build.as_mut()?;
        let Ok(result) = tokio::time::timeout(CONTROL_TIMEOUT, build).await else { return None };
        self.build.take();
        let Ok((client, result)) = result else { return None };
        self.idle_builder = Some(client);
        Some(result.is_ok())
    }

    pub async fn release_and_complete(&mut self) {
        assert!(self.release().await);
        assert_eq!(
            self.settle_build().await,
            Some(true),
            "only the owned test observes acknowledged CREATE completion"
        );
    }

    async fn cleanup(&mut self) -> bool {
        if self.admin_in_flight
            || self.transaction_metadata.is_some() && self.cancellation.is_cancelled()
            || self.actor_control_unconfirmed
            || self.connector.source_shutdown() != culprit_core::telemetry::SourceShutdown::Quiescent
        {
            // Physical close/abort does not confirm SQL cancellation. Stop all further fixture SQL; the exact
            // nonce-owned supervisor destroys this disposable database after the first failure is reported.
            if let Some(state) = &mut self.relation_queue {
                state.fence();
            }
            if let Some(state) = &mut self.transaction_metadata {
                state.fence();
            }
            self.holder.take();
            self.idle_builder.take();
            self.admin.take();
            if let Some(build) = self.build.take() {
                build.abort();
                let _ = build.await;
            }
            return false;
        }
        if self.transaction_metadata.is_some() && !Box::pin(self.retire_transaction_metadata()).await {
            self.actor_control_unconfirmed = true;
            self.admin.take();
            return false;
        }
        if self.relation_queue.is_some() && !Box::pin(self.retire_relation_queue()).await {
            self.actor_control_unconfirmed = true;
            self.admin.take();
            return false;
        }
        let mut settled = self.release().await;
        if self.build.is_some() && self.settle_build().await.is_none() {
            if let Some(cancel) = self.cancel.take() {
                self.cancellation_attempts += 1;
                settled &=
                    matches!(tokio::time::timeout(CONTROL_TIMEOUT, self.connector.cancel(&cancel)).await, Ok(Ok(())));
            }
            settled &= self.settle_build().await.is_some();
        }
        if let Some(build) = self.build.take() {
            build.abort();
            let _ = build.await;
            settled = false;
        }
        self.idle_builder.take();
        if !settled || self.admin_in_flight {
            self.admin.take();
            return false;
        }
        if self.schema_created {
            if self.transaction_metadata.is_some() && !self.admit_transaction_drop() {
                return false;
            }
            let sql = tagged(
                &self
                    .relation_queue
                    .as_ref()
                    .map(|state| state.drop_sql(&self.schema))
                    .unwrap_or_else(|| format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&self.schema))),
            );
            if let Some(state) = &mut self.relation_queue {
                state.statements += if state.other_schema_created { 2 } else { 1 };
            }
            self.fixture_queries += 1;
            if self.relation_queue.is_some() || self.transaction_metadata.is_some() {
                self.admin_in_flight = true;
            }
            let acknowledged = matches!(
                tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().batch_execute(&sql)).await,
                Ok(Ok(()))
            );
            if acknowledged {
                self.admin_in_flight = false;
            }
            settled &= acknowledged;
        }
        settled
    }
}

impl Drop for MaintenanceFixture {
    fn drop(&mut self) {
        if let Some(build) = &self.build {
            build.abort();
        }
        // Dropping clients closes fixture-only sessions; a bounded native statement timeout remains the final guard.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_owned_case_and_companion_keeps_the_full_nonce_without_native_identifier_truncation() {
        let owner = "0123456789abcdef01234567";
        let mut native_names = std::collections::BTreeSet::new();
        for case in [
            "waiting",
            "released",
            "builtin-safety",
            "builtin-recovery",
            "relation-queue-small",
            "relation-queue-medium",
            "relation-queue-parallel",
            transaction_metadata::CASE_NAME,
        ] {
            let primary = owned_schema_name(owner, case);
            let companion = format!("{primary}{COMPANION_SCHEMA_SUFFIX}");
            assert_eq!(primary.len(), 57, "{case}");
            assert_eq!(companion.len(), POSTGRES_IDENTIFIER_BYTES, "{case}");
            assert!(primary.is_ascii() && companion.is_ascii());
            assert!(primary.contains(owner), "the complete nonce is never shortened or hashed");
            assert_eq!(owned_schema_name(owner, case), primary, "deterministic native lookup and cleanup");
            assert!(native_names.insert(primary.clone()), "distinct case identity: {case}");
            assert!(native_names.insert(companion), "distinct companion identity: {case}");
            if RelationQueueCase::from_name(case).is_some() {
                let old = format!("culprit_maintenance_{owner}_{case}");
                let old_companion = format!("{old}{COMPANION_SCHEMA_SUFFIX}");
                assert!(old.len() > POSTGRES_IDENTIFIER_BYTES);
                assert_eq!(
                    &old[..POSTGRES_IDENTIFIER_BYTES],
                    &old_companion[..POSTGRES_IDENTIFIER_BYTES],
                    "the regression actually covers the prior native truncation collision"
                );
            }
        }
        assert_eq!(native_names.len(), 16);
        assert_ne!(
            owned_schema_name("1123456789abcdef01234567", "relation-queue-small"),
            owned_schema_name(owner, "relation-queue-small"),
            "independent owners stay disjoint"
        );
    }
    #[tokio::test]
    async fn unconfirmed_control_with_no_remaining_client_never_heals_or_dispatches_cleanup_sql() {
        let specification = ConnectionSpec {
            engine: Some(culprit_core::Engine::Postgres),
            host: "never-contact.invalid".into(),
            port: Some(9),
            ..Default::default()
        };
        let connector = Connector::new(&specification).unwrap();
        let mut fixture = MaintenanceFixture {
            specification,
            cancellation: CollectionCancellation::default(),
            builder_pid: 0,
            holder_pid: 0,
            connector,
            schema: "never-drop".into(),
            admin: None,
            admin_in_flight: false,
            holder: None,
            build: None,
            cancel: None,
            idle_builder: None,
            schema_created: true,
            fixture_queries: 0,
            phase_observations: 0,
            connections_started: 0,
            cancellation_attempts: 0,
            actor_control_unconfirmed: true,
            automatic_case: None,
            relation_queue: None,
            transaction_metadata: None,
        };
        assert!(!fixture.cleanup().await);
        fixture.cancellation.cancel();
        assert!(!fixture.cleanup().await);
        assert!(fixture.actor_control_unconfirmed);
        assert_eq!(fixture.fixture_queries, 0, "missing clients/physical close cannot admit DROP or replacement SQL");
    }
    #[test]
    fn original_panic_wins_after_settlement_even_when_deadline_cleanup_and_receipt_also_failed() {
        let panic = std::panic::catch_unwind(|| {
            MaintenanceFixture::finish(
                case::Completion { value: Err(Box::new("original-owned-body")), deadline_reached: true },
                false,
                Err(std::io::Error::new(std::io::ErrorKind::StorageFull, "secondary-receipt")),
            )
        })
        .unwrap_err();
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"original-owned-body"));
    }
    #[test]
    fn actual_owned_fixture_future_and_output_stay_bounded_without_constructing_or_executing_native_actors() {
        fn size<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
            (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
        }
        type Body = for<'a> fn(&'a mut MaintenanceFixture) -> LocalBoxFuture<'a, ()>;
        let (future, output) = size(|body: Body| MaintenanceFixture::run_automatic(AutomaticCase::Focused, body));
        eprintln!("actual owned fixture future={future} output={output}");
        assert!(future <= 64 * 1024, "actual owned fixture future is {future} bytes");
        assert!(output <= 8);
    }
}
