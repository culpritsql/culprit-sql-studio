//! Native acceptance only: the existing supervisor owns both ephemeral containers and their physical stream.
use super::{actor::Actor, case};
use crate::{
    connect::Connector,
    native,
    read::{NativeRead, QueryRead, ReadSession},
    rows::quote_ident,
};
use culprit_core::{ConnectionSpec, Engine, Provider, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{panic::AssertUnwindSafe, time::Duration};

const DATABASE: &str = "culprit_replication";
const CASE_TIMEOUT: Duration = Duration::from_secs(240);
const NATIVE_PHASE_TIMEOUT: Duration = Duration::from_secs(5);
const CONFLICT_JOIN_TIMEOUT: Duration = Duration::from_secs(140);
const MAX_ORACLE_READS: u32 = 320;
const UNRELATED_LOCKS: u32 = 4096;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct ReadableSecondaryFixture {
    receipt_case: &'static str,
    pub specification: ConnectionSpec,
    pub cancellation: CollectionCancellation,
    pub reader_identity: NativeActivityIdentity,
    primary: Actor,
    standby: Actor,
    reader: Option<Actor>,
    unrelated: Option<Actor>,
    schema: String,
    schema_created: bool,
    oracle_reads: u32,
    initial_conflicts: i64,
    observed_lock_rows: i64,
    recovery_conflict_confirmed: bool,
}
impl ReadableSecondaryFixture {
    pub async fn run(receipt_case: &'static str, body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        let _serial = SERIAL.lock().await;
        let mut fixture = Box::new(Box::pin(Self::open(receipt_case)).await);
        let token = fixture.cancellation.clone();
        let completed = case::finish(CASE_TIMEOUT, &token, AssertUnwindSafe(body(&mut fixture)).catch_unwind()).await;
        let cleanup = Box::pin(fixture.cleanup()).await;
        let receipt = super::write_automatic_receipt(
            receipt_case,
            "fixture-cleanup.json",
            &serde_json::json!({
                "cleanup_confirmed":cleanup,"deadline_reached":completed.deadline_reached,
                "fixture_case_seconds":CASE_TIMEOUT.as_secs(),"oracle_reads":fixture.oracle_reads,"oracle_read_cap":MAX_ORACLE_READS,
                "unrelated_advisory_locks":UNRELATED_LOCKS,"observed_native_lock_rows":fixture.observed_lock_rows,
                "max_connections":128,"max_locks_per_transaction":256,"max_prepared_transactions":16,
                "standby_streaming_delay_seconds":120,"recovery_conflict_response_confirmed":fixture.recovery_conflict_confirmed,
                "fixture_actor_dispatches":fixture.primary.queries+fixture.standby.queries+fixture.reader.as_ref().map_or(0,|actor|actor.queries)+fixture.unrelated.as_ref().map_or(0,|actor|actor.queries),
                "native_cpu":null,"native_cost_measured":false,"source_limits_changed":false,
            }),
        );
        if let Err(panic) = completed.value {
            std::panic::resume_unwind(panic);
        }
        assert!(!completed.deadline_reached, "owned recovery case deadline; cleanup_confirmed={cleanup}");
        assert!(cleanup, "only acknowledged native retirement permits fixture cleanup SQL");
        receipt.unwrap();
    }
    async fn open(receipt_case: &'static str) -> Self {
        assert_eq!(std::env::var("CULPRIT_PG_RECOVERY_FIXTURE").as_deref(), Ok("owned-physical-standby"));
        let nonce = std::env::var("CULPRIT_PG_REPLICATION_NONCE").unwrap();
        assert_eq!(nonce, super::owner());
        let specification = |variable: &str| {
            let port = std::env::var(variable).unwrap().parse::<u16>().unwrap();
            assert!(port >= 1024 && port != 5432);
            ConnectionSpec {
                engine: Some(Engine::Postgres),
                host: "127.0.0.1".into(),
                port: Some(port),
                database: Some(DATABASE.into()),
                user: Some("postgres".into()),
                password: Some(std::env::var("CULPRIT_PG_REPLICATION_PASSWORD").unwrap()),
                options: [("sslmode".into(), "disable".into())].into(),
                ..Default::default()
            }
        };
        let primary_spec = specification("CULPRIT_PG_REPLICATION_PRIMARY_PORT");
        let standby_spec = specification("CULPRIT_PG_REPLICATION_STANDBY_PORT");
        assert_ne!(primary_spec.port, standby_spec.port);
        let token = CollectionCancellation::default();
        let marker = format!("culprit_replication_{nonce}");
        let mut primary = Box::pin(Actor::open(
            &Connector::new(&primary_spec).unwrap(),
            DATABASE,
            &marker,
            "fixture-recovery-control",
            &token,
        ))
        .await
        .unwrap();
        let mut standby = Box::pin(Actor::open(
            &Connector::new(&standby_spec).unwrap(),
            DATABASE,
            &marker,
            "fixture-recovery-control",
            &token,
        ))
        .await
        .unwrap();
        let context = "SELECT pg_catalog.current_setting('server_version_num')::integer AS version,pg_catalog.pg_is_in_recovery() AS recovery,(pg_catalog.pg_control_system()).system_identifier::text AS system,pg_catalog.current_setting('max_connections')::integer AS connections,pg_catalog.current_setting('max_locks_per_transaction')::integer AS locks,pg_catalog.current_setting('max_prepared_transactions')::integer AS prepared";
        let left = primary.one(context, &[]).await.unwrap();
        let right = standby.one(context, &[]).await.unwrap();
        assert_eq!(left.get::<_, i32>("version") / 10000, 18);
        assert_eq!(left.get::<_, i32>("version"), right.get::<_, i32>("version"));
        assert!(!left.get::<_, bool>("recovery") && right.get::<_, bool>("recovery"));
        assert_eq!(left.get::<_, String>("system"), right.get::<_, String>("system"));
        for row in [&left, &right] {
            assert_eq!(row.get::<_, i32>("connections"), 128);
            assert_eq!(row.get::<_, i32>("locks"), 256);
            assert_eq!(row.get::<_, i32>("prepared"), 16);
        }
        let delay=standby.one("SELECT extract(epoch FROM pg_catalog.current_setting('max_standby_streaming_delay')::interval)::integer AS delay",&[]).await.unwrap();
        assert_eq!(delay.get::<_, i32>(0), 120);
        Self {
            receipt_case,
            specification: standby_spec,
            cancellation: token,
            reader_identity: NativeActivityIdentity { id: "unstarted".into(), generation: "unstarted".into() },
            primary,
            standby,
            reader: None,
            unrelated: None,
            schema: format!("culprit_recovery_{nonce}"),
            schema_created: false,
            oracle_reads: 0,
            initial_conflicts: 0,
            observed_lock_rows: 0,
            recovery_conflict_confirmed: false,
        }
    }
    pub async fn verify_native_reader_retirement(&self) -> (ReadWorkMetrics, ReadWorkMetrics) {
        case::require_active(&self.cancellation).unwrap();
        let connector = Connector::new(&self.specification).unwrap();
        let timeout = Box::pin(crate::read::native_acceptance::timeout_confirmed(&connector)).await;
        case::require_active(&self.cancellation).unwrap();
        let cancellation = Box::pin(crate::read::native_acceptance::dropped_caller_confirmed(&connector)).await;
        (timeout, cancellation)
    }
    pub async fn prepare(&mut self) {
        self.primary.execute("SET statement_timeout='1s';SET lock_timeout='1s'").await.unwrap();
        self.schema_created = true;
        self.primary
            .execute(&format!(
                "CREATE SCHEMA {};CREATE TABLE {}.items(id integer PRIMARY KEY);INSERT INTO {}.items VALUES(1)",
                quote_ident(&self.schema),
                quote_ident(&self.schema),
                quote_ident(&self.schema)
            ))
            .await
            .unwrap();
        self.wait_for_replay().await;
        // Only this admitted disposable database is reset, to give the native delta a known reset marker.
        self.standby.execute("SELECT pg_catalog.pg_stat_reset()").await.unwrap();
        self.initial_conflicts = self.conflicts().await;
        let connector = Connector::new(&self.specification).unwrap();
        let marker = format!("culprit_replication_{}", super::owner());
        self.unrelated = Some(
            Box::pin(Actor::open(&connector, DATABASE, &marker, "fixture-unrelated-locks", &self.cancellation))
                .await
                .unwrap(),
        );
        self.unrelated.as_mut().unwrap().execute(&format!("SELECT pg_catalog.pg_advisory_lock(7315,number) FROM pg_catalog.generate_series(1,{UNRELATED_LOCKS}) number")).await.unwrap();
        self.reader = Some(
            Box::pin(Actor::open(&connector, DATABASE, &marker, "fixture-readable-secondary", &self.cancellation))
                .await
                .unwrap(),
        );
        let reader = self.reader.as_mut().unwrap();
        reader.execute("SET statement_timeout='230s';SET idle_in_transaction_session_timeout='235s'").await.unwrap();
        self.reader_identity = reader.identity.clone();
        reader
            .start_workload(format!(
                "BEGIN;SELECT pg_catalog.pg_sleep(220) FROM {}.items WHERE id=1",
                quote_ident(&self.schema)
            ))
            .unwrap();
    }
    async fn oracle(
        &mut self,
        sql: &str,
        parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> tokio_postgres::Row {
        assert!(self.oracle_reads < MAX_ORACLE_READS);
        self.oracle_reads += 1;
        self.standby.one(sql, parameters).await.unwrap()
    }
    async fn wait_for_replay(&mut self) {
        let position =
            self.primary.one("SELECT pg_catalog.pg_current_wal_lsn()::text", &[]).await.unwrap().get::<_, String>(0);
        let until = tokio::time::Instant::now() + NATIVE_PHASE_TIMEOUT;
        loop {
            let row = self.oracle("SELECT pg_catalog.pg_last_wal_replay_lsn()>=$1::text::pg_lsn", &[&position]).await;
            if row.get::<_, Option<bool>>(0) == Some(true) {
                return;
            }
            assert!(tokio::time::Instant::now() < until, "owned catalog replay barrier");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn wait_for_default_adverse_reader(&mut self) {
        let pid = self.reader_identity.id.parse::<i32>().unwrap();
        let threshold = i64::from(ActivityPolicy::default().running_after_ms);
        let until = tokio::time::Instant::now() + Duration::from_millis(threshold as u64) + NATIVE_PHASE_TIMEOUT;
        loop {
            let row=self.oracle("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND state='active' AND backend_type='client backend' AND pg_catalog.clock_timestamp()-query_start>=($2::bigint*interval '1 millisecond'))",&[&pid,&threshold]).await;
            if row.get::<_, bool>(0) {
                return;
            }
            assert!(tokio::time::Instant::now() < until, "actual unchanged60s running threshold");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    pub async fn begin_replay_conflict(&mut self) {
        self.primary
            .execute(&format!("ALTER TABLE {}.items ADD COLUMN recovered integer", quote_ident(&self.schema)))
            .await
            .unwrap();
    }
    pub async fn relation_edge_observed(&mut self) -> bool {
        let pid = self.reader_identity.id.parse::<i32>().unwrap();
        let relation = format!("{}.items", self.schema);
        let row=self.oracle("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks waiting JOIN pg_catalog.pg_stat_activity startup ON startup.pid=waiting.pid AND startup.backend_type='startup' AND startup.backend_start IS NOT NULL JOIN pg_catalog.pg_locks held ON held.locktype=waiting.locktype AND held.database=waiting.database AND held.relation=waiting.relation WHERE waiting.locktype='relation' AND NOT waiting.granted AND waiting.mode='AccessExclusiveLock' AND held.granted AND held.mode='AccessShareLock' AND held.pid=$1 AND held.relation=$2::text::regclass), (SELECT count(*) FROM pg_catalog.pg_locks) AS locks",&[&pid,&relation]).await;
        self.observed_lock_rows = row.get("locks");
        assert!(self.observed_lock_rows >= i64::from(UNRELATED_LOCKS));
        row.get(0)
    }
    pub async fn await_native_conflict(&mut self) {
        self.reader.as_mut().unwrap().settle_recovery_conflict(CONFLICT_JOIN_TIMEOUT).await.unwrap();
        let until = tokio::time::Instant::now() + NATIVE_PHASE_TIMEOUT;
        loop {
            if self.conflicts().await > self.initial_conflicts {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "real same-database conflict counter must increase");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.recovery_conflict_confirmed = true;
        self.wait_for_replay().await;
    }
    async fn conflicts(&mut self) -> i64 {
        self.oracle("SELECT confl_lock FROM pg_catalog.pg_stat_database_conflicts WHERE datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())",&[]).await.get(0)
    }
    /// Qualification-only point: SAME native endpoint SQL/decoder with exact held before/final authority.
    /// This is not a canonical capture, episode answer, or permission to call an ended reader a culprit.
    pub async fn recovery_endpoint(
        &self,
        original: &ActivityRequestFocus,
    ) -> (Box<PostgresRecoveryEndpoint>, ReadWorkMetrics, u32) {
        case::require_active(&self.cancellation).unwrap();
        let connector = Connector::new(&self.specification).unwrap();
        let mut io = ReadSession::new(
            connector,
            CollectionBudget {
                max_queries: 6,
                statement_timeout_ms: 1000,
                total_timeout_ms: 5000,
                total_bytes: 256 * 1024,
                page_bytes: 64 * 1024,
                ..Default::default()
            },
        );
        let result = async {
            let meta = Box::pin(native::metadata::activity(
                &mut io,
                &self.specification.host,
                self.specification.port.unwrap(),
            ))
            .await?;
            let generation = io.connection_generation().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
            let mut held = native::local_context::SameConnection::new(&mut io, generation);
            let owner = Box::pin(native::local_context::Context::observe(&mut held, &meta))
                .await?
                .ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
            let point = Box::pin(native::readable_secondary_recovery::read(
                &mut EndpointReceipt { reader: &mut held, receipt_case: self.receipt_case },
                &meta,
                original,
            ))
            .await?;
            let final_context = Box::pin(native::local_context::complete(
                &mut held,
                &self.specification.host,
                self.specification.port.unwrap(),
                &meta,
                Some(&owner),
            ))
            .await?;
            if !final_context.full_visibility
                || !focused_request_original_scope_matches(original, &final_context.context)
            {
                return Err(CollectionStop::SourceChanged.error());
            }
            Ok::<_, culprit_core::Error>(point)
        }
        .await;
        assert_eq!(io.close().await, SourceShutdown::Quiescent);
        let metrics = io.metrics().unwrap();
        let queries = io.queries();
        assert_eq!(queries, 6);
        assert!(!metrics.has_unconfirmed_work());
        assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
        (result.unwrap(), metrics, queries)
    }
    pub async fn short_caller_source(&self, request: &CollectionRequest) -> ReadWorkMetrics {
        case::require_active(&self.cancellation).unwrap();
        let mut strict = request.clone();
        strict.budget.max_queries = 8;
        let provider = crate::PostgresProvider::new(&self.specification).unwrap();
        let mut source = provider.telemetry_source(&strict).unwrap();
        let error = source.describe().await.unwrap_err();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
        let queries = source.query_count();
        assert_eq!(queries, 8, "setup is inside the caller's ceiling; final native proof cannot be skipped");
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), queries);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
        let metrics = source.read_metrics().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        metrics
    }
    /// Explicit short-probe shutdown is separate from the real replay-conflict acceptance path.
    pub async fn cancel_reader(&mut self) -> u32 {
        let reader = self.reader.as_mut().unwrap();
        let before = reader.cancellation_attempts;
        reader.cancel_and_rollback().await.unwrap();
        assert_eq!(reader.cancellation_attempts, before + 1);
        reader.cancellation_attempts
    }
    pub async fn retire_reader(&mut self) {
        assert!(self.reader.as_mut().unwrap().retire().await);
        let pid = self.reader_identity.id.parse::<i32>().unwrap();
        let generation = self.reader_identity.generation.clone();
        let until = tokio::time::Instant::now() + NATIVE_PHASE_TIMEOUT;
        loop {
            let row=self.oracle("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND backend_start=$2::text::timestamptz)",&[&pid,&generation]).await;
            if !row.get::<_, bool>(0) {
                return;
            }
            assert!(tokio::time::Instant::now() < until, "the exact owned backend must retire");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn ended_source(
        &self,
        request: &CollectionRequest,
    ) -> (ReadableSecondaryRecoveryPoint, ReadWorkMetrics, u32) {
        case::require_active(&self.cancellation).unwrap();
        let provider = crate::PostgresProvider::new(&self.specification).unwrap();
        let mut source = provider.telemetry_source(request).unwrap();
        let result = async {
            let description = source.describe().await?;
            let CollectionRead::Page { records, next_cursor: None } =
                source.read_page(TelemetryStream::OperationalSamples, None).await?
            else {
                return Err(CollectionStop::InvalidPage.error());
            };
            let [
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::ReadableSecondaryRecovery(point),
                    ..
                }),
            ] = records.as_slice()
            else {
                return Err(CollectionStop::InvalidPage.error());
            };
            assert!(point.fits_source(&description));
            assert_eq!(source.finish().await?, SourceCompletion::Verified);
            Ok::<_, culprit_core::Error>(point.as_ref().clone())
        }
        .await;
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
        let queries = source.query_count();
        let metrics = source.read_metrics().unwrap();
        assert!(!metrics.has_unconfirmed_work());
        assert_eq!(queries, 6);
        (result.unwrap(), metrics, queries)
    }
    async fn cleanup(&mut self) -> bool {
        let mut settled = true;
        if let Some(reader) = &mut self.reader {
            settled &= reader.retire().await;
        }
        if let Some(actor) = &mut self.unrelated {
            settled &= actor.retire().await;
        }
        if settled && self.schema_created && !self.cancellation.is_cancelled() {
            settled &= self
                .primary
                .execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&self.schema)))
                .await
                .is_ok();
        }
        settled &= self.primary.retire().await;
        settled &= self.standby.retire().await;
        settled
    }
}

/// Fixture-only observation of the exact returned native grammar, before the SAME decoder consumes it.
/// No additional dispatch, parser, acceptance rule or source budget is introduced.
struct EndpointReceipt<'a, R> {
    reader: &'a mut R,
    receipt_case: &'static str,
}
#[async_trait::async_trait]
impl<R: QueryRead> QueryRead for EndpointReceipt<'_, R> {
    fn connection_generation(&self) -> Option<u64> {
        self.reader.connection_generation()
    }
    async fn query(&mut self, sql: &str, limit: usize) -> culprit_core::Result<Vec<crate::rows::Row>> {
        let rows = self.reader.query(sql, limit).await?;
        let values = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "native_source":row.text("native_source"),
                    "native_id":row.text("native_id"),
                    "native_values_column_present":row.value("native_values").is_some(),
                    "native_values":row.text("native_values"),
                })
            })
            .collect::<Vec<_>>();
        super::write_automatic_receipt(
            self.receipt_case,
            "native-endpoint-rows.json",
            &serde_json::json!({
                "template_sha256":digest(sql.as_bytes()),"retained_rows":values,"row_limit":limit,
                "same_production_decoder":true,"additional_native_dispatches":0,
            }),
        )
        .unwrap();
        Ok(rows)
    }
}
