//! One finite output stream and collateral lock waiter in the admitted disposable PostgreSQL database.
use super::{actor::Actor, case, owner, specification, write_automatic_receipt};
use crate::{
    connect::Connector,
    rows::{quote_ident, tagged},
};
use culprit_core::{ConnectionSpec, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{panic::AssertUnwindSafe, time::Duration};
use tokio_postgres::{Client, types::ToSql};

const CASE_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(3);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_OBSERVATIONS: u32 = 96;
const MAX_PHASE_OBSERVATIONS: u32 = 32;
const MAX_RETIREMENT_OBSERVATIONS: u32 = 10;
const MAX_FIXTURE_DISPATCHES: u32 = MAX_OBSERVATIONS + 32;
const OUTPUT_ROWS: u64 = 8192;
const OUTPUT_ROW_BYTES: usize = 8192;
const APPLICATION: &str = "fixture-output-consumer";

pub struct OutputConsumerFixture {
    pub specification: ConnectionSpec,
    pub cancellation: CollectionCancellation,
    connector: Connector,
    schema: String,
    schema_created: bool,
    admin: Option<Client>,
    admin_in_flight: bool,
    source_in_flight: bool,
    output: Option<Actor>,
    waiter: Option<Actor>,
    observations: u32,
    admin_dispatches: u32,
    database_oid: String,
    relation_oid: String,
    retired: bool,
}
impl OutputConsumerFixture {
    pub async fn run(body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        let mut fixture = Box::new(Box::pin(Self::open()).await);
        let cancellation = fixture.cancellation.clone();
        let completed =
            case::finish(CASE_TIMEOUT, &cancellation, AssertUnwindSafe(Box::pin(body(&mut fixture))).catch_unwind())
                .await;
        let settled = Box::pin(fixture.cleanup()).await;
        let receipt = write_automatic_receipt(
            "output-consumer",
            "fixture-cleanup.json",
            &serde_json::json!({
                "cleanup_confirmed":settled,"deadline_reached":completed.deadline_reached,
                "fixture":fixture.receipt(),"source_in_flight":fixture.source_in_flight,"admin_in_flight":fixture.admin_in_flight,
            }),
        );
        if let Err(panic) = completed.value {
            std::panic::resume_unwind(panic);
        }
        assert!(!completed.deadline_reached, "owned output case deadline reached; cleanup_confirmed={settled}");
        assert!(settled, "owned original actors must settle before schema cleanup");
        receipt.unwrap();
    }
    async fn open() -> Self {
        let database = std::env::var("CULPRIT_PREVIEW_PG_DATABASE").unwrap();
        let specification = Box::pin(specification(&database, CollectionBudget::default())).await;
        let connector = Connector::new(&specification).unwrap();
        let admin =
            tokio::time::timeout(CONTROL_TIMEOUT, connector.connect_unconfigured()).await.unwrap().unwrap().client;
        Self {
            specification,
            cancellation: CollectionCancellation::default(),
            connector,
            schema: format!("culprit_output_{}", owner()),
            schema_created: false,
            admin: Some(admin),
            admin_in_flight: false,
            source_in_flight: false,
            output: None,
            waiter: None,
            observations: 0,
            admin_dispatches: 0,
            database_oid: String::new(),
            relation_oid: String::new(),
            retired: false,
        }
    }
    pub fn output_identity(&self) -> &NativeActivityIdentity {
        &self.output.as_ref().unwrap().identity
    }
    pub fn waiter_identity(&self) -> &NativeActivityIdentity {
        &self.waiter.as_ref().unwrap().identity
    }
    pub fn database_oid(&self) -> &str {
        &self.database_oid
    }
    pub fn relation_oid(&self) -> &str {
        &self.relation_oid
    }
    pub fn receipt(&self) -> serde_json::Value {
        serde_json::json!({
            "output_actor":self.output.as_ref().map(|actor|&actor.identity),
            "waiter_actor":self.waiter.as_ref().map(|actor|&actor.identity),
            "output_rows":OUTPUT_ROWS,"output_row_bytes":OUTPUT_ROW_BYTES,
            "finite_output_payload_bytes":OUTPUT_ROWS*OUTPUT_ROW_BYTES as u64,
            "fixture_dispatches_including_ownership":self.dispatches(),"fixture_dispatch_cap":MAX_FIXTURE_DISPATCHES,
            "observer_reads":self.observations,"observer_read_cap":MAX_OBSERVATIONS,
            "case_timeout_seconds":CASE_TIMEOUT.as_secs(),"actor_statement_timeout_seconds":50,
            "database_oid":self.database_oid,"relation_oid":self.relation_oid,"actors_retired":self.retired,
            "native_cpu_measured":false,"native_cost_qualified":false,
        })
    }
    fn dispatches(&self) -> u32 {
        2 + self.admin_dispatches
            + self.output.as_ref().map_or(0, |actor| actor.queries)
            + self.waiter.as_ref().map_or(0, |actor| actor.queries)
    }
    fn admit(&self, additional: u32) {
        case::require_active(&self.cancellation).unwrap();
        assert!(!self.admin_in_flight && !self.source_in_flight);
        assert!(self.dispatches() + additional <= MAX_FIXTURE_DISPATCHES);
    }
    async fn execute(&mut self, sql: &str) {
        self.admit(1);
        self.admin_dispatches += 1;
        self.admin_in_flight = true;
        tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().batch_execute(&tagged(sql)))
            .await
            .unwrap()
            .unwrap();
        self.admin_in_flight = false;
    }
    async fn observe(&mut self, sql: &str, parameters: &[&(dyn ToSql + Sync)]) -> Vec<tokio_postgres::Row> {
        self.admit(1);
        assert!(self.observations < MAX_OBSERVATIONS);
        self.observations += 1;
        self.admin_dispatches += 1;
        self.admin_in_flight = true;
        let rows = tokio::time::timeout(CONTROL_TIMEOUT, self.admin.as_ref().unwrap().query(&tagged(sql), parameters))
            .await
            .unwrap()
            .unwrap();
        self.admin_in_flight = false;
        rows
    }
    pub async fn prepare(&mut self) {
        self.execute("SET statement_timeout='1s';SET lock_timeout='1s'").await;
        self.schema_created = true;
        self.execute(&format!(
            "CREATE SCHEMA {};CREATE TABLE {}.items(id integer NOT NULL);INSERT INTO {}.items VALUES(1)",
            quote_ident(&self.schema),
            quote_ident(&self.schema),
            quote_ident(&self.schema)
        ))
        .await;
        let relation = format!("{}.items", self.schema);
        let rows=self.observe("SELECT current_setting('server_version_num')::integer AS version,(SELECT oid::text FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_oid,$1::text::regclass::oid::text AS relation_oid",&[&relation]).await;
        assert_eq!(rows.len(), 1);
        assert!((180_000..190_000).contains(&rows[0].get::<_, i32>(0)), "explicit PostgreSQL18 acceptance");
        self.database_oid = rows[0].get(1);
        self.relation_oid = rows[0].get(2);
        let marker = format!("culprit_owned_{}", owner());
        let database = self.specification.database.as_ref().unwrap();
        self.admit(4);
        self.output = Some(
            Box::pin(Actor::open(&self.connector, database, &marker, APPLICATION, &self.cancellation)).await.unwrap(),
        );
        self.waiter = Some(
            Box::pin(Actor::open(
                &self.connector,
                database,
                &marker,
                "fixture-collateral-lock-waiter",
                &self.cancellation,
            ))
            .await
            .unwrap(),
        );
        self.admit(1);
        self.output.as_mut().unwrap().start_paused_output(
            format!("SELECT pg_catalog.repeat('x',{OUTPUT_ROW_BYTES}) FROM {}.items CROSS JOIN pg_catalog.generate_series(1,{OUTPUT_ROWS}) AS fixture_output_rows",quote_ident(&self.schema)),
            OUTPUT_ROWS,OUTPUT_ROW_BYTES,
        ).unwrap();
        self.wait_for_output(false).await;
        self.admit(1);
        self.waiter
            .as_mut()
            .unwrap()
            .start_workload(format!("BEGIN;LOCK TABLE {}.items IN ACCESS EXCLUSIVE MODE", quote_ident(&self.schema)))
            .unwrap();
        self.wait_for_output(true).await;
    }
    async fn wait_for_output(&mut self, require_victim: bool) {
        for _ in 0..MAX_PHASE_OBSERVATIONS {
            if self.observe_output(require_victim).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the actual native output wait and exact collateral relation edge must be observable");
    }
    pub async fn confirm_output_dependency(&mut self) {
        assert!(self.observe_output(true).await);
    }
    async fn observe_output(&mut self, require_victim: bool) -> bool {
        let output_pid = self.output_identity().id.parse::<i32>().unwrap();
        let waiter_pid = self.waiter_identity().id.parse::<i32>().unwrap();
        let output_generation = self.output_identity().generation.clone();
        let waiter_generation = self.waiter_identity().generation.clone();
        let output_start = crate::native::iso("h.backend_start");
        let waiter_start = crate::native::iso("v.backend_start");
        let sql = format!(
            "SELECT h.state,h.wait_event_type,h.wait_event,({output_start}=$3)::boolean AS output_matches,({waiter_start}=$4)::boolean AS waiter_matches,COALESCE(v.wait_event_type='Lock' AND h.pid=ANY(pg_catalog.pg_blocking_pids(v.pid)),false) AS native_edge,EXISTS(SELECT 1 FROM pg_catalog.pg_locks held JOIN pg_catalog.pg_locks waited ON waited.database=held.database AND waited.relation=held.relation WHERE held.pid=h.pid AND waited.pid=v.pid AND held.locktype='relation' AND waited.locktype='relation' AND held.database::text=$5 AND held.relation::text=$6 AND held.granted AND held.mode='AccessShareLock' AND NOT waited.granted AND waited.mode='AccessExclusiveLock') AS same_relation FROM pg_catalog.pg_stat_activity h LEFT JOIN pg_catalog.pg_stat_activity v ON v.pid=$2 WHERE h.pid=$1 AND h.datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())"
        );
        let database = self.database_oid.clone();
        let relation = self.relation_oid.clone();
        let rows = self
            .observe(&sql, &[&output_pid, &waiter_pid, &output_generation, &waiter_generation, &database, &relation])
            .await;
        rows.len() == 1
            && rows[0].get::<_, String>(0) == "active"
            && rows[0].get::<_, Option<String>>(1).as_deref() == Some("Client")
            && rows[0].get::<_, Option<String>>(2).as_deref() == Some("ClientWrite")
            && rows[0].get::<_, bool>(3)
            && (!require_victim
                || (rows[0].get::<_, bool>(4) && rows[0].get::<_, bool>(5) && rows[0].get::<_, bool>(6)))
    }
    pub fn begin_source(&mut self) {
        self.admit(0);
        self.source_in_flight = true;
    }
    /// Only successful return of the common capture owner, after native settlement/readback, admits another phase.
    pub fn completed_source(&mut self) {
        assert!(self.source_in_flight);
        self.source_in_flight = false;
    }
    pub async fn drain_and_retire(&mut self) {
        self.admit(2);
        Box::pin(self.output.as_mut().unwrap().settle_for(DRAIN_TIMEOUT)).await.unwrap();
        Box::pin(self.waiter.as_mut().unwrap().settle()).await.unwrap();
        Box::pin(self.waiter.as_mut().unwrap().execute("ROLLBACK")).await.unwrap();
        assert!(Box::pin(self.output.as_mut().unwrap().retire()).await);
        assert!(Box::pin(self.waiter.as_mut().unwrap().retire()).await);
        assert!(
            self.wait_for_exit().await,
            "both exact native backend generations must be absent after their joined responses"
        );
        self.retired = true;
    }
    async fn wait_for_exit(&mut self) -> bool {
        let pids = [self.output.as_ref(), self.waiter.as_ref()]
            .into_iter()
            .flatten()
            .map(|actor| actor.identity.id.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        for _ in 0..MAX_RETIREMENT_OBSERVATIONS {
            let rows = self
                .observe(
                    "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=ANY($1::integer[]))",
                    &[&pids],
                )
                .await;
            if rows.len() == 1 && rows[0].get::<_, bool>(0) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }
    async fn cleanup(&mut self) -> bool {
        if self.admin_in_flight || self.source_in_flight {
            return false;
        }
        let mut settled = true;
        for actor in [&mut self.output, &mut self.waiter].into_iter().flatten() {
            settled &= Box::pin(actor.retire()).await;
        }
        if !settled || self.cancellation.is_cancelled() {
            return false;
        }
        if self.output.is_some() && !self.wait_for_exit().await {
            return false;
        }
        if self.schema_created {
            self.execute(&format!("DROP SCHEMA {} CASCADE", quote_ident(&self.schema))).await;
            self.schema_created = false;
        }
        self.admin.take();
        true
    }
}
