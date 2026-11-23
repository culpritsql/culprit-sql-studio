//! Explicit nonce-owned application actors for native automatic-admission acceptance only.
//! No ordinary provider/desktop/MCP build enables this feature.
mod actor;
mod metadata_context;
use crate::connect::Connector;
use actor::Actor;
use culprit_core::{ConnectionSpec, Engine, Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{panic::AssertUnwindSafe, path::PathBuf, sync::Arc, time::Duration};

const CASE_TIMEOUT: Duration = Duration::from_secs(180);
const DEFAULT_RECOVERY_TIMEOUT: Duration = Duration::from_secs(420);
const ORACLE_INTERVAL: Duration = Duration::from_millis(250);
const ORACLE_OBSERVATIONS: u32 = 400;
static CASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct BlockingFixture {
    pub specification: ConnectionSpec,
    pub cancellation: CollectionCancellation,
    pub waiter_id: u32,
    pub holder_id: u32,
    connector: Arc<Connector>,
    owner: String,
    table: String,
    admin: Actor,
    holder: Actor,
    waiter: Actor,
    affected: Option<Actor>,
    table_created: bool,
    pub oracle_observations: u32,
    pub native_relation_observed: bool,
}
impl BlockingFixture {
    pub async fn run(body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Self::run_with_timeout(CASE_TIMEOUT, body).await;
    }
    pub async fn run_default_cadence_recovery(body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        Self::run_with_timeout(DEFAULT_RECOVERY_TIMEOUT, body).await;
    }
    async fn run_with_timeout(timeout: Duration, body: for<'a> fn(&'a mut Self) -> LocalBoxFuture<'a, ()>) {
        let _serial = CASE.lock().await;
        let mut fixture = Box::new(Box::pin(Self::open()).await);
        let cancellation = fixture.cancellation.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        let completed = {
            let work = AssertUnwindSafe(body(&mut fixture)).catch_unwind();
            tokio::pin!(work);
            tokio::select! {
                biased;
                value=&mut work => (value,tokio::time::Instant::now()>=deadline),
                _=tokio::time::sleep_until(deadline) => {
                    cancellation.cancel();
                    (work.await,true)
                }
            }
        };
        if completed.1 {
            cancellation.cancel();
        }
        let settled = Box::pin(fixture.cleanup()).await;
        let receipt = fixture.cleanup_receipt(settled, completed.1, timeout);
        eprintln!("owned MySQL blocking cleanup_confirmed={settled}; source CPU/cost remains unmeasured");
        if let Err(panic) = completed.0 {
            std::panic::resume_unwind(panic);
        }
        assert!(!completed.1, "owned automatic fixture exceeded its explicit case lifetime");
        assert!(settled, "only acknowledged original retirement permits fixture cleanup SQL");
        receipt.expect("private native cleanup receipt");
    }
    fn cleanup_receipt(&self, settled: bool, deadline_reached: bool, timeout: Duration) -> std::io::Result<()> {
        write_receipt(
            "fixture-cleanup.json",
            &serde_json::json!({
                "cleanup_confirmed":settled,"deadline_reached":deadline_reached,
                "native_relation_observed":self.native_relation_observed,
                "fixture_dispatches":self.fixture_dispatches(),
                "oracle_observations":self.oracle_observations,"oracle_observation_cap":ORACLE_OBSERVATIONS,
                "fixture_actor_connection_cap":if self.affected.is_some() {8}else {6},"source_read_and_oracle_connections_paid_separately":true,"case_deadline_seconds":timeout.as_secs(),
                "measured_cpu":null,"measured_cost_qualification":false,
            }),
        )
    }
    async fn open() -> Self {
        let owner = std::env::var("CULPRIT_SMOKE_OWNER").expect("explicit owned supervisor nonce");
        assert!(owner.len() == 24 && owner.bytes().all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase()));
        assert_eq!(std::env::var("CULPRIT_SMOKE_ENGINE").unwrap(), "mysql");
        assert_eq!(std::env::var("CULPRIT_SMOKE_DATABASE").unwrap(), "culprit_smoke");
        let port = std::env::var("CULPRIT_SMOKE_PORT").unwrap().parse::<u16>().unwrap();
        assert!(port >= 1024 && port != 3306, "fixture must be the explicitly published loopback endpoint");
        let specification = ConnectionSpec {
            engine: Some(Engine::Mysql),
            host: "127.0.0.1".into(),
            port: Some(port),
            database: Some("culprit_smoke".into()),
            user: Some(std::env::var("CULPRIT_SMOKE_USER").unwrap()),
            password: Some(std::env::var("CULPRIT_SMOKE_PASSWORD").unwrap()),
            options: [("sslmode".into(), "disabled".into())].into(),
            ..Default::default()
        };
        let connector = Arc::new(Connector::new(&specification).unwrap());
        // Each actor checks the actual native hostname, ID, writable role and supported version before writes.
        let admin = Box::pin(Actor::open(&connector, &owner, "admin")).await.unwrap();
        let holder = Box::pin(Actor::open(&connector, &owner, "holder")).await.unwrap();
        let waiter = Box::pin(Actor::open(&connector, &owner, "waiter")).await.unwrap();
        Self {
            specification,
            cancellation: CollectionCancellation::default(),
            waiter_id: waiter.id,
            holder_id: holder.id,
            connector,
            table: format!("culprit_auto_{owner}"),
            owner,
            admin,
            holder,
            waiter,
            affected: None,
            table_created: false,
            oracle_observations: 0,
            native_relation_observed: false,
        }
    }
    fn active(&self) -> Result<()> {
        if self.cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
    }
    pub async fn prepare(&mut self) {
        self.active().unwrap();
        self.admin.guard(&self.owner).await.unwrap();
        self.admin.execute("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME IN ('global_instrumentation','thread_instrumentation','events_statements_current')").await.unwrap();
        self.admin.execute("UPDATE performance_schema.setup_instruments SET ENABLED='YES',TIMED='YES' WHERE NAME='wait/lock/metadata/sql/mdl'").await.unwrap();
        self.prepare_table().await.unwrap();
        self.start_schema_statement().unwrap();
    }
    async fn prepare_table(&mut self) -> Result<()> {
        self.active()?;
        self.table_created = true; // Retain mutation ownership before dispatch can create the table.
        self.admin
            .execute(&format!("CREATE TABLE `{}`(id INT PRIMARY KEY,value INT NOT NULL) ENGINE=InnoDB", self.table))
            .await?;
        self.active()?;
        self.admin.execute(&format!("INSERT INTO `{}` VALUES(1,0)", self.table)).await?;
        self.active()?;
        self.holder.execute("START TRANSACTION").await?;
        self.active()?;
        let held: Option<(i32,)> = self.holder.one(&format!("SELECT id FROM `{}` WHERE id=1", self.table)).await?;
        assert_eq!(held, Some((1,)));
        Ok(())
    }
    fn start_schema_statement(&mut self) -> Result<()> {
        self.active()?;
        self.waiter.start_workload(format!("ALTER TABLE `{}` ADD COLUMN owned_value INT NULL", self.table))
    }
    fn fixture_dispatches(&self) -> u32 {
        self.admin.dispatches()
            + self.holder.dispatches()
            + self.waiter.dispatches()
            + self.affected.as_ref().map_or(0, Actor::dispatches)
    }
    /// A bounded, independent P_S oracle proves the real pending/granted metadata relation and native wait age.
    /// It is fixture work, not a source query or an engine permission receipt.
    pub async fn wait_for_default_attention(&mut self) {
        for _ in 0..ORACLE_OBSERVATIONS {
            self.active().unwrap();
            self.oracle_observations += 1;
            let sql = format!(
                "SELECT COUNT(*),COALESCE(MAX(wt.PROCESSLIST_TIME),0) FROM performance_schema.metadata_locks p JOIN performance_schema.metadata_locks g ON g.OBJECT_TYPE=p.OBJECT_TYPE AND g.OBJECT_SCHEMA=p.OBJECT_SCHEMA AND g.OBJECT_NAME=p.OBJECT_NAME JOIN performance_schema.threads wt ON wt.THREAD_ID=p.OWNER_THREAD_ID JOIN performance_schema.threads bt ON bt.THREAD_ID=g.OWNER_THREAD_ID WHERE p.OBJECT_TYPE='TABLE' AND p.OBJECT_SCHEMA='culprit_smoke' AND p.OBJECT_NAME='{}' AND p.LOCK_STATUS='PENDING' AND p.LOCK_TYPE='EXCLUSIVE' AND g.LOCK_STATUS='GRANTED' AND wt.PROCESSLIST_ID={} AND bt.PROCESSLIST_ID={}",
                self.table, self.waiter_id, self.holder_id
            );
            let observed: Option<(u64, u64)> = self.admin.one(&sql).await.unwrap();
            if observed.is_some_and(|(count, seconds)| count > 0 && seconds >= 30) {
                self.native_relation_observed = true;
                return;
            }
            tokio::select! {
                _=self.cancellation.cancelled()=>panic!("owned fixture admission cancelled"),
                _=tokio::time::sleep(ORACLE_INTERVAL)=>{}
            }
        }
        panic!("bounded native metadata-lock oracle did not observe default attention");
    }
    pub async fn release_and_complete(&mut self) {
        self.active().unwrap();
        self.holder.execute("ROLLBACK").await.unwrap();
        self.waiter.finish_original().await.unwrap();
        if let Some(affected) = &mut self.affected {
            affected.finish_original().await.unwrap();
        }
    }
    pub async fn strict_caller_refusal(&self, request: &CollectionRequest) -> ReadWorkMetrics {
        use culprit_core::Provider;
        let mut request = request.clone();
        request.budget.max_queries = 32;
        let provider = crate::MysqlProvider::new(&self.specification).unwrap();
        let mut source = provider.telemetry_source(&request).unwrap();
        let error = source.describe().await.err().expect("caller32 cannot pay the mandatory footer");
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::QueryLimit);
        assert_eq!(source.query_count(), 32);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.query_count(), 32, "close cannot dispatch a skipped mandatory source footer");
        let metrics = source.read_metrics().unwrap();
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        metrics
    }
    pub async fn source_cancellation(&self) -> serde_json::Value {
        crate::read::native_acceptance::verify(self.connector.clone(), &self.owner).await
    }
    async fn cleanup(&mut self) -> bool {
        // Both original application clients settle/rollback before any fixture cleanup SQL.
        let affected = match &mut self.affected {
            Some(actor) => Box::pin(actor.retire()).await,
            None => true,
        };
        let waiter = Box::pin(self.waiter.retire()).await;
        let holder = Box::pin(self.holder.retire()).await;
        let mut settled = affected && waiter && holder;
        if settled && self.table_created {
            settled = self.admin.guard(&self.owner).await.is_ok();
            if settled {
                settled = self.admin.execute(&format!("DROP TABLE IF EXISTS `{}`", self.table)).await.is_ok();
            }
            if settled {
                self.table_created = false;
            }
        }
        let admin = Box::pin(self.admin.retire()).await;
        settled && admin
    }
}
pub fn automatic_directory() -> std::io::Result<PathBuf> {
    let root = PathBuf::from(
        std::env::var("CULPRIT_NATIVE_SAFETY_RECEIPT_DIR")
            .map_err(|_| std::io::Error::other("explicit private native receipt directory required"))?,
    );
    let directory = root.join("mysql-builtin-blocking");
    std::fs::create_dir_all(&directory)?;
    Ok(directory)
}
pub fn write_receipt(name: &str, value: &serde_json::Value) -> std::io::Result<()> {
    assert!(name.ends_with(".json") && !name.contains('/') && !name.contains('\\'));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(automatic_directory()?.join(name))?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?)?;
    file.sync_all()
}
