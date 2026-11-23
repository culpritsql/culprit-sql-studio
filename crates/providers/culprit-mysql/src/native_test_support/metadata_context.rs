//! Optional third application actor shares the existing nonce owner, oracle/timeout and awaited cleanup.
use super::*;
impl BlockingFixture {
    pub fn metadata_table(&self) -> &str {
        &self.table
    }
    pub fn metadata_victim_id(&self) -> u32 {
        self.affected.as_ref().expect("explicit three-actor metadata fixture").id
    }
    /// These prerequisites are observed, never enabled here. Unsupported owned-image state fails before writes.
    pub async fn prepare_metadata_context(&mut self) -> Result<()> {
        self.active()?;
        self.admin.guard(&self.owner).await?;
        self.oracle_observations += 1;
        let enabled:Option<(u64,u64)>=self.admin.one("SELECT (SELECT COUNT(*) FROM performance_schema.setup_consumers WHERE NAME IN ('global_instrumentation','thread_instrumentation','events_statements_current') AND ENABLED='YES'),(SELECT COUNT(*) FROM performance_schema.setup_instruments WHERE NAME='wait/lock/metadata/sql/mdl' AND ENABLED='YES')").await?;
        if enabled != Some((3, 1)) {
            eprintln!(
                "owned metadata prerequisite unavailable: observed consumers/instrument={enabled:?}; no instrumentation changed"
            );
            return Err(CollectionStop::SourceUnavailable.error());
        }
        self.affected = Some(Box::pin(Actor::open(&self.connector, &self.owner, "affected")).await?);
        self.active()?;
        self.prepare_table().await?;
        self.start_schema_statement()?;
        self.wait_for_ticket_cohort(false).await?;
        self.active()?;
        let sql = format!("SELECT id FROM `{}` WHERE id=1", self.table);
        self.affected.as_mut().unwrap().start_workload(sql)?;
        Ok(())
    }
    pub async fn wait_for_metadata_context(&mut self) -> Result<()> {
        self.wait_for_ticket_cohort(true).await
    }
    async fn wait_for_ticket_cohort(&mut self, require_victim: bool) -> Result<()> {
        while self.oracle_observations < ORACLE_OBSERVATIONS {
            self.active()?;
            self.oracle_observations += 1;
            let victim = self.metadata_victim_id();
            let sql = format!(
                "SELECT COUNT(CASE WHEN t.PROCESSLIST_ID={} AND m.LOCK_STATUS='PENDING' AND m.LOCK_TYPE='EXCLUSIVE' THEN 1 END),COUNT(CASE WHEN t.PROCESSLIST_ID={} AND m.LOCK_STATUS='GRANTED' THEN 1 END),COUNT(CASE WHEN t.PROCESSLIST_ID={} AND m.LOCK_STATUS='PENDING' THEN 1 END),COALESCE(MAX(CASE WHEN t.PROCESSLIST_ID={} AND m.LOCK_STATUS='PENDING' THEN t.PROCESSLIST_TIME END),0) FROM performance_schema.metadata_locks m JOIN performance_schema.threads t ON t.THREAD_ID=m.OWNER_THREAD_ID WHERE m.OBJECT_TYPE='TABLE' AND m.OBJECT_SCHEMA='culprit_smoke' AND m.OBJECT_NAME='{}'",
                self.waiter_id, self.holder_id, victim, victim, self.table
            );
            let observed: Option<(u64, u64, u64, u64)> = self.admin.one(&sql).await?;
            if observed.is_some_and(|(gate, holder, victim, seconds)| {
                gate > 0 && holder > 0 && (!require_victim || victim > 0 && seconds >= 30)
            }) {
                if require_victim {
                    self.native_relation_observed = true;
                }
                return Ok(());
            }
            tokio::select! {biased;_=self.cancellation.cancelled()=>return Err(CollectionStop::Cancelled.error()),_=tokio::time::sleep(ORACLE_INTERVAL)=>{}}
        }
        Err(CollectionStop::SourceUnavailable.error())
    }
    /// The same production source owner proves a caller36 stop, with original close/debt accounting.
    pub async fn strict_metadata_caller_refusal(&self, request: &CollectionRequest) -> Result<ReadWorkMetrics> {
        use culprit_core::Provider;
        self.active()?;
        let mut request = request.clone();
        request.budget.max_queries = 36;
        let provider = crate::MysqlProvider::new(&self.specification)?;
        let mut source = provider.telemetry_source(&request)?;
        let result = source.describe().await;
        let queries = source.query_count();
        let settled = source.close().await;
        let metrics = source.read_metrics().ok_or_else(|| CollectionStop::InvalidPage.error())?;
        if result.err().is_none_or(|error| CollectionStop::from_error(&error) != CollectionStop::QueryLimit)
            || queries != 36
            || settled != SourceShutdown::Quiescent
            || source.query_count() != queries
            || !metrics.is_valid()
            || metrics.has_unconfirmed_work()
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        Ok(metrics)
    }
    pub fn metadata_fixture_accounting(&self) -> serde_json::Value {
        serde_json::json!({"fixture_dispatches_before_cleanup":self.fixture_dispatches(),
            "fixture_actor_connection_cap":8,"application_actors":3,"oracle_connections_are_existing_admin":true,
            "oracle_observations":self.oracle_observations,"oracle_observation_cap":ORACLE_OBSERVATIONS,
            "instrumentation_changed_by_this_case":false,"case_deadline_seconds":CASE_TIMEOUT.as_secs(),
            "native_ticket_cohort_observed":self.native_relation_observed,"source_dispatches_counted_separately":true})
    }
}
