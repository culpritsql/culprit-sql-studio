//! Nonce-owned SQL Server 2022 worker-starvation & diagnostic-worker availability fixture:
//! 1. Opens sustained worker-occupying actors (`WORKER_LOCK_WAITER_APP`, `WORKER_PARALLEL_APP`, and 24 parallel
//!    contributors = 76 tasks, `LOCK_TIMEOUT 25000`) plus 8 staggered burst waves (`8 * 5 = 40` parallel actors =
//!    120 tasks, `LOCK_TIMEOUT 500..2250ms`) BEFORE tightening `max worker threads = 128`.
//! 2. During full `128`-worker saturation (`0..450ms`), verifies that a diagnostic probe on an already-warmed isolated
//!    `Pool` (`statement_timeout_ms = 250, total_timeout_ms = 420`) is genuinely starved (`!= Complete`).
//! 3. Across the staggered `500..2250ms` turnover window (`15` workers freeing every `250ms` while `16..61` tasks remain
//!    queued on `THREADPOOL`), the caller captures real `Snapshot(Operational)` (`QueueKind::WorkerTasks > 0`), then
//!    after the burst waves drain (`76 < 128` sustained tasks remaining), captures `SourceRead(WorkerStarvation)` on
//!    the clean `SourceEndpoint` (`SourceShutdown::Quiescent`), and finally releases the gate and resets `max worker threads = 0`.
use super::{
    fixture::{self, Fixture, actors, invalid},
    serial_input_profile::SourceEndpoint,
    workload::CompletedWorker,
};
use crate::{
    conn::{MsClient, map_error},
    read::text,
    test_support::owned_case,
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::future::BoxFuture;
use std::time::Duration;
use tokio::task::JoinHandle;

pub const WORKER_LOCK_WAITER_APP: &str = "mssql-worker-lock-waiter";
pub const WORKER_PARALLEL_APP: &str = "mssql-worker-parallel-contributor";
const CASE_TIMEOUT: Duration = Duration::from_secs(90);
const SUSTAINED_EXTRA_ACTORS: usize = 4;
const STAGGERED_WAVES: usize = 8;
const ACTORS_PER_WAVE: usize = 19;

const SEED_PARALLEL_TABLE: &str = "CREATE TABLE dbo.culprit_worker_items(
    id int NOT NULL CONSTRAINT PK_culprit_worker_items PRIMARY KEY CLUSTERED,
    grp int NOT NULL,
    payload char(512) NOT NULL DEFAULT 'w'
);
ALTER TABLE dbo.culprit_worker_items SET (LOCK_ESCALATION = DISABLE);
WITH d AS (SELECT v FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) x(v)),
seq AS (SELECT p.v*1000 + a.v*100 + b.v*10 + c.v AS id
        FROM (VALUES(0),(1),(2),(3)) p(v) CROSS JOIN d a CROSS JOIN d b CROSS JOIN d c)
INSERT dbo.culprit_worker_items(id, grp)
SELECT id, CASE WHEN id % 16 = 15 THEN 1 ELSE 0 END FROM seq;
UPDATE STATISTICS dbo.culprit_worker_items WITH FULLSCAN;
CHECKPOINT;";

const GATE_SQL: &str = "SET LOCK_TIMEOUT 2000;BEGIN TRANSACTION;
    UPDATE dbo.page_fixture_select WITH(ROWLOCK) SET id = -1 WHERE id = 0;
    UPDATE dbo.culprit_worker_items WITH(ROWLOCK) SET payload = 'b' WHERE grp = 1;";

const LOCK_WAITER_SQL: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 25000;
    SELECT CONVERT(bigint, COUNT_BIG(*)) AS completed FROM dbo.page_fixture_select WITH(READCOMMITTEDLOCK) WHERE id = 0";

const PARALLEL_CONTRIBUTOR_SQL: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 25000;
    SELECT CONVERT(bigint, COUNT_BIG(*)) AS completed FROM dbo.culprit_worker_items WITH(READCOMMITTEDLOCK)
    WHERE grp = 0 HAVING COUNT_BIG(*) >= 0 OPTION(MAXDOP 2, USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE'))";

fn burst_sql(lock_timeout_ms: u32) -> String {
    format!(
        "SET NOCOUNT ON;SET LOCK_TIMEOUT {lock_timeout_ms};
        BEGIN TRY
            SELECT COUNT_BIG(*) AS completed FROM dbo.page_fixture_select WITH(READCOMMITTEDLOCK) WHERE id = 0;
        END TRY
        BEGIN CATCH
            SELECT CONVERT(bigint, ERROR_NUMBER()) AS completed;
        END CATCH"
    )
}

#[derive(Debug, Clone)]
pub struct StarvationBurstStart {
    pub instance: SharedResourceIdentity,
    pub waiter_id: NativeActivityIdentity,
    pub parallel_id: NativeActivityIdentity,
    pub warm_open_queries: u32,
    pub starved_probe_status: CollectionStatus,
    pub starved_probe_shutdown: SourceShutdown,
}

pub struct WorkerStarvationFixture<'a> {
    fixture: &'a mut Fixture,
    burst_handles: Vec<JoinHandle<()>>,
    sustained_handles: Vec<JoinHandle<()>>,
}

impl WorkerStarvationFixture<'_> {
    pub fn endpoint(&self) -> Result<SourceEndpoint> {
        SourceEndpoint::from_fixture(self.fixture)
    }

    pub async fn initialize_tables(&mut self, cancellation: &CollectionCancellation) -> Result<()> {
        owned_case::require_active(cancellation)?;
        self.set_max_workers(128).await?;
        let mut setup = self.fixture.client("mssql-worker-starvation-setup").await?;
        let res = fixture::execute(&mut setup, SEED_PARALLEL_TABLE).await;
        let retired = owned_case::retire(setup).await;
        res?;
        retired
    }

    /// Starts sustained worker holders + 8 staggered `THREADPOOL` waves (`500..2250ms`), proves the isolated warm
    /// diagnostic pool is starved during full `128`-worker saturation, and returns right at the start of the
    /// staggered turnover window so `endpoint` can capture genuine `Snapshot(Operational)` (`worker-queue > 0`).
    pub async fn start_starvation_episode(
        &mut self,
        cancellation: &CollectionCancellation,
    ) -> Result<StarvationBurstStart> {
        owned_case::require_active(cancellation)?;
        self.fixture.require_idle_workload()?;

        // Warm both the main endpoint pool and the isolated starved-probe pool BEFORE tightening max worker threads.
        let main_endpoint = self.endpoint()?;
        let warm_req = CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::Activity,
            budget: CollectionBudget {
                max_queries: 10,
                statement_timeout_ms: 2_000,
                total_timeout_ms: 4_000,
                ..CollectionBudget::default()
            },
        };
        let mut main_warm = main_endpoint.open(&warm_req)?;
        let warm_desc = main_warm.describe().await?;
        let SourcePartition::InstanceLocal { member: Some(instance), .. } = warm_desc.partition else {
            let _ = main_warm.close().await;
            return Err(invalid("MSSQL Activity warm capture lacked native InstanceLocal member"));
        };
        let _ = main_warm.read_page(TelemetryStream::ActivityObservations, None).await?;
        let _ = main_warm.finish().await?;
        assert_eq!(main_warm.close().await, SourceShutdown::Quiescent);

        let isolated_pool = self.fixture.isolated_pool("culprit-starved-probe");
        let mut warm_src =
            crate::telemetry::source(isolated_pool.clone(), &warm_req, "127.0.0.1".into(), self.fixture.port)?;
        let _ = warm_src.describe().await?;
        let _ = warm_src.read_page(TelemetryStream::ActivityObservations, None).await?;
        let _ = warm_src.finish().await?;
        let warm_open_queries = warm_src.query_count();
        assert_eq!(warm_src.close().await, SourceShutdown::Quiescent);

        // Pre-connect all actor sessions before setting max worker threads = 128.
        let (waiter_client, waiter_id) = self.open_actor(WORKER_LOCK_WAITER_APP, cancellation).await?;
        let (parallel_client, parallel_id) = self.open_actor(WORKER_PARALLEL_APP, cancellation).await?;
        let mut sustained_clients = Vec::with_capacity(SUSTAINED_EXTRA_ACTORS);
        for idx in 0..SUSTAINED_EXTRA_ACTORS {
            sustained_clients.push(self.fixture.client(&format!("mssql-worker-sustained-{idx}")).await?);
        }
        let mut wave_clients = Vec::with_capacity(STAGGERED_WAVES * ACTORS_PER_WAVE);
        for wave in 0..STAGGERED_WAVES {
            let timeout_ms = 750 + (wave as u32) * 350;
            let names = (0..ACTORS_PER_WAVE).map(|slot| format!("mssql-worker-wave-{wave}-{slot}")).collect::<Vec<_>>();
            let futs = names.iter().map(|app| self.fixture.client(app)).collect::<Vec<_>>();
            for c in futures_util::future::try_join_all(futs).await? {
                wave_clients.push((c, timeout_ms));
            }
        }

        self.fixture.start_holder_gate_with_statement(GATE_SQL).await?;

        actors::start(
            &mut self.fixture.workers,
            vec![(waiter_client, 1u8, LOCK_WAITER_SQL.into()), (parallel_client, 2u8, PARALLEL_CONTRIBUTOR_SQL.into())],
            |pattern, row| Ok(CompletedWorker { pattern, inserts: actors::completion_count(row, "completed")? }),
        )
        .await?;
        // Ensure `parallel_client` acquires its MAXDOP 2 parallel workers BEFORE the 144 serial burst sessions saturate 128 workers!
        tokio::time::sleep(Duration::from_millis(120)).await;

        for mut client in sustained_clients {
            self.sustained_handles.push(tokio::spawn(async move {
                if let Ok(stream) = client.simple_query(LOCK_WAITER_SQL).await {
                    let _ = stream.into_results().await;
                }
                let _ = owned_case::retire(client).await;
            }));
        }
        for (mut client, timeout_ms) in wave_clients {
            let sql = burst_sql(timeout_ms);
            self.burst_handles.push(tokio::spawn(async move {
                if let Ok(stream) = client.simple_query(&sql).await {
                    let _ = stream.into_results().await;
                }
                let _ = owned_case::retire(client).await;
            }));
        }

        // Let all 128 workers saturate and queue ~70 tasks on THREADPOOL (`0..150ms`, before wave 0 expires at 650ms).
        tokio::time::sleep(Duration::from_millis(140)).await;

        let starved_req = CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::Activity,
            budget: CollectionBudget {
                max_queries: 10,
                statement_timeout_ms: 250,
                total_timeout_ms: 380,
                ..CollectionBudget::default()
            },
        };
        let mut starved_src =
            crate::telemetry::source(isolated_pool.clone(), &starved_req, "127.0.0.1".into(), self.fixture.port)?;
        let starved_probe_status = match starved_src.describe().await {
            Ok(_) => CollectionStatus::Complete,
            Err(err) => CollectionStatus::Incomplete(CollectionStop::from_error(&err)),
        };
        let starved_probe_shutdown = starved_src.close().await;
        drop(isolated_pool);

        Ok(StarvationBurstStart {
            instance,
            waiter_id,
            parallel_id,
            warm_open_queries,
            starved_probe_status,
            starved_probe_shutdown,
        })
    }

    /// Waits for the bounded staggered burst waves to finish while the sustained actors remain blocked on the gate,
    /// freeing ~50 workers so `WorkerStarvationSource` can inspect the sustained worker holders cleanly.
    pub async fn drain_staggered_burst_waves(&mut self, cancellation: &CollectionCancellation) -> Result<()> {
        owned_case::require_active(cancellation)?;
        for h in self.burst_handles.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
        }
        Ok(())
    }

    pub async fn finish_all(&mut self, cancellation: &CollectionCancellation) -> Result<()> {
        owned_case::require_active(cancellation)?;
        self.fixture.release_holder_gate().await?;
        for h in self.burst_handles.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
        }
        for h in self.sustained_handles.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
        }
        let completed = self.fixture.finish_workload().await?;
        if completed.len() != 2 {
            return Err(invalid("named worker actors did not both finish cleanly"));
        }
        self.set_max_workers(0).await?;
        Ok(())
    }

    async fn set_max_workers(&mut self, value: u32) -> Result<()> {
        self.fixture
            .execute(&format!(
                "EXEC sys.sp_configure N'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
                 EXEC sys.sp_configure N'max worker threads', {value}; RECONFIGURE WITH OVERRIDE;"
            ))
            .await
    }

    async fn open_actor(
        &self,
        application: &str,
        cancellation: &CollectionCancellation,
    ) -> Result<(MsClient, NativeActivityIdentity)> {
        owned_case::require_active(cancellation)?;
        let mut client = self.fixture.client(application).await?;
        let res: Result<NativeActivityIdentity> = async {
            let rows = crate::conn::query(
                &mut client,
                "SELECT /* culprit fixture */ CONVERT(varchar(16),s.session_id) AS session_id,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS login_time FROM sys.dm_exec_sessions s WHERE s.session_id=@@SPID",
            )
            .await
            .map_err(map_error)?;
            let [row] = rows.as_slice() else { return Err(invalid("worker actor session missing")) };
            let id = NativeActivityIdentity {
                id: text(row, "session_id")?,
                generation: crate::sql_time::native_time(&text(row, "login_time")?)?,
            };
            if !id.is_valid() {
                return Err(invalid("worker actor identity invalid"));
            }
            Ok(id)
        }
        .await;
        match res {
            Ok(id) => Ok((client, id)),
            Err(err) => {
                let _ = owned_case::retire(client).await;
                Err(err)
            }
        }
    }
}

pub async fn run_worker_starvation_case(
    test: impl for<'a> FnOnce(WorkerStarvationFixture<'a>, CollectionCancellation) -> BoxFuture<'a, Result<()>>
    + Send
    + 'static,
) {
    Box::pin(fixture::run(move |fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let (expired, result) = {
                let body = test(
                    WorkerStarvationFixture { fixture, burst_handles: Vec::new(), sustained_handles: Vec::new() },
                    cancellation.clone(),
                );
                owned_case::finish_case(tokio::time::Instant::now() + CASE_TIMEOUT, &cancellation, body).await
            };
            let _ = fixture
                .execute(
                    "EXEC sys.sp_configure N'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
                     EXEC sys.sp_configure N'max worker threads', 0; RECONFIGURE WITH OVERRIDE;",
                )
                .await;
            result?;
            if expired {
                return Err(CollectionStop::TimedOut.error());
            }
            Ok(())
        })
    }))
    .await;
}

pub fn save_worker_starvation_receipt(value: &serde_json::Value) -> Result<()> {
    crate::test_support::save_cost_receipt(
        &crate::test_support::receipt_directory(),
        "mssql-worker-starvation-native",
        value,
    )
}
