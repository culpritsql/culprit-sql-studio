//! Nonce-owned SQL Server parallel operator/task fixture:
//! 1. Seeds `dbo.culprit_parallel_items` (4,000 rows across ~500 pages: 100 rows with `tenant = 7`, 3,900 rows with `tenant = 0`).
//! 2. Locks 10 `tenant = 0` rows (`id % 400 = 399`) with `ROWLOCK` so both `MAXDOP 2` parallel scan threads read non-matching
//!    rows (`rows_read > rows_returned`) and remain bound to their parallel `Clustered Index Scan` tasks (`LCK_M_S`).
//! 3. Comparable intervention: adds selective index `IX_culprit_parallel_items_tenant(tenant)` on a separate setup connection
//!    before locking (or proves an indexed seek on `dbo.culprit_parallel_indexed` with identical rows & identical 10 `tenant = 0`
//!    row locks completes immediately without scanning unrelated rows or waiting on `tenant = 0` locks).
use super::{
    fixture::{self, Fixture, actors, invalid},
    serial_input_profile::SourceEndpoint,
    workload::CompletedWorker,
};
use crate::{
    conn::{MsClient, map_error},
    read::{integer, text},
    test_support::owned_case,
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::future::BoxFuture;
use std::time::Duration;

pub const PARALLEL_APPLICATION: &str = "parallel-operator-owned-actor";
pub const PARALLEL_MATCHING_ROWS: u64 = 100;
const CASE_TIMEOUT: Duration = Duration::from_secs(60);

const CREATE_UNINDEXED: &str = "CREATE TABLE dbo.culprit_parallel_items(
    id int NOT NULL CONSTRAINT PK_culprit_parallel_items PRIMARY KEY CLUSTERED,
    tenant int NOT NULL,
    payload char(512) NOT NULL DEFAULT 'p'
);
ALTER TABLE dbo.culprit_parallel_items SET (LOCK_ESCALATION = DISABLE);";
const SEED_UNINDEXED: &str = "WITH d AS (SELECT v FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) x(v)),
    seq AS (SELECT p.v*1000 + a.v*100 + b.v*10 + c.v AS id
            FROM (VALUES(0),(1),(2),(3)) p(v) CROSS JOIN d a CROSS JOIN d b CROSS JOIN d c)
    INSERT dbo.culprit_parallel_items(id, tenant)
    SELECT id, CASE WHEN id % 40 = 20 THEN 7 ELSE 0 END FROM seq;
    UPDATE STATISTICS dbo.culprit_parallel_items WITH FULLSCAN;
    CHECKPOINT;";

const CREATE_INDEXED: &str = "CREATE TABLE dbo.culprit_parallel_indexed(
    id int NOT NULL CONSTRAINT PK_culprit_parallel_indexed PRIMARY KEY CLUSTERED,
    tenant int NOT NULL,
    payload char(512) NOT NULL DEFAULT 'p'
);
ALTER TABLE dbo.culprit_parallel_indexed SET (LOCK_ESCALATION = DISABLE);
WITH d AS (SELECT v FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) x(v)),
seq AS (SELECT p.v*1000 + a.v*100 + b.v*10 + c.v AS id
        FROM (VALUES(0),(1),(2),(3)) p(v) CROSS JOIN d a CROSS JOIN d b CROSS JOIN d c)
INSERT dbo.culprit_parallel_indexed(id, tenant)
SELECT id, CASE WHEN id % 40 = 20 THEN 7 ELSE 0 END FROM seq;
CREATE NONCLUSTERED INDEX IX_culprit_parallel_indexed_tenant ON dbo.culprit_parallel_indexed(tenant);
UPDATE STATISTICS dbo.culprit_parallel_indexed WITH FULLSCAN;
CHECKPOINT;";

const CREATE_PROCEDURE: &str = "CREATE PROCEDURE dbo.culprit_parallel_scan AS
    SELECT COUNT_BIG(*) AS completed FROM dbo.culprit_parallel_items WITH(READCOMMITTEDLOCK)
    WHERE tenant = 7 HAVING COUNT_BIG(*) >= 0
    OPTION(MAXDOP 2, USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE', 'QUERY_PLAN_PROFILE'))";

const GATE_SQL: &str = "SET LOCK_TIMEOUT 2000;BEGIN TRANSACTION;
    UPDATE dbo.culprit_parallel_items WITH(ROWLOCK) SET payload='g' WHERE id % 16 = 15 AND tenant = 0;
    UPDATE dbo.culprit_parallel_indexed WITH(ROWLOCK) SET payload='g' WHERE id % 16 = 15 AND tenant = 0;";

const WARM_PARALLEL_SQL: &str = "SET STATISTICS PROFILE ON;SET NOCOUNT ON;EXEC dbo.culprit_parallel_scan";

const WORK_PARALLEL_SQL: &str =
    "SET NOCOUNT ON;SET LOCK_TIMEOUT 25000;SET STATISTICS PROFILE ON;EXEC dbo.culprit_parallel_scan";

const INTERVENTION_SEEK_SQL: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 2000;
    SELECT CONVERT(bigint, COUNT_BIG(*)) AS completed
    FROM dbo.culprit_parallel_indexed WITH(READCOMMITTEDLOCK)
    WHERE tenant = 7 HAVING COUNT_BIG(*) >= 0";

pub struct ParallelOperatorFixture<'a> {
    fixture: &'a mut Fixture,
}

impl ParallelOperatorFixture<'_> {
    pub fn endpoint(&self) -> Result<SourceEndpoint> {
        SourceEndpoint::from_fixture(self.fixture)
    }

    pub async fn start(&mut self, cancellation: &CollectionCancellation) -> Result<NativeActivityIdentity> {
        owned_case::require_active(cancellation)?;
        self.fixture.require_idle_workload()?;
        let mut setup = self.fixture.client("parallel-operator-owned-setup").await?;
        let seeded: Result<()> = async {
            for stmt in [CREATE_UNINDEXED, SEED_UNINDEXED, CREATE_PROCEDURE, CREATE_INDEXED] {
                owned_case::require_active(cancellation)?;
                fixture::execute(&mut setup, stmt).await?;
            }
            Ok(())
        }
        .await;
        let retired = owned_case::retire(setup).await;
        seeded?;
        retired?;

        let (actor_client, identity) = Box::pin(self.open_actor(cancellation)).await?;
        if let Err(err) = self.fixture.start_holder_gate_with_statement(GATE_SQL).await {
            let _ = owned_case::retire(actor_client).await;
            return Err(err);
        }

        actors::start(
            &mut self.fixture.workers,
            vec![(actor_client, 0u8, WORK_PARALLEL_SQL.into())],
            |pattern, row| Ok(CompletedWorker { pattern, inserts: actors::completion_count(row, "completed")? }),
        )
        .await?;

        self.wait_for_parallel_tasks(&identity, cancellation).await?;
        tokio::select! {
            _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            _ = tokio::time::sleep(Duration::from_millis(1150)) => (),
        }
        owned_case::require_active(cancellation)?;
        if self.fixture.workers.iter().flatten().any(|w| w.is_finished()) {
            self.fixture.finish_workload().await?;
            return Err(invalid("owned parallel actor finished before its retained row-lock gate was released"));
        }
        Ok(identity)
    }

    /// Comparable intervention under the exact same held `tenant = 0` row locks: the selective index seeks only
    /// `tenant = 7` rows and returns the identical count immediately without blocking on `tenant = 0` pages.
    pub async fn run_indexed_intervention(&self, cancellation: &CollectionCancellation) -> Result<(u64, u64)> {
        owned_case::require_active(cancellation)?;
        let mut client = self.fixture.client("parallel-operator-indexed-intervention").await?;
        let started = std::time::Instant::now();
        let outcome: Result<(u64, u64)> = async {
            let rows = crate::conn::query(&mut client, INTERVENTION_SEEK_SQL).await.map_err(map_error)?;
            let [row] = rows.as_slice() else { return Err(invalid("indexed intervention returned no row")) };
            let count = u64::try_from(integer(row, "completed")?).map_err(|_| invalid("negative count"))?;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let stats = crate::conn::query(
                &mut client,
                "SELECT /* culprit fixture */ CONVERT(bigint, COALESCE(SUM(user_seeks), 0)) AS seeks FROM sys.dm_db_index_usage_stats WHERE database_id=DB_ID() AND object_id=OBJECT_ID(N'dbo.culprit_parallel_indexed') AND index_id>1",
            )
            .await
            .map_err(map_error)?;
            let seeks = u64::try_from(integer(&stats[0], "seeks")?).map_err(|_| invalid("negative seeks"))?;
            if count != PARALLEL_MATCHING_ROWS || seeks == 0 {
                return Err(invalid("indexed intervention did not seek exact matching rows"));
            }
            Ok((count, elapsed_ms))
        }
        .await;
        let retired = owned_case::retire(client).await;
        let res = outcome?;
        retired?;
        Ok(res)
    }

    pub async fn finish(&mut self, cancellation: &CollectionCancellation) -> Result<()> {
        owned_case::require_active(cancellation)?;
        self.fixture.release_holder_gate().await?;
        let completed = self.fixture.finish_workload().await?;
        if completed.len() != 1 || completed[0].inserts != PARALLEL_MATCHING_ROWS {
            return Err(invalid("owned parallel actor did not return exact expected row count after gate release"));
        }
        Ok(())
    }

    async fn open_actor(&self, cancellation: &CollectionCancellation) -> Result<(MsClient, NativeActivityIdentity)> {
        owned_case::require_active(cancellation)?;
        let mut client = self.fixture.client(PARALLEL_APPLICATION).await?;
        let res: Result<NativeActivityIdentity> = async {
            let rows = crate::conn::query(
                &mut client,
                "SELECT /* culprit fixture */ CONVERT(varchar(16),s.session_id) AS session_id,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS login_time FROM sys.dm_exec_sessions s WHERE s.session_id=@@SPID",
            )
            .await
            .map_err(map_error)?;
            let [row] = rows.as_slice() else { return Err(invalid("parallel actor session missing")) };
            let id = NativeActivityIdentity {
                id: text(row, "session_id")?,
                generation: crate::sql_time::native_time(&text(row, "login_time")?)?,
            };
            if !id.is_valid() {
                return Err(invalid("parallel actor identity invalid"));
            }
            fixture::execute(&mut client, WARM_PARALLEL_SQL).await?;
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

    async fn wait_for_parallel_tasks(
        &self,
        actor: &NativeActivityIdentity,
        cancellation: &CollectionCancellation,
    ) -> Result<()> {
        let mut observer = self.fixture.client("parallel-operator-oracle").await?;
        let spid: i64 = actor.id.parse().map_err(|_| invalid("invalid parallel actor spid"))?;
        let probe = format!(
            "SELECT /* culprit fixture */
                CONVERT(bigint, (SELECT COUNT(*) FROM sys.dm_os_tasks t WHERE t.session_id={spid} AND t.exec_context_id>0 AND t.worker_address IS NOT NULL)) AS parallel_workers,
                CONVERT(bigint, (SELECT COUNT(*) FROM sys.dm_os_waiting_tasks w WHERE w.session_id={spid} AND w.exec_context_id>0 AND w.wait_type LIKE N'LCK_M_%')) AS locked_workers,
                CONVERT(bigint, (SELECT COUNT(*) FROM sys.dm_exec_query_profiles p WHERE p.session_id={spid} AND p.thread_id>0)) AS profile_threads,
                CONVERT(bigint, (SELECT COUNT(*) FROM sys.dm_os_tasks t WHERE t.session_id={spid})) AS all_tasks,
                CONVERT(bigint, (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status=N'VISIBLE ONLINE')) AS online_schedulers,
                ISNULL((SELECT TOP(1) NULLIF(r.wait_type, N'') FROM sys.dm_exec_requests r WHERE r.session_id={spid}), N'NONE') AS req_wait"
        );
        let outcome = async {
            let mut last = String::new();
            for _ in 0..80 {
                owned_case::require_active(cancellation)?;
                let rows = crate::conn::query(&mut observer, &probe).await.map_err(map_error)?;
                let [row] = rows.as_slice() else { return Err(invalid("parallel oracle returned no row")) };
                let pw = integer(row, "parallel_workers")?;
                let lw = integer(row, "locked_workers")?;
                let pt = integer(row, "profile_threads")?;
                if pw >= 2 && lw >= 2 && pt >= 2 {
                    return Ok(());
                }
                last = format!(
                    "pw={pw}, lw={lw}, pt={pt}, all_tasks={}, schedulers={}, req_wait={}",
                    integer(row, "all_tasks")?,
                    integer(row, "online_schedulers")?,
                    text(row, "req_wait")?
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(invalid(&format!("parallel operator tasks did not stabilize ({last})")))
        }
        .await;
        let retired = owned_case::retire(observer).await;
        outcome?;
        retired
    }
}

pub async fn run_parallel_operator_case(
    test: impl for<'a> FnOnce(ParallelOperatorFixture<'a>, CollectionCancellation) -> BoxFuture<'a, Result<()>>
    + Send
    + 'static,
) {
    Box::pin(fixture::run(move |fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let body = test(ParallelOperatorFixture { fixture }, cancellation.clone());
            let (expired, result) =
                owned_case::finish_case(tokio::time::Instant::now() + CASE_TIMEOUT, &cancellation, body).await;
            result?;
            if expired {
                return Err(CollectionStop::TimedOut.error());
            }
            Ok(())
        })
    }))
    .await;
}

pub fn save_parallel_operator_receipt(value: &serde_json::Value) -> Result<()> {
    crate::test_support::save_cost_receipt(
        &crate::test_support::receipt_directory(),
        "mssql-parallel-operator-native",
        value,
    )
}
