//! Nonce-owned 2-node SQL Server 2022 Always On AG readable-secondary redo-blocking fixture:
//! 1. Connects to the explicitly admitted AG readable secondary (`OwnedSourceFixture`) and primary (`p<owner[..14]>`).
//! 2. Seeds `dbo.culprit_ag_redo_items` on the primary and waits for synchronization on the readable secondary.
//! 3. Starts a paused multi-row `SELECT` on the readable secondary holding `Sch-S` (Phase 1: long-running reader, no redo wait).
//! 4. Issues `ALTER TABLE dbo.culprit_ag_redo_items ADD redo_col int NULL` on the async-commit primary so secondary
//!    `DB STARTUP` / `PARALLEL REDO TASK` blocks on `LCK_M_SCH_M` with `blocking_session_id = reader_spid` (Phase 2).
//! 5. Drains and retires the secondary reader so redo immediately applies `redo_col` on the secondary (Phase 3).
use super::{fixture::invalid, serial_input_profile::SourceEndpoint};
use crate::{
    conn::{MsClient, SessionKind, map_error},
    read::{integer, invalid_data, text},
    test_support::{OwnedSourceFixture, batch, config, connect, owned_case, receipt_directory, save_cost_receipt},
    value::Rec,
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::{FutureExt, TryStreamExt, future::BoxFuture};
use std::{panic::AssertUnwindSafe, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle};

pub const AG_SECONDARY_READER_APP: &str = "mssql-ag-secondary-reader";
pub const AG_REDO_STREAM_ROWS: u32 = 500;
const CASE_TIMEOUT: Duration = Duration::from_secs(90);
const PHASE_TIMEOUT: Duration = Duration::from_secs(15);

struct PausedSecondaryReader {
    raw_login_time: String,
    release: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<u32>>>,
}

pub struct AgRedoBlockingFixture {
    secondary: OwnedSourceFixture,
    primary_port: u16,
    primary_password: String,
    paused: Option<PausedSecondaryReader>,
    table_created: bool,
}

impl AgRedoBlockingFixture {
    async fn open() -> Result<Self> {
        let role = std::env::var("CULPRIT_MSSQL_REPLICATION_EXPECT_ROLE").map_err(|_| invalid_data())?;
        if role != "secondary" {
            return Err(invalid("AG redo-blocking fixture must target the readable secondary member"));
        }
        let secondary = OwnedSourceFixture::open("culprit-ag-redo-secondary-source").await?;
        let primary_port: u16 = std::env::var("CULPRIT_MSSQL_AG_PRIMARY_PORT")
            .map_err(|_| invalid("missing CULPRIT_MSSQL_AG_PRIMARY_PORT"))?
            .parse()
            .map_err(|_| invalid_data())?;
        let primary_password = std::env::var("CULPRIT_MSSQL_AG_PRIMARY_PASSWORD")
            .map_err(|_| invalid("missing CULPRIT_MSSQL_AG_PRIMARY_PASSWORD"))?;
        let primary_owner = std::env::var("CULPRIT_MSSQL_AG_PRIMARY_OWNER")
            .map_err(|_| invalid("missing CULPRIT_MSSQL_AG_PRIMARY_OWNER"))?;
        if primary_owner.len() != 24
            || !primary_owner.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !secondary.database.starts_with(&format!("culprit_smoke_{primary_owner}_"))
        {
            return Err(invalid_data());
        }
        let mut primary_check =
            connect(&config(primary_port, "master", &primary_password, "culprit-ag-primary-verify"), SessionKind::User)
                .await?;
        let mut verify = tiberius::Query::new(
            "SELECT /* culprit fixture guard */ CONVERT(nvarchar(128),SERVERPROPERTY('MachineName')) AS machine, \
             CONVERT(nvarchar(128),DB_NAME(DB_ID(@P1))) AS owned_database, \
             CONVERT(nvarchar(60),DATABASEPROPERTYEX(@P1,'Updateability')) AS updateability",
        );
        verify.bind(secondary.database.as_str());
        let rows =
            verify.query(&mut primary_check).await.map_err(map_error)?.into_results().await.map_err(map_error)?;
        let [rows] = rows.as_slice() else { return Err(invalid_data()) };
        let [row] = rows.as_slice() else { return Err(invalid_data()) };
        let rec = Rec::from_row(row);
        let expected_machine = format!("p{}", &primary_owner[..14]);
        if text(&rec, "machine")? != expected_machine
            || text(&rec, "owned_database")? != secondary.database
            || text(&rec, "updateability")? != "READ_WRITE"
        {
            return Err(invalid_data());
        }
        owned_case::retire(primary_check).await?;

        Ok(Self { secondary, primary_port, primary_password, paused: None, table_created: false })
    }

    pub fn secondary_endpoint(&self) -> SourceEndpoint {
        SourceEndpoint::from_pool(self.secondary.pool.clone(), self.secondary.port)
    }

    async fn primary_client(&self, application: &str) -> Result<MsClient> {
        connect(
            &config(self.primary_port, &self.secondary.database, &self.primary_password, application),
            SessionKind::User,
        )
        .await
    }

    async fn secondary_client(&self, application: &str) -> Result<MsClient> {
        connect(
            &config(self.secondary.port, &self.secondary.database, self.secondary.password(), application),
            SessionKind::User,
        )
        .await
    }

    /// Seed `dbo.culprit_ag_redo_items` on the primary and wait until all `AG_REDO_STREAM_ROWS` rows replicate
    /// to the readable secondary, then start a paused secondary reader holding `Sch-S` with `elapsed_ms >= 1000`.
    pub async fn prepare_and_start_secondary_reader(
        &mut self,
        cancellation: &CollectionCancellation,
    ) -> Result<NativeActivityIdentity> {
        owned_case::require_active(cancellation)?;
        let mut primary = self.primary_client("culprit-ag-redo-seed").await?;
        batch(
            &mut primary,
            &format!(
                "DROP TABLE IF EXISTS dbo.culprit_ag_redo_items; \
                 CREATE TABLE dbo.culprit_ag_redo_items(id int NOT NULL PRIMARY KEY, payload char(4000) NOT NULL); \
                 WITH n AS (SELECT v FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) x(v)) \
                 INSERT dbo.culprit_ag_redo_items(id, payload) \
                 SELECT TOP({AG_REDO_STREAM_ROWS}) a.v*100 + b.v*10 + c.v + 1, REPLICATE('R', 4000) \
                 FROM n a CROSS JOIN n b CROSS JOIN n c ORDER BY 1;"
            ),
        )
        .await;
        self.table_created = true;
        owned_case::retire(primary).await?;

        let mut sec_probe = self.secondary_client("culprit-ag-secondary-sync-probe").await?;
        let sync_deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            owned_case::require_active(cancellation)?;
            let exists = query_one(
                &mut sec_probe,
                "SELECT /* culprit fixture */ CASE WHEN OBJECT_ID(N'dbo.culprit_ag_redo_items') IS NOT NULL \
                 THEN 1 ELSE 0 END AS present",
            )
            .await?;
            if integer(&exists, "present")? == 1 {
                let row = query_one(
                    &mut sec_probe,
                    "SELECT /* culprit fixture */ COUNT_BIG(*) AS synced FROM dbo.culprit_ag_redo_items",
                )
                .await?;
                if integer(&row, "synced")? == i64::from(AG_REDO_STREAM_ROWS) {
                    break;
                }
            }
            if tokio::time::Instant::now() >= sync_deadline {
                let _ = owned_case::retire(sec_probe).await;
                return Err(invalid("secondary did not replicate seeded culprit_ag_redo_items within bound"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        owned_case::retire(sec_probe).await?;

        let mut reader_client = self.secondary_client(AG_SECONDARY_READER_APP).await?;
        let id_row = query_one(
            &mut reader_client,
            "SELECT /* culprit fixture */ CONVERT(bigint,@@SPID) AS id, \
             CONVERT(varchar(40),CAST(login_time AS datetime2),126) AS generation \
             FROM sys.dm_exec_sessions WHERE session_id=@@SPID",
        )
        .await?;
        let raw_login_time = text(&id_row, "generation")?;
        let reader_id = NativeActivityIdentity {
            id: integer(&id_row, "id")?.to_string(),
            generation: crate::sql_time::native_time(&raw_login_time)?,
        };

        let (first_tx, first_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task_cancel = cancellation.clone();
        let task = tokio::spawn(async move {
            let outcome = AssertUnwindSafe(async {
                owned_case::require_active(&task_cancel)?;
                let mut stream = reader_client
                    .simple_query(
                        "SET NOCOUNT ON; SELECT id, REPLICATE(CAST('R' AS varchar(max)), 65536) AS payload \
                         FROM dbo.culprit_ag_redo_items ORDER BY id OPTION(MAXDOP 1)",
                    )
                    .await
                    .map_err(map_error)?;
                let mut rows = 0u32;
                while let Some(item) = stream.try_next().await.map_err(map_error)? {
                    if matches!(item, tiberius::QueryItem::Row(_)) {
                        rows += 1;
                        break;
                    }
                }
                let _ = first_tx.send(rows);
                release_rx.await.map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
                while let Some(item) = stream.try_next().await.map_err(map_error)? {
                    if matches!(item, tiberius::QueryItem::Row(_)) {
                        rows += 1;
                    }
                }
                drop(stream);
                if rows != AG_REDO_STREAM_ROWS {
                    return Err(invalid("secondary reader stream did not drain exact expected rows"));
                }
                Ok::<u32, culprit_core::Error>(rows)
            })
            .catch_unwind()
            .await;
            let retired = owned_case::retire(reader_client).await;
            let rows = match outcome {
                Ok(res) => res?,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            retired?;
            Ok(rows)
        });
        self.paused = Some(PausedSecondaryReader { raw_login_time, release: Some(release_tx), task: Some(task) });

        let first_rows = tokio::time::timeout(Duration::from_secs(10), first_rx)
            .await
            .map_err(|_| invalid("secondary reader did not yield first row within bound"))?
            .map_err(|_| invalid("secondary reader exited before first row"))?;
        if first_rows != 1 {
            return Err(invalid("secondary reader did not yield row 1"));
        }

        // Confirm SQL Server's own total_elapsed_time crossed the 1,000 ms LongRunning floor while paused on ASYNC_NETWORK_IO.
        let spid: i32 = reader_id.id.parse().map_err(|_| invalid_data())?;
        let mut elapsed_probe = self.secondary_client("culprit-ag-secondary-elapsed-probe").await?;
        let elapsed_deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            owned_case::require_active(cancellation)?;
            let mut q = tiberius::Query::new(
                "SELECT /* culprit fixture */ COUNT_BIG(*) AS ready \
                 FROM sys.dm_exec_requests \
                 WHERE session_id=@P1 AND database_id=DB_ID() \
                   AND wait_type=N'ASYNC_NETWORK_IO' AND total_elapsed_time>=1200",
            );
            q.bind(spid);
            let rows = q.query(&mut elapsed_probe).await.map_err(map_error)?.into_results().await.map_err(map_error)?;
            let rec = Rec::from_row(&rows[0][0]);
            if integer(&rec, "ready")? >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= elapsed_deadline {
                let _ = owned_case::retire(elapsed_probe).await;
                return Err(invalid("secondary reader request did not reach >= 1200 ms elapsed within bound"));
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        owned_case::retire(elapsed_probe).await?;
        Ok(reader_id)
    }

    /// Execute `ALTER TABLE` on the async-commit primary and wait until secondary redo blocks on `LCK_M_SCH_M`
    /// behind `reader_id`.
    pub async fn block_secondary_redo_on_reader(
        &mut self,
        reader_id: &NativeActivityIdentity,
        cancellation: &CollectionCancellation,
    ) -> Result<()> {
        owned_case::require_active(cancellation)?;
        let mut primary = self.primary_client("culprit-ag-primary-ddl").await?;
        batch(&mut primary, "ALTER TABLE dbo.culprit_ag_redo_items ADD redo_col int NULL;").await;
        owned_case::retire(primary).await?;

        let spid: i32 = reader_id.id.parse().map_err(|_| invalid_data())?;
        let mut sec_probe = self.secondary_client("culprit-ag-redo-wait-probe").await?;
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            owned_case::require_active(cancellation)?;
            let mut q = tiberius::Query::new(
                "SELECT /* culprit fixture */ COUNT_BIG(*) AS blocked \
                 FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id \
                 WHERE r.database_id=DB_ID() AND s.is_user_process=0 \
                 AND r.command IN (N'DB STARTUP',N'PARALLEL REDO TASK',N'PARALLEL REDO HELP TASK') \
                 AND r.wait_type=N'LCK_M_SCH_M' AND r.blocking_session_id=@P1",
            );
            q.bind(spid);
            let rows = q.query(&mut sec_probe).await.map_err(map_error)?.into_results().await.map_err(map_error)?;
            let rec = Rec::from_row(&rows[0][0]);
            if integer(&rec, "blocked")? >= 1 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = owned_case::retire(sec_probe).await;
                return Err(invalid("secondary redo did not block on LCK_M_SCH_M behind reader within bound"));
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        owned_case::retire(sec_probe).await?;
        Ok(())
    }

    /// Intervention: drain and retire the paused secondary reader, then confirm redo applied `redo_col` on the secondary
    /// and the reader session is absent from `sys.dm_exec_sessions`.
    pub async fn drain_reader_and_await_redo(
        &mut self,
        reader_id: &NativeActivityIdentity,
        cancellation: &CollectionCancellation,
    ) -> Result<u32> {
        owned_case::require_active(cancellation)?;
        let mut paused = self.paused.take().ok_or_else(|| invalid("secondary reader was not started"))?;
        if let Some(release) = paused.release.take() {
            let _ = release.send(());
        }
        let drained =
            paused.task.take().unwrap().await.map_err(|_| CollectionStop::CancellationUnconfirmed.error())??;

        let spid: i32 = reader_id.id.parse().map_err(|_| invalid_data())?;
        let raw_login_time = paused.raw_login_time;
        let mut sec_probe = self.secondary_client("culprit-ag-redo-complete-probe").await?;
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            owned_case::require_active(cancellation)?;
            let mut q = tiberius::Query::new(
                "SELECT /* culprit fixture */ \
                 CASE WHEN EXISTS (SELECT 1 FROM sys.columns \
                  WHERE object_id = OBJECT_ID(N'dbo.culprit_ag_redo_items') AND name = N'redo_col') \
                 THEN 1 ELSE 0 END AS applied, \
                 (SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions \
                  WHERE session_id=@P1 AND session_id<>@@SPID \
                    AND CONVERT(varchar(40),CAST(login_time AS datetime2),126)=@P2) AS remaining_reader",
            );
            q.bind(spid);
            q.bind(raw_login_time.as_str());
            let rows = q.query(&mut sec_probe).await.map_err(map_error)?.into_results().await.map_err(map_error)?;
            let rec = Rec::from_row(&rows[0][0]);
            if integer(&rec, "applied")? == 1 && integer(&rec, "remaining_reader")? == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = owned_case::retire(sec_probe).await;
                return Err(invalid("secondary redo did not apply redo_col or retire reader within bound"));
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        owned_case::retire(sec_probe).await?;
        Ok(drained)
    }

    async fn cleanup(&mut self) -> Result<()> {
        if let Some(mut paused) = self.paused.take() {
            if let Some(release) = paused.release.take() {
                let _ = release.send(());
            }
            if let Some(task) = paused.task.take() {
                let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
            }
        }
        if self.table_created {
            let mut primary = self.primary_client("culprit-ag-redo-cleanup").await?;
            batch(&mut primary, "DROP TABLE IF EXISTS dbo.culprit_ag_redo_items;").await;
            owned_case::retire(primary).await?;
            self.table_created = false;
        }
        Ok(())
    }
}

async fn query_one(client: &mut MsClient, sql: &str) -> Result<Rec> {
    let sets = client.simple_query(sql).await.map_err(map_error)?.into_results().await.map_err(map_error)?;
    let [rows] = sets.as_slice() else { return Err(invalid_data()) };
    let [row] = rows.as_slice() else { return Err(invalid_data()) };
    Ok(Rec::from_row(row))
}

pub async fn run_ag_redo_blocking_case<F>(body: F)
where
    F: for<'a> FnOnce(&'a mut AgRedoBlockingFixture, CollectionCancellation) -> BoxFuture<'a, Result<()>>,
{
    let mut fixture = AgRedoBlockingFixture::open().await.expect("admitted owned 2-node AG secondary fixture");
    let cancellation = CollectionCancellation::default();
    let token = cancellation.clone();
    let (expired, value) = owned_case::finish_case(
        tokio::time::Instant::now() + CASE_TIMEOUT,
        &token,
        AssertUnwindSafe(body(&mut fixture, cancellation)).catch_unwind(),
    )
    .await;
    let cleaned = fixture.cleanup().await;
    if let Err(panic) = value {
        std::panic::resume_unwind(panic);
    }
    assert!(!expired, "owned AG redo-blocking case exceeded its deadline");
    value.unwrap().expect("owned AG redo-blocking body succeeded");
    cleaned.expect("owned AG redo-blocking cleanup succeeded");
}

pub fn save_ag_redo_blocking_receipt(payload: &serde_json::Value) -> Result<()> {
    save_cost_receipt(&receipt_directory(), "ag-readable-secondary-redo-blocking", payload)
        .map_err(|_| invalid("unable to write AG readable-secondary redo-blocking receipt"))
}
