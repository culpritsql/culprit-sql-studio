//! Nonce-owned SQL Server `ASYNC_NETWORK_IO` fixture: one single-request sort stream paused after its first row
//! retains both a positive execution memory grant and a shared lock while blocking a collateral DDL waiter;
//! draining the original stream unblocks the waiter and retires all actors cleanly.
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
use futures_util::{FutureExt, TryStreamExt, future::BoxFuture};
use std::{panic::AssertUnwindSafe, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle};

pub const OUTPUT_APPLICATION: &str = "mssql-output-retention-owner";
pub const OUTPUT_WAITER_APPLICATION: &str = "mssql-output-collateral-waiter";
pub const OUTPUT_STREAM_ROWS: u32 = 10_000;
const CASE_TIMEOUT: Duration = Duration::from_secs(60);
const PAUSED_STREAM_SQL: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;BEGIN TRANSACTION;SELECT s.id,REPLICATE(CAST(CHAR(65+(s.id%26)) AS varchar(max)),32768) AS payload FROM dbo.page_fixture_select AS s WITH(HOLDLOCK,TABLOCK) CROSS JOIN dbo.page_fixture_select AS x ORDER BY CHECKSUM(s.id*97+x.id*31) DESC,s.id DESC,x.id DESC OPTION(MAXDOP 1,MIN_GRANT_PERCENT=25)";
const COLLATERAL_WAITER_SQL: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;BEGIN TRANSACTION;ALTER TABLE dbo.page_fixture_select ADD output_retention_col int NULL;ROLLBACK TRANSACTION;SELECT CONVERT(bigint,1) AS completed";

struct PausedOutputActor {
    release: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<u32>>>,
}

pub struct OutputRetentionFixture<'a> {
    fixture: &'a mut Fixture,
    paused: Option<PausedOutputActor>,
}

impl OutputRetentionFixture<'_> {
    pub fn endpoint(&self) -> Result<SourceEndpoint> {
        SourceEndpoint::from_fixture(self.fixture)
    }

    pub async fn start(
        &mut self,
        cancellation: &CollectionCancellation,
    ) -> Result<(NativeActivityIdentity, NativeActivityIdentity)> {
        owned_case::require_active(cancellation)?;
        self.fixture.require_idle_workload()?;
        let (mut output_client, output_identity) = self.connect_actor(OUTPUT_APPLICATION, cancellation).await?;
        let (waiter_client, waiter_identity) = match self.connect_actor(OUTPUT_WAITER_APPLICATION, cancellation).await {
            Ok(pair) => pair,
            Err(err) => {
                let _ = owned_case::retire(output_client).await;
                return Err(err);
            }
        };

        let (first_tx, first_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task_cancel = cancellation.clone();
        let task = tokio::spawn(async move {
            let outcome = AssertUnwindSafe(async {
                owned_case::require_active(&task_cancel)?;
                let mut stream = output_client.simple_query(PAUSED_STREAM_SQL).await.map_err(map_error)?;
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
                fixture::execute(&mut output_client, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await?;
                if rows != OUTPUT_STREAM_ROWS {
                    return Err(invalid("owned output stream did not drain exact expected row count"));
                }
                Ok::<u32, culprit_core::Error>(rows)
            })
            .catch_unwind()
            .await;
            let retired = owned_case::retire(output_client).await;
            let rows = match outcome {
                Ok(res) => res?,
                Err(panic) => std::panic::resume_unwind(panic),
            };
            retired?;
            Ok(rows)
        });
        self.paused = Some(PausedOutputActor { release: Some(release_tx), task: Some(task) });

        let first_rows = tokio::time::timeout(Duration::from_secs(10), first_rx)
            .await
            .map_err(|_| invalid("owned output actor did not produce its first row within bound"))?
            .map_err(|_| invalid("owned output actor exited before producing its first row"))?;
        if first_rows != 1 {
            let _ = owned_case::retire(waiter_client).await;
            return Err(invalid("owned output actor first item was not a data row"));
        }

        actors::start(
            &mut self.fixture.workers,
            vec![(waiter_client, 0u8, COLLATERAL_WAITER_SQL.into())],
            |pattern, row| Ok(CompletedWorker { pattern, inserts: actors::completion_count(row, "completed")? }),
        )
        .await?;

        self.wait_for_native_state(&output_identity, &waiter_identity, cancellation).await?;
        // Ensure both native elapsed_ms and wait_time cross the 1,000 ms minimum ActivityPolicy floor.
        tokio::select! {
            _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            _ = tokio::time::sleep(Duration::from_millis(1150)) => (),
        }
        owned_case::require_active(cancellation)?;
        Ok((output_identity, waiter_identity))
    }

    async fn connect_actor(
        &self,
        application: &str,
        cancellation: &CollectionCancellation,
    ) -> Result<(MsClient, NativeActivityIdentity)> {
        owned_case::require_active(cancellation)?;
        let mut client = self.fixture.client(application).await?;
        let res = async {
            let rows = crate::conn::query(
                &mut client,
                "SELECT /* culprit fixture */ CONVERT(varchar(16),s.session_id) AS session_id,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS login_time FROM sys.dm_exec_sessions s WHERE s.session_id=@@SPID",
            )
            .await
            .map_err(map_error)?;
            let [row] = rows.as_slice() else { return Err(invalid("owned output actor session missing")) };
            let id = NativeActivityIdentity {
                id: text(row, "session_id")?,
                generation: crate::sql_time::native_time(&text(row, "login_time")?)?,
            };
            if !id.is_valid() {
                return Err(invalid("owned output actor session malformed"));
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

    async fn wait_for_native_state(
        &self,
        output: &NativeActivityIdentity,
        waiter: &NativeActivityIdentity,
        cancellation: &CollectionCancellation,
    ) -> Result<()> {
        let mut observer = self.fixture.client("mssql-output-retention-oracle").await?;
        let out_spid: i64 = output.id.parse().map_err(|_| invalid("invalid output spid"))?;
        let wait_spid: i64 = waiter.id.parse().map_err(|_| invalid("invalid waiter spid"))?;
        let probe_sql = format!(
            "SELECT /* culprit fixture */
                CONVERT(bigint, CASE WHEN EXISTS(
                    SELECT 1 FROM sys.dm_exec_requests r
                    JOIN sys.dm_exec_query_memory_grants mg ON mg.session_id=r.session_id AND mg.request_id=r.request_id
                    WHERE r.session_id={out_spid} AND r.wait_type=N'ASYNC_NETWORK_IO' AND ISNULL(mg.granted_memory_kb,0)>0
                ) THEN 1 ELSE 0 END) AS output_ready,
                CONVERT(bigint, CASE WHEN EXISTS(
                    SELECT 1 FROM sys.dm_exec_requests w
                    WHERE w.session_id={wait_spid} AND w.blocking_session_id={out_spid}
                ) THEN 1 ELSE 0 END) AS waiter_ready,
                ISNULL((SELECT TOP(1) NULLIF(r.wait_type, N'') FROM sys.dm_exec_requests r WHERE r.session_id={out_spid}), N'NONE') AS out_wait,
                CONVERT(bigint, ISNULL((SELECT TOP(1) mg.granted_memory_kb FROM sys.dm_exec_query_memory_grants mg WHERE mg.session_id={out_spid}), 0)) AS out_grant,
                CONVERT(bigint, ISNULL((SELECT TOP(1) w.blocking_session_id FROM sys.dm_exec_requests w WHERE w.session_id={wait_spid}), 0)) AS wait_blocker"
        );
        let outcome = async {
            let mut last = String::new();
            for _ in 0..80 {
                owned_case::require_active(cancellation)?;
                let rows = crate::conn::query(&mut observer, &probe_sql).await.map_err(map_error)?;
                let [row] = rows.as_slice() else { return Err(invalid("oracle returned no row")) };
                if integer(row, "output_ready")? == 1 && integer(row, "waiter_ready")? == 1 {
                    return Ok(());
                }
                last = format!(
                    "out_wait={}, out_grant={}, wait_blocker={}",
                    text(row, "out_wait")?,
                    integer(row, "out_grant")?,
                    integer(row, "wait_blocker")?
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(invalid(&format!(
                "owned output retention state (ASYNC_NETWORK_IO + grant + blocked waiter) was not observed ({last})"
            )))
        }
        .await;
        let retired = owned_case::retire(observer).await;
        outcome?;
        retired
    }

    pub async fn drain_and_finish(&mut self, cancellation: &CollectionCancellation) -> Result<u32> {
        owned_case::require_active(cancellation)?;
        let mut paused = self.paused.take().ok_or_else(|| invalid("owned output actor missing"))?;
        if let Some(release) = paused.release.take() {
            let _ = release.send(());
        }
        let task = paused.task.take().ok_or_else(|| invalid("owned output actor task missing"))?;
        let drained = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .map_err(|_| CollectionStop::CancellationUnconfirmed.error())?
            .map_err(|_| CollectionStop::CancellationUnconfirmed.error())??;
        let completed = self.fixture.finish_workload().await?;
        if completed.len() != 1 || completed[0].inserts != 1 {
            return Err(invalid("collateral DDL waiter did not complete after output drain"));
        }
        Ok(drained)
    }
}

impl Drop for OutputRetentionFixture<'_> {
    fn drop(&mut self) {
        if let Some(mut paused) = self.paused.take()
            && let Some(task) = paused.task.take()
        {
            task.abort();
        }
    }
}

pub async fn run_output_retention_case(
    test: impl for<'a> FnOnce(OutputRetentionFixture<'a>, CollectionCancellation) -> BoxFuture<'a, Result<()>>
    + Send
    + 'static,
) {
    Box::pin(fixture::run(move |fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let body = test(OutputRetentionFixture { fixture, paused: None }, cancellation.clone());
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

pub fn save_output_retention_receipt(value: &serde_json::Value) -> Result<()> {
    crate::test_support::save_cost_receipt(
        &crate::test_support::receipt_directory(),
        "mssql-output-retention-native",
        value,
    )
}
