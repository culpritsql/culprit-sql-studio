//! One fixture backend owns its original query until completion; labels never select a native participant.
use super::case;
use crate::{
    connect::{Connector, map_error},
    rows::tagged,
};
use culprit_core::{
    Result,
    telemetry::{CollectionStop, NativeActivityIdentity},
};
use culprit_read::cancellation::CollectionCancellation;
use std::time::Duration;
use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{CancelToken, Client};

pub(crate) const CONTROL_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
pub(crate) const SHARED_APPLICATION: &str = "fixture-shared-pooled-driver";
const ACTOR_STATEMENT_TIMEOUT: &str = "50s";
struct StatementResult {
    result: Result<()>,
    database_code: Option<String>,
}
impl StatementResult {
    fn native(result: std::result::Result<(), tokio_postgres::Error>) -> Self {
        let database_code = result
            .as_ref()
            .err()
            .and_then(tokio_postgres::Error::as_db_error)
            .map(|error| error.code().code().to_owned());
        Self { result: result.map_err(map_error), database_code }
    }
    fn refused(error: culprit_core::Error) -> Self {
        Self { result: Err(error), database_code: None }
    }
}
type Completed = (Client, StatementResult);

pub(crate) struct Actor {
    pub identity: NativeActivityIdentity,
    client: Option<Client>,
    task: Option<JoinHandle<Completed>>,
    cancel: CancelToken,
    connector: Connector,
    cancellation: CollectionCancellation,
    pub queries: u32,
    pub cancellation_attempts: u32,
    settlement_failed: bool,
}
impl Actor {
    pub async fn open(
        connector: &Connector,
        database: &str,
        marker: &str,
        application: &str,
        cancellation: &CollectionCancellation,
    ) -> Result<Self> {
        case::require_active(cancellation)?;
        let client =
            timeout(CONTROL_TIMEOUT, connector.connect_unconfigured()).await.map_err(|_| unconfirmed())??.client;
        case::admitted(cancellation,async {
            timeout(CONTROL_TIMEOUT,client.query(&tagged("SELECT pg_catalog.set_config('statement_timeout',$1,false),pg_catalog.set_config('lock_timeout','0',false),pg_catalog.set_config('application_name',$2,false)"),&[&ACTOR_STATEMENT_TIMEOUT,&application]))
                .await.map_err(|_|unconfirmed())?.map(|_|()).map_err(map_error)
        }).await?;
        let sql = format!(
            "SELECT pg_catalog.pg_backend_pid(),{},pg_catalog.current_database(),pg_catalog.current_setting('cluster_name') FROM pg_catalog.pg_stat_activity WHERE pid=pg_catalog.pg_backend_pid()",
            crate::native::iso("backend_start")
        );
        let row = case::admitted(cancellation, async {
            timeout(CONTROL_TIMEOUT, client.query_one(&tagged(&sql), &[]))
                .await
                .map_err(|_| unconfirmed())?
                .map_err(map_error)
        })
        .await?;
        if row.get::<_, String>(2) != database || row.get::<_, String>(3) != marker {
            return Err(CollectionStop::SourceChanged.error());
        }
        let identity = NativeActivityIdentity { id: row.get::<_, i32>(0).to_string(), generation: row.get(1) };
        if !identity.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        let cancel = client.cancel_token();
        Ok(Self {
            identity,
            client: Some(client),
            task: None,
            cancel,
            connector: connector.clone(),
            cancellation: cancellation.clone(),
            queries: 2,
            cancellation_attempts: 0,
            settlement_failed: false,
        })
    }
    #[cfg(test)]
    pub async fn hold_gate(&mut self, key: i64) -> Result<()> {
        if self.settlement_failed {
            return Err(unconfirmed());
        }
        let client = self.client.as_ref().ok_or_else(unconfirmed)?;
        case::require_active(&self.cancellation)?;
        self.queries += 1;
        case::admitted(&self.cancellation, async {
            timeout(CONTROL_TIMEOUT, client.query(&tagged("SELECT pg_catalog.pg_advisory_lock($1)"), &[&key]))
                .await
                .map_err(|_| unconfirmed())?
                .map(|_| ())
                .map_err(map_error)
        })
        .await
    }
    #[cfg(test)]
    pub fn wait_on_gate(&mut self, key: i64) -> Result<()> {
        case::require_active(&self.cancellation)?;
        if self.task.is_some() || self.settlement_failed {
            return Err(unconfirmed());
        }
        let client = self.client.take().ok_or_else(unconfirmed)?;
        let cancellation = self.cancellation.clone();
        self.queries += 1;
        self.task = Some(tokio::spawn(async move {
            let result = match case::require_active(&cancellation) {
                Ok(()) => StatementResult::native(
                    client
                        .query(&tagged("SELECT pg_catalog.pg_advisory_xact_lock_shared($1)"), &[&key])
                        .await
                        .map(|_| ()),
                ),
                Err(error) => StatementResult::refused(error),
            };
            (client, result)
        }));
        Ok(())
    }
    /// Explicit fixture control on this same idle backend. Any lost acknowledgement fences this owner.
    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        case::require_active(&self.cancellation)?;
        if self.task.is_some() || self.settlement_failed {
            return Err(unconfirmed());
        }
        let client = self.client.as_ref().ok_or_else(unconfirmed)?;
        self.queries += 1;
        self.settlement_failed = true;
        let result = timeout(CONTROL_TIMEOUT, client.batch_execute(&tagged(sql)))
            .await
            .map_err(|_| unconfirmed())?
            .map_err(map_error);
        if result.is_ok() {
            self.settlement_failed = false;
        }
        result
    }
    /// The task owns the original command and Client until its complete native response is joined.
    pub fn start(&mut self, sql: String) -> Result<()> {
        self.start_original(tagged(&sql))
    }
    /// Only nonce-owned fixture workload is intentionally visible to Activity; controls remain tagged.
    pub(crate) fn start_workload(&mut self, sql: String) -> Result<()> {
        self.start_original(sql)
    }
    fn start_original(&mut self, sql: String) -> Result<()> {
        case::require_active(&self.cancellation)?;
        if self.task.is_some() || self.settlement_failed {
            return Err(unconfirmed());
        }
        let client = self.client.take().ok_or_else(unconfirmed)?;
        let cancellation = self.cancellation.clone();
        self.queries += 1;
        self.task = Some(tokio::spawn(async move {
            let result = match case::require_active(&cancellation) {
                Ok(()) => StatementResult::native(client.batch_execute(&sql).await),
                Err(error) => StatementResult::refused(error),
            };
            (client, result)
        }));
        Ok(())
    }
    pub async fn settle(&mut self) -> Result<()> {
        self.settle_for(CONTROL_TIMEOUT).await
    }
    pub async fn settle_for(&mut self, allowance: Duration) -> Result<()> {
        let completion = self.join_original(allowance).await?;
        self.settlement_failed |= completion.result.is_err();
        completion.result
    }
    async fn join_original(&mut self, allowance: Duration) -> Result<StatementResult> {
        let task = self.task.as_mut().ok_or_else(unconfirmed)?;
        let joined = match timeout(allowance, task).await {
            Ok(joined) => joined,
            Err(_) => {
                self.settlement_failed = true;
                return Err(unconfirmed());
            }
        };
        self.task.take();
        let (client, completion) = match joined {
            Ok(completed) => completed,
            Err(_) => {
                self.settlement_failed = true;
                return Err(unconfirmed());
            }
        };
        self.client = Some(client);
        Ok(completion)
    }
    /// A real recovery-conflict response plus an acknowledged rollback settles this fixture transaction.
    /// Timeout/transport loss cannot enter this path and retain their original task/fence.
    pub(crate) async fn settle_recovery_conflict(&mut self, allowance: Duration) -> Result<()> {
        let completion = self.join_original(allowance).await?;
        self.rollback_after_native_stop(completion, "40001").await
    }
    /// Stop only this fixture's original statement, join its native cancellation response, then
    /// acknowledge rollback on that SAME client before schema cleanup is allowed.
    pub(crate) async fn cancel_and_rollback(&mut self) -> Result<()> {
        case::require_active(&self.cancellation)?;
        if self.task.is_none() || self.settlement_failed {
            return Err(unconfirmed());
        }
        self.settlement_failed = true;
        self.cancellation_attempts += 1;
        timeout(CONTROL_TIMEOUT, self.connector.cancel(&self.cancel)).await.map_err(|_| unconfirmed())??;
        let completion = self.join_original(CONTROL_TIMEOUT).await?;
        self.rollback_after_native_stop(completion, "57014").await
    }
    async fn rollback_after_native_stop(&mut self, completion: StatementResult, required_code: &str) -> Result<()> {
        self.settlement_failed = true;
        if completion.database_code.as_deref() != Some(required_code) || completion.result.is_ok() {
            return Err(unconfirmed());
        }
        case::require_active(&self.cancellation)?;
        let client = self.client.as_ref().ok_or_else(unconfirmed)?;
        self.queries += 1;
        timeout(CONTROL_TIMEOUT, client.batch_execute(&tagged("ROLLBACK")))
            .await
            .map_err(|_| unconfirmed())?
            .map_err(map_error)?;
        self.settlement_failed = false;
        Ok(())
    }
    pub(crate) async fn one(
        &mut self,
        sql: &str,
        parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<tokio_postgres::Row> {
        case::require_active(&self.cancellation)?;
        if self.task.is_some() || self.settlement_failed {
            return Err(unconfirmed());
        }
        let client = self.client.as_ref().ok_or_else(unconfirmed)?;
        self.queries += 1;
        self.settlement_failed = true;
        let row = timeout(CONTROL_TIMEOUT, client.query_one(&tagged(sql), parameters))
            .await
            .map_err(|_| unconfirmed())?
            .map_err(map_error)?;
        self.settlement_failed = false;
        Ok(row)
    }
    #[cfg(test)]
    pub fn close_idle(&mut self) -> Result<()> {
        if self.task.is_some() {
            return Err(unconfirmed());
        }
        self.client.take();
        Ok(())
    }
    pub async fn retire(&mut self) -> bool {
        self.client.take();
        let mut settled = !self.settlement_failed;
        if self.task.is_some() {
            // Cancellation is cleanup of the original backend; acknowledgement alone never proves settlement.
            self.cancellation_attempts += 1;
            let _ = timeout(CONTROL_TIMEOUT, self.connector.cancel(&self.cancel)).await;
            settled &= self.settle().await.is_ok();
            self.client.take();
        }
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
            settled = false;
        }
        settled
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
fn unconfirmed() -> culprit_core::Error {
    CollectionStop::CancellationUnconfirmed.error()
}
