//! Fixture-only application work. Original task/connection stay held through the production cancel+response owner.
use crate::{
    cancellation,
    connect::{Connector, TAG, map_error},
    read::accounting::Accounting,
};
use culprit_core::{Result, telemetry::*};
use mysql_async::{Conn, prelude::Queryable};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
struct Completion {
    client: Conn,
    result: mysql_async::Result<()>,
}
pub(super) struct Actor {
    pub id: u32,
    pub queries: u32,
    accounting: Accounting,
    client: Option<Conn>,
    control: Option<Conn>,
    task: Option<JoinHandle<Completion>>,
    marker: String,
    uncertain: bool,
}
impl Actor {
    pub fn dispatches(&self) -> u32 {
        self.queries + self.accounting.query_count()
    }

    pub async fn open(connector: &Arc<Connector>, nonce: &str, role: &str) -> Result<Self> {
        let client =
            tokio::time::timeout(CONTROL_TIMEOUT, connector.connect_unconfigured()).await.map_err(|_| unknown())??;
        let control = tokio::time::timeout(CONTROL_TIMEOUT, connector.connect_for_cancellation())
            .await
            .map_err(|_| unknown())??;
        let mut actor = Self {
            id: client.id(),
            queries: 0,
            accounting: Accounting::new(CollectionBudget::default()),
            client: Some(client),
            control: Some(control),
            task: None,
            marker: format!("/* native-fixture:{nonce}:{role} */"),
            uncertain: false,
        };
        actor.guard(nonce).await?;
        actor.execute("SET SESSION TRANSACTION READ WRITE").await?;
        actor.execute("SET SESSION lock_wait_timeout=150").await?;
        actor.execute("SET SESSION innodb_lock_wait_timeout=150").await?;
        actor.execute("USE `culprit_smoke`").await?;
        Ok(actor)
    }
    pub async fn guard(&mut self, nonce: &str) -> Result<()> {
        let row: Option<(String, u32, u64, u64, String)> =
            self.one("SELECT @@hostname,CONNECTION_ID(),@@GLOBAL.read_only,@@performance_schema,VERSION()").await?;
        let Some((host, id, readonly, instrumented, version)) = row else { return Err(unknown()) };
        if host != format!("culprit-smoke-mysql-{nonce}")
            || id != self.id
            || readonly != 0
            || instrumented != 1
            || !crate::builtin_safety::supported(crate::connect::Flavor::of_version(&version), &version)
        {
            self.uncertain = true;
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
    pub async fn execute(&mut self, sql: &str) -> Result<()> {
        self.require_idle()?;
        self.uncertain = true;
        self.queries += 1;
        let result =
            tokio::time::timeout(CONTROL_TIMEOUT, self.client.as_mut().unwrap().query_drop(format!("{TAG} {sql}")))
                .await
                .map_err(|_| unknown())?
                .map_err(map_error);
        if result.is_ok() {
            self.uncertain = false;
        }
        result
    }
    pub async fn one<T: mysql_async::prelude::FromRow + Send + 'static>(&mut self, sql: &str) -> Result<Option<T>> {
        self.require_idle()?;
        self.uncertain = true;
        self.queries += 1;
        let result =
            tokio::time::timeout(CONTROL_TIMEOUT, self.client.as_mut().unwrap().query_first(format!("{TAG} {sql}")))
                .await
                .map_err(|_| unknown())?
                .map_err(map_error);
        if result.is_ok() {
            self.uncertain = false;
        }
        result
    }
    fn require_idle(&self) -> Result<()> {
        if self.uncertain || self.task.is_some() || self.client.is_none() { Err(unknown()) } else { Ok(()) }
    }
    pub fn start_workload(&mut self, sql: String) -> Result<()> {
        self.require_idle()?;
        let mut client = self.client.take().ok_or_else(unknown)?;
        let sql = format!("{} {sql}", self.marker);
        self.queries += 1;
        self.task = Some(tokio::spawn(async move {
            let result = client.query_drop(sql).await;
            Completion { client, result }
        }));
        Ok(())
    }
    async fn stop_original(&mut self) -> bool {
        let Some(task) = self.task.as_mut() else { return !self.uncertain };
        let Some(control) = self.control.as_mut() else {
            self.uncertain = true;
            return false;
        };
        let slot = &mut self.client;
        let response = async {
            match task.await {
                Ok(completed) => {
                    *slot = Some(completed.client);
                    completed.result
                }
                Err(_) => Err(std::io::Error::other("owned fixture task did not return its native response").into()),
            }
        };
        let result = cancellation::settle(
            response,
            cancellation::request(control, self.id, &self.marker, &self.accounting),
            CONTROL_TIMEOUT,
        )
        .await;
        if result == SourceShutdown::Quiescent {
            self.task.take();
            true
        } else {
            self.uncertain = true;
            false
        }
    }
    pub async fn finish_original(&mut self) -> Result<()> {
        let task = self.task.as_mut().ok_or_else(unknown)?;
        let completed =
            tokio::time::timeout(CONTROL_TIMEOUT, task).await.map_err(|_| unknown())?.map_err(|_| unknown())?;
        self.task.take();
        self.client = Some(completed.client);
        let result = completed.result.map_err(map_error);
        self.uncertain = result.is_err();
        result
    }
    pub async fn retire(&mut self) -> bool {
        let settled = self.stop_original().await;
        if settled && !self.uncertain && self.client.is_some() {
            // This exact client may still hold the original explicit transaction after KILL QUERY.
            if self.execute("ROLLBACK").await.is_err() {
                self.uncertain = true;
            }
        }
        let known = settled && !self.uncertain;
        if let Some(client) = self.client.take() {
            let _ = tokio::time::timeout(CONTROL_TIMEOUT, client.disconnect()).await;
        }
        if let Some(control) = self.control.take() {
            let _ = tokio::time::timeout(CONTROL_TIMEOUT, control.disconnect()).await;
        }
        known
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
fn unknown() -> culprit_core::Error {
    CollectionStop::CancellationUnconfirmed.error()
}
