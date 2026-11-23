//! A real READONLY TVP enters one bounded serial request; only native source results establish its profile.
use super::{
    fixture::{self, Fixture, actors, invalid},
    workload::CompletedWorker,
};
use crate::{
    conn::{MsClient, Pool, map_error},
    read::text,
    test_support::owned_case,
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::future::BoxFuture;
use std::{sync::Arc, time::Duration};

pub const INPUT_ROWS: u64 = 512;
pub const APPLICATION: &str = "serial-tvp-owned-actor";
// Existing fixture admin/seed8 +setup SET/type/proc3 +actor SET/identity/warm/work4 +gate SET/control/release3 +DROP1.
pub const FIXTURE_DISPATCH_CAP: u32 = 19;
const CASE_TIMEOUT: Duration = Duration::from_secs(45);
const CREATE_TYPE: &str = "CREATE TYPE dbo.CulpritSerialRows AS TABLE(id int NOT NULL PRIMARY KEY)";
const CREATE_PROCEDURE: &str = "CREATE PROCEDURE dbo.culprit_serial_tvp @Rows dbo.CulpritSerialRows READONLY AS SELECT COUNT_BIG(*) AS completed FROM @Rows AS r INNER HASH JOIN dbo.page_fixture_select AS s WITH(READCOMMITTEDLOCK) ON s.id=r.id OPTION(FORCE ORDER,MAXDOP 1,USE HINT('QUERY_PLAN_PROFILE'))";
// An actual uncommitted key change keeps the owned reader behind native write visibility. The retained
// gate's acknowledged rollback restores the seed key before the actor returns its unchanged count.
const PROBE_GATE: &str =
    "SET LOCK_TIMEOUT 1000;BEGIN TRANSACTION;UPDATE dbo.page_fixture_select WITH(ROWLOCK) SET id=-1 WHERE id=0;";
// Full profiling is an explicit owned-actor fixture control. The production reader only observes whatever
// instrumentation the application already has; it never enables this session setting on a production caller.
const WARM: &str = "SET STATISTICS PROFILE ON;SET NOCOUNT ON;DECLARE @input dbo.CulpritSerialRows;INSERT @input VALUES(0);EXEC dbo.culprit_serial_tvp @Rows=@input";
// Only the observed procedure is profiled. Profiling the loader INSERT would put its statistics result
// ahead of the procedure's completion row; the existing actor still drains every native result to EOF.
const WORK: &str = "SET STATISTICS PROFILE OFF;SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;DECLARE @input dbo.CulpritSerialRows;WITH n AS(SELECT v FROM(VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9))x(v)) INSERT @input SELECT TOP(512) a.v*100+b.v*10+c.v FROM n a CROSS JOIN n b CROSS JOIN n c ORDER BY 1;SET STATISTICS PROFILE ON;EXEC dbo.culprit_serial_tvp @Rows=@input";

/// A selected provider facade. It opens only the production source against the already-owned native database.
#[derive(Clone)]
pub struct SourceEndpoint {
    pool: Arc<Pool>,
    port: u16,
}
impl SourceEndpoint {
    pub fn request(&self, selection: CollectionSelection) -> Result<CollectionRequest> {
        let request =
            CollectionRequest { subject: CollectionSubject::Database, selection, budget: CollectionBudget::default() };
        let recipe = crate::telemetry::recipes()
            .into_iter()
            .find(|recipe| recipe.selector.matches(&request))
            .ok_or_else(|| invalid("owned native selection has no declared provider recipe"))?;
        Ok(CollectionRequest { budget: request.budget.intersection(&recipe.budget), ..request })
    }
    pub fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        crate::telemetry::source(self.pool.clone(), request, "127.0.0.1".into(), self.port)
    }
}
/// A borrow of the one existing fixture owner, never a second actor lifecycle.
pub struct SerialInputFixture<'a> {
    fixture: &'a mut Fixture,
}
impl SerialInputFixture<'_> {
    pub fn setup_elapsed_ms(&self) -> f64 {
        self.fixture.setup_elapsed_ms
    }
    pub fn endpoint(&self) -> Result<SourceEndpoint> {
        Ok(SourceEndpoint {
            pool: self.fixture.pool.as_ref().ok_or_else(|| invalid("owned source pool absent"))?.clone(),
            port: self.fixture.port,
        })
    }
    pub async fn start(&mut self, cancellation: &CollectionCancellation) -> Result<NativeActivityIdentity> {
        owned_case::require_active(cancellation)?;
        self.fixture.require_idle_workload()?;
        let mut setup = self.fixture.client("serial-tvp-owned-setup").await?;
        let setup_result: Result<()> = async {
            for statement in [CREATE_TYPE, CREATE_PROCEDURE] {
                owned_case::require_active(cancellation)?;
                fixture::execute(&mut setup, statement).await?;
            }
            Ok(())
        }
        .await;
        let retired = owned_case::retire(setup).await;
        setup_result?;
        retired?;
        let (client, identity) = Box::pin(self.actor(cancellation)).await?;
        let admitted = async {
            owned_case::require_active(cancellation)?;
            self.fixture.start_holder_gate_with_statement(PROBE_GATE).await?;
            owned_case::require_active(cancellation)
        }
        .await;
        if let Err(original) = admitted {
            let _ = owned_case::retire(client).await;
            return Err(original);
        }
        actors::start(&mut self.fixture.workers, vec![(client, (), WORK.into())], |(), row| {
            Ok(CompletedWorker { pattern: 0, inserts: actors::completion_count(row, "completed")? })
        })
        .await?;
        // This is actual fixture wall time; Activity still has to report its own qualifying native elapsed time.
        tokio::select! {
            _ = cancellation.cancelled() => return Err(CollectionStop::Cancelled.error()),
            _ = tokio::time::sleep(Duration::from_millis(1100)) => (),
        }
        owned_case::require_active(cancellation)?;
        if self.fixture.workers.iter().flatten().any(|worker| worker.is_finished()) {
            self.fixture.finish_workload().await?;
            return Err(invalid("owned TVP actor completed before its retained native probe gate was released"));
        }
        Ok(identity)
    }
    async fn actor(&self, cancellation: &CollectionCancellation) -> Result<(MsClient, NativeActivityIdentity)> {
        owned_case::require_active(cancellation)?;
        let mut client = self.fixture.client(APPLICATION).await?;
        let result = async {
            owned_case::require_active(cancellation)?;
            let rows = crate::conn::query(&mut client, "SELECT /* culprit fixture */ CONVERT(varchar(16),s.session_id) AS session_id,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS login_time FROM sys.dm_exec_sessions s WHERE s.session_id=@@SPID").await.map_err(map_error)?;
            let [row] = rows.as_slice() else { return Err(invalid("owned TVP actor native login unavailable")) };
            let identity = NativeActivityIdentity { id: text(row, "session_id")?, generation: crate::sql_time::native_time(&text(row,"login_time")?)? };
            if !identity.is_valid() { return Err(invalid("owned TVP actor native login malformed")); }
            owned_case::require_active(cancellation)?;
            fixture::execute(&mut client, WARM).await?;
            Ok(identity)
        }.await;
        match result {
            Ok(identity) => Ok((client, identity)),
            Err(original) => {
                let _ = owned_case::retire(client).await;
                Err(original)
            }
        }
    }
    pub async fn finish(&mut self, cancellation: &CollectionCancellation) -> Result<()> {
        owned_case::require_active(cancellation)?;
        self.fixture.release_holder_gate().await?;
        let completed = self.fixture.finish_workload().await?;
        if completed.len() != 1 || completed[0].pattern != 0 || completed[0].inserts != 100 {
            return Err(invalid("owned TVP request did not acknowledge its bounded native COUNT result"));
        }
        Ok(())
    }
}
pub async fn run_serial_tvp_case(
    test: impl for<'a> FnOnce(SerialInputFixture<'a>, CollectionCancellation) -> BoxFuture<'a, Result<()>> + Send + 'static,
) {
    Box::pin(fixture::run(move |fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let body = test(SerialInputFixture { fixture }, cancellation.clone());
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
pub fn save_native_receipt(value: &serde_json::Value) -> Result<()> {
    crate::test_support::save_cost_receipt(
        &crate::test_support::receipt_directory(),
        "serial-tvp-current-execution-native",
        value,
    )
}
pub fn save_native_observation(value: &serde_json::Value) -> Result<()> {
    crate::test_support::save_cost_receipt(
        &crate::test_support::receipt_directory(),
        "serial-tvp-current-execution-observed",
        value,
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_serial_work_is_a_real_parameter_call_with_bounded_query_local_profiling() {
        assert!(CREATE_PROCEDURE.contains("@Rows dbo.CulpritSerialRows READONLY"));
        assert!(CREATE_PROCEDURE.contains("FROM @Rows AS r INNER HASH JOIN"));
        assert!(CREATE_PROCEDURE.contains("MAXDOP 1,USE HINT('QUERY_PLAN_PROFILE')"));
        assert!(PROBE_GATE.contains("WITH(ROWLOCK) SET id=-1 WHERE id=0"));
        assert!(!PROBE_GATE.contains("TABLOCK"));
        assert!(WORK.contains("TOP(512)"));
        assert!(WORK.contains("EXEC dbo.culprit_serial_tvp @Rows=@input"));
        assert!(WARM.starts_with("SET STATISTICS PROFILE ON;"));
        for sql in [CREATE_TYPE, CREATE_PROCEDURE, WARM, WORK] {
            assert!(!sql.contains("CREATE EVENT") && !sql.contains("TRACEON"));
        }
    }
}
