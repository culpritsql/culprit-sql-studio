//! Cancellation is observed from another session on an owned loopback SQL Server, never a production endpoint.
use super::*;
use crate::{conn::SessionKind, test_support::config};
use culprit_core::telemetry::SourceShutdown;
mod protocol;

pub(super) struct ObservedSession {
    id: i64,
    generation: String,
}
struct RequestState {
    active: bool,
    cpu_ms: i32,
    wait_type: Option<String>,
}

pub(super) async fn fixture() -> (ReadSession, MsClient, ObservedSession) {
    let port = std::env::var("CULPRIT_MSSQL_SOURCE_TEST_PORT").unwrap().parse::<u16>().unwrap();
    let password = std::env::var("CULPRIT_MSSQL_SOURCE_TEST_PASSWORD").unwrap();
    let pool = Arc::new(Pool::new(config(port, "master", &password, "culprit"), None));
    let observer = pool.connect(SessionKind::User).await.unwrap();
    let mut reader = ReadSession::new(pool, CollectionBudget::default());
    assert_eq!(NativeRead::connection_generation(&reader), None, "no original connection has yet been acquired");
    let identity = identity(&mut reader).await;
    assert!(NativeRead::connection_generation(&reader).is_some(), "a successful first read retains its native client");
    reader.budget.statement_timeout_ms = 1500;
    (reader, observer, identity)
}

async fn identity(reader: &mut ReadSession) -> ObservedSession {
    let rows = reader.query("SELECT CONVERT(bigint,@@SPID) AS id,CONVERT(varchar(33),CONVERT(datetime2(7),login_time),126) AS generation FROM sys.dm_exec_sessions WHERE session_id=@@SPID",vec![],1).await.unwrap();
    ObservedSession { id: integer(&rows[0], "id").unwrap(), generation: rows[0].string("generation") }
}

async fn state(observer: &mut MsClient, session: &ObservedSession) -> Option<RequestState> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut query = Query::new("SELECT CAST(CASE WHEN r.session_id IS NULL THEN 0 ELSE 1 END AS bit) AS active,COALESCE(r.cpu_time,0) AS cpu_ms,r.wait_type FROM sys.dm_exec_sessions s LEFT JOIN sys.dm_exec_requests r ON r.session_id=s.session_id WHERE s.session_id=@P1 AND CONVERT(datetime2(7),s.login_time)=CONVERT(datetime2(7),@P2,126)");
        query.bind(session.id);
        query.bind(session.generation.as_str());
        query.query(observer).await.unwrap().into_row().await.unwrap().map(|row| RequestState {
            active: row.get::<bool,_>("active").unwrap(),
            cpu_ms: row.get::<i32,_>("cpu_ms").unwrap(),
            wait_type: row.get::<&str,_>("wait_type").map(str::to_owned),
        })
    }).await.expect("the independent native observer must remain responsive")
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn a_schema_lock_bounds_metadata_waiting_and_does_not_leave_a_request_running() {
    let (mut reader, mut observer, session) = fixture().await;
    reader.budget.statement_timeout_ms = 5000;
    let table = format!("culprit_safety_{}_{}", std::process::id(), chrono::Utc::now().timestamp_millis());
    crate::test_support::batch(
        &mut observer,
        &format!("CREATE TABLE dbo.[{table}](id int); BEGIN TRAN; ALTER TABLE dbo.[{table}] ADD note int"),
    )
    .await;
    let mut monitor = reader.pool.connect(SessionKind::User).await.unwrap();
    let started = Instant::now();
    let sql = format!("SELECT OBJECT_ID(N'dbo.{table}') AS id");
    {
        let work = reader.query(&sql, vec![], 1);
        tokio::pin!(work);
        loop {
            tokio::select! {
                result=&mut work=>panic!("metadata fixture ended before a native schema wait was observed: {result:?}"),
                observed=state(&mut monitor,&session)=>{
                    if observed.and_then(|state|state.wait_type).as_deref()==Some("LCK_M_SCH_S") {break;}
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(work.await.is_err(), "the statistics session must not wait indefinitely on schema modification");
    }
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "LOCK_TIMEOUT must end the wait before the client statement deadline"
    );
    let shutdown = reader.close().await;
    assert_quiet(&mut monitor, &session).await;
    if shutdown == SourceShutdown::Unconfirmed {
        assert!(matches!(reader.admit(), Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))));
    }
    crate::test_support::batch(&mut observer, "ROLLBACK").await;
    crate::test_support::batch(&mut observer, &format!("DROP TABLE dbo.[{table}]")).await;
}

pub(super) const MAX_QUIET_OBSERVER_QUERIES: u32 = 152;
pub(super) async fn assert_quiet(observer: &mut MsClient, session: &ObservedSession) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(3);
    for count in 1..=MAX_QUIET_OBSERVER_QUERIES {
        if !state(observer, session).await.is_some_and(|state| state.active) {
            return count;
        }
        assert!(Instant::now() < deadline, "the original server request survived cancellation");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the original request exceeded its finite native quiescence observation cap")
}

pub(super) const CPU_CANCELLATION_SQL: &str = "SELECT SUM(CONVERT(bigint,CHECKSUM(NEWID()))) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c OPTION(MAXDOP 1)";
pub(super) const MAX_CPU_OBSERVER_QUERIES: u32 = 64;

/// One bounded readiness observer around the same original CPU request; no replacement SQL is submitted.
pub(super) async fn observe_cpu_start<F: std::future::Future<Output = ReadResult<Vec<Rec>>>>(
    mut work: std::pin::Pin<&mut F>,
    observer: &mut MsClient,
    session: &ObservedSession,
) -> u32 {
    for count in 1..=MAX_CPU_OBSERVER_QUERIES {
        tokio::select! {
            biased;
            result = &mut work => panic!("CPU fixture ended before independent observation: {result:?}"),
            observed = state(observer, session) => {
                if observed.is_some_and(|state| state.active && state.cpu_ms > 0) { return count; }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the bounded original CPU request did not establish independent native readiness")
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn fully_received_server_errors_preserve_source_failure_without_claiming_unknown_work() {
    for sql in [
        "SELECT * FROM dbo.culprit_missing_safety_fixture",
        "SELECT 1 AS n; RAISERROR('owned completed error fixture',16,1)",
    ] {
        let (mut reader, mut observer, session) = fixture().await;
        let result = reader.query(sql, vec![], 1).await;
        let shutdown = reader.close().await;
        assert_quiet(&mut observer, &session).await;
        assert!(
            matches!(result, Err(ReadError::Source(_))),
            "a complete server response must preserve its typed source error: {result:?}"
        );
        assert_eq!(shutdown, SourceShutdown::Quiescent);
        assert!(reader.query("SELECT 1 AS n", vec![], 1).await.is_ok());
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn an_error_token_before_unfinished_work_is_not_a_quiescence_acknowledgement() {
    let (mut reader, mut observer, session) = fixture().await;
    let result = {
        let work =
            reader.query("RAISERROR('owned unfinished error fixture',16,1); WAITFOR DELAY '00:00:15'", vec![], 1);
        tokio::pin!(work);
        loop {
            tokio::select! {
                result=&mut work=>panic!("the early error must not conceal the unfinished server request: {result:?}"),
                observed=state(&mut observer,&session)=>{
                    if observed.is_some_and(|state|state.active && state.wait_type.as_deref()==Some("WAITFOR")) {break;}
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        work.await
    };
    let shutdown = reader.close().await;
    assert_quiet(&mut observer, &session).await;
    match result {
        Err(ReadError::Stopped(CollectionStop::TimedOut)) => assert_eq!(shutdown, SourceShutdown::Quiescent),
        Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)) => {
            assert_eq!(shutdown, SourceShutdown::Unconfirmed);
            assert!(matches!(reader.admit(), Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))));
        }
        result => panic!("an early SQL error does not prove later work has stopped: {result:?}"),
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn cpu_timeout_stops_the_original_request_on_the_server() {
    let (mut reader, mut observer, session) = fixture().await;
    let result = {
        let work = reader.query(CPU_CANCELLATION_SQL, vec![], 1);
        tokio::pin!(work);
        observe_cpu_start(work.as_mut(), &mut observer, &session).await;
        work.await
    };
    let shutdown = reader.close().await;
    assert_quiet(&mut observer, &session).await;
    eprintln!("CPU timeout observed server-side quiescence; driver result={result:?}, shutdown={shutdown:?}");
    match result {
        Err(ReadError::Stopped(CollectionStop::TimedOut)) => assert_eq!(shutdown, SourceShutdown::Quiescent),
        Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed)) => {
            // An observed quiet fixture does not repair a missing driver acknowledgement in production.
            assert_eq!(shutdown, SourceShutdown::Unconfirmed);
            let count = reader.queries();
            assert!(matches!(
                reader.query("SELECT 1", vec![], 1).await,
                Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))
            ));
            assert_eq!(reader.queries(), count);
        }
        result => panic!("the CPU deadline must be reported without accepting query output: {result:?}"),
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn backpressured_stream_cancellation_stops_native_work_and_preserves_uncertainty() {
    let (mut reader, mut observer, session) = fixture().await;
    reader.budget.page_rows = 1;
    reader.begin_stream("SELECT TOP(10000) REPLICATE(CONVERT(varchar(max),'x'),1024) AS payload FROM sys.all_objects a CROSS JOIN sys.all_objects b; WAITFOR DELAY '00:00:15'",vec![],10000).await.unwrap();
    let first = reader.next_batch().await.unwrap();
    assert!(!first.complete);
    assert!(state(&mut observer, &session).await.is_some_and(|state| state.active));
    // Keep the bounded output channel full while the native deadline expires.
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let shutdown = reader.close().await;
    assert_quiet(&mut observer, &session).await;
    if shutdown == SourceShutdown::Unconfirmed {
        let count = reader.queries();
        assert!(matches!(
            reader.query("SELECT 1", vec![], 1).await,
            Err(ReadError::Stopped(CollectionStop::CancellationUnconfirmed))
        ));
        assert_eq!(reader.queries(), count, "a closed socket alone cannot admit replacement SQL");
    }
}

#[tokio::test]
#[ignore = "requires the existing owned loopback SQL Server fixture"]
async fn current_owned_generation_refuses_the_retired_native_error_without_erasing_its_acquisition_counter() {
    let (mut reader, mut observer, session) = fixture().await;
    let original_generation = NativeRead::connection_generation(&reader).unwrap();
    let result =
        reader.query("SELECT /* culprit */ * FROM sys.culprit_missing_optional_epoch_fixture", vec![], 1).await;
    let after_error = NativeRead::connection_generation(&reader);
    let counter = reader.connection_generation;
    let queries = reader.queries();
    let shutdown = reader.close().await;
    assert_quiet(&mut observer, &session).await;
    assert!(matches!(result, Err(ReadError::Source(_))));
    assert_eq!(
        after_error, None,
        "the failed read retired its socket; the old counter cannot attest current ownership"
    );
    assert_eq!(
        counter, original_generation,
        "the private acquisition counter remains available for original work receipts"
    );
    assert_eq!(queries, 2, "one fixture identity +one native error; no replacement query");
    assert_eq!(NativeRead::connection_generation(&reader), None);
    assert_eq!(shutdown, SourceShutdown::Quiescent);
}

struct LostEpoch<'a> {
    reader: &'a mut ReadSession,
    calls: u32,
    original_error: Option<Error>,
}
#[async_trait::async_trait]
impl NativeRead for LostEpoch<'_> {
    async fn query(&mut self, sql: &str, parameters: Vec<Parameter>, limit: usize) -> ReadResult<Vec<Rec>> {
        self.calls += 1;
        assert!(self.calls <= 2, "no database projection or replacement is authorized after epoch failure");
        let result = if self.calls == 2 {
            self.reader.query("SELECT /* culprit */ * FROM sys.culprit_missing_optional_epoch_fixture", vec![], 1).await
        } else {
            self.reader.query(sql, parameters, limit).await
        };
        if let Err(error) = &result {
            self.original_error = Some(match error {
                ReadError::Source(error) => error.clone(),
                ReadError::Stopped(stop) => stop.error(),
            });
        }
        result
    }
    fn queries(&self) -> u32 {
        self.reader.queries()
    }
    fn connection_generation(&self) -> Option<u64> {
        NativeRead::connection_generation(&*self.reader)
    }
    async fn close(&mut self) -> SourceShutdown {
        self.reader.close().await
    }
}
#[tokio::test]
#[ignore = "requires the existing owned loopback SQL Server fixture"]
async fn actual_retired_epoch_error_stops_local_authority_before_any_database_projection_or_replacement() {
    let (mut reader, mut observer, session) = fixture().await;
    let mut read = LostEpoch { reader: &mut reader, calls: 0, original_error: None };
    let result = crate::local_source::Context::read(&mut read, "127.0.0.1", 1433).await;
    let held = read.connection_generation();
    let calls = read.calls;
    let original_error = read.original_error.take();
    let shutdown = read.close().await;
    assert_quiet(&mut observer, &session).await;
    let original_error = original_error.expect("the actual native epoch query must fail");
    let error = result.err().expect("a retired epoch query cannot establish authority");
    assert_eq!(error.code, original_error.code);
    assert_eq!(error.detail, original_error.detail);
    assert_eq!(error.message, original_error.message);
    assert_eq!(held, None);
    assert_eq!(calls, 2);
    assert_eq!(reader.queries(), 3, "one fixture identity plus the actual platform/failed epoch; no projection");
    assert_eq!(shutdown, SourceShutdown::Quiescent);
}

#[test]
fn an_unacquired_session_or_an_old_counter_without_a_client_has_no_current_owned_generation() {
    let pool = Arc::new(Pool::new(config(1, "master", "controlled-unused", "culprit"), None));
    let mut reader = ReadSession::new(pool, CollectionBudget::default());
    assert_eq!(NativeRead::connection_generation(&reader), None);
    // The counter is deliberately kept for original work receipts, independently of current ownership.
    reader.connection_generation = 7;
    assert_eq!(NativeRead::connection_generation(&reader), None);
}
