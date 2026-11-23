//! Existing native driver acceptance assertions shared by nonce-owned fixture entry points only.
use super::*;
use culprit_core::ErrorCode;

pub(crate) async fn timeout_confirmed(connector: &Connector) -> ReadWorkMetrics {
    let mut reader = ReadSession::new(
        connector.clone(),
        CollectionBudget { statement_timeout_ms: 500, total_timeout_ms: 10000, ..Default::default() },
    );
    let started = Instant::now();
    let error = reader.query("SELECT pg_catalog.pg_sleep(30)", 1).await.err().unwrap();
    assert!(error.code == ErrorCode::Timeout || CollectionStop::from_error(&error) == CollectionStop::TimedOut);
    assert!(started.elapsed() < Duration::from_secs(8));
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let metrics = reader.metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!(metrics.active_tasks, 0);
    assert_eq!(reader.queries(), 2);
    assert_eq!(metrics.templates.iter().map(|metric| metric.timeouts).sum::<u32>(), 1);
    metrics
}
pub(crate) async fn dropped_caller_confirmed(connector: &Connector) -> ReadWorkMetrics {
    let mut reader = ReadSession::new(
        connector.clone(),
        CollectionBudget { statement_timeout_ms: 10000, total_timeout_ms: 30000, ..Default::default() },
    );
    reader.query("SELECT 1", 1).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), reader.query("SELECT pg_catalog.pg_sleep(30)", 1))
            .await
            .is_err()
    );
    let dispatched = reader.queries();
    let error = reader.query("SELECT 42", 1).await.err().unwrap();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::CancellationUnconfirmed);
    assert_eq!(reader.queries(), dispatched, "an unsettled caller cannot start a replacement task");
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let metrics = reader.metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!(metrics.active_tasks, 0);
    assert_eq!(reader.queries(), 3);
    assert_eq!(metrics.templates.iter().map(|metric| metric.cancellations).sum::<u32>(), 1);
    assert!(!reader.fence.is_uncertain());
    metrics
}
