//! Fixture-only invocation of the production owned reader/cancellation implementation.
use super::*;
const SLOW_CATALOG: &str =
    "SELECT 1 FROM information_schema.COLLATIONS WHERE SLEEP(IF(COLLATION_NAME='',29,30))=0 LIMIT 1";

pub(crate) async fn timeout(connector: Arc<Connector>) -> (u32, ReadWorkMetrics) {
    let mut reader = ReadSession::new(
        connector.clone(),
        CollectionBudget { statement_timeout_ms: 200, ..CollectionBudget::default() },
        Scope::Server,
    );
    let result = reader.read(SLOW_CATALOG, vec![], 1).await;
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    assert_eq!(connector.source_shutdown(), SourceShutdown::Quiescent);
    assert_eq!(result.err().expect("real native catalog read must time out").code, culprit_core::ErrorCode::Timeout);
    let metrics = reader.read_metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    (reader.query_count(), metrics)
}
#[cfg(feature = "native-test-support")]
pub(crate) async fn verify(connector: Arc<Connector>, owner: &str) -> serde_json::Value {
    let (timeout_queries, timeout_metrics) = timeout(connector.clone()).await;
    let mut reader = ReadSession::new(
        connector.clone(),
        CollectionBudget { statement_timeout_ms: 5_000, total_timeout_ms: 10_000, ..CollectionBudget::default() },
        Scope::Server,
    );
    reader.read("SELECT 1 AS answer", vec![], 1).await.unwrap();
    let native_id = reader.peer.as_ref().unwrap().client.id();
    let mut control =
        tokio::time::timeout(Duration::from_secs(3), connector.connect_for_cancellation()).await.unwrap().unwrap();
    let selector = format!("native-cancel-proof:{owner}");
    reader.begin_stream(&format!("/* {selector} */ {SLOW_CATALOG}"), vec![], 1).await.unwrap();
    let mut observed = false;
    let mut oracle_queries = 0;
    for _ in 0..60 {
        oracle_queries += 1;
        let sql = crate::rows::tagged(&format!(
            "SELECT LEFT(INFO,512) FROM information_schema.PROCESSLIST WHERE ID={native_id}"
        ));
        let row: Option<(Option<String>,)> =
            tokio::time::timeout(Duration::from_secs(1), control.query_first(sql)).await.unwrap().unwrap();
        if row.and_then(|(sql,)| sql).is_some_and(|sql| sql.contains(&selector)) {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let shutdown = reader.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(3), control.disconnect()).await;
    assert!(observed, "independent bounded oracle must see this exact original before cancellation");
    assert_eq!(shutdown, SourceShutdown::Quiescent);
    assert_eq!(connector.source_shutdown(), SourceShutdown::Quiescent);
    let metrics = reader.read_metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    assert!(metrics.templates.iter().any(|row| row.cancellations > 0));
    assert!(reader.peer.is_none(), "interrupted client cannot be returned for another statement");
    serde_json::json!({"timeout_queries":timeout_queries,"timeout_metrics":timeout_metrics,
        "caller_retirement_queries":reader.query_count(),"caller_retirement_metrics":metrics,
        "exact_original_observed":observed,"oracle_queries":oracle_queries,"oracle_query_cap":60})
}
