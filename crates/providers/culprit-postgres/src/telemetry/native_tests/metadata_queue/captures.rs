//! Heap ownership surrounds existing actual capture helpers; the same Outbox owns parent and follow-ups.
use super::super::{owned_fixture, retained_activity};
use super::*;
pub(super) async fn original(fixture: &mut MaintenanceFixture) -> Box<retained_activity::Original> {
    fixture.begin_relation_source_capture();
    let pid = fixture.relation_identity(RelationQueueRole::Affected).id.parse().unwrap();
    let result =
        Box::new(Box::pin(retained_activity::original(&fixture.specification, pid, &fixture.cancellation)).await);
    fixture.completed_relation_source_capture();
    result
}
pub(super) async fn blocking(
    fixture: &mut MaintenanceFixture,
    original: &mut retained_activity::Original,
    partial: bool,
) -> Box<owned_fixture::Captured> {
    fixture.begin_relation_source_capture();
    let result = if partial {
        let budget = CollectionBudget { max_snapshot_rows: 2, page_rows: 2, ..CollectionBudget::default() };
        Box::new(Box::pin(original.blocking_with_budget(&fixture.specification, &fixture.cancellation, budget)).await)
    } else {
        Box::new(Box::pin(original.blocking(&fixture.specification, &fixture.cancellation)).await)
    };
    fixture.completed_relation_source_capture();
    result
}
pub(super) fn report(capture: &owned_fixture::Captured) -> culprit_core::BlockingReport {
    let refs = capture.records.iter().enumerate().map(|(position, _)| capture.reference(position)).collect::<Vec<_>>();
    let rows = capture
        .records
        .iter()
        .zip(&refs)
        .map(|(row, reference)| match &row.record {
            TelemetryRecord::Activity(observation) => ActivityFrameRow { observation, reference: Some(reference) },
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    culprit_checks::focused_blocking::explain(capture.source(), &rows, true).unwrap()
}
pub(super) fn receipt(capture: &owned_fixture::Captured, elapsed: std::time::Duration) -> serde_json::Value {
    let metrics = capture.manifest.read_metrics.as_ref().unwrap();
    owned_fixture::assert_settled(metrics, capture.manifest.source_queries);
    assert!(capture.manifest.source_queries <= capture.manifest.request.budget.max_queries);
    assert!(capture.manifest.request.budget.max_queries <= 25);
    assert!(capture.manifest.request.budget.statement_timeout_ms <= 1_000);
    assert!(capture.manifest.request.budget.total_timeout_ms <= 5_000);
    let native_points = capture
        .records
        .iter()
        .filter_map(|record| match &record.record {
            TelemetryRecord::Activity(point) => Some(point),
            _ => None,
        })
        .flat_map(|point| point.wait.iter().flat_map(|wait| wait.resources.iter().flatten()))
        .filter_map(|resource| resource.native.as_deref())
        .collect::<Vec<_>>();
    let bytes = capture.manifest.files.iter().map(|file| u64::from(file.bytes)).sum::<u64>();
    serde_json::json!({"capture":capture.manifest.capture,"manifest_sha256":capture.manifest_sha256,
        "source":capture.source(),"source_queries_including_setup":capture.manifest.source_queries,
        "source_query_cap":capture.manifest.request.budget.max_queries,"source_byte_cap":capture.manifest.request.budget.total_bytes,
        "retained_rows":capture.records.len(),"canonical_file_bytes":bytes,"elapsed_ms":elapsed.as_millis().to_string(),
        "native_lock_points":native_points,"native_work":metrics,"native_cpu_measured":false,"cost_qualified":false})
}
