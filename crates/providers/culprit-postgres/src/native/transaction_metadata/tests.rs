use super::*;
use crate::progress::test_support::{self as fixture, Reader};
fn coverage(standby: bool) -> Row {
    let value = serde_json::to_string(&cleanup::coverage(standby)).unwrap();
    fixture::row(&[("native_source", Some(RetentionCoverage::SOURCE)), ("native_values", Some(&value))])
}
fn slot(name: &str) -> Row {
    let values=serde_json::json!({"native_id":name,"name":name,"observed_at":"2026-11-15T10:00:00.000003Z",
        "native_scope":"server","database":"foreign","database_oid":"99","active":false,"active_pid":null,"backend_start":null,
        "data_xmin":"700","data_xmin_age":"42","catalog_xmin":"699","catalog_xmin_age":"43","slot_type":"logical"}).to_string();
    fixture::row(&[("native_source", Some("pg_replication_slots")), ("native_values", Some(&values))])
}
#[tokio::test]
async fn same_cleanup_decoder_keeps_exact_slot_horizons_shared_scope_and_standby_boundary_unknown() {
    for standby in [false, true] {
        let mut io =
            Reader { replies: [Ok(vec![coverage(standby), slot("observed-slot")])].into(), ..Default::default() };
        let read = read_retainers(&mut io, 180_000, standby, 32).await.unwrap();
        assert_eq!(io.calls.len(), 1);
        assert_eq!(read.native_rows, 1);
        assert!(!read.limited);
        let holder = &read.snapshot.holders[0];
        assert_eq!(holder.source, "pg_replication_slots");
        assert_eq!(holder.scope, culprit_core::MeasurementScope::Server);
        assert_eq!(holder.horizons[0].native_value, "700");
        assert_eq!(holder.horizons[1].native_value, "699");
        assert_eq!(read.snapshot.is_horizon_complete(CleanupHorizon::Data), !standby);
        assert!(io.calls[0].contains("LIMIT 34"));
    }
}
#[tokio::test]
async fn retainer_lookahead_and_unknown_coverage_cannot_turn_a_prefix_into_a_complete_horizon() {
    let mut io = Reader { replies: [Ok(vec![coverage(false), slot("one"), slot("two")])].into(), ..Default::default() };
    let read = read_retainers(&mut io, 180_000, false, 1).await.unwrap();
    assert!(read.limited);
    assert_eq!(read.native_rows, 2);
    assert_eq!(read.snapshot.holders.len(), 1);
    assert!(read.snapshot.sources.iter().all(|source| source.state == culprit_core::SourceState::Partial));
    assert!(!read.snapshot.is_horizon_complete(CleanupHorizon::Data));
    for rows in
        [vec![slot("one")], vec![coverage(false), coverage(false)], vec![coverage(false), slot("one"), slot("one")]]
    {
        let mut io = Reader { replies: [Ok(rows)].into(), ..Default::default() };
        assert!(read_retainers(&mut io, 180_000, false, 32).await.is_err());
        assert_eq!(io.calls.len(), 1);
    }
}
