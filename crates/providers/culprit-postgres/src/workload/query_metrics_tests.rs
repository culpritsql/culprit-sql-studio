use super::*;
use crate::rows::Row;

fn native_row(calls: Option<&str>, total: Option<&str>) -> Row {
    let mut fields = vec![("calls".into(), calls.map(str::to_owned)), ("total_ms".into(), total.map(str::to_owned))];
    fields.extend(
        [
            ("id", "17/41/9/true/2026-01-01T10:00:00Z/2026-01-01T00:00:00Z"),
            ("queryid", "17"),
            ("userid", "41"),
            ("dbid", "9"),
            ("toplevel", "true"),
            ("stats_since", "2026-01-01T10:00:00Z"),
            ("postmaster_started_at", "2026-01-01T00:00:00Z"),
            ("query", "SELECT native_value FROM app_table WHERE id = $1"),
            ("mean_ms", "5"),
            ("min_ms", "1"),
            ("max_ms", "9"),
            ("stddev_ms", "2"),
            ("rows", "12"),
            ("reads", "16"),
            ("phys", "3"),
            ("io_ms", "4"),
            ("temp_blks_read", "2"),
            ("temp_blks_written", "0"),
            ("temp_block_bytes", "8192"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), Some(value.into()))),
    );
    Row::from_fields(fields)
}

#[test]
fn scan_uses_the_shared_metric_owner_without_losing_its_native_resource_fields() {
    let row = native_row(Some("4"), Some("20"));
    let query = query_stat(&row).unwrap();
    assert_eq!(query.id, "17/41/9/true/2026-01-01T10:00:00Z/2026-01-01T00:00:00Z");
    assert_eq!(query.text, "SELECT native_value FROM app_table WHERE id = $1");
    assert_eq!(query.executions, 4.0);
    assert_eq!(query.total_time_ms, 20.0);
    assert_eq!(query.mean_time_ms, 5.0);
    assert_eq!(query.max_time_ms, Some(9.0));
    assert_eq!(query.stddev_time_ms, Some(2.0));
    assert_eq!(query.spread.unwrap().min_ms, Some(1.0));
    assert_eq!(query.rows, Some(12.0));
    assert_eq!(query.logical_reads, Some(16.0));
    assert_eq!(query.physical_reads, Some(3.0));
    assert_eq!(query.waits.len(), 1);
    assert_eq!(query.waits[0].wait_ms, 4.0);
    assert!(matches!(query.temporary_work, Some(culprit_core::StatementTemporaryWork::BlockIo {
        bytes_per_block: Some(8192), read_blocks: Some(ref read), written_blocks: Some(ref written), ..
    }) if read == "2" && written == "0"));
    assert_eq!(query.native_identity.as_ref().unwrap().query_id, "17");
    assert!(query.native_plan_identity.is_none());
}

#[test]
fn scan_refuses_missing_or_inexact_required_totals_instead_of_defaulting_them_to_zero() {
    for row in
        [native_row(None, Some("20")), native_row(Some("4"), None), native_row(Some("9007199254740993"), Some("20"))]
    {
        assert!(query_stat(&row).is_err());
    }
}
