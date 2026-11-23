use super::*;
use culprit_core::telemetry::{
    MeasurementValue, RuntimeMetricKey, RuntimeMetricUnit, StatementCounter, StatementQueryMetrics,
};

const SINCE: &str = "2026-01-01T10:00:00Z";
const STARTED: &str = "2026-01-01T00:00:00Z";

fn row(user: &str, since: Option<&str>, started: &str, wal: &str) -> Row {
    let mut fields = [
        ("queryid", "17"),
        ("userid", user),
        ("dbid", "9"),
        ("toplevel", "true"),
        ("postmaster_started_at", started),
        ("wal_bytes", wal),
        ("wal_fpi", "2"),
        ("plans", "1"),
        ("plan_ms", "3"),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), Some(value.into())))
    .collect::<Vec<_>>();
    fields.push(("stats_since".into(), since.map(str::to_owned)));
    Row::from_fields(fields)
}
fn query(row: &Row) -> QueryStat {
    let counters = [
        StatementCounter {
            key: RuntimeMetricKey::Executions,
            unit: RuntimeMetricUnit::Count,
            total: MeasurementValue::Integer("4".into()),
        },
        StatementCounter {
            key: RuntimeMetricKey::Duration,
            unit: RuntimeMetricUnit::Milliseconds,
            total: MeasurementValue::Real(20.0),
        },
    ];
    let identity = crate::native::statements::scan_identity(row).unwrap();
    let mut query = StatementQueryMetrics::from_counters(&counters, None)
        .unwrap()
        .unwrap()
        .into_query(identity.native().key().unwrap(), "SELECT original_entry FROM native_table".into());
    query.native_identity = Some(identity);
    query
}
fn total(from: Option<&str>, until: Option<&str>, unknown: &str) -> Row {
    Row::from_fields([
        ("reset_from".into(), from.map(str::to_owned)),
        ("reset_until".into(), until.map(str::to_owned)),
        ("unknown_reset".into(), Some(unknown.into())),
        ("wal".into(), Some("100".into())),
    ])
}

#[test]
fn cost_enrichment_matches_the_full_user_reset_and_process_identity_instead_of_queryid() {
    let own = row("41", Some(SINCE), STARTED, "10");
    let other = row("42", Some(SINCE), STARTED, "90");
    let mut queries = [query(&own), query(&other)];
    attach_cost_rows(&mut queries, &[other, own], None).unwrap();
    assert_eq!(queries[0].costs.as_ref().unwrap().log_bytes, Some(10.0));
    assert_eq!(queries[1].costs.as_ref().unwrap().log_bytes, Some(90.0));
    assert!(queries.iter().all(|query| query.costs.as_ref().unwrap().log_share.is_none()));
}

#[test]
fn ended_recreated_and_foreign_process_cost_rows_cannot_repair_an_old_selected_entry() {
    let original = row("41", Some(SINCE), STARTED, "10");
    for changed in [
        row("41", Some("2026-01-01T11:00:00Z"), STARTED, "90"),
        row("41", Some(SINCE), "2026-01-01T01:00:00Z", "90"),
        row("42", Some(SINCE), STARTED, "90"),
    ] {
        let mut queries = [query(&original)];
        attach_cost_rows(&mut queries, &[changed], None).unwrap();
        assert!(queries[0].costs.is_none());
    }
    let mut ended = [query(&original)];
    attach_cost_rows(&mut ended, &[], None).unwrap();
    assert!(ended[0].costs.is_none());
}

#[test]
fn duplicate_native_cost_entries_refuse_before_any_query_is_enriched() {
    let native = row("41", Some(SINCE), STARTED, "10");
    let mut queries = [query(&native)];
    assert!(attach_cost_rows(&mut queries, &[native.clone(), native], None).is_err());
    assert!(queries[0].costs.is_none());
}

#[test]
fn database_wal_share_requires_every_entry_to_have_the_same_known_reset() {
    for (basis, expected) in [
        (Some(total(Some(SINCE), Some(SINCE), "0")), Some(0.10)),
        (Some(total(Some(SINCE), Some("2026-01-01T11:00:00Z"), "0")), None),
        (Some(total(Some(SINCE), Some(SINCE), "1")), None),
        (Some(total(None, None, "0")), None),
        (None, None),
    ] {
        let native = row("41", Some(SINCE), STARTED, "10");
        let mut queries = [query(&native)];
        attach_cost_rows(&mut queries, &[native], basis.as_ref()).unwrap();
        let costs = queries[0].costs.as_ref().unwrap();
        assert_eq!(costs.log_bytes, Some(10.0));
        assert_eq!(costs.log_share, expected);
    }
    let unknown = row("41", None, STARTED, "10");
    let mut queries = [query(&unknown)];
    attach_cost_rows(&mut queries, &[unknown], Some(&total(Some(SINCE), Some(SINCE), "0"))).unwrap();
    assert_eq!(queries[0].costs.as_ref().unwrap().log_share, None);
}

#[test]
fn missing_native_identity_does_not_fall_back_to_a_matching_display_id_or_sql_text() {
    let native = row("41", Some(SINCE), STARTED, "10");
    let mut original = query(&native);
    original.native_identity = None;
    let mut queries = [original];
    attach_cost_rows(&mut queries, &[native], None).unwrap();
    assert!(queries[0].costs.is_none());
}
