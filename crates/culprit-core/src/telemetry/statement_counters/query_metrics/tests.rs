use super::*;
use crate::{MeasurementScope, ResetMarker};

fn counter(key: RuntimeMetricKey, unit: RuntimeMetricUnit, total: MeasurementValue) -> StatementCounter {
    StatementCounter { key, unit, total }
}
fn point() -> StatementCounterSample {
    StatementCounterSample {
        member: None,
        identity: NativeQueryIdentity {
            query_id: "17".into(),
            qualifiers: [("userid".into(), "41".into()), ("dbid".into(), "9".into())].into(),
        },
        scope: MeasurementScope::Database,
        observed_at: "2026-01-01T12:00:00Z".into(),
        reset: ResetMarker::At("2026-01-01T10:00:00Z".into()),
        totals: vec![
            counter(RuntimeMetricKey::Executions, RuntimeMetricUnit::Count, MeasurementValue::Integer("4".into())),
            counter(RuntimeMetricKey::Duration, RuntimeMetricUnit::Milliseconds, MeasurementValue::Real(20.0)),
        ],
        distributions: vec![],
    }
}
fn metrics(point: &StatementCounterSample) -> StatementQueryMetrics {
    StatementQueryMetrics::from_sample(point).unwrap().unwrap()
}
fn distribution(point: &StatementCounterSample) -> StatementDistribution {
    StatementDistribution {
        statistics: RuntimeMetric {
            key: RuntimeMetricKey::Duration,
            unit: RuntimeMetricUnit::Microseconds,
            average: Some(5_000.0),
            minimum: Some(MeasurementValue::Real(1_000.0)),
            maximum: Some(MeasurementValue::Real(10_000.0)),
            last: None,
            standard_deviation: Some(2_000.0),
        },
        reset: point.reset.clone(),
        extrema_reset: point.reset.clone(),
    }
}

#[test]
fn declared_native_time_units_share_one_checked_conversion() {
    for (unit, value) in [
        (RuntimeMetricUnit::Picoseconds, 20_000_000_000.0),
        (RuntimeMetricUnit::Nanoseconds, 20_000_000.0),
        (RuntimeMetricUnit::Microseconds, 20_000.0),
        (RuntimeMetricUnit::Milliseconds, 20.0),
        (RuntimeMetricUnit::Seconds, 0.020),
    ] {
        let mut sample = point();
        sample.totals[1].unit = unit;
        sample.totals[1].total = MeasurementValue::Real(value);
        let projected = metrics(&sample);
        assert_eq!(projected.total_time_ms, 20.0);
        assert_eq!(projected.mean_time_ms, 5.0);
    }
}

#[test]
fn missing_required_metrics_are_unavailable_instead_of_measured_zero() {
    for missing in 0..2 {
        let mut sample = point();
        sample.totals.remove(missing);
        assert!(sample.is_valid());
        assert_eq!(StatementQueryMetrics::from_sample(&sample).unwrap(), None);
    }
}

#[test]
fn exact_required_count_ceiling_does_not_round_large_native_integers() {
    let mut sample = point();
    sample.totals[0].total = MeasurementValue::Integer("9007199254740992".into());
    assert_eq!(metrics(&sample).executions, 9_007_199_254_740_992.0);
    sample.totals[0].total = MeasurementValue::Integer("9007199254740993".into());
    let original = sample.clone();
    assert!(sample.is_valid());
    assert_eq!(StatementQueryMetrics::from_sample(&sample).unwrap(), None);
    assert_eq!(sample, original);
}

#[test]
fn optional_unrepresentable_counts_stay_unknown_while_exact_totals_survive() {
    let mut sample = point();
    sample.totals.extend([
        counter(RuntimeMetricKey::Rows, RuntimeMetricUnit::Count, MeasurementValue::Integer("9007199254740993".into())),
        counter(RuntimeMetricKey::CpuTime, RuntimeMetricUnit::Seconds, MeasurementValue::Real(0.008)),
        counter(
            RuntimeMetricKey::LogicalReads,
            RuntimeMetricUnit::Pages { bytes_per_page: 8192 },
            MeasurementValue::Integer("16".into()),
        ),
    ]);
    let projected = metrics(&sample);
    assert_eq!(projected.rows, None);
    assert_eq!(projected.cpu_time_ms, Some(8.0));
    assert_eq!(projected.logical_reads, Some(16.0));
    assert_eq!(projected.physical_reads, None);
}

#[test]
fn same_known_reset_preserves_native_distribution_with_its_own_units() {
    let mut sample = point();
    sample.distributions.push(distribution(&sample));
    let projected = metrics(&sample);
    assert_eq!(projected.mean_time_ms, 5.0);
    assert_eq!(projected.min_time_ms, Some(1.0));
    assert_eq!(projected.max_time_ms, Some(10.0));
    assert_eq!(projected.stddev_time_ms, Some(2.0));
}

#[test]
fn independent_extrema_reset_does_not_relabel_partial_extrema_as_the_total_lifetime() {
    let mut sample = point();
    let mut spread = distribution(&sample);
    spread.extrema_reset = ResetMarker::At("2026-01-01T11:00:00Z".into());
    sample.distributions.push(spread);
    let original = sample.clone();
    let projected = metrics(&sample);
    assert_eq!(projected.min_time_ms, None);
    assert_eq!(projected.max_time_ms, None);
    assert_eq!(projected.stddev_time_ms, Some(2.0));
    assert_eq!(sample, original);
}

#[test]
fn unknown_or_foreign_distribution_reset_does_not_gain_continuity_from_equal_labels() {
    for reset in [ResetMarker::Unknown, ResetMarker::Generation("another-entry".into())] {
        let mut sample = point();
        let mut spread = distribution(&sample);
        spread.reset = reset.clone();
        spread.extrema_reset = reset;
        spread.statistics.average = Some(99_000.0);
        sample.distributions.push(spread);
        let projected = metrics(&sample);
        assert_eq!(projected.mean_time_ms, 5.0);
        assert_eq!(projected.stddev_time_ms, None);
        assert_eq!(projected.min_time_ms, None);
        assert_eq!(projected.max_time_ms, None);
    }
    let mut unknown = point();
    unknown.reset = ResetMarker::Unknown;
    unknown.distributions.push(distribution(&unknown));
    assert_eq!(metrics(&unknown).stddev_time_ms, None);
}

#[test]
fn byte_read_totals_and_native_metric_names_do_not_become_page_counts() {
    let mut sample = point();
    sample.totals.extend([
        counter(RuntimeMetricKey::LogicalReads, RuntimeMetricUnit::Bytes, MeasurementValue::Integer("8192".into())),
        counter(
            RuntimeMetricKey::Native("postgres.shared-block-reads".into()),
            RuntimeMetricUnit::Count,
            MeasurementValue::Integer("99".into()),
        ),
    ]);
    let projected = metrics(&sample);
    assert_eq!(projected.logical_reads, None);
    assert_eq!(projected.physical_reads, None);
}

#[test]
fn genuine_zero_counts_remain_zero_but_nonzero_duration_without_runs_has_no_average() {
    let mut sample = point();
    sample.totals[0].total = MeasurementValue::Integer("0".into());
    assert_eq!(StatementQueryMetrics::from_sample(&sample).unwrap(), None);
    sample.totals[1].total = MeasurementValue::Real(0.0);
    let projected = metrics(&sample);
    assert_eq!(projected.executions, 0.0);
    assert_eq!(projected.mean_time_ms, 0.0);
}

#[test]
fn invalid_duplicates_wrong_units_and_time_overflow_cannot_create_a_query() {
    let mut duplicate = point();
    duplicate.totals.push(duplicate.totals[0].clone());
    assert!(StatementQueryMetrics::from_sample(&duplicate).is_err());
    let mut wrong_unit = point();
    wrong_unit.totals[1].unit = RuntimeMetricUnit::Count;
    assert!(StatementQueryMetrics::from_sample(&wrong_unit).is_err());
    let mut overflow = point();
    overflow.totals[1].unit = RuntimeMetricUnit::Seconds;
    overflow.totals[1].total = MeasurementValue::Real(f64::MAX);
    assert_eq!(StatementQueryMetrics::from_sample(&overflow).unwrap(), None);
    assert_eq!(RuntimeMetricUnit::Milliseconds.milliseconds(f64::NAN), None);
    assert_eq!(RuntimeMetricUnit::Milliseconds.milliseconds(-1.0), None);
}

#[test]
fn query_assembly_preserves_the_callers_text_without_inventing_execution_history_or_native_authority() {
    let sample = point();
    let query =
        metrics(&sample).into_query("selected-original-id".into(), "SELECT private_value FROM original_table".into());
    assert_eq!(query.executions, 4.0);
    assert_eq!(query.mean_time_ms, 5.0);
    assert_eq!(query.text, "SELECT private_value FROM original_table");
    assert_eq!(query.native_identity, None);
    assert_eq!(query.first_seen, None);
    assert_eq!(query.last_seen, None);
    assert!(query.plan.is_none() && query.plans.is_empty() && query.waits.is_empty() && query.trend.is_none());
}
