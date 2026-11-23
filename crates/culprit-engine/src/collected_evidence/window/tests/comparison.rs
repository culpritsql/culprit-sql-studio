//! Every comparison starts from the existing canonical proof/EOF owner, never a fabricated Workload or capture.
use super::*;

fn scale_duration(rows: &mut [TelemetryRecord], factor: f64) {
    for row in rows {
        if let TelemetryRecord::QueryRuntime(runtime) = row {
            let duration = runtime.metrics.iter_mut().find(|metric| metric.key == RuntimeMetricKey::Duration).unwrap();
            duration.average = duration.average.map(|value| value * factor);
            for endpoint in [&mut duration.minimum, &mut duration.maximum, &mut duration.last].into_iter().flatten() {
                let MeasurementValue::Real(value) = endpoint else {
                    panic!("this controlled duration corpus uses native float endpoints")
                };
                *value *= factor;
            }
            duration.standard_deviation = duration.standard_deviation.map(|value| value * factor);
        }
    }
}
fn comparison(result: &CollectedQueryWindowAnalysis) -> &CollectedWindowComparison {
    assert_eq!(result.sections.len(), 1);
    &result.sections[0].comparison
}

#[tokio::test]
async fn compatible_periods_keep_native_weighted_totals_and_feed_existing_regression_rules_once() {
    let before = standard(6, 1).await;
    let (source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    scale_duration(&mut rows, 2.0);
    let after = capture(source, rows, 2).await;
    let report = analyze_window(&[&before, &after], &window(6, 8)).unwrap();
    let result = comparison(&report);
    assert_eq!(result.baseline_inputs[0].capture, *before.capture());
    assert_eq!(result.recent_inputs[0].capture, *after.capture());
    let changes = result.changes.as_ref().unwrap();
    assert_eq!(changes.source, TimelineSource::NativeIntervals);
    assert_eq!(changes.baseline_total.executions, 50.0);
    assert_eq!(changes.recent_total.executions, 50.0);
    assert_eq!(changes.baseline_total.total_ms, 9500.0);
    assert_eq!(changes.recent_total.total_ms, 19000.0);
    assert_eq!(changes.baseline_total.mean_ms, 190.0);
    assert_eq!(changes.recent_total.mean_ms, 380.0);
    assert_eq!(report.sections[0].analysis.workload.as_ref().unwrap().total_time_ms, 28500.0);
    let [query] = changes.queries.as_slice() else { panic!() };
    assert_eq!(query.status, culprit_core::ChangeStatus::Regressed);
    assert_eq!(query.ratio, Some(2.0));
    assert!(
        query.plan_switch.is_none(),
        "two independently observed plans do not establish a plan switch or harmful reuse"
    );
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedComparisonGap::PlanMixUnestablished { .. })));
    assert_eq!(report.sections[0].inputs.len(), 2);
    assert_eq!(report.sections[0].analysis.runtime_totals.as_ref().unwrap().executions, "100");
}

#[tokio::test]
async fn higher_execution_volume_is_not_a_per_execution_regression_or_a_running_counter_delta() {
    let before = standard(6, 1).await;
    let (source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    for row in &mut rows {
        if let TelemetryRecord::QueryRuntime(runtime) = row {
            runtime.executions = (runtime.executions.parse::<u64>().unwrap() * 2).to_string();
        }
    }
    let after = capture(source, rows, 2).await;
    let report = analyze_window(&[&before, &after], &window(6, 8)).unwrap();
    let changes = comparison(&report).changes.as_ref().unwrap();
    assert_eq!(changes.baseline_total.executions, 50.0);
    assert_eq!(changes.recent_total.executions, 100.0);
    assert_eq!(changes.baseline_total.total_ms, 9500.0);
    assert_eq!(changes.recent_total.total_ms, 19000.0);
    assert_eq!(changes.baseline_total.mean_ms, changes.recent_total.mean_ms);
    assert!(changes.queries.iter().all(|query| query.status == culprit_core::ChangeStatus::Unchanged));
    assert_eq!(changes.unchanged, 1);
    assert_eq!(report.sections[0].analysis.runtime_totals.as_ref().unwrap().executions, "150");
}

fn context_case(hour: u32) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, mut rows) = test_support::query_cohort_evidence(QueryCohortCase::OtherContext);
    let CollectionTarget::Database(database) = &mut source.target else { panic!() };
    database.incarnation = Some("controlled-native-database-epoch".into());
    move_interval(&mut source, &mut rows, hour, hour + 1);
    rebind(&source, &mut rows);
    (source, rows)
}
#[tokio::test]
async fn a_query_unobserved_in_one_period_is_not_declared_new_stopped_or_zero() {
    let (source, rows) = context_case(6);
    let queries = rows
        .iter()
        .filter_map(|row| match row {
            TelemetryRecord::QueryDefinition(query) => Some(query.id.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(queries.len(), 2);
    let missing = queries[1].clone();
    let before = capture(source, rows, 1).await;
    let (source, mut rows) = context_case(7);
    rows.retain(|row| {
        row.measurement_key()
            .and_then(|key| key.references.query.as_ref())
            .is_none_or(|query| query.definition_id.as_deref() != Some(missing.as_str()))
    });
    let after = capture(source, rows, 2).await;
    assert!(before.is_read_complete() && after.is_read_complete());
    let report = analyze_window(&[&before, &after], &window(6, 8)).unwrap();
    let result = comparison(&report);
    let changes = result.changes.as_ref().unwrap();
    assert!(changes.queries.iter().any(|query| query.status == culprit_core::ChangeStatus::NotComparable));
    assert!(
        changes.queries.iter().all(|query| !matches!(
            query.status,
            culprit_core::ChangeStatus::New | culprit_core::ChangeStatus::Stopped
        ))
    );
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedComparisonGap::QueryMissingFromPeriod { .. })));
}

#[tokio::test]
async fn requested_period_edges_follow_whole_native_boundaries_without_prorating_or_overlapping() {
    let first = standard(6, 1).await;
    let (mut source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    move_interval(&mut source, &mut rows, 7, 11);
    let middle = capture(source, rows, 2).await;
    let last = standard(11, 3).await;
    let report = analyze_window(&[&last, &first, &middle], &window(6, 12)).unwrap();
    let result = comparison(&report);
    let changes = result.changes.as_ref().unwrap();
    assert_eq!(changes.baseline.start, canonical_time(&window(6, 7).from).unwrap());
    assert_eq!(changes.baseline.end, canonical_time(&window(6, 7).until).unwrap());
    assert_eq!(changes.recent.start, changes.baseline.end);
    assert_eq!(changes.recent.end, canonical_time(&window(7, 12).until).unwrap());
    assert_eq!(result.baseline_inputs.len(), 1);
    assert_eq!(result.recent_inputs.len(), 2);
    assert_eq!(changes.baseline_total.executions, 50.0);
    assert_eq!(changes.recent_total.executions, 100.0);
    assert_eq!(
        result.gaps.iter().filter(|gap| matches!(gap, CollectedComparisonGap::NativeBoundarySelected { .. })).count(),
        2
    );
    assert_eq!(report.sections[0].analysis.runtime_totals.as_ref().unwrap().executions, "150");
}

#[tokio::test]
async fn a_missing_mean_keeps_execution_receipts_but_withholds_the_period_comparison() {
    let before = standard(6, 1).await;
    let (source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    let runtime = rows
        .iter_mut()
        .find_map(|row| match row {
            TelemetryRecord::QueryRuntime(runtime) => Some(runtime),
            _ => None,
        })
        .unwrap();
    runtime.metrics.iter_mut().find(|metric| metric.key == RuntimeMetricKey::Duration).unwrap().average = None;
    let after = capture(source, rows, 2).await;
    let report = analyze_window(&[&before, &after], &window(6, 8)).unwrap();
    let result = comparison(&report);
    assert!(result.changes.is_none());
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedComparisonGap::MissingRuntimeDenominator)));
    assert_eq!(result.baseline_inputs[0].capture, *before.capture());
    assert_eq!(result.recent_inputs[0].capture, *after.capture());
    assert_eq!(report.sections[0].analysis.runtime_totals.as_ref().unwrap().executions, "100");
    assert!(matches!(
        report.sections[0].analysis.runtime_totals.as_ref().unwrap().duration,
        CollectedRuntimeTimeTotal::Incomplete
    ));
}

#[tokio::test]
async fn native_generation_and_unknown_population_never_create_a_cross_section_change() {
    let first = standard(6, 1).await;
    let (source, rows) = case(7, Some("another-native-database-epoch"));
    let other = capture(source, rows, 2).await;
    let result = analyze_window(&[&first, &other], &window(6, 8)).unwrap();
    assert_eq!(result.sections.len(), 2);
    assert!(result.sections.iter().all(|section| section.comparison.changes.is_none()));
    assert!(result.sections.iter().all(|section| {
        section.comparison.gaps.iter().any(|gap| matches!(gap, CollectedComparisonGap::TooFewIntervals))
    }));
    let (mut source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    let SourceObservation::QueryHistory(history) = &mut source.observation else { panic!() };
    history.population_binding = QueryHistoryPopulationBinding::Unknown;
    history.population_catalog = None;
    rebind(&source, &mut rows);
    let unknown = capture(source, rows, 3).await;
    let result = analyze_window(&[&first, &unknown], &window(6, 8)).unwrap();
    let unknown =
        result.sections.iter().find(|section| section.population == CollectedQueryPopulation::Unknown).unwrap();
    assert!(unknown.comparison.changes.is_none());
    assert!(unknown.comparison.gaps.iter().any(|gap| matches!(gap, CollectedComparisonGap::UnknownPopulation)));
}
