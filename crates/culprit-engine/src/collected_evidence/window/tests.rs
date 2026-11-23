//! Controlled canonical source/rows are sealed before the existing warehouse proof reader opens them.
//! These are source-free interpretation controls, not native cost/readiness qualification.
use super::*;
use culprit_evidence::{
    analytical::{
        evidence::*,
        indexes::ProjectionIndexes,
        test_support::{self, QueryCohortCase, ReadOnlyFixture},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::reopen},
};
use std::sync::Arc;

fn window(from: u32, until: u32) -> PublishedResourceWindow {
    PublishedResourceWindow {
        from: format!("2020-01-01T{from:02}:00:00Z"),
        until: format!("2020-01-01T{until:02}:00:00Z"),
    }
}
pub(crate) fn case(hour: u32, epoch: Option<&str>) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, mut rows) = test_support::query_cohort_evidence(QueryCohortCase::PlanVariation);
    let CollectionTarget::Database(database) = &mut source.target else { panic!() };
    database.incarnation = epoch.map(str::to_owned);
    move_interval(&mut source, &mut rows, hour, hour + 1);
    rebind(&source, &mut rows);
    (source, rows)
}
fn move_interval(source: &mut CollectionDescription, rows: &mut [TelemetryRecord], from: u32, until: u32) {
    let SourceObservation::QueryHistory(history) = &mut source.observation else { panic!() };
    history.interval = TelemetryInterval {
        native_id: format!("native-hour-{from}"),
        from: window(from, until).from,
        until: window(from, until).until,
    };
    if let Some(catalog) = &mut history.population_catalog
        && let RuntimePopulationMembership::ObservedWindow { from: start, until: end } = &mut catalog.membership
    {
        *start = history.interval.from.clone();
        *end = history.interval.until.clone();
    }
    let delta = chrono::Duration::hours(i64::from(from) - 6);
    for runtime in rows.iter_mut().filter_map(|row| match row {
        TelemetryRecord::QueryRuntime(runtime) => Some(runtime),
        _ => None,
    }) {
        for value in [&mut runtime.first_execution_at, &mut runtime.last_execution_at].into_iter().flatten() {
            *value = (chrono::DateTime::parse_from_rfc3339(value).unwrap() + delta).to_rfc3339();
        }
    }
}
fn rebind(source: &CollectionDescription, rows: &mut [TelemetryRecord]) {
    for row in rows {
        match row {
            TelemetryRecord::QueryDefinition(query) => query.id = query.identity.definition_id(source).unwrap(),
            TelemetryRecord::PlanDefinition(plan) => {
                plan.id = plan.identity.definition_id(source).unwrap();
                plan.query_definition_id = Some(plan.identity.query.definition_id(source).unwrap());
            }
            TelemetryRecord::QueryRuntime(runtime) => rebind_key(source, &mut runtime.key),
            TelemetryRecord::QueryWaits(wait) => rebind_key(source, &mut wait.key),
            _ => panic!("controlled query corpus contains only its original four families"),
        }
    }
}
fn rebind_key(source: &CollectionDescription, key: &mut NativeMeasurementKey) {
    let query = key.references.query.as_mut().unwrap();
    let identity =
        NativeQueryIdentity { query_id: query.native.id.clone(), qualifiers: query.native.qualifiers.clone() };
    query.definition_id = Some(identity.definition_id(source).unwrap());
    if let Some(plan) = &mut key.references.plan {
        plan.definition_id = Some(
            NativePlanIdentity {
                plan_id: plan.native.id.clone(),
                query: identity,
                qualifiers: plan.native.qualifiers.clone(),
            }
            .definition_id(source)
            .unwrap(),
        );
    }
}
async fn capture(source: CollectionDescription, rows: Vec<TelemetryRecord>, sequence: u64) -> EvidenceCapture {
    capture_selected(
        source,
        rows,
        sequence,
        "2020-01-02T12:00:00Z",
        &[
            EvidenceFamily::QueryRuntime,
            EvidenceFamily::QueryWaits,
            EvidenceFamily::QueryDefinitions,
            EvidenceFamily::PlanDefinitions,
        ],
    )
    .await
}
async fn capture_selected(
    source: CollectionDescription,
    rows: Vec<TelemetryRecord>,
    sequence: u64,
    at: &str,
    families: &[EvidenceFamily],
) -> EvidenceCapture {
    let until = (chrono::DateTime::parse_from_rfc3339(at).unwrap() + chrono::Duration::seconds(1)).to_rfc3339();
    let fixture = ReadOnlyFixture::capture_at(source, rows, sequence, at, &until).await;
    let recovery = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport())))
        .read_capture(&range, families, EvidenceReadBudget::default(), Some(&recovery))
        .await
        .unwrap()
}

async fn standard(hour: u32, sequence: u64) -> EvidenceCapture {
    let (source, rows) = case(hour, Some("controlled-native-database-epoch"));
    capture(source, rows, sequence).await
}

#[tokio::test]
async fn contiguous_native_intervals_feed_one_real_weighted_workload_and_the_existing_pure_findings_pipeline() {
    let first = standard(6, 1).await;
    let next = standard(7, 2).await;
    assert!(first.is_read_complete() && next.is_read_complete());
    for original in [&first, &next] {
        let fragments = original
            .records()
            .iter()
            .filter_map(|row| match &row.row().record {
                TelemetryRecord::QueryRuntime(runtime) => {
                    let duration = runtime.metric(&RuntimeMetricKey::Duration).unwrap();
                    let cpu = runtime.metric(&RuntimeMetricKey::CpuTime).unwrap();
                    assert_eq!(duration.unit, RuntimeMetricUnit::Milliseconds);
                    assert_eq!(cpu.unit, RuntimeMetricUnit::Milliseconds);
                    Some((runtime.executions.as_str(), duration.average.unwrap(), cpu.average.unwrap()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(fragments, [("1", 100.0, 50.0), ("9", 1000.0, 500.0), ("40", 10.0, 5.0)]);
    }
    // Independent native fragments, weighted by their exact original execution counts, in each of two intervals.
    let expected_time = 2.0 * (1.0 * 100.0 + 9.0 * 1000.0 + 40.0 * 10.0);
    let expected_cpu = 2.0 * (1.0 * 50.0 + 9.0 * 500.0 + 40.0 * 5.0);
    let endpoint = |capture: &EvidenceCapture, first: bool| {
        capture
            .records()
            .iter()
            .find_map(|row| match &row.row().record {
                TelemetryRecord::QueryRuntime(runtime) => {
                    if first {
                        runtime.first_execution_at.clone()
                    } else {
                        runtime.last_execution_at.clone()
                    }
                }
                _ => None,
            })
            .unwrap()
    };
    let first_endpoint = endpoint(&first, true);
    let last_endpoint = endpoint(&next, false);
    assert_eq!(canonical_time(&first_endpoint).unwrap(), canonical_time("2020-01-01T06:01:00Z").unwrap());
    assert_eq!(canonical_time(&last_endpoint).unwrap(), canonical_time("2020-01-01T07:59:00Z").unwrap());
    let result = analyze_window(&[&next, &first], &window(6, 8)).unwrap();
    let [section] = result.sections.as_slice() else { panic!() };
    assert!(result.gaps.is_empty());
    assert_eq!(section.inputs.len(), 2);
    assert_eq!(section.analysis.runtime_totals.as_ref().unwrap().executions, "100");
    let workload = section.analysis.workload.as_ref().unwrap();
    assert_eq!(workload.window_hours, 2.0);
    assert_eq!(workload.total_executions, 100.0);
    assert_eq!(workload.total_time_ms, expected_time);
    assert_eq!(workload.total_cpu_ms, Some(expected_cpu));
    let [query] = workload.queries.as_slice() else { panic!() };
    assert_eq!(query.executions, 100.0);
    assert_eq!(query.mean_time_ms, expected_time / 100.0);
    assert_eq!(query.stddev_time_ms, None);
    assert_eq!(query.first_seen.as_ref(), Some(&first_endpoint));
    assert_eq!(query.last_seen.as_ref(), Some(&last_endpoint));
    assert_eq!(query.plans.len(), 2);
    assert_eq!(query.observed_wait_ms(WaitClass::Lock), Some(60.0));
    assert!(section.analysis.findings.iter().any(|finding| finding.check == "parameter-sensitive"));
    assert_eq!(section.inputs[0].original.capture, *first.capture());
    assert_eq!(section.inputs[1].original.capture, *next.capture());
    assert_ne!(section.inputs[0].original, section.inputs[1].original);
    assert_eq!(section.inputs[0].queries[0].measurements.len(), 3);
}
#[tokio::test]
async fn newest_same_owner_interval_revision_replaces_prior_counts_and_retains_exact_superseded_receipt() {
    let old = standard(6, 1).await;
    let (source, mut rows) = case(6, Some("controlled-native-database-epoch"));
    let runtime = rows
        .iter_mut()
        .find_map(|row| match row {
            TelemetryRecord::QueryRuntime(runtime) => Some(runtime),
            _ => None,
        })
        .unwrap();
    runtime.executions = "11".into();
    let new = capture(source, rows, 2).await;
    let result = analyze_window(&[&old, &new], &window(6, 7)).unwrap();
    let [section] = result.sections.as_slice() else { panic!() };
    assert_eq!(section.analysis.runtime_totals.as_ref().unwrap().executions, "60");
    assert_eq!(result.superseded.len(), 1);
    assert_eq!(result.superseded[0].previous.capture, *old.capture());
    assert_eq!(result.superseded[0].selected.capture, *new.capture());
    assert!(result.gaps.is_empty());
}
#[tokio::test]
async fn native_epoch_and_unknown_identity_never_become_one_time_window_total() {
    for epoch in [Some("another-native-database-epoch"), None] {
        let first = standard(6, 1).await;
        let (source, rows) = case(7, epoch);
        let next = capture(source, rows, 2).await;
        let result = analyze_window(&[&first, &next], &window(6, 8)).unwrap();
        assert_eq!(result.sections.len(), 2);
        assert!(
            result.sections.iter().all(|section| section.analysis.runtime_totals.as_ref().unwrap().executions == "50")
        );
        if epoch.is_none() {
            assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::UnqualifiedScope { .. })));
        }
    }
}
#[tokio::test]
async fn overlapping_native_intervals_and_changed_native_definition_bodies_remain_separate_evidence_sections() {
    let first = standard(6, 1).await;
    let (mut source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    let SourceObservation::QueryHistory(history) = &mut source.observation else { panic!() };
    history.interval.from = "2020-01-01T06:30:00Z".into();
    let overlap = capture(source, rows.clone(), 2).await;
    let result = analyze_window(&[&first, &overlap], &window(6, 8)).unwrap();
    assert_eq!(result.sections.len(), 2);
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::OverlappingIntervals { .. })));
    let (source, original_rows) = case(7, Some("controlled-native-database-epoch"));
    rows = original_rows;
    let query = rows
        .iter_mut()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(query) => Some(query),
            _ => None,
        })
        .unwrap();
    query.sql = DefinitionPayload::sql("SELECT DIFFERENT FROM public.orders", source.target.engine());
    let changed = capture(source, rows, 3).await;
    let result = analyze_window(&[&first, &changed], &window(6, 8)).unwrap();
    assert_eq!(result.sections.len(), 2);
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::ConflictingDefinition { .. })));
}
#[tokio::test]
async fn missing_native_mean_withholds_the_joint_denominator_but_preserves_exact_executions_and_original_fragments() {
    let first = standard(6, 1).await;
    let (source, mut rows) = case(7, Some("controlled-native-database-epoch"));
    let runtime = rows
        .iter_mut()
        .find_map(|row| match row {
            TelemetryRecord::QueryRuntime(runtime) => Some(runtime),
            _ => None,
        })
        .unwrap();
    runtime.metrics.iter_mut().find(|metric| metric.key == RuntimeMetricKey::Duration).unwrap().average = None;
    let next = capture(source, rows, 2).await;
    let result = analyze_window(&[&first, &next], &window(6, 8)).unwrap();
    let [section] = result.sections.as_slice() else { panic!() };
    let totals = section.analysis.runtime_totals.as_ref().unwrap();
    assert_eq!(totals.executions, "100");
    assert!(matches!(totals.duration, CollectedRuntimeTimeTotal::Incomplete));
    assert!(section.analysis.workload.is_none());
    assert_eq!(section.inputs.len(), 2);
    assert!(!section.inputs[0].queries.is_empty());
    assert_eq!(section.inputs[1].runtime_rows, Some(3));
    assert_eq!(section.inputs[1].original.capture, *next.capture());
    assert!(next.records().iter().any(|row|matches!(&row.row().record,TelemetryRecord::QueryRuntime(runtime) if runtime.metric(&RuntimeMetricKey::Duration).is_some_and(|duration|duration.average.is_none() && duration.minimum.is_some()) && runtime.first_execution_at.is_some() && runtime.last_execution_at.is_some())));
    assert!(
        section
            .analysis
            .limitations
            .iter()
            .any(|limit| limit.reason == CollectedAnalysisLimit::RuntimeDenominatorUnavailable)
    );
}
#[tokio::test]
async fn native_interval_edges_are_expanded_not_prorated_and_uncovered_gaps_remain_explicit() {
    let first = standard(6, 1).await;
    let next = standard(8, 2).await;
    let result = analyze_window(&[&first, &next], &window(6, 9)).unwrap();
    assert_eq!(result.sections.len(), 2);
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::SectionDoesNotCoverWindow { .. })));
    let selected =
        PublishedResourceWindow { from: "2020-01-01T06:30:00Z".into(), until: "2020-01-01T06:45:00Z".into() };
    let result = analyze_window(&[&first], &selected).unwrap();
    let full_interval = PublishedResourceWindow {
        from: canonical_time("2020-01-01T06:00:00Z").unwrap(),
        until: canonical_time("2020-01-01T07:00:00Z").unwrap(),
    };
    assert_eq!(result.sections[0].observed, full_interval);
    assert_eq!(result.sections[0].analysis.runtime_totals.as_ref().unwrap().executions, "50");
    assert!(result.gaps.iter().any(|gap| matches!(gap, CollectedWindowGap::NativeBoundaryExpanded { .. })));
}
#[tokio::test]
async fn pure_window_cancellation_and_input_ceiling_refuse_output_without_reopening_originals() {
    use std::cell::Cell;
    let capture = standard(6, 1).await;
    let original = capture.verified_original().unwrap();
    let originals: [&dyn OriginalProjection; 1] = [&original];
    let calls = Cell::new(0);
    let error = analyze_originals(&originals, &window(6, 7), &culprit_checks::Pipeline::builtin(), &|| {
        calls.set(calls.get() + 1);
        if calls.get() > 2 { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
    })
    .unwrap_err();
    assert_eq!(error.detail, CollectionStop::Cancelled.error().detail);
    let copies = vec![&capture; MAX_COLLECTED_WINDOW_INPUTS + 1];
    assert_eq!(analyze_window(&copies, &window(6, 7)).unwrap_err().detail, DeliveryProblem::Capacity.error().detail);
    assert!(capture.is_read_complete());
    assert_eq!(capture.records().len(), 8);
}

#[tokio::test]
async fn exact_execution_sum_can_exceed_a_native_u64_without_inventing_a_float_denominator() {
    let mut captures = Vec::new();
    for (hour, sequence) in [(6, 1), (7, 2)] {
        let (source, mut rows) = case(hour, Some("controlled-native-database-epoch"));
        for row in &mut rows {
            if let TelemetryRecord::QueryRuntime(runtime) = row {
                runtime.executions = u64::MAX.to_string();
            }
        }
        captures.push(capture(source, rows, sequence).await);
    }
    let result = analyze_window(&captures.iter().collect::<Vec<_>>(), &window(6, 8)).unwrap();
    let [section] = result.sections.as_slice() else { panic!() };
    assert_eq!(section.analysis.runtime_totals.as_ref().unwrap().executions, (u128::from(u64::MAX) * 6).to_string());
    assert!(section.analysis.workload.is_none());
    assert!(section.inputs.iter().all(|input| input.runtime_rows == Some(3)));
}

#[tokio::test]
async fn separately_bound_native_replica_populations_receive_independent_joint_workloads() {
    let mut captures = Vec::new();
    for (hour, sequence) in [(6, 1), (7, 2)] {
        let (mut source, mut rows) = test_support::query_population_evidence();
        let CollectionTarget::Database(database) = &mut source.target else { panic!() };
        database.incarnation = Some("controlled-native-database-epoch".into());
        move_interval(&mut source, &mut rows, hour, hour + 1);
        rebind(&source, &mut rows);
        captures.push(capture(source, rows, sequence).await);
    }
    let original = super::super::analyze(&captures[0], None).unwrap();
    assert_eq!(original.analyses.len(), 2);
    let result = analyze_window(&captures.iter().collect::<Vec<_>>(), &window(6, 8)).unwrap();
    assert_eq!(result.sections.len(), 2);
    for part in &original.analyses {
        let section = result.sections.iter().find(|section| section.population == part.population).unwrap();
        assert_eq!(section.inputs.len(), 2);
        assert_eq!(
            section.analysis.runtime_totals.as_ref().unwrap().executions,
            (part.runtime_totals.as_ref().unwrap().executions.parse::<u128>().unwrap() * 2).to_string()
        );
        assert!(matches!(section.population, CollectedQueryPopulation::Population { .. }));
    }
}

#[test]
fn an_empty_native_window_is_unavailable_instead_of_a_healthy_empty_analysis() {
    let result = analyze_window(&[], &window(6, 8)).unwrap();
    assert!(result.sections.is_empty());
    assert!(matches!(result.gaps.as_slice(), [CollectedWindowGap::NoNativeIntervals]));
}

#[tokio::test]
async fn an_unselected_wait_family_withholds_zero_and_retains_independently_observed_positive_waits() {
    let first = standard(6, 1).await;
    let (source, rows) = case(7, Some("controlled-native-database-epoch"));
    let second = capture_selected(
        source,
        rows,
        2,
        "2020-01-02T12:00:00Z",
        &[EvidenceFamily::QueryRuntime, EvidenceFamily::QueryDefinitions, EvidenceFamily::PlanDefinitions],
    )
    .await;
    let result = analyze_window(&[&first, &second], &window(6, 8)).unwrap();
    let [section] = result.sections.as_slice() else { panic!() };
    let [query] = section.analysis.workload.as_ref().unwrap().queries.as_slice() else { panic!() };
    assert_eq!(query.observed_wait_ms(WaitClass::Lock), Some(30.0));
    assert_eq!(query.measured_wait_ms(WaitClass::Lock), None);
    assert!(
        section.analysis.limitations.iter().any(|limit| limit.reason == CollectedAnalysisLimit::WaitFamilyIncomplete)
    );
    assert_eq!(section.inputs[1].wait_rows, None);
}
fn setting_source(primary: &CollectionDescription, complete: bool) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mut source = primary.clone();
    source.source = "controlled-retained-settings".into();
    source.observation = SourceObservation::Snapshot(StateSnapshotDescription {
        dataset: SnapshotDataset::Settings,
        observed_from: "2020-01-01T03:00:00Z".into(),
        observed_until: "2020-01-01T03:00:01Z".into(),
        records: 1,
        sources: vec![StateSourceCoverage::observed(source_id::SETTINGS.into(), 1, complete)],
    });
    let rows = vec![TelemetryRecord::Setting(SettingObservation {
        source: source_id::SETTINGS.into(),
        namespace: "sys.database_scoped_configurations".into(),
        name: "MAXDOP".into(),
        scope: MeasurementScope::Database,
        nature: SettingNature::Configuration,
        value: Some("8".into()),
        value_in_use: None,
        value_for_secondary: None,
        unit: None,
        native_context: None,
        native_source: None,
        pending_restart: None,
        native_is_default: None,
        default_value: None,
        default_source: SettingDefaultSource::Unknown,
    })];
    (source, rows)
}
#[tokio::test]
async fn catalog_as_of_uses_original_acquisition_scope_and_never_falls_back_over_a_newer_partial_head() {
    let original = test_support::query_compilation_followup_original();
    let period = original.source.query_history().unwrap().interval.clone();
    let primary_source = original.source.clone();
    let query = capture(original.source, original.records, 1).await;
    let (source, rows) = setting_source(&primary_source, true);
    let old = capture_selected(
        source.clone(),
        rows.clone(),
        2,
        "2020-01-02T10:00:00Z",
        &[EvidenceFamily::SettingObservations],
    )
    .await;
    let future =
        capture_selected(source, rows, 3, "2020-01-02T13:00:00Z", &[EvidenceFamily::SettingObservations]).await;
    let selected = PublishedResourceWindow { from: period.from, until: period.until };
    let result = analyze_window(&[&query, &old, &future], &selected).unwrap();
    let section = result.sections.first().unwrap();
    assert_eq!(section.catalog_inputs.len(), 1);
    assert_eq!(section.catalog_inputs[0].capture.capture, *old.capture());
    let provenance = section.catalog_inputs[0].catalog.as_ref().unwrap();
    assert_eq!(provenance.observed_from, canonical_time("2020-01-02T10:00:00Z").unwrap());
    assert!(
        matches!(&provenance.native_time,Some(DerivedCatalogTime::Utc{at}) if at==&canonical_time("2020-01-01T03:00:01Z").unwrap())
    );
    assert!(!section.analysis.limitations.iter().any(|limit| limit.reason == CollectedAnalysisLimit::MissingSettings));
    let (source, rows) = setting_source(&primary_source, false);
    let partial =
        capture_selected(source, rows, 4, "2020-01-02T11:00:00Z", &[EvidenceFamily::SettingObservations]).await;
    let result = analyze_window(&[&query, &old, &partial, &future], &selected).unwrap();
    let section = result.sections.first().unwrap();
    assert!(section.catalog_inputs.is_empty());
    assert!(section.context_issues.contains(&DerivedContextIssue::PartialCatalog));
    assert!(section.analysis.limitations.iter().any(|limit| limit.reason == CollectedAnalysisLimit::MissingSettings));
    assert!(section.analysis.findings.iter().all(|finding| finding.fix.as_ref().is_none_or(|fix| fix.sql.is_none())));
}
#[test]
fn pure_input_byte_admission_counts_the_whole_scalar_and_refuses_without_a_native_or_archive_owner() {
    let value = "private-input-canary".repeat(100);
    let mut remaining = 128;
    let error = admit_input(&value, &mut remaining).unwrap_err();
    assert_eq!(error.detail, DeliveryProblem::Capacity.error().detail);
    assert_eq!(remaining, 128);
    assert!(!serde_json::to_string(&error).unwrap().contains("private-input-canary"));
}

mod comparison;
