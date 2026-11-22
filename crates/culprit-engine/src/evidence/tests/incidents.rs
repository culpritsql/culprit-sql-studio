//! Genuine canonical stage/commit/projection proofs feed the actual database/time service. No native source runs.
use super::{super::IncidentRequest, lifecycle::fixture::Fixture};
use crate::collected_resources::references;
use culprit_core::{telemetry::*, *};
use culprit_evidence::analytical::evidence::*;
use culprit_evidence::analytical::test_support;
use culprit_evidence::delivery::recovery::CommittedRange;
use culprit_read::cancellation::CollectionCancellation;
use std::sync::atomic::Ordering;

struct Scenario {
    fixture: Fixture,
    request: IncidentRequest,
    gold: CommittedRange,
    original: DiagnosticEvidenceRef,
    empty: DiagnosticCaptureRef,
}
async fn scenario() -> Scenario {
    let (source, rows) = test_support::activity_evidence_cases().remove(0);
    let fixture = Fixture::capture(source.clone(), rows).await;
    let cancellation = CollectionCancellation::default();
    let mut session = fixture.service.session(IncidentRequest::default_budget(), &cancellation).unwrap();
    let baseline = session.read(&fixture.request.range_id, &[EvidenceFamily::ActivityObservations]).await.unwrap();
    assert!(baseline.is_read_complete());
    let actor = baseline
        .records()
        .iter()
        .find(|row| matches!(&row.row().record,TelemetryRecord::Activity(row) if row.key.request.is_some()))
        .unwrap();
    let original = references::from_record(&baseline, actor).unwrap();
    let NativeRecordIdentity::Activity(key) = &original.native_key else { panic!() };
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&source).unwrap(),
        DiagnosticSignal::Activity {
            key: key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        source.observation.activity().unwrap().at.clone(),
        original.clone(),
    )
    .unwrap();
    let first = DiagnosticRevision::between(None, episode, "2020-01-01T12:00:02Z").unwrap().unwrap();
    let mut empty_source = source.clone();
    let source_name = empty_source.source.clone();
    let SourceObservation::Activity(frame) = &mut empty_source.observation else { panic!() };
    let later = chrono::DateTime::parse_from_rfc3339(&frame.at).unwrap() + chrono::Duration::seconds(1);
    frame.at = later.to_rfc3339();
    frame.observed_from = frame.at.clone();
    frame.observed_until = frame.at.clone();
    frame.observations = 0;
    frame.native_rows = 0;
    frame.coverage = StateSourceCoverage::observed(source_name, 0, true);
    let empty_range = fixture.append_capture(empty_source, vec![], 100001).await;
    let empty = capture_ref(&empty_range);
    let mut resolved = first.episode.clone();
    resolved.lifecycle = DiagnosticLifecycle::Resolved;
    resolved.analysis = DiagnosticAnalysisStatus::Unresolved;
    resolved.issue = Some(DiagnosticAnalysisIssue::EvidenceUnavailable);
    resolved.recovery = Some(DiagnosticRecoveryProof::CompleteScope { capture: empty.clone() });
    let final_revision =
        DiagnosticRevision::between(Some(&first.episode), resolved, "2020-01-01T12:00:03Z").unwrap().unwrap();
    let first_range = append_revision(&fixture, &source.target, first, original.capture_ref(), 100002).await;
    let final_range = append_revision(&fixture, &source.target, final_revision, empty.clone(), 100003).await;
    let mut session = fixture.service.session(IncidentRequest::default_budget(), &cancellation).unwrap();
    let mut history_entries = Vec::new();
    for range in [&first_range, &final_range] {
        let diagnostic = session.read(&range.manifest().range_id, &[EvidenceFamily::OperationalSamples]).await.unwrap();
        history_entries.extend(crate::collected_resources::incident_history(&diagnostic).unwrap().report.entries);
    }
    let database = IncidentDatabaseSelector::from_route(source.target.database().unwrap()).unwrap();
    let mut description = DerivedDiagnosisDescription {
        analysis_id: String::new(),
        analyzer: ComponentVersion::package("controlled-canonical-synthesis", "1", None),
        rules_version: "1".into(),
        language: "en".into(),
        recorded_at: "2020-01-01T12:01:00Z".into(),
        observed_from: "2020-01-01T12:00:00Z".into(),
        observed_until: "2020-01-01T12:00:01Z".into(),
        inputs: vec![
            DerivedInputReference {
                capture: capture_ref(&first_range),
                role: DerivedInputRole::Revision,
                catalog: None,
            },
            DerivedInputReference {
                capture: capture_ref(&final_range),
                role: DerivedInputRole::Revision,
                catalog: None,
            },
            DerivedInputReference {
                capture: original.capture_ref(),
                role: DerivedInputRole::Observation,
                catalog: None,
            },
            DerivedInputReference { capture: empty.clone(), role: DerivedInputRole::Observation, catalog: None },
        ],
        origin: AnalysisOrigin::ArchivedTelemetry,
        input_read_complete: true,
        counts: DerivedDiagnosisCounts { incidents: 2, findings: 0, changes: 0 },
        finding_evaluations: Default::default(),
        change_evaluations: vec![],
        context_issues: vec![],
        output_coverage: DerivedOutputCoverageSet {
            incidents: DerivedOutputCoverage::Evaluated,
            findings: DerivedOutputCoverage::ContextUnavailable { reason: DerivedOutputGap::AnalysisSourceUnavailable },
            changes: DerivedOutputCoverage::ContextUnavailable {
                reason: DerivedOutputGap::CatalogComparisonUnavailable,
            },
        },
    };
    description.analysis_id = description.expected_analysis_id().unwrap();
    let incidents = history_entries
        .iter()
        .map(|entry| DerivedIncident {
            analysis_id: description.analysis_id.clone(),
            database: Some(database.clone()),
            display: DerivedDisplayContext {
                connection_alias: "saved-alias".into(),
                environment: None,
                database_name: Some(source.target.database().unwrap().database.clone()),
            },
            entry: entry.clone(),
            original_interval: None,
            revision_reference: entry.original.clone().unwrap(),
            victim_summary: entry.title.clone(),
            affected_queries: vec![],
            culprit_queries: vec![],
            candidate_queries: vec![],
            attribution_status: DerivedAttributionStatus::Unestablished,
            mechanism: None,
            developer_action: entry.next_step.clone(),
            verification: entry.next_step.clone(),
            caller: None,
        })
        .collect::<Vec<_>>();
    let derived = DerivedDiagnosis { description: description.clone(), incidents, findings: vec![], changes: vec![] };
    derived.validate().unwrap();
    let gold_source = CollectionDescription {
        target: source.target.clone(),
        partition: SourcePartition::Target,
        source: DERIVED_DIAGNOSIS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::DerivedDiagnosis(Box::new(description)),
    };
    let gold = fixture.append_capture(gold_source, derived.into_records().collect(), 100004).await;
    let rows = fixture.rows_for(&gold, TelemetryStream::DerivedIncidents);
    fixture.candidate_pages(vec![rows.clone(), rows]);
    let mut request = IncidentRequest::around(
        source.target.database().unwrap(),
        "2020-01-01T12:00:00Z",
        IncidentRequest::default_budget(),
    )
    .unwrap();
    request.recorded_before = "2020-01-01T13:00:00Z".into();
    Scenario { fixture, request, gold, original, empty }
}
async fn append_revision(
    fixture: &Fixture,
    target: &CollectionTarget,
    revision: DiagnosticRevision,
    parent: DiagnosticCaptureRef,
    sequence: u64,
) -> CommittedRange {
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: revision.controller_version,
        target: target.clone(),
        input: parent.clone(),
        part: 0,
        parts: 1,
        episodes: revision.episode.retained_in_checkpoint().then(|| revision.episode.clone()).into_iter().collect(),
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let source = CollectionDescription {
        target: target.clone(),
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            parent,
            controller_version: revision.controller_version,
            step_id: digest(format!("controlled-step-{sequence}").as_bytes()),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(revision.episode.scope.key().unwrap(), revision.episode.scope.clone())].into(),
        }),
    };
    let records = vec![
        TelemetryRecord::Operational(OperationalObservation {
            source: DIAGNOSTICS_SOURCE.into(),
            key: revision.id.clone(),
            fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
        }),
        TelemetryRecord::DiagnosticCheckpoint(checkpoint),
    ];
    fixture.append_capture(source, records, sequence).await
}

fn capture_ref(range: &CommittedRange) -> DiagnosticCaptureRef {
    DiagnosticCaptureRef {
        capture: range.manifest().capture.clone(),
        manifest_sha256: range.manifest().capture_manifest_sha256.clone(),
        source_scope_id: range.manifest().source.captured_range().unwrap().scope_id,
    }
}
#[tokio::test]
async fn database_around_resolves_exact_revision_ancestry_recovery_and_empty_original_without_manual_hashes_or_native_access()
 {
    let scenario = scenario().await;
    let before = scenario.fixture.profiles.0.lock().unwrap().clone();
    let cancellation = CollectionCancellation::default();
    let before_queries = scenario.fixture.store.requests.load(Ordering::SeqCst);
    let report = scenario.fixture.service.incident(&scenario.request, &cancellation).await.unwrap();
    assert!(report.discovery_complete, "{:?}", report.gaps);
    assert_eq!(report.incidents.len(), 2);
    assert_eq!(report.history.entries.len(), 2);
    assert_eq!(report.history.entries[1].lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(report.history.entries[0].revision, "1");
    assert_eq!(report.history.entries[1].previous_revision.as_deref(), Some("1"));
    assert!(report.relationships.iter().any(|link| link.reference.as_ref() == Some(&scenario.original)
        && link.availability == IncidentEvidenceAvailability::Authenticated));
    assert!(report.relationships.iter().any(|link| link.capture == scenario.empty
        && link.reference.is_none()
        && link.availability == IncidentEvidenceAvailability::Authenticated));
    assert!(
        report
            .incidents
            .iter()
            .all(|row| row.culprit_queries.is_empty()
                && row.attribution_status == DerivedAttributionStatus::Unestablished)
    );
    assert_eq!(*scenario.fixture.profiles.0.lock().unwrap(), before, "native credentials are never requested");
    assert_eq!(scenario.fixture.store.candidate_requests.lock().unwrap().len(), 2);
    assert_eq!(
        report.archive_work.queries,
        (scenario.fixture.store.requests.load(Ordering::SeqCst) - before_queries) as u32,
        "all action warehouse reads, independent of setup reads, are charged"
    );
    let json = serde_json::to_string(&report).unwrap();
    for secret in ["private-diagnostic-host", "native-session-password", "RecipeCostReceipt", "SourceProfileAdmission"]
    {
        assert!(!json.contains(secret));
    }
}
#[tokio::test]
async fn changed_original_archive_manifest_cannot_become_a_successful_incident() {
    let scenario = scenario().await;
    // Corrupt the immutable manifest after synthesis, rather than inventing a public row or native authority.
    scenario.fixture.objects.corrupt_manifest();
    let cancellation = CollectionCancellation::default();
    assert!(scenario.fixture.service.incident(&scenario.request, &cancellation).await.is_err());
    assert_eq!(scenario.fixture.objects.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn cancelled_incident_action_does_not_open_discovery_or_native_work() {
    let scenario = scenario().await;
    let before = scenario.fixture.store.requests.load(Ordering::SeqCst);
    let cancellation = CollectionCancellation::default();
    cancellation.cancel();
    let error = scenario.fixture.service.incident(&scenario.request, &cancellation).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    assert_eq!(scenario.fixture.store.requests.load(Ordering::SeqCst), before);
    assert_eq!(scenario.fixture.objects.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unpublished_and_missing_gold_rows_remain_partial_without_substituting_current_or_neighboring_originals() {
    for unpublished in [false, true] {
        let scenario = scenario().await;
        if unpublished {
            scenario.fixture.unpublish(&scenario.gold);
        } else {
            scenario.fixture.omit_row(&scenario.gold, TelemetryStream::DerivedIncidents, 0);
        }
        let cancellation = CollectionCancellation::default();
        let report = scenario.fixture.service.incident(&scenario.request, &cancellation).await.unwrap();
        assert!(!report.discovery_complete);
        assert!(report.gaps.iter().any(|gap| matches!(gap,IncidentReadGap::CandidateUnavailable { gaps,.. } if gaps.iter().any(|gap| gap.reason == if unpublished { EvidenceGapReason::NotPublished } else { EvidenceGapReason::MissingRows }))));
        assert_eq!(report.incidents.len(), if unpublished { 0 } else { 1 });
        assert!(report.incidents.iter().all(|incident| incident.entry.revision == "2"));
        if !unpublished {
            assert!(
                report.gaps.iter().any(
                    |gap| matches!(gap,IncidentReadGap::PredecessorNotSelected { revision,.. } if revision == "1")
                )
            );
        }
    }
}
#[tokio::test]
async fn foreign_database_candidate_is_refused_before_any_original_archive_open() {
    let mut scenario = scenario().await;
    scenario.request.database = IncidentDatabaseSelector::from_key(&digest(b"different saved native route")).unwrap();
    let before = scenario.fixture.objects.reads.lock().unwrap().len();
    let cancellation = CollectionCancellation::default();
    assert!(scenario.fixture.service.incident(&scenario.request, &cancellation).await.is_err());
    assert_eq!(scenario.fixture.objects.reads.lock().unwrap().len(), before);
}
#[tokio::test]
async fn discovery_and_originals_share_one_query_allowance_with_no_reopen_after_exhaustion() {
    let mut scenario = scenario().await;
    scenario.request.budget.max_queries = 1;
    let before = scenario.fixture.store.requests.load(Ordering::SeqCst);
    let objects = scenario.fixture.objects.reads.lock().unwrap().len();
    let cancellation = CollectionCancellation::default();
    let error = scenario.fixture.service.incident(&scenario.request, &cancellation).await.unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert_eq!(scenario.fixture.store.requests.load(Ordering::SeqCst), before + 1);
    assert_eq!(scenario.fixture.objects.reads.lock().unwrap().len(), objects);
}
#[tokio::test]
async fn cancellation_during_archive_ancestry_releases_the_existing_pending_owner_and_cannot_return_partial_success() {
    let scenario = scenario().await;
    scenario.fixture.objects.stall.store(true, Ordering::SeqCst);
    let cancellation = CollectionCancellation::default();
    let reading = scenario.fixture.service.incident(&scenario.request, &cancellation);
    tokio::pin!(reading);
    tokio::select! {
        result = &mut reading => panic!("unexpected completed ancestry: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => (),
    }
    assert_eq!(scenario.fixture.objects.active.load(Ordering::SeqCst), 1);
    cancellation.cancel();
    let error = reading.await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::Cancelled);
    assert_eq!(scenario.fixture.objects.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn injected_incident_capability_uses_the_same_canonical_assembler_and_default_request_policy() {
    let scenario = scenario().await;
    let database = scenario.gold.manifest().source.target.database().unwrap();
    let recovery = culprit_evidence::delivery::object_store::ObjectRecoveryReader::open(
        scenario.fixture.service.recovery.clone(),
        scenario.fixture.service.contract.clone(),
    )
    .await
    .unwrap();
    let resource = PublishedResourceSelector {
        namespace_id: resource_namespace_id(
            &scenario.fixture.service.contract,
            culprit_evidence::delivery::recovery::RecoveryRead::id(&recovery),
        )
        .unwrap(),
        database: IncidentDatabaseSelector::from_route(database).unwrap(),
    };
    let cancellation = CollectionCancellation::default();
    let before_queries = scenario.fixture.store.requests.load(Ordering::SeqCst);
    let report = crate::WarehouseIncidentReader::incident(
        &scenario.fixture.service,
        &resource,
        "2020-01-01T12:00:00Z",
        &cancellation,
    )
    .await
    .unwrap();
    assert!(report.discovery_complete, "{:?}", report.gaps);
    assert_eq!(report.incidents.len(), 2);
    assert!(report.history.entries.iter().any(|row| row.lifecycle == DiagnosticLifecycle::Resolved));
    assert!(report.relationships.iter().any(|row| row.reference.as_ref() == Some(&scenario.original)));
    assert_ne!(
        report.recorded_before, scenario.request.recorded_before,
        "the facade retains its real original action cutoff"
    );
    assert_eq!(
        report.archive_work.queries,
        (scenario.fixture.store.requests.load(Ordering::SeqCst) - before_queries) as u32
    );
}
