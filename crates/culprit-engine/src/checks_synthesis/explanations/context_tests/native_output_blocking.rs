//! Real finite client backpressure -> existing focused reader -> canonical originals -> shared Gold.
//! This does not qualify cost, establish a network cause or identify the lock-acquiring SQL.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisInput, SynthesisRequest},
};
use culprit_core::{
    Provider,
    blocking::{OutputBlockingDependency, OutputBlockingEvidence, OutputBlockingStatus},
    limits::OperationGuard,
};
use culprit_postgres::{
    PostgresProvider,
    native_test_support::{self as owned, OutputConsumerFixture},
};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;
use std::{future::Future, mem::size_of};

const HISTORY_SEQUENCE: u64 = 997_000;
struct Factory(PostgresProvider);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.telemetry_source(request)
    }
}
#[tokio::test]
#[ignore = "requires explicit nonce-owned PostgreSQL18 output-consumer fixture and private native receipt directory"]
async fn owned_client_write_retains_a_relation_lock_and_exact_collateral_victim_through_canonical_gold_and_retirement()
{
    OutputConsumerFixture::run(|fixture| Box::pin(prove(fixture))).await;
}
async fn prove(fixture: &mut OutputConsumerFixture) {
    eprintln!("owned output-consumer: prepare finite actors and native dependency oracle");
    Box::pin(fixture.prepare()).await;
    let mut captures = OwnedCaptures::new().unwrap();
    let basis = Box::pin(capture(fixture, &mut captures, CollectionSelection::Activity, false)).await;
    let (episode, focus) = original_episode(&basis, fixture);
    let selection = CollectionSelection::FocusedBlocking { focus: Box::new(focus) };
    eprintln!("owned output-consumer: selected native lock/output capture");
    let focused = Box::pin(capture(fixture, &mut captures, selection.clone(), false)).await;
    let evidence = output(&focused, true);
    owned::write_automatic_receipt(
        "output-consumer",
        "selected-native-point.json",
        &json!({
            "assertions_passed":false,"manifest":focused.manifest,"evidence":evidence,"fixture":fixture.receipt(),
        }),
    )
    .unwrap();
    verify_native(&focused, &evidence, fixture);
    let answered = answer(&basis, &focused, &episode);
    assert!(answered.episode.report.is_some());
    eprintln!("owned output-consumer: real one-row caller ceiling refuses an output conclusion");
    let partial = Box::pin(capture(fixture, &mut captures, selection.clone(), true)).await;
    assert_eq!(partial.manifest.request.budget.max_snapshot_rows, 1);
    assert_ne!(output(&partial, true).status, OutputBlockingStatus::Observed);
    assert!(output(&partial, true).cases.is_empty());
    assert_ne!(output(&focused, false).status, OutputBlockingStatus::Observed);
    Box::pin(fixture.confirm_output_dependency()).await;
    eprintln!("owned output-consumer: drain original output, acknowledge waiter and retire both actors");
    Box::pin(fixture.drain_and_retire()).await;
    let ended = Box::pin(capture(fixture, &mut captures, selection, false)).await;
    assert!(ended.records.is_empty());
    assert!(output(&ended, true).cases.is_empty());
    let SourceObservation::FocusedBlocking { result, .. } = &ended.source().unwrap().observation else { panic!() };
    assert_eq!(result.membership, FocusedRequestMembership::NotObserved);
    assert_ne!(result.binding, FocusedRequestBinding::Matched);
    let recovered = Box::pin(capture(fixture, &mut captures, CollectionSelection::Activity, false)).await;
    assert!(
        recovered.records.iter().all(|row| match &row.record {
            TelemetryRecord::Activity(row) =>
                row.key.session != *fixture.output_identity() && row.key.session != *fixture.waiter_identity(),
            _ => false,
        }),
        "the complete current census may retain idle fixture observers, never the retired actors"
    );
    let resolved = resolve(&basis, &recovered, &answered);
    assert_eq!(resolved.episode.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(resolved.episode.report, answered.episode.report);
    for captured in [&basis, &focused, &partial, &ended, &recovered] {
        Box::pin(captures.verify_retained(captured)).await.unwrap();
    }
    let input =
        Box::pin(reopen(&captures, [&basis, &focused, &partial, &ended, &recovered], &fixture.cancellation)).await;
    let positive = Box::pin(gold(&input, &answered, HISTORY_SEQUENCE)).await;
    verify_gold(&positive, &focused, &evidence, DiagnosticLifecycle::Active);
    let history = Box::pin(gold(&input, &resolved, HISTORY_SEQUENCE + 1)).await;
    verify_gold(&history, &focused, &evidence, DiagnosticLifecycle::Resolved);
    save_receipt(fixture, [&basis, &focused, &partial, &ended, &recovered], &evidence, &positive, &history);
}
fn save_receipt(
    fixture: &OutputConsumerFixture,
    captures: [&Captured; 5],
    evidence: &OutputBlockingEvidence,
    positive: &DerivedDiagnosis,
    history: &DerivedDiagnosis,
) {
    let [basis, focused, partial, ended, recovered] = captures;
    let source_queries = captures.map(|capture| capture.manifest.source_queries);
    assert!(source_queries.iter().sum::<u32>() <= 87);
    owned::write_automatic_receipt("output-consumer", "native-output-retention.json", &json!({
        "basis":basis.manifest,"focused":focused.manifest,"partial":partial.manifest,"ended":ended.manifest,
        "recovered":recovered.manifest,"native_output_evidence":evidence,"positive_gold":positive,"resolved_gold":history,
        "source_queries_including_each_setup":source_queries,"aggregate_source_query_ceiling":87,
        "fixture":fixture.receipt(),"same_originals_reopened_after_actor_exit":true,
        "fully_drained_original_output":true,"same_native_relation_and_blocker_independently_observed":true,
        "acquiring_statement_proved":false,"client_network_or_service_cause_established":false,
        "automatic_scheduler_or_cost_admission_validated":false,"native_cost_qualified":false,"source_cpu":null,
    })).unwrap();
}

async fn capture(
    fixture: &mut OutputConsumerFixture,
    captures: &mut OwnedCaptures,
    selection: CollectionSelection,
    partial: bool,
) -> Box<Captured> {
    let activity = matches!(selection, CollectionSelection::Activity);
    let budget = CollectionBudget {
        page_rows: if partial { 1 } else { 100 },
        max_snapshot_rows: if partial { 1 } else { 100 },
        max_queries: if activity { 6 } else { 25 },
        page_bytes: 1024 * 1024,
        total_bytes: 1024 * 1024,
        statement_timeout_ms: 1000,
        total_timeout_ms: 5000,
        ..CollectionBudget::default()
    };
    let specification = fixture.specification.clone();
    let request = CollectionRequest { subject: CollectionSubject::Database, selection, budget };
    fixture.begin_source();
    let captured = Box::new(
        Box::pin(captures.capture("owned-output-consumer", request, &fixture.cancellation, move |_| {
            Factory(PostgresProvider::new(&specification).unwrap())
        }))
        .await
        .unwrap(),
    );
    fixture.completed_source();
    let metrics = captured.manifest.read_metrics.as_ref().unwrap();
    assert_eq!(captured.manifest.status, CollectionStatus::Complete);
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    assert_eq!(
        metrics.templates.iter().map(|task| task.completed_tasks).sum::<u32>(),
        captured.manifest.source_queries
    );
    assert!(captured.manifest.source_queries <= captured.manifest.request.budget.max_queries);
    assert_eq!(captured.manifest.request.budget.max_queries, if activity { 6 } else { 25 });
    captured
}
fn original_episode(capture: &Captured, fixture: &OutputConsumerFixture) -> (DiagnosticEpisode, ActivityRequestFocus) {
    let native = capture.source().unwrap().observation.activity().unwrap();
    let matches = capture
        .records
        .iter()
        .enumerate()
        .filter_map(|(index, row)| match &row.record {
            TelemetryRecord::Activity(row) if row.key.session == *fixture.waiter_identity() => {
                Some((index, row.as_ref()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(index, waiter)] = matches.as_slice() else { panic!("one authentic waiter generation") };
    let parent = capture.reference(*index).unwrap();
    let focus = ActivityRequestFocus {
        parent: parent.clone(),
        target: capture.source().unwrap().target.clone(),
        partition: capture.source().unwrap().partition.clone(),
        database_writable: native.database_writable,
        source: capture.source().unwrap().source.clone(),
        epoch: native.epoch.clone().unwrap(),
        original_observed_at: native.at.clone(),
        original: (**waiter).clone(),
        statement_identity: waiter.statement_identity.clone().unwrap(),
    };
    assert!(focus.matches_parent(capture.source().unwrap(), waiter, &parent));
    assert_eq!(waiter.waiting_on_lock, Some(true));
    let update = with_diagnostic_rows(capture, |rows| {
        observe_diagnostic_activity(
            capture.source().unwrap(),
            &capture.capture_ref().unwrap(),
            rows,
            None,
            &ActivityPolicy::default(),
            &chrono::Utc::now().to_rfc3339(),
        )
        .unwrap()
    });
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|episode| matches!(&episode.signal, DiagnosticSignal::LockWait { .. }))
        .unwrap();
    (episode, focus)
}
fn verify_native(capture: &Captured, evidence: &OutputBlockingEvidence, fixture: &OutputConsumerFixture) {
    assert_eq!(evidence.status, OutputBlockingStatus::Observed);
    let [observed] = evidence.cases.as_slice() else { panic!("one actual output owner") };
    assert_eq!(observed.waiter.session, *fixture.waiter_identity());
    assert_eq!(observed.owner.actor.as_ref().unwrap().session, *fixture.output_identity());
    assert_eq!(observed.owner.native_wait.as_deref(), Some("Client: ClientWrite"));
    assert!(observed.owner.has_single_current_request());
    let OutputBlockingDependency::GrantedRelation { resource, requested_mode, granted_modes } = &observed.dependency
    else {
        panic!("actual granted relation required, not only a session edge");
    };
    assert_eq!(resource.database_oid, fixture.database_oid());
    assert_eq!(resource.relation_oid, fixture.relation_oid());
    assert_eq!(requested_mode, "AccessExclusiveLock");
    assert_eq!(granted_modes, &["AccessShareLock"]);
    assert_eq!(capture.records.len(), 2);
    for row in &capture.records {
        let TelemetryRecord::Activity(row) = &row.record else { panic!() };
        assert!([fixture.output_identity(), fixture.waiter_identity()].contains(&&row.key.session));
    }
}
fn output(capture: &Captured, read_complete: bool) -> OutputBlockingEvidence {
    let references = (0..capture.records.len()).map(|index| capture.reference(index).unwrap()).collect::<Vec<_>>();
    let rows = capture
        .records
        .iter()
        .zip(&references)
        .map(|(row, reference)| {
            let TelemetryRecord::Activity(observation) = &row.record else { panic!() };
            ActivityFrameRow { observation, reference: Some(reference) }
        })
        .collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(capture.source().unwrap(), &rows, read_complete).unwrap();
    culprit_core::blocking::output_blocking(&frame).unwrap()
}
fn with_diagnostic_rows<T>(capture: &Captured, inspect: impl FnOnce(&[DiagnosticActivityRow<'_>]) -> T) -> T {
    let references = (0..capture.records.len()).map(|index| capture.reference(index).unwrap()).collect::<Vec<_>>();
    let rows = capture
        .records
        .iter()
        .zip(&references)
        .map(|(row, reference)| {
            let TelemetryRecord::Activity(observation) = &row.record else { panic!() };
            DiagnosticActivityRow { reference, observation }
        })
        .collect::<Vec<_>>();
    inspect(&rows)
}
fn answer(basis: &Captured, result: &Captured, episode: &DiagnosticEpisode) -> DiagnosticRevision {
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().unwrap().target.clone(),
        input: basis.capture_ref().unwrap(),
        episodes: vec![episode.clone()],
    };
    with_diagnostic_rows(result, |rows| {
        analyze_diagnostic_blocking(
            result.source().unwrap(),
            &result.capture_ref().unwrap(),
            rows,
            &checkpoint,
            &chrono::Utc::now().to_rfc3339(),
            None,
        )
        .unwrap()
        .revisions
        .into_iter()
        .find(|revision| revision.episode.id == episode.id)
        .unwrap()
    })
}
fn resolve(basis: &Captured, recovered: &Captured, answered: &DiagnosticRevision) -> DiagnosticRevision {
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source().unwrap().target.clone(),
        input: answered.episode.latest.capture_ref(),
        episodes: vec![answered.episode.clone()],
    };
    with_diagnostic_rows(recovered, |rows| {
        observe_diagnostic_activity(
            recovered.source().unwrap(),
            &recovered.capture_ref().unwrap(),
            rows,
            Some(&checkpoint),
            &ActivityPolicy::default(),
            &chrono::Utc::now().to_rfc3339(),
        )
        .unwrap()
        .revisions
        .into_iter()
        .find(|revision| revision.episode.id == answered.episode.id)
        .unwrap()
    })
}
async fn reopen(
    captures: &OwnedCaptures,
    originals: [&Captured; 5],
    cancellation: &CollectionCancellation,
) -> SynthesisInput {
    owned::case::require_active(cancellation).unwrap();
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout).unwrap();
    let request = SynthesisRequest {
        primary: originals[0].capture_ref().unwrap(),
        context: originals[1..].iter().map(|capture| capture.capture_ref().unwrap()).collect(),
        budget,
    };
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await.unwrap()
}
async fn gold(input: &SynthesisInput, revision: &DiagnosticRevision, sequence: u64) -> DerivedDiagnosis {
    let history = history_revision(revision.clone(), sequence).await;
    let history_original = history.verified_original().unwrap();
    let native = input.captures().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>().unwrap();
    let originals = std::iter::once(&history_original as &dyn OriginalProjection)
        .chain(native.iter().map(|capture| capture as &dyn OriginalProjection))
        .collect::<Vec<_>>();
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-output-retention-acceptance-1")
        .unwrap()
        .analyze_originals(&originals, input.recorded_at(), input.display(), vec![])
        .unwrap()
}
fn verify_gold(
    diagnosis: &DerivedDiagnosis,
    focused: &Captured,
    evidence: &OutputBlockingEvidence,
    lifecycle: DiagnosticLifecycle,
) {
    diagnosis.validate().unwrap();
    let [incident] = diagnosis.incidents.as_slice() else { panic!("one authentic collateral-victim incident") };
    assert_eq!(incident.entry.lifecycle, lifecycle);
    assert_eq!(incident.affected_queries.len(), 1);
    let proof = &evidence.cases[0];
    let owner = incident
        .culprit_queries
        .iter()
        .find(|query| query.identity == Some(DerivedQueryIdentity::NativeWaitOwner(proof.owner.owner.clone())))
        .unwrap();
    assert!(!owner.sql.is_available(), "current output SQL is not the acquiring statement");
    assert!(owner.evidence.iter().any(|reference| reference.capture == focused.manifest.capture));
    assert!(
        owner.caller.as_ref().is_some_and(|caller| caller
            .client
            .as_ref()
            .and_then(|client| client.application.as_deref())
            == Some("fixture-output-consumer"))
    );
    assert!(incident.candidate_queries.iter().any(|query| query.sql.is_available() && query.causal_role.is_none()));
    assert!(incident.mechanism.is_some());
    assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
    assert!(incident.developer_action.contains(&t!("view.output_blocking.action")));
    assert!(incident.verification.contains(&t!("view.output_blocking.verify")));
    assert!(incident.entry.reasons.contains(&t!("view.output_blocking.limit.cause")));
    assert!(diagnosis.findings.is_empty(), "output retention does not establish a generic network problem");
}
#[test]
fn actual_output_consumer_proof_and_output_future_storage_stay_bounded_without_polling_native_work() {
    fn storage<F: Future>(_: impl FnOnce(&'static mut OutputConsumerFixture) -> F) -> (usize, usize) {
        (size_of::<F>(), size_of::<F::Output>())
    }
    let (body, output) = storage(prove);
    assert!(body <= 32 * 1024, "actual output proof future is {body} bytes");
    assert_eq!(output, 0);
}
