//! Owned native TABLE tickets -> production deep reader -> unchanged canonical originals -> builtin Gold.
//! The answer is recorded manually because the new37 recipe remains Unqualified; no admission is fabricated.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisInput, SynthesisRequest},
};
use culprit_core::{Provider, blocking::*, limits::OperationGuard};
use culprit_evidence::original_projection::OriginalProjection;
use culprit_mysql::{
    MysqlProvider,
    native_test_support::{self as owned, BlockingFixture},
};
use culprit_read::cancellation::CollectionCancellation;
use std::{
    future::Future,
    mem::{size_of, size_of_val},
};
struct Factory(Arc<MysqlProvider>);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.telemetry_source(request)
    }
}
#[tokio::test]
#[ignore = "requires existing nonce-owned MySQL8.4/MariaDB11.8 supervisor, already-enabled native current/MDL instruments and private safety receipt directory"]
async fn owned_metadata_ticket_context_retains_three_native_actors_canonical_eof_and_candidate_only_gold_with_ended_refusal()
 {
    BlockingFixture::run(|fixture| {
        Box::pin(async move {
            let work = prove(fixture);
            assert!(size_of_val(&work) <= 64 * 1024, "actual owned deep proof future exceeds64KiB");
            Box::pin(work).await.unwrap();
        })
    })
    .await;
}
async fn prove(fixture: &mut BlockingFixture) -> Result<()> {
    let cancellation = fixture.cancellation.clone();
    active(&cancellation)?;
    let native_cancel = Box::pin(fixture.source_cancellation()).await;
    eprintln!("owned metadata context phase: observe prerequisites and prepare three actors");
    Box::pin(fixture.prepare_metadata_context()).await?;
    Box::pin(fixture.wait_for_metadata_context()).await?;
    let provider = Arc::new(MysqlProvider::new(&fixture.specification)?);
    let mut captures = OwnedCaptures::new()?;
    eprintln!("owned metadata context phase: actual baseline");
    let basis = capture(&mut captures, provider.clone(), CollectionSelection::Activity, &cancellation).await?;
    if basis.manifest.source_queries != 20 || basis.manifest.request.budget.max_queries != 20 {
        return Err(invalid());
    }
    let (episode, focus) = original_question(&basis, fixture.metadata_victim_id())?;
    let execution = SourceReadExecutionScope::from_context(focus.question, &basis.activity_context()?)?;
    let selection = CollectionSelection::SourceRead { focus: focus.clone(), execution: Box::new(execution) };
    eprintln!("owned metadata context phase: deep37 capture and native proof");
    let selected = capture(&mut captures, provider.clone(), selection.clone(), &cancellation).await?;
    owned::write_receipt("metadata-ticket-observed.json",&serde_json::json!({
        "assertions_passed":false,"purpose":"actual original/source facts before required native candidate and Gold assertions",
        "basis":{"manifest":basis.manifest,"records":basis.records},
        "selected":{"manifest":selected.manifest,"records":selected.records},
        "fixture":fixture.metadata_fixture_accounting(),"native_cost_qualified":false
    })).map_err(|_|invalid())?;
    let answer = Box::new(native_answer(&basis, &selected, &focus)?);
    require_native_context(fixture, &selected, &answer)?;
    eprintln!("owned metadata context phase: strict36 refusal");
    let strict = Box::pin(fixture.strict_metadata_caller_refusal(&selected.manifest.request)).await?;
    eprintln!("owned metadata context phase: release original actors and capture ended refusal");
    Box::pin(fixture.release_and_complete()).await;
    let ended = capture(&mut captures, provider, selection, &cancellation).await?;
    let ended_answer = Box::new(native_answer(&basis, &ended, &focus)?);
    if !ended.records.is_empty()
        || !ended_answer.evidence.candidates.is_empty()
        || !ended_answer.evidence.cascades.is_empty()
        || ended.manifest.source_queries > 37
        || ended.manifest.request.budget.max_queries != 37
    {
        return Err(invalid());
    }
    for original in [&basis, &selected, &ended] {
        active(&cancellation)?;
        Box::pin(captures.verify_retained(original)).await?;
    }
    eprintln!("owned metadata context phase: source-free canonical reopening and builtin Gold");
    let input = reopen(&captures, [&basis, &selected, &ended], &cancellation).await?;
    let positive = Box::pin(gold(&input, &episode, &focus, &answer, 993001)).await?;
    verify_gold(&positive, &selected, &answer, true)?;
    let negative = Box::pin(gold(&input, &episode, &focus, &ended_answer, 993002)).await?;
    verify_gold(&negative, &ended, &ended_answer, false)?;
    for original in [&basis, &selected, &ended] {
        Box::pin(captures.verify_retained(original)).await?;
    }
    owned::write_receipt("metadata-ticket-context.json",&serde_json::json!({
        "assertions_passed":true,"basis":{"manifest":basis.manifest,"records":basis.records},
        "selected":{"manifest":selected.manifest,"records":selected.records,"answer":answer},
        "ended":{"manifest":ended.manifest,"records":ended.records,"answer":ended_answer},
        "gold":positive,"ended_gold":negative,"fixture":fixture.metadata_fixture_accounting(),
        "source_queries_including_setup":[basis.manifest.source_queries,selected.manifest.source_queries,ended.manifest.source_queries],
        "source_query_allowances_including_setup":[20,37,37],"strict36_metrics":strict,
        "shared_native_read_cancellation_control":native_cancel,"manual_recorded_answer":true,
        "builtin_pipeline_and_rules":true,"controller_native_admission":false,"measured_cpu":null,"cost_qualification":false,
        "pending_blocker_pointer_exposed":false,"established_sandwich":false,"source_originals_rebound":false,
        "canonical_empty_ended_original":true,"post_retirement_analysis_native_queries":0
    })).map_err(|_|invalid())?;
    Ok(())
}
fn request(selection: CollectionSelection) -> Result<CollectionRequest> {
    let mut request =
        CollectionRequest { subject: CollectionSubject::Database, selection, budget: CollectionBudget::default() };
    let recipe =
        culprit_mysql::telemetry_recipes().into_iter().find(|recipe| recipe.accepts(&request)).ok_or_else(invalid)?;
    if matches!(&request.selection, CollectionSelection::SourceRead { .. })
        && (recipe.id != "mysql.metadata-lock-context"
            || !matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_))
            || recipe.budget.max_queries != 37)
    {
        return Err(invalid());
    }
    request.budget = request.budget.intersection(&recipe.budget);
    request.budget.page_rows = 1;
    request.validate()?;
    Ok(request)
}
async fn capture(
    captures: &mut OwnedCaptures,
    provider: Arc<MysqlProvider>,
    selection: CollectionSelection,
    cancellation: &CollectionCancellation,
) -> Result<Box<Captured>> {
    active(cancellation)?;
    let captured = Box::new(
        Box::pin(
            captures.capture("owned-metadata-context", request(selection)?, cancellation, move |_| Factory(provider)),
        )
        .await?,
    );
    if captured.manifest.status != CollectionStatus::Complete {
        return Err(invalid());
    }
    Ok(captured)
}
fn original_question(basis: &Captured, victim: u32) -> Result<(Box<DiagnosticEpisode>, Box<SourceReadFocus>)> {
    let refs = (0..basis.records.len()).map(|position| basis.reference(position)).collect::<Result<Vec<_>>>()?;
    let rows = basis
        .records
        .iter()
        .zip(&refs)
        .map(|(row, reference)| match &row.record {
            TelemetryRecord::Activity(observation) => Ok(DiagnosticActivityRow { reference, observation }),
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let update = observe_diagnostic_activity(
        basis.source()?,
        &basis.capture_ref()?,
        &rows,
        None,
        &ActivityPolicy::default(),
        &chrono::Utc::now().to_rfc3339(),
    )?;
    let mut selected = update.checkpoint.episodes.into_iter().filter(
        |episode| matches!(&episode.signal,DiagnosticSignal::LockWait {key} if key.session.id==victim.to_string()),
    );
    let episode = Box::new(selected.next().ok_or_else(invalid)?);
    if selected.next().is_some() {
        return Err(invalid());
    }
    let (position, row) = basis
        .records
        .iter()
        .enumerate()
        .find(|(_, row)| row.record.native_identity() == episode.first.native_key)
        .ok_or_else(invalid)?;
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::MetadataLockContext,
        &episode,
        basis.source()?,
        &basis.reference(position)?,
        row,
    )?
    .ok_or_else(invalid)?;
    Ok((episode, Box::new(focus)))
}
fn native_answer(basis: &Captured, result: &Captured, focus: &SourceReadFocus) -> Result<MetadataLockContextAnswer> {
    let original =
        basis.records.iter().find(|row| row.record.native_identity() == focus.basis.native_key).ok_or_else(invalid)?;
    let refs = (0..result.records.len()).map(|position| result.reference(position)).collect::<Result<Vec<_>>>()?;
    let rows = refs.iter().zip(&result.records).collect::<Vec<_>>();
    MetadataLockContextAnswer::from_originals(
        focus,
        basis.source()?,
        original,
        result.source()?,
        &result.capture_ref()?,
        &rows,
    )
}
fn require_native_context(
    fixture: &BlockingFixture,
    capture: &Captured,
    answer: &MetadataLockContextAnswer,
) -> Result<()> {
    if capture.manifest.source_queries != 37
        || capture.manifest.request.budget.max_queries != 37
        || capture.manifest.request.budget.page_rows != 1
        || answer.evidence.status != MetadataQueueStatus::Unavailable
        || !answer.evidence.cascades.is_empty()
        || answer.evidence.candidates.len() != 1
        || !answer.evidence.issues.contains(&MetadataQueueIssue::PendingBlockerNotExposed)
    {
        return Err(invalid());
    }
    let case = &answer.evidence.candidates[0];
    if case.resource.schema != "culprit_smoke"
        || case.resource.name != fixture.metadata_table()
        || case.affected.session.id != fixture.metadata_victim_id().to_string()
        || !case.pending_exclusive.iter().any(|actor| actor.actor.session.id == fixture.waiter_id.to_string())
        || !case.granted_holders.iter().any(|actor| actor.actor.session.id == fixture.holder_id.to_string())
    {
        return Err(invalid());
    }
    capture.with_activity_frame(|frame| {
        let SourceObservation::MetadataLockContext { result, .. } = &frame.source().observation else {
            return Err(invalid());
        };
        let point = result.point(frame).ok_or_else(invalid)?;
        if !point.subject_matches || !point.subject_settled || !point.snapshot_complete || !result.accepts_frame(frame)
        {
            return Err(invalid());
        }
        for id in [fixture.metadata_victim_id(), fixture.waiter_id, fixture.holder_id] {
            if !point.tickets.iter().any(|ticket| {
                ticket.owner_process_id.as_deref() == Some(id.to_string().as_str()) && ticket.is_qualified_actor()
            }) {
                return Err(invalid());
            }
        }
        if point.tickets.iter().any(|ticket| {
            ticket.actor.as_ref().is_some_and(|actor| actor != &point.subject)
                && ticket.duration == "TRANSACTION"
                && ticket.transaction.as_ref().is_none_or(|event| !event.is_active())
        }) && !answer.evidence.issues.contains(&MetadataQueueIssue::NativeLifetimeNotRecorded)
        {
            return Err(invalid());
        }
        let holder = frame
            .rows()
            .find(|row| row.observation.key.session.id == fixture.holder_id.to_string())
            .ok_or_else(invalid)?;
        if !holder.observation.idle || holder.observation.statement.is_available() {
            return Err(invalid());
        }
        Ok(())
    })
}
async fn reopen(
    captures: &OwnedCaptures,
    originals: [&Captured; 3],
    cancellation: &CollectionCancellation,
) -> Result<SynthesisInput> {
    active(cancellation)?;
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout)?;
    captures
        .delivery()
        .with_synthesis_inputs(
            SynthesisRequest {
                primary: originals[0].capture_ref()?,
                context: originals[1..].iter().map(|capture| capture.capture_ref()).collect::<Result<_>>()?,
                budget,
            },
            operation.handle(),
            (),
            Ok,
        )
        .await
}
async fn gold(
    input: &SynthesisInput,
    episode: &DiagnosticEpisode,
    focus: &SourceReadFocus,
    answer: &MetadataLockContextAnswer,
    sequence: u64,
) -> Result<DerivedDiagnosis> {
    let recorded = episode
        .clone()
        .enroll_source_read(focus.clone())?
        .transition_source_read(focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::MetadataLockContext(Box::new(answer.clone()))),
            },
        )?;
    let revision = DiagnosticRevision::between(None, recorded, input.recorded_at())?.ok_or_else(invalid)?;
    let history = history_revision(revision, sequence).await;
    let history_original = history.verified_original()?;
    let native = input.captures().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
    let originals = std::iter::once(&history_original as &dyn OriginalProjection)
        .chain(native.iter().map(|capture| capture as &dyn OriginalProjection))
        .collect::<Vec<_>>();
    ChecksSynthesizer::new(
        Arc::new(culprit_checks::Pipeline::builtin()),
        crate::checks_synthesis::BUILTIN_RULES_VERSION,
    )?
    .analyze_originals(&originals, input.recorded_at(), input.display(), vec![])
}
fn verify_gold(
    output: &DerivedDiagnosis,
    selected: &Captured,
    answer: &MetadataLockContextAnswer,
    positive: bool,
) -> Result<()> {
    output.validate()?;
    let [incident] = output.incidents.as_slice() else {
        return Err(invalid());
    };
    if !incident.culprit_queries.is_empty()
        || incident.affected_queries.is_empty()
        || incident.entry.lifecycle == DiagnosticLifecycle::Resolved
    {
        return Err(invalid());
    }
    if positive {
        if incident.attribution_status != DerivedAttributionStatus::CandidatesOnly
            || incident.candidate_queries.len() < 2
            || incident.mechanism.is_none()
            || incident.developer_action.is_empty()
            || incident.verification.is_empty()
        {
            return Err(invalid());
        }
        if !incident.affected_queries.iter().any(|query| {
            query.sql.is_available()
                && query.caller.is_some()
                && query.evidence.iter().any(|reference| reference.capture == selected.manifest.capture)
        }) {
            return Err(invalid());
        }
        for actor in answer
            .evidence
            .candidates
            .iter()
            .flat_map(|case| case.pending_exclusive.iter().chain(&case.granted_holders))
        {
            let query=incident.candidate_queries.iter().find(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity {actor:key,..}) if key==&actor.actor)).ok_or_else(invalid)?;
            if query.causal_role.is_some()
                || query
                    .caller
                    .as_ref()
                    .is_none_or(|caller| caller.client.as_ref().and_then(|client| client.user.as_ref()).is_none())
                || !query.evidence.iter().any(|reference| reference.capture == selected.manifest.capture)
            {
                return Err(invalid());
            }
        }
    } else if incident.attribution_status != DerivedAttributionStatus::Unestablished
        || !incident.candidate_queries.is_empty()
    {
        return Err(invalid());
    }
    if !incident.entry.reasons.contains(&culprit_core::t!("view.metadata_queue.issue.pending_native")) {
        return Err(invalid());
    }
    Ok(())
}
fn active(cancellation: &CollectionCancellation) -> Result<()> {
    if cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
#[test]
fn actual_deep_metadata_proof_future_and_output_are_bounded_without_polling_native_work() {
    fn storage<F: Future>(_: impl FnOnce(&'static mut BlockingFixture) -> F) -> (usize, usize) {
        (size_of::<F>(), size_of::<F::Output>())
    }
    let (future, output) = storage(prove);
    eprintln!("actual owned deep metadata future: body={future},output={output}");
    assert!(future <= 64 * 1024);
    assert!(output <= 256);
}
