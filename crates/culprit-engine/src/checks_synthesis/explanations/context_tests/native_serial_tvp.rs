//! Genuine owned native TVP -> production selected reader -> unchanged local canonical originals -> shared Gold.
//! The answer is manually recorded; no cost receipt, automatic controller admission or harmful-reuse claim exists.
use super::*;
use culprit_collector::{
    analytical::test_support::native_capture::{Captured, OwnedCaptures},
    capture::SourceFactory,
    outbox::CompletedCaptureBudget,
    synthesis::{SynthesisInput, SynthesisRequest},
};
use culprit_core::{limits::OperationGuard, plan_facts::RelationalInputKind};
use culprit_evidence::original_projection::OriginalProjection;
use culprit_mssql::native_test_support::{self, APPLICATION, INPUT_ROWS, SerialInputFixture, SourceEndpoint};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::json;
use std::{
    future::Future,
    mem::{size_of, size_of_val},
};

struct Factory(SourceEndpoint);
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.0.open(request)
    }
}
#[tokio::test]
#[ignore = "requires nonce-owned loopback SQL Server with CULPRIT_MSSQL_SOURCE_TEST_PORT/PASSWORD and explicit CULPRIT_NATIVE_COST_RECEIPT_DIR"]
async fn owned_serial_tvp_current_execution_retains_real_input_proof_and_gold_without_culprit_or_stale_reuse() {
    native_test_support::run_serial_tvp_case(|mut fixture, cancellation| {
        Box::pin(async move {
            let future = prove(&mut fixture, &cancellation);
            assert!(
                size_of_val(&future) <= 32 * 1024,
                "concrete native proof future exceeds the existing bounded test phase"
            );
            Box::pin(future).await
        })
    })
    .await;
}
async fn prove(fixture: &mut SerialInputFixture<'_>, cancellation: &CollectionCancellation) -> Result<()> {
    eprintln!("owned serial TVP phase: source endpoint");
    let endpoint = fixture.endpoint()?;
    eprintln!("owned serial TVP phase: retained capture owner");
    let mut captures = OwnedCaptures::new()?;
    eprintln!("owned serial TVP phase: native actor start");
    let identity = Box::pin(fixture.start(cancellation)).await?;
    eprintln!("owned serial TVP phase: original Activity capture");
    let basis = capture(&mut captures, &endpoint, CollectionSelection::Activity, cancellation).await?;
    eprintln!("owned serial TVP phase: original participant selection");
    let participant = Box::new(request_focus(&basis, &identity)?);
    eprintln!("owned serial TVP phase: current request work");
    let work = capture(
        &mut captures,
        &endpoint,
        CollectionSelection::CurrentRequestWork { focus: participant.clone() },
        cancellation,
    )
    .await?;
    eprintln!("owned serial TVP phase: original episode and profile focus");
    let episode = activity_episode(&basis, &identity)?;
    let (episode, focus) = question(episode, &work)?;
    let execution = SourceReadExecutionScope::from_context(focus.question, &basis.activity_context()?)?;
    let selection = CollectionSelection::SourceRead { focus: focus.clone(), execution: Box::new(execution) };
    eprintln!("owned serial TVP phase: current execution profile");
    let profiled = capture(&mut captures, &endpoint, selection.clone(), cancellation).await?;
    let answer = Box::new(profile_answer(&basis, &profiled, &focus)?);
    eprintln!(
        "owned serial TVP native observation: {}",
        json!({
            "source_queries":profiled.manifest.source_queries,"binding":answer.binding,"membership":answer.membership,
            "profiles":answer.profiles,"profile_rows":answer.evidence.profile_rows,
            "profiles_limited":answer.evidence.profiles_limited,
            "original_relational_input_proof":answer.evidence.relational_inputs.as_ref().is_some_and(|input|input.proof.is_some()),
            "nodes":answer.evidence.nodes.iter().map(|node|json!({"node":node.node_id,"input_rows":node.input_rows,"relational_kind":node.operator.as_ref().and_then(|operator|operator.relational_input).map(|input|input.qualified_kind())})).collect::<Vec<_>>()
        })
    );
    native_test_support::save_native_observation(&json!({
        "assertions_passed":false,
        "purpose":"native observation before required serial input and Gold assertions",
        "manifest":profiled.manifest,"records":profiled.records,"answer":answer,
    }))?;
    require_serial_tvp(&profiled, &answer)?;
    eprintln!("owned serial TVP phase: native READONLY and closed512 proof passed; retiring actor");
    Box::pin(fixture.finish(cancellation)).await?;
    eprintln!("owned serial TVP phase: ended-request control");
    let ended = capture(&mut captures, &endpoint, selection, cancellation).await?;
    let ended_answer = Box::new(profile_answer(&basis, &ended, &focus)?);
    eprintln!(
        "owned serial TVP ended observation: {}",
        json!({
            "binding":ended_answer.binding,"membership":ended_answer.membership,
            "profile_rows":ended_answer.evidence.profile_rows,
            "relational_inputs":ended_answer.evidence.relational_inputs.is_some(),
            "source_queries":ended.manifest.source_queries,
        })
    );
    require_ended(&ended, &ended_answer)?;
    eprintln!("owned serial TVP phase: retained original verification");
    for original in [&basis, &work, &profiled, &ended] {
        require_active(cancellation)?;
        Box::pin(captures.verify_retained(original)).await?;
    }
    eprintln!("owned serial TVP phase: canonical originals reopen");
    let input = reopen(&captures, [&basis, &work, &profiled, &ended], cancellation).await?;
    eprintln!("owned serial TVP phase: positive Gold synthesis");
    let positive = Box::pin(gold(&input, &episode, &focus, &answer, 992001)).await?;
    eprintln!(
        "owned serial TVP positive Gold: {}",
        json!({
            "attribution":positive.incidents.first().map(|incident|incident.attribution_status),
            "candidates":positive.incidents.first().map(|incident|incident.candidate_queries.len()),
            "culprits":positive.incidents.first().map(|incident|incident.culprit_queries.len()),
        })
    );
    verify_gold(&positive, &basis, &profiled, &participant, &answer)?;
    eprintln!("owned serial TVP phase: ended Gold synthesis");
    let negative = Box::pin(gold(&input, &episode, &focus, &ended_answer, 992002)).await?;
    verify_ended_gold(&negative, &profiled)?;
    native_test_support::save_native_receipt(&json!({
        "basis": {"manifest":basis.manifest,"records":basis.records},
        "current_work": {"manifest":work.manifest,"records":work.records},
        "profiled": {"manifest":profiled.manifest,"records":profiled.records,"answer":answer},
        "ended": {"manifest":ended.manifest,"records":ended.records,"answer":ended_answer},
        "gold":positive,"ended_gold":negative,
        "fixture_setup_elapsed_ms":fixture.setup_elapsed_ms(),
        "fixture_dispatch_cap_including_setup_identity_warm_gates_cleanup":native_test_support::FIXTURE_DISPATCH_CAP,
        "source_queries":[basis.manifest.source_queries,work.manifest.source_queries,profiled.manifest.source_queries,ended.manifest.source_queries],
        "source_query_allowances_including_session_setup":[basis.manifest.request.budget.max_queries,work.manifest.request.budget.max_queries,profiled.manifest.request.budget.max_queries,ended.manifest.request.budget.max_queries],
        "tvp_input_rows":INPUT_ROWS,"expected_join_count":100,"case_timeout_ms":45000,"actor_lock_timeout_ms":20000,
        "activity_running_after_ms":1000,"profile_query_limit_including_setup":16,
        "manual_recorded_answer":true,"controller_native_admission":false,"native_cost_qualified":false,
        "harmful_reuse_established":false,"native_plan_or_profile_manufactured":false,
        "fixture_actor_full_profiling_explicitly_enabled":true,"normal_lightweight_scan_counters_proven":false,
        "source_originals_rebound":false,"canonical_originals_reopened_after_actor_retirement":true,
        "all_provider_equivalence_established":false
    }))?;
    Ok(())
}
async fn capture(
    captures: &mut OwnedCaptures,
    endpoint: &SourceEndpoint,
    selection: CollectionSelection,
    cancellation: &CollectionCancellation,
) -> Result<Box<Captured>> {
    require_active(cancellation)?;
    let endpoint = endpoint.clone();
    let request = endpoint.request(selection)?;
    let captured = Box::new(
        Box::pin(captures.capture("owned-serial-tvp-fixture", request, cancellation, move |_| Factory(endpoint)))
            .await?,
    );
    let metrics = captured.manifest.read_metrics.as_ref().ok_or_else(invalid)?;
    if metrics.templates.iter().any(|template| template.failures != 0) {
        return Err(invalid());
    }
    Ok(captured)
}
fn request_focus(capture: &Captured, session: &NativeActivityIdentity) -> Result<ActivityRequestFocus> {
    let source = capture.source()?;
    let native = source.observation.activity().ok_or_else(invalid)?;
    let context = capture.activity_context().inspect_err(|_| {
        eprintln!("owned serial TVP participant: native Activity context unavailable");
    })?;
    let matches = capture
        .records
        .iter()
        .enumerate()
        .filter_map(|(position, row)| match &row.record {
            TelemetryRecord::Activity(row)
                if row.key.session == *session && row.scope == ActivityScopeBasis::RequestDatabase =>
            {
                Some((position, row))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    eprintln!(
        "owned serial TVP participant: {}",
        json!({
            "matching_request_rows":matches.len(),
            "rows":capture.records.iter().filter_map(|row|match &row.record {
                TelemetryRecord::Activity(row)=>Some(json!({
                    "same_original_actor":row.key.session==*session,
                    "scope":row.scope,
                    "fixture_application":row.client.as_ref().and_then(|client|client.application.as_deref())==Some(APPLICATION),
                    "statement_identity_present":row.statement_identity.is_some(),
                })),
                _=>None,
            }).collect::<Vec<_>>(),
            "native_epoch_present":native.epoch.is_some(),
        })
    );
    let [(position, row)] = matches.as_slice() else { return Err(invalid()) };
    if row.client.as_ref().and_then(|client| client.application.as_deref()) != Some(APPLICATION) {
        return Err(invalid());
    }
    let focus = ActivityRequestFocus {
        target: context.target,
        partition: context.partition,
        source: source.source.clone(),
        database_writable: context.database_writable,
        parent: capture.reference(*position)?,
        epoch: native.epoch.clone().ok_or_else(invalid)?,
        original_observed_at: native.at.clone(),
        statement_identity: row.statement_identity.clone().ok_or_else(invalid)?,
        original: (***row).clone(),
    };
    if !CurrentRequestWorkProfile::MssqlRequest.accepts_focus(&focus) {
        return Err(invalid());
    }
    Ok(focus)
}
fn activity_episode(capture: &Captured, identity: &NativeActivityIdentity) -> Result<Box<DiagnosticEpisode>> {
    let references =
        (0..capture.records.len()).map(|position| capture.reference(position)).collect::<Result<Vec<_>>>()?;
    let rows = capture
        .records
        .iter()
        .zip(&references)
        .map(|(row, reference)| match &row.record {
            TelemetryRecord::Activity(observation) => Ok(DiagnosticActivityRow { reference, observation }),
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    // Explicit fixture policy; the elapsed value still comes from the genuine native request, never from sleep.
    let update = observe_diagnostic_activity(
        capture.source()?,
        &capture.capture_ref()?,
        &rows,
        None,
        &ActivityPolicy { blocked_after_ms: 30000, running_after_ms: 1000 },
        &chrono::Utc::now().to_rfc3339(),
    )?;
    let mut episodes = update.checkpoint.episodes.into_iter().filter(|episode| {
        matches!(&episode.signal,
        DiagnosticSignal::Activity{key,condition:ActivityCondition::LongRunning,..} if key.session==*identity)
    });
    let episode = episodes.next().ok_or_else(invalid)?;
    if episodes.next().is_some() {
        return Err(invalid());
    }
    Ok(Box::new(episode))
}
fn question(
    mut episode: Box<DiagnosticEpisode>,
    work: &Captured,
) -> Result<(Box<DiagnosticEpisode>, Box<SourceReadFocus>)> {
    let [row] = work.records.as_slice() else { return Err(invalid()) };
    let reference = work.reference(0)?;
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: reference.clone() });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        work.source()?,
        &reference,
        row,
    )?
    .ok_or_else(invalid)?;
    Ok((episode, Box::new(focus)))
}
fn profile_answer(
    basis: &Captured,
    captured: &Captured,
    focus: &SourceReadFocus,
) -> Result<CurrentExecutionProfileAnswer> {
    let original =
        basis.records.iter().find(|row| row.record.native_identity() == focus.basis.native_key).ok_or_else(invalid)?;
    let [row] = captured.records.as_slice() else { return Err(invalid()) };
    let reference = captured.reference(0)?;
    CurrentExecutionProfileAnswer::from_originals(
        focus,
        basis.source()?,
        original,
        captured.source()?,
        &captured.capture_ref()?,
        &[(&reference, row)],
    )
}
fn require_serial_tvp(capture: &Captured, answer: &CurrentExecutionProfileAnswer) -> Result<()> {
    let [row] = capture.records.as_slice() else { return Err(invalid()) };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &row.record
    else {
        return Err(invalid());
    };
    if answer.binding != FocusedRequestBinding::Matched
        || answer.membership != FocusedRequestMembership::Observed
        || answer.profiles != ExecutionProfileAvailability::Observed
        || answer.evidence.profiles_limited
        || capture.manifest.source_queries == 0
        || capture.manifest.source_queries > 16
        || capture.manifest.request.budget.max_queries != 16
        || !matches!(&point.context.target,CollectionTarget::Database(database) if database.incarnation.is_none())
        || point
            .relational_inputs
            .as_ref()
            .is_none_or(|inputs| inputs.proof.is_none() || inputs.module_parameters.is_none())
    {
        return Err(invalid());
    }
    let nodes = answer
        .evidence
        .nodes
        .iter()
        .filter(|node| {
            node.operator
                .as_ref()
                .and_then(|operator| operator.relational_input)
                .is_some_and(|input| input.qualified_kind() == RelationalInputKind::TableValuedParameter)
        })
        .collect::<Vec<_>>();
    if nodes.is_empty() || !nodes.iter().any(|node| matches!(&node.input_rows,Some(ExecutionInputRows::SerialScan{returned,..}) if returned==&INPUT_ROWS.to_string())) {
        return Err(invalid());
    }
    Ok(())
}
fn require_ended(capture: &Captured, answer: &CurrentExecutionProfileAnswer) -> Result<()> {
    if answer.membership != FocusedRequestMembership::NotObserved
        || answer.evidence.profile_rows != 0
        || answer.evidence.relational_inputs.is_some()
        || capture.manifest.source_queries > 16
    {
        return Err(invalid());
    }
    Ok(())
}
async fn reopen(
    captures: &OwnedCaptures,
    originals: [&Captured; 4],
    cancellation: &CollectionCancellation,
) -> Result<SynthesisInput> {
    require_active(cancellation)?;
    let budget = CompletedCaptureBudget::default();
    let operation = OperationGuard::new(budget.timeout)?;
    let request = SynthesisRequest {
        primary: originals[0].capture_ref()?,
        context: originals[1..].iter().map(|capture| capture.capture_ref()).collect::<Result<_>>()?,
        budget,
    };
    // This is the same production EOF/receipt worker; no native row or capture identity is copied/rebound.
    captures.delivery().with_synthesis_inputs(request, operation.handle(), (), Ok).await
}
async fn gold(
    input: &SynthesisInput,
    episode: &DiagnosticEpisode,
    focus: &SourceReadFocus,
    answer: &CurrentExecutionProfileAnswer,
    sequence: u64,
) -> Result<DerivedDiagnosis> {
    // A manual model answer is not native/controller admission. Only this new local history row is sealed by
    // the existing canonical history fixture; every native capture/row/reference below remains unchanged.
    let recorded = episode
        .clone()
        .enroll_source_read(focus.clone())?
        .transition_source_read(focus, SourceReadStageState::Collecting)?
        .transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(answer.clone()))),
            },
        )?;
    let revision = DiagnosticRevision::between(None, recorded, input.recorded_at())?.ok_or_else(invalid)?;
    let history = history_revision(revision, sequence).await;
    let history_original = history.verified_original()?;
    let native = input.captures().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
    let originals = std::iter::once(&history_original as &dyn OriginalProjection)
        .chain(native.iter().map(|capture| capture as &dyn OriginalProjection))
        .collect::<Vec<_>>();
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "native-serial-tvp-acceptance-1")?
        .analyze_originals(&originals, input.recorded_at(), input.display(), vec![])
}
fn verify_gold(
    result: &DerivedDiagnosis,
    basis: &Captured,
    profile: &Captured,
    focus: &ActivityRequestFocus,
    answer: &CurrentExecutionProfileAnswer,
) -> Result<()> {
    result.validate()?;
    let [incident] = result.incidents.as_slice() else { return Err(invalid()) };
    if incident.attribution_status != DerivedAttributionStatus::CandidatesOnly
        || !incident.culprit_queries.is_empty()
        || incident.affected_queries.is_empty()
        || incident.candidate_queries.is_empty()
        || incident.mechanism.is_none()
        || incident.developer_action.is_empty()
        || incident.verification.is_empty()
    {
        return Err(invalid());
    }
    let selected = incident.candidate_queries.iter().find(|query|matches!(&query.identity,Some(DerivedQueryIdentity::Activity{actor,..}) if actor==&focus.original.key)).ok_or_else(invalid)?;
    if selected.causal_role.is_some()
        || !selected.sql.is_available()
        || !selected.evidence.contains(&profile.reference(0)?)
        || selected.caller.as_ref().is_none_or(|caller| {
            caller.evidence.capture != basis.manifest.capture
                || caller.client.as_ref().and_then(|client| client.application.as_deref()) != Some(APPLICATION)
        })
    {
        return Err(invalid());
    }
    let [row] = profile.records.as_slice() else { return Err(invalid()) };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &row.record
    else {
        return Err(invalid());
    };
    let report = culprit_checks::current_execution_profile::explain(answer, point)?;
    if report.mechanisms.iter().any(|line| !incident.mechanism.as_ref().unwrap().contains(line))
        || !incident.developer_action.contains(&report.investigation)
        || !incident.verification.contains(&report.verify)
        || !incident.entry.reasons.contains(&t!("view.execution_profile.input.causality"))
    {
        return Err(invalid());
    }
    let roundtrip: DerivedDiagnosis =
        serde_json::from_slice(&serde_json::to_vec(result).map_err(|_| invalid())?).map_err(|_| invalid())?;
    if &roundtrip != result {
        return Err(invalid());
    }
    Ok(())
}
fn verify_ended_gold(result: &DerivedDiagnosis, positive: &Captured) -> Result<()> {
    result.validate()?;
    let [incident] = result.incidents.as_slice() else { return Err(invalid()) };
    if incident.entry.lifecycle == DiagnosticLifecycle::Resolved
        || !incident.culprit_queries.is_empty()
        || incident.attribution_status != DerivedAttributionStatus::Unestablished
        || incident.candidate_queries.iter().any(|query| query.evidence.contains(&positive.reference(0).unwrap()))
    {
        return Err(invalid());
    }
    Ok(())
}
fn require_active(cancellation: &CollectionCancellation) -> Result<()> {
    if cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
#[test]
fn actual_serial_tvp_proof_future_type_and_output_stay_bounded_without_polling_native_work() {
    fn storage<F: Future>(
        _: impl FnOnce(&'static mut SerialInputFixture<'static>, &'static CollectionCancellation) -> F,
    ) -> (usize, usize) {
        (size_of::<F>(), size_of::<F::Output>())
    }
    let (body, output) = storage(prove);
    eprintln!("actual serial TVP proof future storage: body={body},output={output}");
    assert!(body <= 32 * 1024, "concrete proof future is {body} bytes");
    assert!(output <= 128, "concrete proof output is {output} bytes");
}
