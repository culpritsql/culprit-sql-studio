use super::*;
use crate::{
    activity::{current_execution_profile::tests as native, current_request_work::tests as current},
    read::ReadError,
    resources::test_support::FakeRead,
};
use std::sync::atomic::Ordering;

async fn source_focus_result(origin: StatementOrigin, defect: Option<&str>) -> Result<Option<SourceReadFocus>> {
    let mut request = current::focus().await;
    request.original.elapsed_ms = Some("60000".into());
    request.original.wait = Some(ActivityWait {
        class: None,
        native_type: Some("CXCONSUMER".into()),
        elapsed_ms: Some("1000".into()),
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    let basis_source = CollectionDescription {
        target: request.target.clone(),
        partition: request.partition.clone(),
        source: request.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            at: request.original_observed_at.clone(),
            observed_from: request.original_observed_at.clone(),
            observed_until: request.original_observed_at.clone(),
            epoch: Some(request.epoch.clone()),
            database_writable: request.database_writable,
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(request.source.clone(), 1),
        }),
    };
    let basis = ExportedRecord::new(
        &basis_source,
        &request.parent.capture,
        TelemetryRecord::Activity(Box::new(request.original.clone())),
    )
    .unwrap();
    request.parent =
        DiagnosticEvidenceRef::from_export(&basis_source, &digest(b"controlled-parallel-original"), &basis, 0).unwrap();
    assert!(request.matches_parent(&basis_source, &request.original, &request.parent));
    // Use the existing real CurrentWork source/decoder over a separately bounded controlled prior point.
    // Its typed original clock precedes this selected read; this is structural test evidence, never cost proof.
    let mut replies = current::replies();
    for (index, response) in replies.iter_mut().enumerate() {
        let clock = match index {
            0..=2 => "2020-01-01T05:59:59.1Z",
            3 => "2020-01-01T05:59:59.2Z",
            4 => "2020-01-01T05:59:59.3Z",
            5 => "2020-01-01T05:59:59.4Z",
            6 => "2020-01-01T05:59:59.5Z",
            _ => "2020-01-01T05:59:59.9Z",
        };
        if let Ok(rows) = response {
            for row in rows {
                *row = crate::resources::test_support::replace(
                    row,
                    &[("observed_at", crate::value::Val::Text(clock.into()))],
                );
                if matches!(index, 4 | 5) {
                    let prefix = match origin {
                        StatementOrigin::Culprit => format!("/* culprit */ {}", current::SQL),
                        StatementOrigin::CulpritMonitoring => {
                            format!("/* culprit */ /* culprit:monitor */ {}", current::SQL)
                        }
                        _ => current::SQL.into(),
                    };
                    *row = crate::resources::test_support::replace(
                        row,
                        &[
                            (
                                "engine_internal",
                                if origin == StatementOrigin::Unknown {
                                    crate::value::Val::Null
                                } else {
                                    crate::value::Val::Int(i64::from(origin == StatementOrigin::EngineInternal))
                                },
                            ),
                            ("cloud_internal", crate::value::Val::Int(0)),
                            ("source_text_bytes", crate::value::Val::Int((prefix.encode_utf16().count() * 2) as i64)),
                            ("text_prefix", crate::value::Val::Text(prefix)),
                        ],
                    );
                }
            }
        }
    }
    let capture_request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::CurrentRequestWork { focus: Box::new(request.clone()) },
        budget: CollectionBudget::default(),
    };
    let mut source = super::super::current_request_work::Source::with_io(
        current::io(replies),
        &capture_request,
        "fixture.invalid".into(),
        1433,
    )
    .unwrap();
    let mut description = source.describe().await.unwrap();
    let CollectionRead::Page { mut records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 10, "pure nomination performs no additional source dispatch");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let work_capture = CaptureIdentity {
        observation_id: "controlled-parallel-work".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-parallel-outbox".into(),
            owner_epoch: "1".into(),
            sequence: "2".into(),
        }),
    };
    let mut record = records.remove(0);
    if defect == Some("partial") {
        let SourceObservation::CurrentRequestWork { snapshot, .. } = &mut description.observation else { panic!() };
        snapshot.sources = vec![StateSourceCoverage::observed(description.source.clone(), 1, false)];
    }
    if defect == Some("missing-plan") {
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::CurrentRequestWork(point),
            ..
        }) = &mut record
        else {
            panic!()
        };
        let CurrentRequestWorkNative::Mssql { work } = &mut point.native else { panic!() };
        work.current_plan = MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::NotObserved };
    }
    if defect == Some("foreign-member") {
        let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut description.partition else { panic!() };
        member.native.id = "different-native-member".into();
    }
    let work = ExportedRecord::new(&description, &work_capture, record)?;
    let mut observation =
        DiagnosticEvidenceRef::from_export(&description, &digest(b"controlled-parallel-work-manifest"), &work, 0)
            .unwrap();
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::CurrentRequestWork(point), ..
    }) = &work.record
    else {
        panic!()
    };
    let CurrentRequestWorkNative::Mssql { work: native } = &point.native else { panic!() };
    if let MssqlCurrentRequestPlan::Observed { context, .. } = &native.current_plan {
        assert_eq!(context.origin, origin);
    } else {
        assert_eq!(defect, Some("missing-plan"));
    }
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&basis_source).unwrap(),
        DiagnosticSignal::Activity {
            key: request.original.key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        request.original_observed_at.clone(),
        request.parent.clone(),
    )
    .unwrap();
    if defect == Some("foreign-receipt") {
        observation.payload_sha256 = digest(b"foreign-source-body");
    }
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: observation.clone() });
    let candidate = SourceReadFocus {
        basis: Box::new(request.parent.clone()),
        target: request.target.clone(),
        question: SourceReadQuestion::CurrentExecutionProfile,
        prerequisite: SourceReadPrerequisite::ObservedCurrentExecutionRequest {
            observation: Box::new(observation.clone()),
            request: Box::new(request),
            observed_until: description.observation.snapshot().unwrap().observed_until.clone(),
            exchange_wait: Some(MssqlParallelExchangeWait::Consumer),
        },
    };
    assert!(candidate.is_valid());
    let TelemetryRecord::Operational(operational) = &work.record else { panic!() };
    if defect != Some("foreign-receipt") {
        assert_eq!(
            candidate.matches_prerequisite_input(&basis_source, &basis, &description, [(&observation, operational)]),
            defect.is_none() && matches!(origin, StatementOrigin::Application | StatementOrigin::Unknown),
            "prerequisite consistency must apply the same source-origin refusal as nomination"
        );
    }
    // The nominal consistency predicate consumes authenticated receipts; the nomination owner below
    // separately validates the deliberately tampered body digest before admitting this prerequisite.
    SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        &description,
        &observation,
        &work,
    )
}
async fn source_focus() -> SourceReadFocus {
    source_focus_result(StatementOrigin::Unknown, None).await.unwrap().unwrap()
}
#[tokio::test]
async fn real_current_work_decoder_nominates_ad_hoc_requests_without_classifying_cache_or_tvp_origin() {
    for origin in [StatementOrigin::Application, StatementOrigin::Unknown] {
        let focus = source_focus_result(origin, None).await.unwrap().unwrap();
        assert_eq!(focus.question, SourceReadQuestion::CurrentExecutionProfile);
        assert!(focus.is_valid());
        assert!(matches!(focus.prerequisite, SourceReadPrerequisite::ObservedCurrentExecutionRequest { .. }));
    }
    for origin in [StatementOrigin::EngineInternal, StatementOrigin::Culprit, StatementOrigin::CulpritMonitoring] {
        let result = source_focus_result(origin, None).await;
        assert_eq!(CollectionStop::from_error(&result.unwrap_err()), CollectionStop::InvalidPage, "{origin:?}");
    }
}
#[tokio::test]
async fn incomplete_missing_plan_and_foreign_receipts_cannot_nominate_from_an_otherwise_exact_ad_hoc_request() {
    for defect in ["partial", "missing-plan"] {
        assert!(source_focus_result(StatementOrigin::Unknown, Some(defect)).await.unwrap().is_none(), "{defect}");
    }
    for defect in ["foreign-receipt", "foreign-member"] {
        let error = source_focus_result(StatementOrigin::Unknown, Some(defect)).await.unwrap_err();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::InvalidPage, "{defect}");
    }
}
pub(in crate::telemetry) async fn request() -> CollectionRequest {
    let focus = source_focus().await;
    let original = focus.execution_profile_request().unwrap();
    let context = NativeReadContext {
        target: original.target.clone(),
        partition: original.partition.clone(),
        observed_from: original.original_observed_at.clone(),
        observed_until: original.original_observed_at.clone(),
        database_writable: original.database_writable,
    };
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead {
            execution: Box::new(SourceReadExecutionScope::from_context(focus.question, &context).unwrap()),
            focus: Box::new(focus),
        },
        budget: CollectionBudget::default(),
    }
}
#[tokio::test]
async fn exact_frozen_original_pages_and_finish_reuse_completed_final_proof_without_extra_native_work() {
    let request = request().await;
    let io = current::io(native::replies());
    let trace = io.trace.clone();
    let mut source = CurrentExecutionProfileSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(records.len(), 1);
    assert_eq!(source.query_count(), 15);
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 15);
    assert_eq!(
        description.observation.snapshot().unwrap().coverage_state().unwrap(),
        culprit_core::SourceState::Partial
    );
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    assert_eq!(trace.reads_requiring_replacement.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn stricter_total_caller_cannot_spend_setup_allowance_on_final_identity_or_expand_declared_profile() {
    let mut request = request().await;
    request.budget.max_queries = 15;
    let reader_limit = ReadSession::budget_after_setup(request.budget.clone()).unwrap().max_queries;
    let mut io = current::io(native::replies());
    io.inner = FakeRead::new(native::replies()).with_query_limit(reader_limit);
    let trace = io.trace.clone();
    let mut source = CurrentExecutionProfileSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 14);
    assert!(source.finish().await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    let budget = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(
        (budget.max_queries, budget.max_snapshot_rows, budget.statement_timeout_ms, budget.total_timeout_ms),
        (16, 1024, 1000, 5000)
    );
}
#[tokio::test]
async fn cancellation_deadline_native_failure_and_unconfirmed_shutdown_publish_no_source_or_answer() {
    for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
        let request = request().await;
        let mut replies = native::replies();
        replies[7] = Err(ReadError::Stopped(stop));
        let io = current::io(replies);
        let trace = io.trace.clone();
        let mut source = CurrentExecutionProfileSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stop);
        assert_eq!(source.query_count(), 8);
        assert!(source.finish().await.is_err());
        assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    }
    let request = request().await;
    let mut io = current::io(native::replies());
    io.inner = FakeRead::new(native::replies()).pause_on_query(7);
    io.inner.shutdown = SourceShutdown::Unconfirmed;
    let trace = io.trace.clone();
    let mut source = CurrentExecutionProfileSource::with_io(io, &request, "fixture.invalid".into(), 1433).unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(1), source.describe()).await.is_err());
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
    assert!(source.finish().await.is_err());
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn unknown_original_role_scope_and_foreign_question_refuse_before_any_native_work() {
    for defect in ["role", "scope", "question"] {
        let mut request = request().await;
        let CollectionSelection::SourceRead { focus, .. } = &mut request.selection else { panic!() };
        let SourceReadPrerequisite::ObservedCurrentExecutionRequest { request: participant, .. } =
            &mut focus.prerequisite
        else {
            panic!()
        };
        match defect {
            "role" => participant.database_writable = None,
            "scope" => participant.partition = SourcePartition::Target,
            _ => focus.question = SourceReadQuestion::RelationalInputContext,
        };
        assert!(
            CurrentExecutionProfileSource::with_io(current::io(vec![]), &request, "fixture.invalid".into(), 1433)
                .is_err()
        );
    }
    let recipe =
        crate::telemetry::recipes().into_iter().find(|recipe| recipe.id == "mssql.current-execution-profile").unwrap();
    assert!(matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)));
    assert_eq!(recipe.budget.max_queries, 16);
    assert_eq!(recipe.version, 3);
}

#[tokio::test]
async fn safety_profile_guard_enforces_exact_budget_and_refuses_revoked_execution_profile_admission() {
    use std::sync::atomic::AtomicBool;
    let mut req = request().await;
    let recipe = crate::telemetry::recipes().into_iter().find(|r| r.id == "mssql.current-execution-profile").unwrap();
    req.budget = recipe.budget.clone();
    let CollectionSelection::SourceRead { execution, .. } = &req.selection else { panic!() };
    let execution_admission =
        RecipeExecutionAdmission::for_source_read(&recipe, execution, &req.budget).expect("builtin source-read safety");
    let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else { panic!() };
    let active = Arc::new(AtomicBool::new(true));
    let flag = active.clone();
    let profile = SourceProfileAdmission::for_builtin(
        digest(b"mssql-current-execution-profile-route"),
        *policy,
        Some(execution_admission.scope().clone()),
        req.budget.clone(),
        move || flag.load(Ordering::Acquire),
    )
    .unwrap();
    let guard = culprit_read::SourceProfileGuard::new(&req, &profile).unwrap();
    let inner = CurrentExecutionProfileSource::with_io(current::io(vec![]), &req, "fixture.invalid".into(), 1433)
        .expect("lazy guarded profile source");
    let mut source = culprit_read::GuardedTelemetrySource::new(inner, guard);
    assert_eq!(source.query_count(), 0);
    active.store(false, Ordering::Release);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 0, "revoked builtin admission refuses before any native query");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    assert!(culprit_read::SourceProfileGuard::new(&req, &profile).is_err());
}
