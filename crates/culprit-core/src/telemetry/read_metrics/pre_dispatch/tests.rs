use super::*;

fn failure() -> ReadConnectionOutcome {
    ReadConnectionOutcome::Failed { code: crate::ErrorCode::ConnectionFailed, native_code: None }
}
fn failed_roster(count: usize) -> ReadWorkMetrics {
    let recorder = ReadConnectionRecorder::default();
    for _ in 0..count {
        let attempt = recorder.begin();
        attempt.connection_use(ReadConnectionUse::FreshConnection);
        let acquire = attempt.phase(ReadConnectionStage::Acquire, None);
        attempt.phase(ReadConnectionStage::RouteSelection, None).finish(ReadConnectionOutcome::Completed);
        attempt.phase(ReadConnectionStage::Transport, None).finish(failure());
        acquire.finish(failure());
    }
    let mut metrics = ReadMetricsRecorder::new(crate::Engine::Mssql).snapshot().unwrap();
    metrics.connection_work = recorder.snapshot().map(Box::new);
    metrics
}
fn unavailable_native() -> NativeSessionReadWork {
    NativeSessionReadWork::MssqlSession(Box::new(MssqlSessionReadWork {
        qualification_work: None,
        settlement: NativeReadSettlement::Quiescent,
        issue: Some(NativeReadWorkIssue::NativeScopeUnqualified),
        interval: None,
    }))
}
fn add_phase(metrics: &mut ReadWorkMetrics, stage: ReadConnectionStage, outcome: ReadConnectionOutcome) {
    let attempt = &mut metrics.connection_work.as_mut().unwrap().attempts[0];
    let mut phase = attempt.phases.last().unwrap().clone();
    phase.stage = stage;
    phase.template_id = (stage == ReadConnectionStage::FirstQuery).then(|| digest(b"first-query"));
    phase.outcome = outcome;
    attempt.phases.push(phase);
}

#[test]
fn complete_failed_fresh_acquisitions_need_no_dispatched_work_or_native_counter_record() {
    for count in [1, MAX_READ_CONNECTION_ATTEMPTS] {
        for native in [None, Some(Box::new(unavailable_native()))] {
            let mut metrics = failed_roster(count);
            metrics.native_session_work = native;
            let original = metrics.clone();
            assert!(metrics.is_valid());
            assert!(metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
            assert_eq!(metrics, original, "the proof cannot rewrite metrics or a native accounting issue");
            let replay: ReadWorkMetrics = serde_json::from_slice(&serde_json::to_vec(&metrics).unwrap()).unwrap();
            assert_eq!(replay, metrics);
            assert!(replay.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
        }
    }
}

#[test]
fn the_actual_source_owner_must_positively_confirm_quiescence() {
    let metrics = failed_roster(1);
    for settlement in [SourceWorkState::NotStarted, SourceWorkState::Active, SourceWorkState::Unconfirmed] {
        assert!(!metrics.proves_pre_dispatch_connection_failure(settlement), "{settlement:?}");
    }
}

#[test]
fn every_acquisition_must_be_fresh_failed_and_without_an_acquired_reader() {
    type Alter = fn(&mut ReadConnectionAttempt);
    for (name, alter) in [
        (
            "unknown use",
            (|attempt: &mut ReadConnectionAttempt| attempt.connection_use = ReadConnectionUse::Unknown) as Alter,
        ),
        ("reuse", |attempt| {
            attempt.connection_use = ReadConnectionUse::AuthenticatedReuse;
            attempt.phases.truncate(1);
        }),
        ("generation", |attempt| attempt.reader_generation = Some("1".into())),
        ("completed", |attempt| attempt.phases[0].outcome = ReadConnectionOutcome::Completed),
        ("stopped", |attempt| {
            attempt.phases[0].outcome = ReadConnectionOutcome::Stopped { reason: CollectionStop::Cancelled }
        }),
        ("malformed", |attempt| attempt.ordinal = 0),
    ] {
        let mut metrics = failed_roster(2);
        alter(&mut metrics.connection_work.as_mut().unwrap().attempts[1]);
        assert_eq!(metrics.is_valid(), name != "malformed", "{name}");
        assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent), "{name}");
    }
}

#[test]
fn completed_login_setup_query_and_redirect_are_not_pre_dispatch_failure_proof() {
    for (stage, outcome) in [
        (ReadConnectionStage::HandshakeAndLogin, ReadConnectionOutcome::Completed),
        (ReadConnectionStage::HandshakeAndLogin, ReadConnectionOutcome::Redirected),
        (ReadConnectionStage::SessionSetup, failure()),
        (ReadConnectionStage::SessionSetup, ReadConnectionOutcome::Completed),
        (ReadConnectionStage::FirstQuery, failure()),
    ] {
        let mut metrics = failed_roster(1);
        add_phase(&mut metrics, stage, outcome);
        if stage == ReadConnectionStage::FirstQuery {
            let attempt = &mut metrics.connection_work.as_mut().unwrap().attempts[0];
            attempt.reader_generation = Some("1".into());
            attempt.phases[0].outcome = ReadConnectionOutcome::Completed;
        }
        assert!(metrics.is_valid(), "{stage:?}");
        assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent), "{stage:?}");
    }
    let mut failed_login = failed_roster(1);
    add_phase(&mut failed_login, ReadConnectionStage::HandshakeAndLogin, failure());
    assert!(failed_login.is_valid());
    assert!(failed_login.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    let NativeSessionReadWork::MssqlSession(mut native) = unavailable_native();
    native.settlement = NativeReadSettlement::Unconfirmed;
    failed_login.native_session_work = Some(Box::new(NativeSessionReadWork::MssqlSession(native)));
    assert!(!failed_login.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
}

#[test]
fn omitted_active_unconfirmed_and_absent_connection_observations_are_not_complete_proof() {
    let mut missing = failed_roster(1);
    missing.connection_work = None;
    assert!(!missing.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    assert!(!failed_roster(0).proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    let omitted = failed_roster(MAX_READ_CONNECTION_ATTEMPTS + 1);
    assert!(omitted.is_valid());
    assert!(!omitted.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    let recorder = ReadConnectionRecorder::default();
    let attempt = recorder.begin();
    attempt.connection_use(ReadConnectionUse::FreshConnection);
    let phase = attempt.phase(ReadConnectionStage::Acquire, None);
    let mut metrics = failed_roster(1);
    metrics.connection_work = recorder.snapshot().map(Box::new);
    assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    drop(phase);
    metrics.connection_work = recorder.snapshot().map(Box::new);
    assert!(metrics.is_valid());
    assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    for _ in 0..MAX_READ_CONNECTION_PHASES + 1 {
        attempt.phase(ReadConnectionStage::RouteSelection, None).finish(ReadConnectionOutcome::Completed);
    }
    metrics.connection_work = recorder.snapshot().map(Box::new);
    assert!(metrics.connection_work.as_ref().unwrap().omitted_phases > 0);
    assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
}

#[test]
fn any_dispatched_active_or_untracked_read_work_withholds_the_proof() {
    let mut active = failed_roster(1);
    active.active_tasks = 1;
    assert!(!active.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    let mut untracked = failed_roster(1);
    untracked.untracked_tasks = 1;
    assert!(!untracked.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    let recorder = ReadMetricsRecorder::new(crate::Engine::Mssql);
    let template = recorder.begin("SELECT /* culprit */ 1");
    recorder.finish(
        template.as_deref(),
        std::time::Duration::ZERO,
        &ReadProgress::default(),
        &ReadTaskOutcome::default(),
    );
    let mut dispatched = recorder.snapshot().unwrap();
    dispatched.connection_work = failed_roster(1).connection_work;
    assert!(dispatched.is_valid());
    assert!(!dispatched.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
}

#[test]
fn unavailable_native_work_must_not_hide_intervals_qualification_or_another_unknown_state() {
    for issue in [
        NativeReadWorkIssue::MissingSession,
        NativeReadWorkIssue::MissingCounters,
        NativeReadWorkIssue::InvalidObservation,
        NativeReadWorkIssue::SourceChanged,
        NativeReadWorkIssue::SessionChanged,
        NativeReadWorkIssue::ConnectionChanged,
        NativeReadWorkIssue::CounterDecreased,
        NativeReadWorkIssue::ClockChanged,
        NativeReadWorkIssue::MissingFinalObservation,
    ] {
        let mut metrics = failed_roster(1);
        let NativeSessionReadWork::MssqlSession(mut native) = unavailable_native();
        native.issue = Some(issue);
        metrics.native_session_work = Some(Box::new(NativeSessionReadWork::MssqlSession(native)));
        assert!(metrics.is_valid());
        assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent), "{issue:?}");
    }
    for settlement in [NativeReadSettlement::Pending, NativeReadSettlement::Unconfirmed] {
        let mut metrics = failed_roster(1);
        let NativeSessionReadWork::MssqlSession(mut native) = unavailable_native();
        native.settlement = settlement;
        metrics.native_session_work = Some(Box::new(NativeSessionReadWork::MssqlSession(native)));
        assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
    }
    let mut metrics = failed_roster(1);
    let NativeSessionReadWork::MssqlSession(mut native) = unavailable_native();
    let mut context = crate::telemetry::native_read_context::tests::read_context();
    context.observed_until = context.observed_from.clone();
    native.interval = Some(Box::new(MssqlSessionReadInterval {
        context,
        session: NativeActivityIdentity { id: "71".into(), generation: "2020-01-01T10:00:00.0030000".into() },
        reader_generation: "1".into(),
        initial: MssqlSessionReadPoint {
            query_ordinal: 0,
            cpu_ms: None,
            reads: None,
            logical_reads: None,
            dirty_marks: None,
            memory_pages: None,
        },
        latest: None,
    }));
    metrics.native_session_work = Some(Box::new(NativeSessionReadWork::MssqlSession(native)));
    assert!(metrics.is_valid());
    assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));

    let empty = ReadMetricsRecorder::new(crate::Engine::Mssql).snapshot().unwrap();
    let phase =
        || ReadPhaseAccounting { queries: 0, metrics: Box::new(empty.clone()), session_setup: Box::new(empty.clone()) };
    let NativeSessionReadWork::MssqlSession(mut native) = unavailable_native();
    native.qualification_work = Some(Box::new(MssqlQualificationReadWork {
        aggregate_budget: CollectionBudget::default(),
        preparation: phase(),
        observer: phase(),
    }));
    metrics.native_session_work = Some(Box::new(NativeSessionReadWork::MssqlSession(native)));
    assert!(metrics.is_valid());
    assert!(!metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent));
}
