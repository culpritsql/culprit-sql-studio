use super::*;
use crate::{DatabaseIdentity, Engine, MeasurementScope, SourceState};

struct Fixture {
    basis_source: CollectionDescription,
    basis: ExportedRecord,
    work_source: CollectionDescription,
    work: ExportedRecord,
    work_ref: DiagnosticEvidenceRef,
    focus: SourceReadFocus,
    episode: DiagnosticEpisode,
}
fn capture(id: &str, sequence: &str) -> CaptureIdentity {
    CaptureIdentity {
        observation_id: id.into(),
        revision: Some(CaptureRevision {
            outbox_id: "owned-relational-outbox".into(),
            owner_epoch: "1".into(),
            sequence: sequence.into(),
        }),
    }
}
fn cache() -> CachedStatementContext {
    CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: "0xCCDD".into(),
            sql_handle: "0xAABB".into(),
            statement_start_bytes: 0,
            statement_end_bytes: -1,
            created_without_offset: "2020-01-01T11:59:00.0030000".into(),
            plan_generation: "9007199254740993".into(),
        },
        database_id: 7,
        query_hash: None,
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: None,
        source_text_complete: false,
    }
}
fn fixture() -> Fixture {
    fixture_with_updateability(true)
}
fn fixture_with_updateability(database_writable: bool) -> Fixture {
    let mut context = crate::telemetry::native_read_context::tests::read_context();
    context.target = DatabaseIdentity::server(Engine::Mssql, "sample", "owned.invalid", 1433, None).into();
    context.observed_from = "2020-01-01T12:00:00Z".into();
    context.observed_until = context.observed_from.clone();
    context.database_writable = Some(database_writable);
    let mut actor = crate::telemetry::activity::tests::request("42");
    actor.key = ActivityKey {
        session: NativeActivityIdentity { id: "42".into(), generation: "2020-01-01T10:00:00.0030000".into() },
        request: Some(NativeActivityIdentity { id: "0".into(), generation: "2020-01-01T11:59:00.0030000".into() }),
        transaction: None,
    };
    actor.statement_identity = mssql_request_statement_identity("AABB", 0, -1, 7);
    actor.database = Some("sample".into());
    actor.scope = ActivityScopeBasis::RequestDatabase;
    actor.wait = None;
    actor.idle = false;
    actor.state = "running".into();
    actor.elapsed_ms = Some("120000".into());
    let basis_source = CollectionDescription {
        target: context.target.clone(),
        partition: context.partition.clone(),
        source: crate::source_id::SESSIONS.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            at: context.observed_until.clone(),
            observed_from: context.observed_from.clone(),
            observed_until: context.observed_until.clone(),
            epoch: context.partition.native_epoch().map(str::to_owned),
            database_writable: Some(database_writable),
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(crate::source_id::SESSIONS.into(), 1),
        }),
    };
    let basis = ExportedRecord::new(
        &basis_source,
        &capture("adverse-original", "1"),
        TelemetryRecord::Activity(Box::new(actor.clone())),
    )
    .unwrap();
    let basis_ref =
        DiagnosticEvidenceRef::from_export(&basis_source, &digest(b"adverse-original-manifest"), &basis, 0).unwrap();
    let request = ActivityRequestFocus {
        parent: basis_ref.clone(),
        target: basis_source.target.clone(),
        partition: basis_source.partition.clone(),
        source: basis_source.source.clone(),
        database_writable: Some(database_writable),
        epoch: basis_source.partition.native_epoch().unwrap().into(),
        original_observed_at: context.observed_until,
        statement_identity: actor.statement_identity.clone().unwrap(),
        original: actor.clone(),
    };
    let point = CurrentRequestWorkObservation {
        observed_at: "2020-01-01T12:00:01.1Z".into(),
        actor: actor.key.clone(),
        database: "sample".into(),
        scope: ActivityScopeBasis::RequestDatabase,
        statement_identity: request.statement_identity.clone(),
        statement: actor.statement.clone(),
        native: CurrentRequestWorkNative::Mssql {
            work: Box::new(MssqlCurrentRequestWork {
                native_database_id: 7,
                checked_until: "2020-01-01T12:00:01.6Z".into(),
                elapsed_ms: Some(120000),
                cpu_ms: Some(0),
                reads: Some("0".into()),
                writes: Some("0".into()),
                logical_reads: None,
                rows_returned: None,
                request_plan: Some(
                    NativeIdentity::new("CCDD").qualified("native_object", "sys.dm_exec_requests.plan_handle"),
                ),
                current_plan: MssqlCurrentRequestPlan::Observed {
                    observed_from: "2020-01-01T12:00:01.2Z".into(),
                    observed_until: "2020-01-01T12:00:01.4Z".into(),
                    context: Box::new(cache()),
                    plan: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
                },
            }),
        },
    };
    let work_source = CollectionDescription {
        target: basis_source.target.clone(),
        partition: basis_source.partition.clone(),
        source: crate::source_id::MSSQL_CURRENT_REQUEST_WORK.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::CurrentRequestWork {
            profile: CurrentRequestWorkProfile::MssqlRequest,
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::CurrentRequestWork,
                observed_from: "2020-01-01T12:00:01Z".into(),
                observed_until: "2020-01-01T12:00:02Z".into(),
                records: 1,
                sources: vec![StateSourceCoverage::collected(crate::source_id::MSSQL_CURRENT_REQUEST_WORK.into(), 1)],
            },
            result: Box::new(FocusedRequestResult {
                focus: Some(Box::new(request)),
                database_writable: Some(database_writable),
                binding: FocusedRequestBinding::Matched,
                membership: FocusedRequestMembership::Observed,
            }),
        },
    };
    let key = point.key().unwrap();
    let work = ExportedRecord::new(
        &work_source,
        &capture("current-native-point", "2"),
        TelemetryRecord::Operational(OperationalObservation {
            source: work_source.source.clone(),
            key,
            fact: OperationalFact::CurrentRequestWork(Box::new(point)),
        }),
    )
    .unwrap();
    let work_ref =
        DiagnosticEvidenceRef::from_export(&work_source, &digest(b"current-native-manifest"), &work, 0).unwrap();
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&basis_source).unwrap(),
        DiagnosticSignal::Activity {
            key: actor.key,
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        "2020-01-01T12:00:00Z".into(),
        basis_ref,
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: work_ref.clone() });
    assert!(episode.is_valid());
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::RelationalInputContext,
        &episode,
        &work_source,
        &work_ref,
        &work,
    )
    .unwrap()
    .unwrap();
    Fixture { basis_source, basis, work_source, work, work_ref, focus, episode }
}
fn result(
    f: &Fixture,
    generation: Option<&str>,
) -> (CollectionDescription, DiagnosticCaptureRef, Vec<(DiagnosticEvidenceRef, ExportedRecord)>) {
    let context = NativeReadContext {
        target: f.focus.target.clone(),
        partition: f.basis_source.partition.clone(),
        observed_from: "2020-01-01T12:00:03Z".into(),
        observed_until: "2020-01-01T12:00:04Z".into(),
        database_writable: Some(true),
    };
    let source = CollectionDescription {
        target: f.focus.target.clone(),
        partition: SourcePartition::Target,
        source: "retained-plans-state".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::RetainedPlans,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: u32::from(generation.is_some()),
                sources: vec![StateSourceCoverage::observed(
                    crate::source_id::RETAINED_PLANS.into(),
                    u32::from(generation.is_some()),
                    false,
                )],
            },
            execution: Box::new(context),
            focus_key: f.focus.key().unwrap(),
        },
    };
    let capture = capture("selected-retained-original", "3");
    let rows = generation
        .map(|generation| {
            let mut context = cache();
            context.identity.plan_generation = generation.into();
            let missing = MssqlCachedPayload {
                source_utf16_bytes: None,
                native_sha256: None,
                payload: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
            };
            let definition = MssqlRetainedPlanDefinition {
                context,
                statement: missing.clone(),
                module_parameters: None,
                current_type_schema: None,
                relational_input_proof: None,
                parameter_context_proof: None,
                statement_executions: Some("0".into()),
                last_start_without_offset: None,
                database_last_query_plan_stats: None,
                batch_plan: missing,
            };
            let record = TelemetryRecord::ObjectDefinition(
                ObjectDefinitionRecord::new(
                    crate::source_id::RETAINED_PLANS.into(),
                    ObjectDefinitionFact::MssqlRetainedPlan(Box::new(definition)),
                )
                .unwrap(),
            );
            let row = ExportedRecord::new(&source, &capture, record).unwrap();
            let reference =
                DiagnosticEvidenceRef::from_export(&source, &digest(b"retained-result-manifest"), &row, 0).unwrap();
            vec![(reference, row)]
        })
        .unwrap_or_default();
    let parent = DiagnosticCaptureRef {
        capture,
        source_scope_id: source.captured_range().unwrap().scope_id,
        manifest_sha256: digest(b"retained-result-manifest"),
    };
    (source, parent, rows)
}
fn answer(f: &Fixture, generation: Option<&str>) -> RelationalInputAnswer {
    let (source, capture, rows) = result(f, generation);
    RelationalInputAnswer::from_originals(
        &f.focus,
        &f.basis_source,
        &f.basis,
        &source,
        &capture,
        &rows.iter().map(|(reference, row)| (reference, row)).collect::<Vec<_>>(),
    )
    .unwrap()
}
#[test]
fn adverse_original_and_exact_current_plan_share_local_scope_without_manufactured_global_incarnation() {
    let f = fixture();
    assert!(f.focus.target.database().unwrap().incarnation.is_none());
    let TelemetryRecord::Operational(work) = &f.work.record else { panic!() };
    assert!(f.focus.matches_prerequisite_input(&f.basis_source, &f.basis, &f.work_source, [(&f.work_ref, work)]));
    let execution = SourceReadExecutionScope::from_context(
        f.focus.question,
        &NativeReadContext::from_activity(&f.basis_source).unwrap(),
    )
    .unwrap();
    assert!(matches!(execution, SourceReadExecutionScope::Local(_)));
    assert!(f.focus.matches_basis(&f.basis_source, &f.basis, &execution));
}
fn later_current_work_parent(f: &mut Fixture) -> (CollectionDescription, ExportedRecord, DiagnosticEvidenceRef) {
    let mut later_source = f.basis_source.clone();
    let SourceObservation::Activity(snapshot) = &mut later_source.observation else { panic!() };
    snapshot.at = "2020-01-01T12:00:00.5Z".into();
    snapshot.observed_from = snapshot.at.clone();
    snapshot.observed_until = snapshot.at.clone();
    let later =
        ExportedRecord::new(&later_source, &capture("later-adverse-original", "2"), f.basis.record.clone()).unwrap();
    let selected =
        DiagnosticEvidenceRef::from_export(&later_source, &digest(b"later-adverse-original-manifest"), &later, 0)
            .unwrap();
    let SourceObservation::CurrentRequestWork { result, .. } = &mut f.work_source.observation else { panic!() };
    let request = result.focus.as_mut().unwrap();
    request.parent = selected.clone();
    request.original_observed_at = "2020-01-01T12:00:00.5Z".into();
    f.work = ExportedRecord::new(&f.work_source, &capture("current-native-point", "3"), f.work.record.clone()).unwrap();
    f.work_ref =
        DiagnosticEvidenceRef::from_export(&f.work_source, &digest(b"later-current-native-manifest"), &f.work, 0)
            .unwrap();
    f.episode.latest = selected.clone();
    f.episode.last_seen_at = "2020-01-01T12:00:00.5Z".into();
    f.episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: f.work_ref.clone() });
    assert!(f.episode.is_valid());
    (later_source, later, selected)
}
#[test]
fn later_authenticated_current_work_parent_is_the_basis_while_the_first_observation_and_exact_proof_stay_immutable() {
    let mut f = fixture();
    let first = f.episode.first.clone();
    let (later_source, later, selected) = later_current_work_parent(&mut f);
    assert_ne!(selected, first);
    assert_eq!(selected.source_scope_id, first.source_scope_id);
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::RelationalInputContext,
        &f.episode,
        &f.work_source,
        &f.work_ref,
        &f.work,
    )
    .unwrap()
    .unwrap();
    assert_eq!(focus.basis.as_ref(), &selected);
    let TelemetryRecord::Operational(work) = &f.work.record else { panic!() };
    assert!(focus.matches_prerequisite_input(&later_source, &later, &f.work_source, [(&f.work_ref, work)]));
    let mut substituted_first = focus.clone();
    *substituted_first.basis = first.clone();
    assert!(!substituted_first.matches_prerequisite_input(
        &f.basis_source,
        &f.basis,
        &f.work_source,
        [(&f.work_ref, work)],
    ));
    let enrolled = f.episode.enroll_source_read(focus.clone()).unwrap();
    assert_eq!(enrolled.first, first);
    let revision = DiagnosticRevision::between(Some(&f.episode), enrolled, "2020-01-01T12:00:02Z").unwrap().unwrap();
    assert!(revision.inputs.contains(&selected));
    assert!(revision.referenced_evidence().unwrap().iter().any(|link| {
        link.role == crate::DiagnosticAncestryRole::FirstObservation
            && matches!(link.target, crate::diagnostic_ancestry::DiagnosticAncestryTarget::Record(value)
                if value == &first)
    }));
    assert!(revision.referenced_evidence().unwrap().iter().any(|link| {
        link.role == crate::DiagnosticAncestryRole::SourceBasis
            && matches!(link.target, crate::diagnostic_ancestry::DiagnosticAncestryTarget::Record(value)
                if value == &selected)
    }));
    let mut foreign_scope = focus;
    foreign_scope.basis.source_scope_id = digest(b"foreign-native-source-scope");
    assert!(f.episode.enroll_source_read(foreign_scope).is_err());
}
#[test]
fn type_or_cached_plan_without_adverse_original_and_complete_current_binding_cannot_enroll() {
    let mut f = fixture();
    f.episode.report = None;
    f.episode.analysis = DiagnosticAnalysisStatus::Pending;
    assert!(
        SourceReadFocus::from_followup_input(
            SourceReadQuestion::RelationalInputContext,
            &f.episode,
            &f.work_source,
            &f.work_ref,
            &f.work
        )
        .unwrap()
        .is_none()
    );
    let f = fixture();
    let mut source = f.work_source.clone();
    let SourceObservation::CurrentRequestWork { snapshot, .. } = &mut source.observation else { panic!() };
    snapshot.sources[0].state = SourceState::Partial;
    snapshot.sources[0].complete_scope = false;
    assert!(
        SourceReadFocus::from_followup_input(
            SourceReadQuestion::RelationalInputContext,
            &f.episode,
            &source,
            &f.work_ref,
            &f.work
        )
        .unwrap()
        .is_none()
    );
}
#[test]
fn selected_unavailable_body_remains_answered_context_without_runtime_or_harm_claims() {
    let f = fixture();
    let answer = answer(&f, Some("9007199254740993"));
    assert!(matches!(
        answer.selection,
        RelationalInputSelection::Observed { input_proof: None, retained_plan_available: false, .. }
    ));
    assert_eq!(answer.unresolved, RelationalInputGap::ALL);
    assert_eq!(answer.references().count(), 3);
    let stored: RelationalInputAnswer = serde_json::from_slice(&serde_json::to_vec(&answer).unwrap()).unwrap();
    assert_eq!(stored, answer);
    assert!(stored.is_valid());
    let mut foreign = f.focus.clone();
    let SourceReadPrerequisite::ObservedCurrentRequestPlan { context, .. } = &mut foreign.prerequisite else {
        panic!()
    };
    context.identity.plan_generation = "2".into();
    assert!(!SourceReadAnswer::RelationalInputs(Box::new(answer)).matches_focus(&foreign));
}
#[test]
fn completed_absence_and_changed_incarnation_are_distinct_and_neither_substitutes_originals() {
    let f = fixture();
    assert!(matches!(answer(&f, None).selection, RelationalInputSelection::NotObserved));
    assert!(matches!(answer(&f, Some("2")).selection, RelationalInputSelection::IncarnationChanged));
    let (mut source, capture, rows) = result(&f, Some("9007199254740993"));
    let SourceObservation::SourceRead { execution, .. } = &mut source.observation else { panic!() };
    execution.observed_from = "2020-01-01T11:00:00Z".into();
    assert!(
        RelationalInputAnswer::from_originals(
            &f.focus,
            &f.basis_source,
            &f.basis,
            &source,
            &capture,
            &rows.iter().map(|(reference, row)| (reference, row)).collect::<Vec<_>>()
        )
        .is_err()
    );
}

#[test]
fn readonly_long_running_primary_completion_preserves_its_exact_independent_child() {
    let f = fixture_with_updateability(false);
    assert!(f.basis_source.target.database().unwrap().incarnation.is_none());
    let mut before = f
        .episode
        .enroll_source_read(f.focus.clone())
        .unwrap()
        .transition_source_read(
            &f.focus,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
        )
        .unwrap();
    before.analysis = DiagnosticAnalysisStatus::Deferred;
    before.issue = Some(DiagnosticAnalysisIssue::PolicyChanged);
    let TelemetryRecord::Activity(actor) = &f.basis.record else { panic!() };
    let DiagnosticFocusPlan::Ready(primary) = before.plan_focus(&f.basis_source, actor, &before.first).unwrap() else {
        panic!("an unqualified child cannot hide an eligible readonly primary question")
    };
    assert!(before.needs_question(&primary));
    assert_eq!(primary.activity_participant().unwrap().database_writable, Some(false));
    let TelemetryRecord::Operational(work) = &f.work.record else { panic!() };
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: f.basis_source.target.clone(),
        input: before.first.capture_ref(),
        episodes: vec![before.clone()],
    };
    let update = analyze_diagnostic_current_request_work(
        &f.work_source,
        &f.work_ref.capture_ref(),
        &[DiagnosticCurrentRequestWorkRow { reference: &f.work_ref, observation: work }],
        &checkpoint,
        "2020-01-01T12:00:02Z",
        None,
    )
    .unwrap();
    let [revision] = update.revisions.as_slice() else { panic!("one genuine primary completion") };
    let after = &revision.episode;
    assert_eq!(after.analysis, DiagnosticAnalysisStatus::Analyzed);
    assert!(after.issue.is_none());
    assert_eq!(after.source_reads, before.source_reads);
    assert_eq!(after.first, before.first);
    assert_eq!(after.report, before.report);
    assert_eq!(after.lifecycle, before.lifecycle);
    assert!(!after.needs_question(&primary));
    assert!(after.has_owed_question(), "only the explicitly deferred child remains owed");
}

#[test]
fn readonly_original_can_nominate_recovery_context_before_any_primary_result_without_borrowing_role_or_member() {
    let f = fixture_with_updateability(false);
    let original = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&f.basis_source).unwrap(),
        f.episode.signal.clone(),
        f.episode.first_seen_at.clone(),
        f.episode.first.clone(),
    )
    .unwrap();
    assert!(original.report.is_none());
    let question = SourceReadQuestion::ReadableSecondaryRecovery;
    let focus = SourceReadFocus::from_followup_input(question, &original, &f.basis_source, &original.first, &f.basis)
        .unwrap()
        .unwrap();
    assert!(
        focus.prerequisite.observation().is_none(),
        "the original Activity basis is not an independent operational prerequisite"
    );
    assert!(focus.references().any(|reference| reference == &original.first));
    let context = focus.original_context(&f.basis_source).unwrap();
    let execution = SourceReadExecutionScope::from_context(question, &context).unwrap();
    assert!(matches!(execution, SourceReadExecutionScope::Local(_)));
    assert!(execution.target().database().unwrap().incarnation.is_none());
    assert!(focus.matches_basis(&f.basis_source, &f.basis, &execution));
    let enrolled = original
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
        )
        .unwrap();
    let TelemetryRecord::Activity(actor) = &f.basis.record else { panic!() };
    let DiagnosticFocusPlan::Ready(primary) = enrolled.plan_focus(&f.basis_source, actor, &original.first).unwrap()
    else {
        panic!("a source question cannot hide the unperformed native primary")
    };
    assert!(enrolled.needs_question(&primary));
    let TelemetryRecord::Operational(work) = &f.work.record else { panic!() };
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: f.basis_source.target.clone(),
        input: original.first.capture_ref(),
        episodes: vec![enrolled.clone()],
    };
    let update = analyze_diagnostic_current_request_work(
        &f.work_source,
        &f.work_ref.capture_ref(),
        &[DiagnosticCurrentRequestWorkRow { reference: &f.work_ref, observation: work }],
        &checkpoint,
        "2020-01-01T12:00:02Z",
        None,
    )
    .unwrap();
    assert_eq!(update.revisions.len(), 1);
    assert_eq!(update.revisions[0].episode.source_reads, enrolled.source_reads);
    assert!(matches!(update.revisions[0].episode.report, Some(DiagnosticReport::CurrentRequestWork { .. })));
    for missing in ["writable", "role", "member", "epoch"] {
        let mut source = f.basis_source.clone();
        match missing {
            "writable" | "role" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.database_writable = if missing == "writable" { Some(true) } else { None };
            }
            "member" => {
                let SourcePartition::InstanceLocal { member, .. } = &mut source.partition else { panic!() };
                *member = None;
            }
            _ => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut source.partition else { panic!() };
                *epoch = None;
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.epoch = None;
            }
        }
        if matches!(missing, "member" | "epoch") {
            let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
            frame.coverage = StateSourceCoverage::observed(source.source.clone(), frame.observations, false);
        }
        let row = ExportedRecord::new(&source, &f.basis.capture, f.basis.record.clone()).unwrap();
        let reference = DiagnosticEvidenceRef::from_export(
            &source,
            &digest(format!("missing-native-{missing}").as_bytes()),
            &row,
            0,
        )
        .unwrap();
        let episode = DiagnosticEpisode::start(
            DiagnosticScope::from_source(&source).unwrap(),
            original.signal.clone(),
            original.first_seen_at.clone(),
            reference.clone(),
        )
        .unwrap();
        assert!(
            SourceReadFocus::from_followup_input(question, &episode, &source, &reference, &row).unwrap().is_none(),
            "{missing}"
        );
    }
    for defect in ["member", "epoch", "role"] {
        let mut other = context.clone();
        match defect {
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut other.partition else {
                    panic!()
                };
                member.native.id = "foreign".into();
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut other.partition else { panic!() };
                *epoch = Some("different-process-or-recovery".into());
            }
            _ => other.database_writable = Some(true),
        }
        if let Ok(foreign) = SourceReadExecutionScope::from_context(question, &other) {
            assert!(!focus.matches_basis(&f.basis_source, &f.basis, &foreign), "{defect}");
        }
    }
}

#[test]
fn catalog_refusal_refresh_preserves_exact_readonly_primary_and_child_owners_without_starting_work() {
    let f = fixture_with_updateability(false);
    let original = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&f.basis_source).unwrap(),
        f.episode.signal.clone(),
        f.episode.first_seen_at.clone(),
        f.episode.first.clone(),
    )
    .unwrap();
    let question = SourceReadQuestion::ReadableSecondaryRecovery;
    let focus = SourceReadFocus::from_followup_input(question, &original, &f.basis_source, &original.first, &f.basis)
        .unwrap()
        .unwrap();
    let before = original
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Deferred {
                issue: DiagnosticAnalysisIssue::SourceUnavailable { code: None, stopped: None },
            },
        )
        .unwrap();
    let mut next = before
        .transition_source_read(
            &focus,
            SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified },
        )
        .unwrap();
    next.analysis = DiagnosticAnalysisStatus::Deferred;
    next.issue = Some(DiagnosticAnalysisIssue::CostUnqualified);
    let revision = DiagnosticRevision::between(Some(&before), next.clone(), "2020-01-01T12:00:01Z").unwrap().unwrap();
    assert_eq!(revision.episode.first, before.first);
    assert_eq!(revision.episode.signal, before.signal);
    assert_eq!(revision.episode.report, before.report);
    assert_eq!(revision.episode.source_reads.get(question).unwrap().focus, focus);
    assert!(revision.episode.has_owed_question());
    let mut collecting = before.transition_source_read(&focus, SourceReadStageState::Collecting).unwrap();
    collecting.analysis = next.analysis;
    collecting.issue = next.issue.clone();
    assert!(DiagnosticRevision::between(Some(&before), collecting, "2020-01-01T12:00:01Z").is_err());
    let mut completed = next;
    completed.analysis = DiagnosticAnalysisStatus::Analyzed;
    completed.issue = None;
    completed.report = Some(DiagnosticReport::CurrentRequestWork { observation: f.work_ref });
    assert!(DiagnosticRevision::between(Some(&before), completed, "2020-01-01T12:00:02Z").is_err());
}

#[test]
fn catalog_refresh_cannot_rewrite_completed_partial_primary_or_an_actual_answer_transition() {
    let f = fixture();
    let mut before = f
        .episode
        .enroll_source_read(f.focus.clone())
        .unwrap()
        .transition_source_read(&f.focus, SourceReadStageState::Collecting)
        .unwrap();
    before.analysis = DiagnosticAnalysisStatus::Partial;
    assert!(before.is_valid());
    let answer = answer(&f, None);
    let mut next = before
        .transition_source_read(
            &f.focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::RelationalInputs(Box::new(answer))) },
        )
        .unwrap();
    next.analysis = DiagnosticAnalysisStatus::Deferred;
    next.issue = Some(DiagnosticAnalysisIssue::CostUnqualified);
    assert!(DiagnosticRevision::between(Some(&before), next, "2020-01-01T12:00:04Z").is_err());
}
