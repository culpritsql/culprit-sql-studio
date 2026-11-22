//! Controlled originals reuse the existing TVP native corpus; only ordinary canonical writers create proof handles.
use super::*;

pub struct RelationalInputQuestionFixture {
    pub activity_source: CollectionDescription,
    pub activity_row: ExportedRecord,
    pub work_source: CollectionDescription,
    pub work_row: ExportedRecord,
    pub work_ref: DiagnosticEvidenceRef,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub source: CollectionDescription,
    pub records: Vec<TelemetryRecord>,
}
fn capture(id: &str, sequence: &str) -> CaptureIdentity {
    CaptureIdentity {
        observation_id: id.into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-relational-question-outbox".into(),
            owner_epoch: "1".into(),
            sequence: sequence.into(),
        }),
    }
}
pub fn relational_input_question_fixture(case: TvpLoopCase) -> RelationalInputQuestionFixture {
    fixture(case, None)
}
pub fn relational_input_question_fixture_for_owner(
    case: TvpLoopCase,
    owner: &NativeReadContext,
) -> RelationalInputQuestionFixture {
    assert!(owner.has_qualified_local_owner() && owner.database_writable.is_some());
    fixture(case, Some(owner))
}
fn fixture(case: TvpLoopCase, owner: Option<&NativeReadContext>) -> RelationalInputQuestionFixture {
    let (mut native_source, mut native_rows) = tvp_loop_evidence(case);
    if let Some(owner) = owner {
        native_source.target = owner.target.clone();
        native_source.partition = owner.partition.clone();
    }
    let writable = owner.and_then(|owner| owner.database_writable).unwrap_or(true);
    let TelemetryRecord::ObjectDefinition(native) = native_rows.remove(0) else { panic!() };
    let ObjectDefinitionFact::MssqlRetainedPlan(mut retained) = native.fact else { panic!() };
    let (mut work_source, mut work_records) = mssql_current_work_evidence(MssqlWorkCase::Observed);
    let SourceObservation::CurrentRequestWork { result, snapshot, .. } = &mut work_source.observation else { panic!() };
    let focus = result.focus.as_mut().unwrap();
    focus.target = native_source.target.clone();
    focus.partition = native_source.partition.clone();
    focus.epoch = native_source.partition.native_epoch().unwrap().into();
    focus.original.database = Some(focus.target.database().unwrap().database.clone());
    focus.original.elapsed_ms = Some("120000".into());
    focus.original.statement = retained.statement.payload.clone();
    focus.statement_identity = mssql_request_statement_identity(
        retained.context.identity.sql_handle.strip_prefix("0x").unwrap(),
        retained.context.identity.statement_start_bytes,
        retained.context.identity.statement_end_bytes,
        retained.context.database_id,
    )
    .unwrap();
    focus.original.statement_identity = Some(focus.statement_identity.clone());
    focus.database_writable = Some(writable);
    result.database_writable = Some(writable);
    focus.original_observed_at = "2020-01-01T12:00:00Z".into();
    let activity_source = CollectionDescription {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        source: focus.source.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            at: focus.original_observed_at.clone(),
            observed_from: focus.original_observed_at.clone(),
            observed_until: focus.original_observed_at.clone(),
            epoch: Some(focus.epoch.clone()),
            database_writable: Some(writable),
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(focus.source.clone(), 1),
        }),
    };
    let activity_row = ExportedRecord::new(
        &activity_source,
        &capture("relational-adverse-activity", "1"),
        TelemetryRecord::Activity(Box::new(focus.original.clone())),
    )
    .unwrap();
    focus.parent =
        DiagnosticEvidenceRef::from_export(&activity_source, &digest(b"relational-adverse-manifest"), &activity_row, 0)
            .unwrap();
    let baseline = focus.clone();
    work_source.target = native_source.target.clone();
    work_source.partition = native_source.partition.clone();
    snapshot.observed_from = "2020-01-01T12:00:01Z".into();
    snapshot.observed_until = "2020-01-01T12:00:02Z".into();
    let TelemetryRecord::Operational(work) = &mut work_records[0] else { panic!() };
    let OperationalFact::CurrentRequestWork(point) = &mut work.fact else { panic!() };
    point.actor = baseline.original.key.clone();
    point.database = baseline.original.database.clone().unwrap();
    point.statement_identity = baseline.statement_identity.clone();
    point.statement = baseline.original.statement.clone();
    let CurrentRequestWorkNative::Mssql { work: native } = &mut point.native else { panic!() };
    native.native_database_id = retained.context.database_id;
    native.request_plan = Some(
        NativeIdentity::new(retained.context.identity.plan_handle.strip_prefix("0x").unwrap())
            .qualified("native_object", "sys.dm_exec_requests.plan_handle"),
    );
    // This metadata point carries no compiled or actual plan body. It does not attest the later retained run.
    native.current_plan = MssqlCurrentRequestPlan::Observed {
        observed_from: "2020-01-01T12:00:01.2Z".into(),
        observed_until: "2020-01-01T12:00:01.4Z".into(),
        context: Box::new(retained.context.clone()),
        plan: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
    };
    work.key = point.key().unwrap();
    let work_row =
        ExportedRecord::new(&work_source, &capture("relational-current-work", "2"), work_records.remove(0)).unwrap();
    let work_ref =
        DiagnosticEvidenceRef::from_export(&work_source, &digest(b"relational-current-work-manifest"), &work_row, 0)
            .unwrap();
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&activity_source).unwrap(),
        DiagnosticSignal::Activity {
            key: baseline.original.key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        baseline.original_observed_at.clone(),
        baseline.parent.clone(),
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: work_ref.clone() });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::RelationalInputContext,
        &episode,
        &work_source,
        &work_ref,
        &work_row,
    )
    .unwrap()
    .unwrap();
    let mut execution = NativeReadContext::from_activity(&activity_source).unwrap();
    execution.observed_from = "2020-01-01T12:00:03Z".into();
    execution.observed_until = "2020-01-01T12:00:04Z".into();
    let source = CollectionDescription {
        target: native_source.target,
        partition: SourcePartition::Target,
        source: "retained-plans-state".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::RetainedPlans,
                observed_from: execution.observed_from.clone(),
                observed_until: execution.observed_until.clone(),
                records: 1,
                sources: vec![StateSourceCoverage::observed(
                    culprit_core::source_id::RETAINED_PLANS.into(),
                    1,
                    case != TvpLoopCase::PartialSource,
                )],
            },
            execution: Box::new(execution),
            focus_key: focus.key().unwrap(),
        },
    };
    if let Some(proof) = retained.relational_input_proof.take() {
        retained.relational_input_proof =
            Some(MssqlRelationalInputProof::for_retained(&source, &retained, proof.bindings).unwrap());
    }
    let records = vec![TelemetryRecord::ObjectDefinition(
        ObjectDefinitionRecord::new(
            culprit_core::source_id::RETAINED_PLANS.into(),
            ObjectDefinitionFact::MssqlRetainedPlan(retained),
        )
        .unwrap(),
    )];
    for record in &records {
        ExportedRecord::new(&source, &capture("relational-selected-result", "3"), record.clone()).unwrap();
    }
    RelationalInputQuestionFixture {
        activity_source,
        activity_row,
        work_source,
        work_row,
        work_ref,
        episode,
        focus,
        source,
        records,
    }
}

/// Same selected source originals for both warehouse adapters, SDK interpretation and canonical tamper controls.
pub fn relational_input_result_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    [TvpLoopCase::RepeatedAccess, TvpLoopCase::Estimated, TvpLoopCase::MissingBinding, TvpLoopCase::PartialSource]
        .into_iter()
        .map(|case| {
            let fixture = relational_input_question_fixture(case);
            (fixture.source, fixture.records)
        })
        .collect()
}
