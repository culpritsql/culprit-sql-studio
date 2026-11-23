//! Controlled native-shaped originals. Only the ordinary canonical writer/EOF reader produces proof handles.
use super::*;
use crate::original_projection::OriginalProjection;
use culprit_core::{Engine, MeasurementScope, NodeKind};
#[derive(Clone, Copy, Debug)]
pub enum ExecutionProfileCase {
    Observed,
    Empty,
    Unavailable,
    ChangedRequest,
    UnknownWorker,
    NoPlanContext,
    UnknownCounters,
}
const XML: &str = r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT id FROM dbo.items WHERE tenant=124" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="true" EstimateRows="10" EstimatedTotalSubtreeCost="1"><TableScan><Object Schema="[dbo]" Table="[items]"/></TableScan></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#;
mod reuse;
pub use reuse::current_execution_profile_context_reuse_fixture;

pub fn current_execution_profile_basis() -> (CollectionDescription, Vec<TelemetryRecord>) {
    basis_with_exchange_wait(Some("CXCONSUMER"))
}
fn basis_with_exchange_wait(native: Option<&str>) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (source, _) = mssql_current_work_evidence(MssqlWorkCase::Observed);
    let SourceObservation::CurrentRequestWork { result, .. } = &source.observation else { panic!() };
    let request = result.focus.as_ref().unwrap();
    let mut original = request.original.clone();
    original.elapsed_ms = Some("120000".into());
    original.wait = native.map(|native| ActivityWait {
        class: None,
        native_type: Some(native.into()),
        elapsed_ms: Some("1200".into()),
        owners: vec![],
        identified_resources: vec![],
        resources: None,
    });
    let source = CollectionDescription {
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
            scope: MeasurementScope::Database,
            population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected(request.source.clone(), 1),
        }),
    };
    (source, vec![TelemetryRecord::Activity(Box::new(original))])
}
pub fn current_execution_profile_work(request: &ActivityRequestFocus) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, mut records) = mssql_current_work_evidence(MssqlWorkCase::Observed);
    source.target = request.target.clone();
    source.partition = request.partition.clone();
    let SourceObservation::CurrentRequestWork { result, .. } = &mut source.observation else { panic!() };
    result.focus = Some(Box::new(request.clone()));
    result.database_writable = request.database_writable;
    let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
    let OperationalFact::CurrentRequestWork(point) = &mut row.fact else { panic!() };
    point.actor = request.original.key.clone();
    point.database = request.original.database.clone().unwrap();
    point.statement_identity = request.statement_identity.clone();
    point.statement = request.original.statement.clone();
    let CurrentRequestWorkNative::Mssql { work } = &mut point.native else { panic!() };
    if let MssqlCurrentRequestPlan::Observed { context, .. } = &mut work.current_plan {
        // Controlled native cache fields follow the original selected statement before any canonical parent exists.
        context.identity.sql_handle = format!("0x{}", request.statement_identity.id);
        context.identity.statement_start_bytes =
            request.statement_identity.qualifiers["statement_start_offset"].parse().unwrap();
        context.identity.statement_end_bytes =
            request.statement_identity.qualifiers["statement_end_offset"].parse().unwrap();
        context.database_id = request.statement_identity.qualifiers["database_id"].parse().unwrap();
    }
    assert!(work.current_plan.matches_statement(&request.statement_identity));
    row.key = point.key().unwrap();
    (source, records)
}
fn task(request: &ActivityRequestFocus, id: u32) -> MssqlWorkerTask {
    MssqlWorkerTask {
        address: format!("{id:016X}"),
        state: "RUNNING".into(),
        scheduler_id: Some(0),
        parent_node_id: Some(0),
        scheduler_work_queue_count: Some("0".into()),
        session_id: Some(request.original.key.session.id.parse().unwrap()),
        request_id: Some(request.original.key.request.as_ref().unwrap().id.parse().unwrap()),
        execution_context_id: Some(id),
        worker: Some(MssqlWorkerBinding {
            address: format!("{:016X}", id + 100),
            task_address: Some(format!("{id:016X}")),
            created_ms_ticks: Some("10".into()),
            task_bound_ms_ticks: Some("20".into()),
            state: Some("RUNNING".into()),
            is_preemptive: Some(false),
        }),
        wait: None,
        request: Some(request.original.key.clone()),
    }
}
fn profile(id: u32, rows: &str) -> MssqlOperatorThreadProfile {
    MssqlOperatorThreadProfile {
        node_id: 0,
        thread_id: id + 10,
        task_address: Some(format!("{id:016X}")),
        physical_operator: "Table Scan".into(),
        counters: MssqlOperatorThreadCounters {
            rows_returned: Some(rows.into()),
            rows_read: Some(rows.into()),
            estimated_rows: Some("10".into()),
            elapsed_ms: Some("12".into()),
            cpu_ms: Some("0".into()),
            logical_reads: Some("12".into()),
            physical_reads: None,
            read_ahead_reads: None,
            spill_write_pages: Some("0".into()),
            end_of_scans: Some("0".into()),
            scans: Some("1".into()),
            rebinds: Some("0".into()),
            rewinds: Some("0".into()),
        },
        open_ticks: Some("0".into()),
        close_ticks: Some("0".into()),
        database_id: Some(7),
        object_id: Some(101),
        index_id: Some(0),
    }
}
pub fn current_execution_profile_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: ExecutionProfileCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let request = focus.execution_profile_request().unwrap();
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        observed_from: "2020-01-01T12:00:03Z".into(),
        observed_until: "2020-01-01T12:00:04Z".into(),
        database_writable: execution.database_writable(),
    };
    let (_, mut work_rows) = current_execution_profile_work(request);
    let TelemetryRecord::Operational(row) = work_rows.remove(0) else { panic!() };
    let OperationalFact::CurrentRequestWork(row) = row.fact else { panic!() };
    let CurrentRequestWorkNative::Mssql { mut work } = row.native else { panic!() };
    work.checked_until = "2020-01-01T12:00:03.8Z".into();
    let MssqlCurrentRequestPlan::Observed { observed_from, observed_until, context: cache, plan } =
        &mut work.current_plan
    else {
        panic!()
    };
    *observed_from = "2020-01-01T12:00:03.25Z".into();
    *observed_until = "2020-01-01T12:00:03.65Z".into();
    *plan = DefinitionPayload::plan(XML, Engine::Mssql);
    let DefinitionPayload::Available { sha256, .. } = plan else { panic!() };
    let operators = MssqlExecutionOperators::Observed {
        context: cache.clone(),
        plan_sha256: sha256.clone(),
        nodes: vec![MssqlExecutionOperator {
            node_id: 0,
            parent_node_id: None,
            kind: NodeKind::Scan,
            parallel: Some(true),
            inner_join: None,
            hash_build_input: None,
            relational_input: None,
            native_operator: "Table Scan".into(),
            object: Some("dbo.items".into()),
            index: None,
            predicate: DefinitionPayload::sql("[tenant]=(124)", Engine::Mssql),
            seek_predicate: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        }],
    };
    let tasks = vec![task(request, 1), task(request, 2)];
    let mut after = tasks.clone();
    for (index, kind) in ["LCK_M_S", "PAGEIOLATCH_SH"].into_iter().enumerate() {
        let selected = &mut after[index];
        selected.state = "SUSPENDED".into();
        selected.worker.as_mut().unwrap().state = Some("SUSPENDED".into());
        selected.wait = Some(MssqlWorkerWait {
            task_address: selected.address.clone(),
            session_id: selected.session_id,
            request_id: selected.request_id,
            execution_context_id: selected.execution_context_id,
            native_type: kind.into(),
            elapsed_ms: Some("1".into()),
            scheduler_id: selected.scheduler_id,
            parent_node_id: selected.parent_node_id,
            request: selected.request.clone(),
            blocking_task_address: None,
            blocking_session_id: if index == 0 { Some(99) } else { None },
            blocking_execution_context_id: None,
        });
    }
    let mut point = MssqlCurrentExecutionProfilePoint {
        context: context.clone(),
        focus: Box::new(request.clone()),
        observed_at: "2020-01-01T12:00:03.1Z".into(),
        checked_until: work.checked_until.clone(),
        binding: FocusedRequestBinding::Matched,
        membership: FocusedRequestMembership::Observed,
        statement: request.original.statement.clone(),
        work: Some(work),
        workers_before: Some(MssqlSelectedWorkerPoint {
            observed_at: "2020-01-01T12:00:03.2Z".into(),
            tasks,
            limited: false,
        }),
        workers_after: Some(MssqlSelectedWorkerPoint {
            observed_at: "2020-01-01T12:00:03.7Z".into(),
            tasks: after,
            limited: false,
        }),
        profiles: MssqlExecutionProfiles::Observed {
            observed_from: "2020-01-01T12:00:03.3Z".into(),
            observed_until: "2020-01-01T12:00:03.6Z".into(),
            rows: vec![profile(1, "1"), profile(2, "9007199254740993")],
            limited: false,
            operators,
        },
        relational_inputs: None,
    };
    match case {
        ExecutionProfileCase::ChangedRequest => {
            point.binding = FocusedRequestBinding::ParticipantChanged;
            point.membership = FocusedRequestMembership::NotObserved;
            point.work = None;
            point.workers_before = None;
            point.workers_after = None;
            point.profiles =
                MssqlExecutionProfiles::Unavailable { reason: MssqlExecutionProfilesUnavailable::ParticipantChanged };
        }
        ExecutionProfileCase::Unavailable => {
            point.profiles =
                MssqlExecutionProfiles::Unavailable { reason: MssqlExecutionProfilesUnavailable::LayoutUnavailable }
        }
        ExecutionProfileCase::UnknownWorker => {
            point.workers_after.as_mut().unwrap().tasks[0].worker.as_mut().unwrap().created_ms_ticks = Some("11".into())
        }
        ExecutionProfileCase::Empty => {
            let MssqlExecutionProfiles::Observed { rows, operators, .. } = &mut point.profiles else { panic!() };
            rows.clear();
            *operators =
                MssqlExecutionOperators::Unavailable { reason: MssqlExecutionOperatorsUnavailable::PlanUnavailable };
        }
        ExecutionProfileCase::NoPlanContext => {
            let MssqlExecutionProfiles::Observed { operators, .. } = &mut point.profiles else { panic!() };
            *operators = MssqlExecutionOperators::Unavailable {
                reason: MssqlExecutionOperatorsUnavailable::QueryIdentityUnavailable,
            };
        }
        ExecutionProfileCase::UnknownCounters => {
            let MssqlExecutionProfiles::Observed { rows, .. } = &mut point.profiles else { panic!() };
            rows[0].counters.rows_returned = None;
            rows[1].counters.rows_returned = Some("-1".into());
        }
        ExecutionProfileCase::Observed => (),
    }
    let key = point.key().unwrap();
    let source = CollectionDescription {
        target: focus.target.clone(),
        partition: SourcePartition::Target,
        source: culprit_core::source_id::CURRENT_EXECUTION_PROFILE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::CurrentExecutionProfile,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: 1,
                sources: vec![StateSourceCoverage::observed(
                    culprit_core::source_id::CURRENT_EXECUTION_PROFILE.into(),
                    1,
                    false,
                )],
            },
            execution: Box::new(context),
            focus_key: focus.key().unwrap(),
        },
    };
    assert!(point.fits_source(&source));
    let records = vec![TelemetryRecord::Operational(OperationalObservation {
        source: source.source.clone(),
        key,
        fact: OperationalFact::MssqlCurrentExecutionProfile(Box::new(point)),
    })];
    (source, records)
}
fn capture(id: &str, sequence: &str) -> CaptureIdentity {
    CaptureIdentity {
        observation_id: id.into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-execution-profile-outbox".into(),
            owner_epoch: "1".into(),
            sequence: sequence.into(),
        }),
    }
}
pub struct ExecutionProfileFixture {
    pub basis_source: CollectionDescription,
    pub basis: ExportedRecord,
    pub work_source: CollectionDescription,
    pub work: ExportedRecord,
    pub work_ref: DiagnosticEvidenceRef,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub source: CollectionDescription,
    pub capture: DiagnosticCaptureRef,
    pub row: ExportedRecord,
    pub reference: DiagnosticEvidenceRef,
    pub answer: CurrentExecutionProfileAnswer,
}
pub fn current_execution_profile_fixture(case: ExecutionProfileCase) -> ExecutionProfileFixture {
    fixture_with_exchange_wait(case, Some("CXCONSUMER"))
}
pub fn current_execution_profile_fixture_without_exchange_wait(case: ExecutionProfileCase) -> ExecutionProfileFixture {
    fixture_with_exchange_wait(case, None)
}
fn fixture_with_exchange_wait(case: ExecutionProfileCase, native: Option<&str>) -> ExecutionProfileFixture {
    fixture_from_basis(basis_with_exchange_wait(native), |focus, execution| {
        current_execution_profile_result(focus, execution, case)
    })
}
fn fixture_from_basis(
    basis: (CollectionDescription, Vec<TelemetryRecord>),
    result: impl FnOnce(&SourceReadFocus, &SourceReadExecutionScope) -> (CollectionDescription, Vec<TelemetryRecord>),
) -> ExecutionProfileFixture {
    let (basis_source, mut rows) = basis;
    let basis = ExportedRecord::new(&basis_source, &capture("profile-original", "1"), rows.remove(0)).unwrap();
    let basis_ref =
        DiagnosticEvidenceRef::from_export(&basis_source, &digest(b"controlled-profile-original"), &basis, 0).unwrap();
    let TelemetryRecord::Activity(actor) = &basis.record else { panic!() };
    let header = basis_source.observation.activity().unwrap();
    let request = ActivityRequestFocus {
        parent: basis_ref.clone(),
        target: basis_source.target.clone(),
        partition: basis_source.partition.clone(),
        source: basis_source.source.clone(),
        database_writable: header.database_writable,
        epoch: header.epoch.clone().unwrap(),
        original_observed_at: header.at.clone(),
        original: actor.as_ref().clone(),
        statement_identity: actor.statement_identity.clone().unwrap(),
    };
    let (work_source, mut rows) = current_execution_profile_work(&request);
    let work = ExportedRecord::new(&work_source, &capture("profile-work", "2"), rows.remove(0)).unwrap();
    let work_ref =
        DiagnosticEvidenceRef::from_export(&work_source, &digest(b"controlled-profile-work"), &work, 0).unwrap();
    let mut episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(&basis_source).unwrap(),
        DiagnosticSignal::Activity {
            key: actor.key.clone(),
            condition: ActivityCondition::LongRunning,
            policy: ActivityPolicy::default(),
        },
        header.at.clone(),
        basis_ref,
    )
    .unwrap();
    episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    episode.report = Some(DiagnosticReport::CurrentRequestWork { observation: work_ref.clone() });
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::CurrentExecutionProfile,
        &episode,
        &work_source,
        &work_ref,
        &work,
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&basis_source).unwrap())
            .unwrap();
    let (source, mut rows) = result(&focus, &execution);
    let row = ExportedRecord::new(&source, &capture("profile-result", "3"), rows.remove(0)).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-profile-result"), &row, 0).unwrap();
    let capture = reference.capture_ref();
    let answer = CurrentExecutionProfileAnswer::from_originals(
        &focus,
        &basis_source,
        &basis,
        &source,
        &capture,
        &[(&reference, &row)],
    )
    .unwrap();
    ExecutionProfileFixture {
        basis_source,
        basis,
        work_source,
        work,
        work_ref,
        episode,
        focus,
        source,
        capture,
        row,
        reference,
        answer,
    }
}
pub fn current_execution_profile_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    let mut cases = [
        ExecutionProfileCase::Observed,
        ExecutionProfileCase::Empty,
        ExecutionProfileCase::Unavailable,
        ExecutionProfileCase::ChangedRequest,
        ExecutionProfileCase::UnknownWorker,
        ExecutionProfileCase::NoPlanContext,
        ExecutionProfileCase::UnknownCounters,
    ]
    .into_iter()
    .map(|case| {
        let fixture = current_execution_profile_fixture(case);
        (fixture.source, vec![fixture.row.record])
    })
    .collect::<Vec<_>>();
    for case in [ExecutionProfileCase::Observed, ExecutionProfileCase::Unavailable] {
        let fixture = current_execution_profile_fixture(case);
        let execution = SourceReadExecutionScope::from_context(
            fixture.focus.question,
            &fixture.focus.original_context(&fixture.basis_source).unwrap(),
        )
        .unwrap();
        cases.push(current_execution_profile_result_with_input_context(&fixture.focus, &execution, case));
    }
    for case in ExecutionRelationalCase::ALL {
        let fixture = current_execution_profile_relational_fixture(case);
        cases.push((fixture.source, vec![fixture.row.record]));
    }
    for case in ParallelOperatorCase::ALL {
        let fixture = parallel_operator_fixture(case);
        cases.push((fixture.source, vec![fixture.row.record]));
    }
    cases
}

pub fn assert_current_execution_profile_capture(
    capture: &crate::analytical::evidence::EvidenceCapture,
    source: &CollectionDescription,
    records: &[TelemetryRecord],
) {
    assert!(capture.is_read_complete(), "{:?}", capture.gaps());
    let original = capture.verified_original().unwrap();
    assert_eq!(original.source(), source);
    assert_eq!(original.records().len(), 1);
    assert_eq!(&original.records()[0].row().record, &records[0]);
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCurrentExecutionProfile(point),
        ..
    }) = &records[0]
    else {
        panic!()
    };
    assert!(point.fits_source(source));
    assert!(CurrentExecutionProfileEvidence::from_point(point).unwrap().is_valid());
}

#[cfg(test)]
mod tests;

mod relational_inputs;
pub use relational_inputs::current_execution_profile_result_with_input_context;

mod relational_work;
pub use relational_work::{
    ExecutionRelationalCase, current_execution_profile_relational_fixture, current_execution_profile_relational_result,
};

mod operator_context;
pub use operator_context::{ParallelOperatorCase, parallel_operator_fixture, parallel_operator_result};
