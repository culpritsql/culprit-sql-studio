//! Same canonical current-request family, native SQL Server counters and independent cache context.
use super::*;
#[derive(Clone, Copy)]
pub enum MssqlWorkCase {
    Observed,
    CacheMissing,
    CacheChanged,
    CacheDenied,
    Partial,
    NotObserved,
    Changed,
    NegativeTime,
}
pub fn mssql_current_work_evidence(case: MssqlWorkCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let fixture = compiler_admission_investigation_fixture();
    let mut focus = fixture.intent.focus.activity_participant().unwrap().clone();
    focus.original.session_context = None;
    focus.original.wait = None;
    focus.original.key.transaction = None;
    focus.original.key.session.id = "42".into();
    focus.original.key.session.generation = "2020-01-01T10:00:00.0030000".into();
    focus.original.key.request =
        Some(NativeActivityIdentity { id: "0".into(), generation: "2020-01-01T11:59:00.0030000".into() });
    focus.statement_identity = mssql_request_statement_identity("AABB", 0, -1, 7).unwrap();
    focus.original.statement_identity = Some(focus.statement_identity.clone());
    focus.original.statement =
        DefinitionPayload::sql("SELECT id FROM dbo.items WHERE secret='private-current-request-secret'", Engine::Mssql);
    focus.original.database = Some(focus.target.database().unwrap().database.clone());
    focus.original.scope = ActivityScopeBasis::RequestDatabase;
    let mut parent = fixture.baseline_source;
    parent.source = culprit_core::source_id::SESSIONS.into();
    let SourceObservation::Activity(header) = &mut parent.observation else { panic!() };
    header.observed_from = PARENT_AT.into();
    header.observed_until = PARENT_AT.into();
    header.at = PARENT_AT.into();
    header.native_rows = 1;
    header.observations = 1;
    header.coverage.records = 1;
    let row = ExportedRecord::new(
        &parent,
        &fixture.baseline_record.capture,
        TelemetryRecord::Activity(Box::new(focus.original.clone())),
    )
    .unwrap();
    focus.parent =
        DiagnosticEvidenceRef::from_export(&parent, &digest(b"controlled-current-work-parent"), &row, 0).unwrap();
    focus.original_observed_at = PARENT_AT.into();
    focus.source = parent.source.clone();
    assert!(focus.matches_parent(&parent, &focus.original, &focus.parent));
    let plan_token = NativeIdentity::new("CCDD").qualified("native_object", "sys.dm_exec_requests.plan_handle");
    let cache = CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: "0xCCDD".into(),
            sql_handle: "0xAABB".into(),
            statement_start_bytes: 0,
            statement_end_bytes: -1,
            created_without_offset: "2020-01-01T11:59:00.0030000".into(),
            plan_generation: "9007199254740993".into(),
        },
        database_id: 7,
        query_hash: Some("0x0123456789ABCDEF".into()),
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: None,
        source_text_complete: false,
    };
    let current_plan = match case {
        MssqlWorkCase::CacheMissing => {
            MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::NotObserved }
        }
        MssqlWorkCase::CacheChanged => {
            MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::ChangedDuringRead }
        }
        MssqlWorkCase::CacheDenied => {
            MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::PermissionDenied }
        }
        _ => MssqlCurrentRequestPlan::Observed {
            observed_from: "2020-01-01T12:00:01.2Z".into(),
            observed_until: "2020-01-01T12:00:01.4Z".into(),
            context: Box::new(cache),
            plan: DefinitionPayload::plan(
                r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT id FROM dbo.items WHERE secret='private-current-request-secret'" /></Statements></Batch></BatchSequence></ShowPlanXML>"#,
                Engine::Mssql,
            ),
        },
    };
    let membership = if matches!(case, MssqlWorkCase::NotObserved | MssqlWorkCase::Changed) {
        FocusedRequestMembership::NotObserved
    } else {
        FocusedRequestMembership::Observed
    };
    let binding = if matches!(case, MssqlWorkCase::Changed) {
        FocusedRequestBinding::ParticipantChanged
    } else {
        FocusedRequestBinding::Matched
    };
    let records = if membership == FocusedRequestMembership::Observed {
        let point = CurrentRequestWorkObservation {
            observed_at: FROM.into(),
            actor: focus.original.key.clone(),
            database: focus.target.database().unwrap().database.clone(),
            scope: ActivityScopeBasis::RequestDatabase,
            statement_identity: focus.statement_identity.clone(),
            statement: focus.original.statement.clone(),
            native: CurrentRequestWorkNative::Mssql {
                work: Box::new(MssqlCurrentRequestWork {
                    native_database_id: 7,
                    checked_until: "2020-01-01T12:00:01.6Z".into(),
                    elapsed_ms: Some(if matches!(case, MssqlWorkCase::NegativeTime) { -1 } else { 60000 }),
                    cpu_ms: Some(3200),
                    reads: Some("9007199254740993".into()),
                    writes: Some("0".into()),
                    logical_reads: None,
                    rows_returned: Some("42".into()),
                    request_plan: Some(plan_token),
                    current_plan,
                }),
            },
        };
        vec![TelemetryRecord::Operational(OperationalObservation {
            source: culprit_core::source_id::MSSQL_CURRENT_REQUEST_WORK.into(),
            key: point.key().unwrap(),
            fact: OperationalFact::CurrentRequestWork(Box::new(point)),
        })]
    } else {
        vec![]
    };
    let source = CollectionDescription {
        target: focus.target.clone(),
        partition: focus.partition.clone(),
        source: culprit_core::source_id::MSSQL_CURRENT_REQUEST_WORK.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::CurrentRequestWork {
            profile: CurrentRequestWorkProfile::MssqlRequest,
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::CurrentRequestWork,
                observed_from: FROM.into(),
                observed_until: UNTIL.into(),
                records: records.len() as u32,
                sources: vec![StateSourceCoverage::observed(
                    culprit_core::source_id::MSSQL_CURRENT_REQUEST_WORK.into(),
                    records.len() as u32,
                    !matches!(case, MssqlWorkCase::Partial | MssqlWorkCase::NotObserved | MssqlWorkCase::Changed),
                )],
            },
            result: Box::new(FocusedRequestResult {
                focus: Some(Box::new(focus.clone())),
                database_writable: focus.database_writable,
                binding,
                membership,
            }),
        },
    };
    assert!(source.matches_request(&CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::CurrentRequestWork { focus: Box::new(focus) },
        budget: CollectionBudget::default()
    }));
    for record in &records {
        ExportedRecord::new(
            &source,
            &CaptureIdentity { observation_id: "controlled-mssql-current-work".into(), revision: None },
            record.clone(),
        )
        .unwrap();
    }
    (source, records)
}
pub fn mssql_current_work_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    [
        MssqlWorkCase::Observed,
        MssqlWorkCase::CacheMissing,
        MssqlWorkCase::CacheChanged,
        MssqlWorkCase::CacheDenied,
        MssqlWorkCase::Partial,
        MssqlWorkCase::NotObserved,
        MssqlWorkCase::Changed,
        MssqlWorkCase::NegativeTime,
    ]
    .into_iter()
    .map(mssql_current_work_evidence)
    .collect()
}

pub fn mssql_current_work_tamper_cases() -> Vec<(&'static str, serde_json::Value)> {
    use serde_json::json;
    vec![
        ("/native/work/reads", json!("9007199254740992")),
        ("/native/work/native_database_id", json!(8)),
        ("/native/work/checked_until", json!("2020-01-01T12:00:00Z")),
        ("/native/work/current_plan/context/identity/plan_generation", json!("5")),
        ("/native/work/current_plan/context/identity/created_without_offset", json!("2020-01-01T11:59:00.007")),
        ("/actor/request/generation", json!("2020-01-01T11:59:01.0030000")),
    ]
}
