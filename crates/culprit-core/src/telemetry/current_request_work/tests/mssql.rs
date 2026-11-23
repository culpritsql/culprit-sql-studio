use super::*;
pub(in crate::telemetry) fn input() -> CurrentRequestWorkSnapshot {
    let mut input = snapshot();
    input.profile = CurrentRequestWorkProfile::MssqlRequest;
    input.source = crate::source_id::MSSQL_CURRENT_REQUEST_WORK.into();
    input.context.target = DatabaseIdentity::server(Engine::Mssql, "sample", "owned.invalid", 1433, None).into();
    let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut input.context.partition else { panic!() };
    member.engine = Engine::Mssql;
    input.coverage = StateSourceCoverage::collected(input.source.clone(), 1);
    let row = &mut input.observations[0];
    row.actor = ActivityKey {
        session: NativeActivityIdentity { id: "42".into(), generation: "2020-01-01T10:00:00.0030000".into() },
        request: Some(NativeActivityIdentity { id: "0".into(), generation: "2020-01-01T11:59:00.0030000".into() }),
        transaction: None,
    };
    row.statement_identity = mssql_request_statement_identity("AABB", 0, -1, 7).unwrap();
    row.database = "sample".into();
    row.scope = ActivityScopeBasis::RequestDatabase;
    row.native = CurrentRequestWorkNative::Mssql {
        work: Box::new(MssqlCurrentRequestWork {
            native_database_id: 7,
            checked_until: "2020-01-01T12:00:00.9Z".into(),
            elapsed_ms: Some(-1),
            cpu_ms: Some(3),
            reads: Some("9007199254740993".into()),
            writes: Some("0".into()),
            logical_reads: None,
            rows_returned: Some("42".into()),
            request_plan: None,
            current_plan: MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::NotObserved },
        }),
    };
    input
}
#[test]
fn request_native_identity_and_signed_time_preserve_independent_unknowns_without_cache_totals() {
    let input = input();
    assert!(input.is_valid());
    let native = serde_json::to_value(&input.observations[0].native).unwrap();
    assert_eq!(native["work"]["elapsed_ms"], -1);
    assert_eq!(native["work"]["reads"], "9007199254740993");
    assert!(native["work"]["logical_reads"].is_null());
    let source = source(&input);
    assert!(input.observations[0].fits_source_scope(&source));
    let record = TelemetryRecord::Operational(OperationalObservation {
        source: input.source.clone(),
        key: input.observations[0].key().unwrap(),
        fact: OperationalFact::CurrentRequestWork(Box::new(input.observations[0].clone())),
    });
    assert_eq!(record.definition_payloads().count(), 1);
}
#[test]
fn malformed_or_foreign_mssql_statement_and_request_identity_cannot_fit_a_current_work_source() {
    for defect in ["database", "offset", "handle", "request", "clock", "counter-width", "kind"] {
        let mut input = input();
        let row = &mut input.observations[0];
        match defect {
            "database" => {
                row.statement_identity.qualifiers.insert("database_id".into(), "8".into());
            }
            "offset" => {
                row.statement_identity.qualifiers.insert("statement_start_offset".into(), "01".into());
            }
            "handle" => row.statement_identity.id = "0000".into(),
            "request" => row.actor.request.as_mut().unwrap().generation = "0".into(),
            "clock" => {
                let CurrentRequestWorkNative::Mssql { work } = &mut row.native else { panic!() };
                work.checked_until = "2020-01-01T12:00:02Z".into();
            }
            "counter-width" => {
                let CurrentRequestWorkNative::Mssql { work } = &mut row.native else { panic!() };
                work.reads = Some("9223372036854775808".into());
            }
            _ => input.profile = CurrentRequestWorkProfile::MysqlStatementEvent,
        }
        assert!(!input.is_valid(), "{defect}");
    }
}
#[test]
fn native_request_work_profile_accepts_only_original_request_database_locators_without_global_incarnation() {
    let input = input();
    let focus = focus(&input, input.context.partition.clone());
    assert!(focus.is_current_request_work_focus());
    assert!(focus.target.database().unwrap().incarnation.is_none());
    let mut unknown = focus.clone();
    unknown.original.scope = ActivityScopeBasis::SessionDatabase;
    assert!(!unknown.is_current_request_work_focus());
    for (start, end, db) in [(1, -1, 7), (0, 1, 7), (0, -1, 0), (0, -1, 32768)] {
        assert!(mssql_request_statement_identity("AB", start, end, db).is_none());
    }
}

#[test]
fn selected_cache_payload_keeps_both_payload_owners_and_requires_ordered_request_cache_endpoints() {
    let mut input = input();
    let row = &mut input.observations[0];
    let CurrentRequestWorkNative::Mssql { work } = &mut row.native else { panic!() };
    work.request_plan =
        Some(NativeIdentity::new("CCDD").qualified("native_object", "sys.dm_exec_requests.plan_handle"));
    work.current_plan = MssqlCurrentRequestPlan::Observed {
        observed_from: "2020-01-01T12:00:00.6Z".into(),
        observed_until: "2020-01-01T12:00:00.8Z".into(),
        context: Box::new(CachedStatementContext {
            identity: CachedStatementIdentity {
                plan_handle: "0xCCDD".into(),
                sql_handle: "0xAABB".into(),
                statement_start_bytes: 0,
                statement_end_bytes: -1,
                created_without_offset: "2020-01-01T11:59:00.003".into(),
                plan_generation: "9007199254740993".into(),
            },
            database_id: 7,
            query_hash: None,
            origin: StatementOrigin::Application,
            monitoring_template_id: None,
            source_text_bytes: None,
            source_text_complete: false,
        }),
        plan: DefinitionPayload::plan(
            r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT 1"/></Statements></Batch></BatchSequence></ShowPlanXML>"#,
            crate::Engine::Mssql,
        ),
    };
    assert!(input.is_valid());
    let record = TelemetryRecord::Operational(OperationalObservation {
        source: input.source.clone(),
        key: input.observations[0].key().unwrap(),
        fact: OperationalFact::CurrentRequestWork(Box::new(input.observations[0].clone())),
    });
    assert_eq!(record.definition_payloads().count(), 2);
    for (from, until) in [
        ("2020-01-01T11:59:59Z", "2020-01-01T12:00:00.6Z"),
        ("2020-01-01T12:00:00.6Z", "2020-01-01T12:00:00.3Z"),
        ("2020-01-01T12:00:00.6Z", "2020-01-01T12:00:00.95Z"),
    ] {
        let mut edited = input.clone();
        let CurrentRequestWorkNative::Mssql { work } = &mut edited.observations[0].native else { panic!() };
        let MssqlCurrentRequestPlan::Observed { observed_from, observed_until, .. } = &mut work.current_plan else {
            panic!()
        };
        *observed_from = from.into();
        *observed_until = until.into();
        assert!(!edited.is_valid());
    }
}
