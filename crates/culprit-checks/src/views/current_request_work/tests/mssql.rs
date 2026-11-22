use super::*;
fn input() -> CurrentRequestWorkSnapshot {
    let mut value = snapshot();
    value.profile = CurrentRequestWorkProfile::MssqlRequest;
    value.source = "mssql-current-request-work".into();
    value.context.target = DatabaseIdentity::server(Engine::Mssql, "shop", "fixture.invalid", 1433, None).into();
    let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut value.context.partition else { panic!() };
    member.engine = Engine::Mssql;
    value.coverage = StateSourceCoverage::collected(value.source.clone(), 1);
    let row = &mut value.observations[0];
    row.actor = ActivityKey {
        session: NativeActivityIdentity { id: "42".into(), generation: "2026-11-15T08:00:00.0030000".into() },
        request: Some(NativeActivityIdentity { id: "0".into(), generation: "2026-11-15T09:59:00.0030000".into() }),
        transaction: None,
    };
    row.statement_identity = mssql_request_statement_identity("AABB", 0, -1, 7).unwrap();
    row.native = CurrentRequestWorkNative::Mssql {
        work: Box::new(MssqlCurrentRequestWork {
            native_database_id: 7,
            checked_until: "2026-11-15T10:00:00.9Z".into(),
            elapsed_ms: Some(-1),
            cpu_ms: Some(3200),
            reads: Some("9007199254740993".into()),
            writes: Some("0".into()),
            logical_reads: None,
            rows_returned: Some("42".into()),
            request_plan: None,
            current_plan: MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::NotObserved },
        }),
    };
    value
}
#[test]
fn exact_request_counters_and_negative_time_do_not_become_cached_work_or_health() {
    let input = input();
    let report = explain(&input, true).unwrap();
    let row = &report.entries[0];
    assert!(row.explanation.contains(&t!("view.current_request_work.mssql.negative_time")));
    assert!(row.metrics.iter().all(|metric| !metric.is("view.current_request_work.mssql.metric.elapsed")));
    assert_eq!(
        row.metrics.iter().find(|m| m.is("view.current_request_work.mssql.metric.reads")).unwrap().text.as_deref(),
        Some(format::digits("9007199254740993".into()).as_str())
    );
    assert!(report.limitations.contains(&t!("view.current_request_work.mssql.limit.parallel")));
    assert!(report.limitations.contains(&t!("view.current_request_work.mssql.limit.plan")));
}
#[test]
fn not_observed_is_explicit_partial_visibility_without_ended_or_recovered_claim() {
    let mut input = input();
    input.observations.clear();
    input.coverage = StateSourceCoverage::observed(input.source.clone(), 0, false);
    input.result.membership = FocusedRequestMembership::NotObserved;
    let report = explain(&input, true).unwrap();
    assert_eq!(report.source_state, SourceState::Partial);
    assert_eq!(report.summary, t!("view.current_request_work.mssql.not_observed"));
    assert!(report.entries.is_empty());
    assert!(report.limitations.contains(&t!("view.current_request_work.mssql.limit.visibility")));
}
