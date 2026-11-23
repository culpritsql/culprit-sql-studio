use super::*;
use crate::activity::current_execution_profile::tests as native;

fn plan(xml: &str) -> MssqlCurrentRequestPlan {
    MssqlCurrentRequestPlan::Observed {
        observed_from: "2020-01-01T06:00:00.2Z".into(),
        observed_until: "2020-01-01T06:00:00.7Z".into(),
        context: Box::new(crate::cached_statements::context(&native::cache("2020-01-01T06:00:00.2Z", 4)).unwrap()),
        plan: DefinitionPayload::plan(xml, Engine::Mssql),
    }
}
fn rows() -> Vec<MssqlOperatorThreadProfile> {
    vec![MssqlOperatorThreadProfile {
        node_id: 0,
        thread_id: 1,
        task_address: None,
        physical_operator: "Table Scan".into(),
        counters: Default::default(),
        open_ticks: None,
        close_ticks: None,
        database_id: None,
        object_id: None,
        index_id: None,
    }]
}
#[test]
fn selected_parser_supplies_typed_node_context_without_copying_cached_runtime_or_raw_literals() {
    let native = plan(native::XML);
    let result = selected(&native, Some("0123456789ABCDEF"), &rows());
    let MssqlExecutionOperators::Observed { context, plan_sha256, nodes } = result else { panic!() };
    assert_eq!(context.identity.plan_generation, "4");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].kind, culprit_core::NodeKind::Scan);
    assert_eq!(nodes[0].parallel, Some(true));
    let MssqlCurrentRequestPlan::Observed { plan: DefinitionPayload::Available { sha256, .. }, .. } = native else {
        panic!()
    };
    assert_eq!(sha256, plan_sha256);
    let encoded = serde_json::to_string(&nodes).unwrap();
    assert!(!encoded.contains("private-profile-literal"));
    assert!(!encoded.contains("ActualCPUms"));
    assert!(
        nodes[0].relational_input.is_none_or(|input| {
            input.qualified_kind() != culprit_core::plan_facts::RelationalInputKind::TableValuedParameter
        }),
        "a current module/type binding was not acquired"
    );
}
#[test]
fn unknown_or_foreign_query_and_ambiguous_native_statement_or_node_never_choose_a_representative() {
    for query in [None, Some("0000000000000000"), Some("FEDCBA9876543210")] {
        assert!(matches!(selected(&plan(native::XML), query, &rows()), MssqlExecutionOperators::Unavailable { .. }));
    }
    let mut wrong = rows();
    wrong[0].node_id = 1;
    assert!(matches!(
        selected(&plan(native::XML), Some("0123456789ABCDEF"), &wrong),
        MssqlExecutionOperators::Unavailable { reason: MssqlExecutionOperatorsUnavailable::AmbiguousNode }
    ));
    let duplicate = native::XML.replace(
        "</Statements>",
        &format!(
            "{}{}",
            native::XML.split("<Statements>").nth(1).unwrap().split("</Statements>").next().unwrap(),
            "</Statements>"
        ),
    );
    assert!(matches!(
        selected(&plan(&duplicate), Some("0123456789ABCDEF"), &rows()),
        MssqlExecutionOperators::Unavailable { .. }
    ));
}
