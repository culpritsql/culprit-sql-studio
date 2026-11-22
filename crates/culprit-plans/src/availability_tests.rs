use super::*;

fn portable_plan() -> String {
    let scan: culprit_core::Snapshot = serde_json::from_str(include_str!("../../../fixtures/mssql-demo.json")).unwrap();
    let plan = scan.workload.as_ref().unwrap().queries.iter().find_map(|query| query.plan.as_ref()).unwrap();
    serde_json::to_string(plan).unwrap()
}

#[test]
fn every_native_format_parses_without_database_provider_features() {
    let portable = portable_plan();
    let inputs = [
        (PlanFormat::ShowplanXml, include_str!("../../culprit-engine/tests/plans/mssql-seek.sqlplan")),
        (PlanFormat::PostgresJson, include_str!("../../culprit-engine/tests/plans/pg-index.json")),
        (PlanFormat::YugabyteJson, include_str!("../../providers/culprit-yugabyte/tests/data/loops.json")),
        (PlanFormat::PostgresText, include_str!("../../culprit-engine/tests/plans/pg-analyze.txt")),
        (PlanFormat::MysqlJson, include_str!("../../culprit-engine/tests/plans/mysql-v2.json")),
        (PlanFormat::MysqlTree, include_str!("../../culprit-engine/tests/plans/mysql-tree-g.txt")),
        (PlanFormat::SqliteText, "QUERY PLAN\n|--SCAN o\n`--USE TEMP B-TREE FOR ORDER BY\n"),
        (PlanFormat::Culprit, portable.as_str()),
    ];
    for (format, text) in inputs {
        assert_eq!(detect(text), Some(format), "{format:?}");
        let plan = parse_plan(text).unwrap_or_else(|error| panic!("{format:?}: {}", error.message));
        assert!(!plan.nodes().is_empty(), "{format:?}");
    }
}

#[test]
fn native_yugabyte_context_selects_its_parser_for_an_unmarked_qpm_plan() {
    let text = include_str!("../../providers/culprit-yugabyte/tests/data/qpm.json");
    assert_eq!(detect(text), Some(PlanFormat::PostgresJson));
    assert_eq!(detect_native(text, Engine::Yugabyte), Some(PlanFormat::YugabyteJson));
    let plan = parse_native_plan(text, Engine::Yugabyte).unwrap();
    {
        assert_eq!(plan.engine, Engine::Yugabyte);
        assert!(plan.facts().estimated_cost_unavailable);
        assert!(plan.nodes().iter().all(|node| node.facts().estimated_rows_unavailable));
        assert!(plan.root.act_rows.is_none() && plan.facts().elapsed_ms.is_none());
    }
    assert_eq!(parse_plan(text).unwrap().engine, Engine::Postgres, "pasted-plan detection is unchanged");
}

#[test]
fn native_context_cannot_relabel_a_serialized_foreign_plan() {
    let portable = portable_plan();
    assert_eq!(parse_plan(&portable).unwrap().engine, Engine::Mssql);
    assert!(parse_native_plan(&portable, Engine::Mysql).is_err());
    assert_eq!(parse_native_plan(&portable, Engine::Mssql).unwrap().engine, Engine::Mssql);
}
