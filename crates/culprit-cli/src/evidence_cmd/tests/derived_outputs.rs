use super::*;
#[test]
fn every_materialized_gold_family_is_an_explicit_shared_read_selection_with_one_caller_budget() {
    let range = culprit_core::telemetry::digest(b"selected original Gold capture");
    let parsed = crate::Cli::try_parse_from([
        "culprit",
        "collect",
        "evidence",
        "read",
        "--config",
        "archive.json",
        "--range",
        &range,
        "--family",
        "derived-incidents,derived-findings,derived-changes",
        "--max-rows",
        "3",
        "--max-queries",
        "5",
        "--json",
    ])
    .unwrap();
    let crate::Command::Collect(crate::collect_cmd::CollectCommand::Evidence(EvidenceCommand::Read(args))) =
        parsed.command
    else {
        panic!("read all existing Gold outputs")
    };
    let request = args.common.request(args.families.into_iter().map(Into::into).collect(), Format::Json).unwrap();
    assert_eq!(
        request.families,
        [EvidenceFamily::DerivedIncidents, EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges]
    );
    assert_eq!(request.budget.max_rows, 3);
    assert_eq!(request.budget.max_queries, 5);
    assert!(request.budget.recovery_payloads.is_none());
}
