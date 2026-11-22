use super::*;
const EXPORTED_AT: &str = "2026-11-22 12:30 UTC";

#[test]
fn comparison_exports_rebuild_scenarios_from_redacted_plans() {
    let snapshot: culprit_core::Snapshot =
        serde_json::from_str(include_str!("../../../../fixtures/mssql-demo.json")).unwrap();
    let mut a = snapshot.queries().iter().find_map(|q| q.plan.clone()).unwrap();
    let mut b = a.clone();
    a.facts_mut().compiled_parameters = Some("@p0 = N'private-first-canary'".into());
    b.facts_mut().compiled_parameters = Some("@p0 = N'private-second-canary'".into());
    let diff = culprit_core::plan_diff_named(&a, "A", &b, "B");
    assert!(serde_json::to_string(&diff).unwrap().contains("private-first-canary"));
    for redacted in [true, false] {
        let options = ExportOptions { redact: redacted, ..Default::default() };
        for output in [
            diff_html(&a, &b, &diff, "A", "B", &options, EXPORTED_AT),
            diff_markdown(&a, &b, &diff, "A", "B", &options, EXPORTED_AT),
        ] {
            assert_eq!(output.contains("private-first-canary"), !redacted);
            assert_eq!(output.contains("private-second-canary"), !redacted);
        }
    }
}

#[test]
fn file_names_are_short_safe_and_lowercase() {
    assert_eq!(
        file_stem("A type mismatch on customers.email stops a read"),
        "a-type-mismatch-on-customers-email-stops-a-read"
    );
    assert_eq!(file_stem("../../etc/passwd"), "etc-passwd");
    assert_eq!(file_stem("   "), "culprit");
    assert!(file_stem(&"x".repeat(500)).len() <= 61);
}

#[test]
fn native_exports_keep_the_engine_format() {
    let (plan_facts, node_facts) = (
        serde_json::to_value(culprit_core::PlanFacts::default()).unwrap(),
        serde_json::to_value(culprit_core::PlanNodeFacts::default()).unwrap(),
    );
    let mut plan: Plan = serde_json::from_value(serde_json::json!({
        "engine": "postgres", "kind": "estimated", "statement": null, "total_cost": 1.0, "warnings": [],
        "index_hints": [], "props": {}, "facts": plan_facts, "raw": "[{\"Plan\": {}}]",
        "root": {"id": 0, "op": "Seq Scan", "kind": "scan", "detail": null, "object": "orders", "index": null,
            "seek_predicate": null, "predicate": "(status = 'open'::text)", "est_rows": 1.0, "est_executions": 1.0,
            "act_rows": null, "act_executions": null, "rows_read": null, "table_rows": null, "subtree_cost": 1.0,
            "self_cost": 1.0, "act_time_ms": null, "warnings": [], "props": {}, "facts": node_facts, "children": []}
    }))
    .unwrap();
    assert_eq!(native_extension(&plan), "json");
    plan.raw = Some("Seq Scan on orders  (cost=0.00..1.00 rows=1 width=4)".into());
    assert_eq!(native_extension(&plan), "txt");
    plan.raw = Some("<ShowPlanXML/>".into());
    assert_eq!(native_extension(&plan), "sqlplan");
    plan.raw = None;
    assert!(plan_native(&plan, &ExportOptions::default()).is_none());
}

#[test]
fn redaction_is_on_unless_asked() {
    assert!(ExportOptions::default().redact);
    let parsed: ExportOptions = serde_json::from_str("{}").unwrap();
    assert!(parsed.redact && parsed.notes.is_empty());
}

#[test]
fn a_plan_file_never_puts_markup_into_an_html_page() {
    let text = include_str!("../../../culprit-engine/tests/plans/mariadb-index.json");
    let mut plan = crate::parse_plan(text).unwrap();
    assert_eq!(plan_engine_label(&plan), "MariaDB");
    // A plan opened from a saved JSON file can carry any properties.
    for key in ["flavor", "server_version", "query_cost", "dop"] {
        plan.props.insert(key.into(), "<img src=x onerror=alert(1)>".into());
    }
    plan.facts_mut().flavor = Some("<img src=x onerror=alert(1)>".into());
    plan.facts_mut().server_version = Some("<img src=x onerror=alert(1)>".into());
    assert_eq!(plan_engine_label(&plan), "MySQL");
    let options = ExportOptions::default();
    let page = plan_html(&plan, "Plan", &options, EXPORTED_AT);
    assert!(!page.contains("onerror"), "plan page");
    let diff = culprit_core::plan_diff_named(&plan, "A", &plan, "B");
    assert!(!diff_html(&plan, &plan, &diff, "A", "B", &options, EXPORTED_AT).contains("onerror"), "diff page");
}

#[test]
fn caller_owned_clock_makes_plan_and_comparison_exports_deterministic_without_touching_native_text() {
    let plan = crate::parse_plan(include_str!("../../../culprit-engine/tests/plans/pg-index.json")).unwrap();
    let options = ExportOptions::default();
    let difference = culprit_core::plan_diff_named(&plan, "Before", &plan, "After");
    let comparison =
        DiffExport { before: &plan, after: &plan, difference: &difference, before_name: "Before", after_name: "After" };
    for format in [ExportFormat::Markdown, ExportFormat::Html] {
        let first = export_plan(&plan, "Plan", format, &options, EXPORTED_AT).unwrap();
        let second = export_plan(&plan, "Plan", format, &options, EXPORTED_AT).unwrap();
        assert_eq!(first.contents, second.contents);
        assert!(first.contents.contains(EXPORTED_AT));
        let first = export_diff(&comparison, format, &options, EXPORTED_AT).unwrap();
        let second = export_diff(&comparison, format, &options, EXPORTED_AT).unwrap();
        assert_eq!(first.contents, second.contents);
        assert!(first.contents.contains(EXPORTED_AT));
    }
    let native = export_plan(&plan, "Plan", ExportFormat::Native, &options, EXPORTED_AT).unwrap();
    assert_eq!(native.contents, plan_native(&plan, &options).unwrap());
    assert!(!native.contents.contains(EXPORTED_AT));
}

#[test]
fn an_explicit_footer_clock_is_text_in_html_and_markdown() {
    let plan = crate::parse_plan(include_str!("../../../culprit-engine/tests/plans/pg-index.json")).unwrap();
    let supplied_time = "<script>alert(1)</script> & caller label";
    for output in [
        plan_html(&plan, "Plan", &ExportOptions::default(), supplied_time),
        plan_markdown(&plan, "Plan", &ExportOptions::default(), supplied_time),
    ] {
        assert!(!output.contains("<script>"));
        assert!(output.contains("&lt;script&gt;alert(1)&lt;/script&gt; &amp; caller label"));
    }
}
