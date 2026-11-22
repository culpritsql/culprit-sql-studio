//! Exact inherited English and native-branch controls. These are pure presentation fixtures, not native admission.
use super::*;
use culprit_core::{Engine, NodeKind, PlanKind, Snapshot};

fn plan(engine: Engine, cost: f64) -> Plan {
    let mut plan = crate::postgres::parse(
        r#"[{"Plan":{"Node Type":"Index Scan","Relation Name":"items","Plan Rows":1,"Total Cost":1}}]"#,
        PlanKind::Estimated,
        None,
    )
    .unwrap();
    plan.engine = engine;
    plan.total_cost = cost;
    plan.root.subtree_cost = cost;
    plan.root.self_cost = cost;
    plan.root.kind = NodeKind::Seek;
    plan.root.op = "Index Seek".into();
    plan.root.table_rows = None;
    plan.root.est_rows = 1.0;
    plan.root.est_executions = 1.0;
    plan
}
fn query() -> QueryStat {
    let snapshot: Snapshot = serde_json::from_str(include_str!("../../../../../fixtures/postgres-demo.json")).unwrap();
    let mut query = snapshot.queries()[0].clone();
    query.plans.clear();
    query.plan_id = None;
    query.plan_history = None;
    query.plan = None;
    query.text = "SELECT id FROM items".into();
    query.executions = 1.0;
    query.total_time_ms = 1.0;
    query.mean_time_ms = 1.0;
    query.max_time_ms = None;
    query.rows = Some(1.0);
    query.rows_affected = None;
    query.logical_reads = Some(1.0);
    query
}
fn slow_query(runs: f64, total: f64, mean: f64, maximum: Option<f64>) -> QueryStat {
    let mut query = query();
    query.executions = runs;
    query.total_time_ms = total;
    query.mean_time_ms = mean;
    query.max_time_ms = maximum;
    query.rows = Some(runs);
    query.logical_reads = Some(runs);
    query
}

#[test]
fn uncosted_native_steps_keep_singular_plural_and_detail_wording() {
    culprit_core::i18n::scope("en", || {
        let mut plan = plan(Engine::Mysql, 2000.0);
        let mut sort = plan.root.clone();
        sort.id = 1;
        sort.kind = NodeKind::Sort;
        sort.op = "Sort".into();
        sort.detail = Some("using `filesort`".into());
        sort.self_cost = 0.0;
        sort.subtree_cost = 0.0;
        plan.root.children.push(sort);
        assert_eq!(
            no_share_reason(&plan, &plan.root, None).unwrap(),
            "`Sort` (using filesort) has no cost of its own in the plan, so no share is given."
        );
        let mut aggregate = plan.root.children[0].clone();
        aggregate.id = 2;
        aggregate.kind = NodeKind::Aggregate;
        aggregate.op = "Aggregate".into();
        aggregate.detail = None;
        plan.root.children.push(aggregate);
        assert_eq!(
            shares_note(&plan, None).unwrap(),
            "`Sort` (using filesort) and `Aggregate` have no cost of their own in the plan, so no share is given."
        );
    });
}

#[test]
fn native_guess_rows_and_repeated_inner_runs_keep_distinct_explanations() {
    culprit_core::i18n::scope("en", || {
        let mut plan = plan(Engine::Mssql, 1.0);
        plan.root.facts_mut().selectivity_guess = Some("captured default estimate".into());
        assert_eq!(
            no_share_reason(&plan, &plan.root, None).unwrap(),
            "The rows it works on rest on the planner's default guess for a parameter, not on the table's statistics, so no share is given."
        );
        assert_eq!(
            shares_note(&plan, None).unwrap(),
            "No share is given for a step whose rows rest on the planner's default guess for a parameter, nor for the steps above it."
        );
        let mut outer = plan.root.clone();
        outer.id = 1;
        let mut inner = plan.root.clone();
        inner.id = 2;
        inner.facts_mut().selectivity_guess = None;
        plan.root.facts_mut().selectivity_guess = None;
        plan.root.op = "Nested Loops".into();
        plan.root.kind = NodeKind::Join;
        plan.root.children = vec![outer, inner];
        assert_eq!(
            no_share_reason(&plan, &plan.root.children[1], None).unwrap(),
            "It runs once for each row of a step whose rows rest on the planner's default guess for a parameter, so how often it runs is that guess, and no share is given."
        );
        assert_eq!(
            shares_note(&plan, None).unwrap(),
            "No share is given for a step whose rows rest on the planner's default guess for a parameter, nor for the steps above it or run once for each of its rows."
        );
    });
}

#[test]
fn whole_read_grammar_uses_native_count_without_turning_rounded_decimals_or_compact_counts_into_one_row() {
    culprit_core::i18n::scope("en", || {
        let mut plan = plan(Engine::Postgres, 1.0);
        plan.root.op = "Seq Scan".into();
        plan.root.kind = NodeKind::Scan;
        plan.root.object = Some("public.items".into());
        for (native, wording) in
            [(1.0, "1 row"), (2.0, "2 rows"), (0.96, "1.0 rows"), (1.04, "1.0 rows"), (1000.0, "1.0K rows")]
        {
            plan.root.table_rows = Some(native);
            assert_eq!(
                whole_read_reason(&plan, &plan.root),
                format!(
                    "Its estimated cost (1.00) is too small for a share of it to say where the time goes, but `Seq Scan` reads all {wording} of public.items on every run."
                )
            );
        }
        plan.root.table_rows = None;
        assert_eq!(
            whole_read_reason(&plan, &plan.root),
            "Its estimated cost (1.00) is too small for a share of it to say where the time goes, but `Seq Scan` reads all of public.items on every run."
        );
        plan.root.object = None;
        assert_eq!(
            whole_read_reason(&plan, &plan.root),
            "Its estimated cost (1.00) is too small for a share of it to say where the time goes, but `Seq Scan` reads all of its table on every run."
        );
        plan.root.table_rows = Some(1.0);
        assert_eq!(
            whole_read_reason(&plan, &plan.root),
            "Its estimated cost (1.00) is too small for a share of it to say where the time goes, but `Seq Scan` reads all 1 row of its table on every run."
        );
    });
}

#[test]
fn row_misestimates_keep_returned_and_changed_rows_distinct_including_fractional_estimates() {
    culprit_core::i18n::scope("en", || {
        let mut plan = plan(Engine::Mssql, 0.01);
        let mut query = query();
        query.rows = Some(50000.0);
        assert_eq!(
            cheap_plan_reason(&plan, Some(&query)),
            "Its costs rest on an estimate of 1 row a run, and its runs return far more (50K rows a run), so no share of them is put on one step."
        );
        query.text = "UPDATE items SET id=id".into();
        query.rows = Some(0.0);
        query.rows_affected = Some(50000.0);
        plan.root.est_rows = 1.04;
        assert_eq!(
            cheap_plan_reason(&plan, Some(&query)),
            "Its costs rest on an estimate of 1.0 rows a run, and its runs change far more (50K rows a run), so no share of them is put on one step."
        );
    });
}

#[test]
fn measured_read_units_follow_existing_native_metric_facts_and_keep_current_english() {
    culprit_core::i18n::scope("en", || {
        let mut query = query();
        query.logical_reads = Some(200.0);
        for (engine, unit) in [(Engine::Mssql, "pages"), (Engine::Postgres, "pages"), (Engine::Mysql, "rows examined")]
        {
            let plan = plan(engine, 0.01);
            assert_eq!(
                cheap_plan_reason(&plan, Some(&query)),
                format!(
                    "Its runs read 200 {unit} a run; the plan's estimated cost is too small to say where that reading happens, so no share of it is put on one step."
                )
            );
        }
    });
}

#[test]
fn slow_completed_runs_keep_one_two_outlier_and_average_sentences_and_native_maximum_clamp() {
    culprit_core::i18n::scope("en", || {
        let plan = plan(Engine::Mssql, 0.01);
        for (query, timing) in [
            (slow_query(1.0, 1500.0, 1500.0, None), "its one run took 1.5 s"),
            (slow_query(2.0, 3000.0, 1500.0, Some(2500.0)), "its runs took 2.5 s and 500 ms"),
            (slow_query(2.0, 3000.0, 1500.0, Some(1500.0)), "its runs averaged 1.5 s"),
            (
                slow_query(5.0, 10000.0, 2000.0, Some(6000.0)),
                "its 5 runs took 10.0 s in all, with the slowest taking 6.0 s",
            ),
            (slow_query(20.0, 30000.0, 1500.0, Some(29000.0)), "its runs averaged 1.5 s"),
            (slow_query(2.0, 3000.0, 1500.0, Some(10000.0)), "its runs took 3.0 s and 0 ms"),
        ] {
            assert_eq!(
                cheap_plan_reason(&plan, Some(&query)),
                format!(
                    "The plan is estimated to be cheap, yet {timing}: the time isn't in what it costs, so no share of its cost is put on one step."
                )
            );
        }
    });
}

#[test]
fn selected_completed_plan_timing_does_not_borrow_overall_or_unselected_variant_means() {
    culprit_core::i18n::scope("en", || {
        let plan = plan(Engine::Mssql, 0.01);
        let mut query = slow_query(100.0, 100000.0, 1000.0, Some(90000.0));
        query.plan_id = Some("selected".into());
        query.plans = vec![
            serde_json::from_value(serde_json::json!({
                "id":"selected", "executions":1.0,"total_time_ms":2000.0,"mean_time_ms":2000.0,"logical_reads":1.0
            }))
            .unwrap(),
        ];
        assert_eq!(
            cheap_plan_reason(&plan, Some(&query)),
            "The plan is estimated to be cheap, yet its one run took 2.0 s: the time isn't in what it costs, so no share of its cost is put on one step."
        );
        let mut other = query.plans[0].clone();
        other.id = "other".into();
        query.plans.push(other);
        query.plan_id = None;
        assert_eq!(
            cheap_plan_reason(&plan, Some(&query)),
            "The plan as a whole is cheap, so no share of its cost is put on one step."
        );
    });
}

#[test]
fn unknown_calibration_no_own_cost_and_missing_counts_never_create_a_cost_share() {
    culprit_core::i18n::scope("en", || {
        let unknown = plan(Engine::Yugabyte, 1.0);
        assert_eq!(
            shares_note(&unknown, None).unwrap(),
            "The optimizer's cost scale has no calibrated work threshold here, so this estimate establishes neither a cheap run nor a share of statement time. Compare measured operator work and elapsed time."
        );
        let mut measured = plan(Engine::Mssql, 1.0);
        let mut step = measured.root.clone();
        step.self_cost = 0.0;
        step.id = 1;
        step.op = "Compute Scalar".into();
        measured.root.children.push(step);
        assert_eq!(
            no_share_reason(&measured, &measured.root.children[0], None).unwrap(),
            "`Compute Scalar` has no cost of its own in the plan, so no share is given."
        );
        let cheap = plan(Engine::Mssql, 0.01);
        assert_eq!(
            shares_note(&cheap, None).unwrap(),
            "The plan as a whole is cheap, so no share of its cost is put on one step."
        );
        let mut unweighed = cheap;
        unweighed.total_cost = 0.0;
        unweighed.root.self_cost = 0.0;
        unweighed.root.subtree_cost = 0.0;
        assert!(shares_note(&unweighed, None).is_none());
    });
}

mod languages;
