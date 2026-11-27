//! Source-free archived execution-plan insights and side-by-side plan comparison over verified recovery artifacts.
use super::{
    warehouse::{arguments, check},
    *,
};
use culprit_core::{
    Plan, WarehouseArtifactQuery, WarehouseArtifactReport, WarehouseArtifactTextRead, plan_diff_named,
    telemetry::{DefinitionContentKind, DeliveryProblem, DiagnosticEvidenceRef},
};
use culprit_read::cancellation::CollectionCancellation;

pub(crate) const WAREHOUSE_PLAN_INSIGHTS_TOOL: &str = "warehouse_plan_insights";
pub(crate) const WAREHOUSE_PLAN_COMPARE_TOOL: &str = "warehouse_plan_compare";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WarehousePlanInsightsArgs {
    #[schemars(schema_with = "super::warehouse::resource_schema")]
    resource: culprit_core::PublishedResourceSelector,
    /// Exact `DiagnosticEvidenceRef` copied from an archived incident query, finding, or window entry.
    reference: Value,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WarehousePlanCompareArgs {
    #[schemars(schema_with = "super::warehouse::resource_schema")]
    resource: culprit_core::PublishedResourceSelector,
    /// Baseline plan `DiagnosticEvidenceRef` (e.g. before regression).
    plan_a: Value,
    /// Comparison plan `DiagnosticEvidenceRef` (e.g. regressed plan).
    plan_b: Value,
    /// Optional short display label for plan A (default `"Plan A"`).
    label_a: Option<String>,
    /// Optional short display label for plan B (default `"Plan B"`).
    label_b: Option<String>,
}

pub(super) fn insights_definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            WAREHOUSE_PLAN_INSIGHTS_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("mcp.warehouse_plan_insights.description")),
            schema_for_type::<WarehousePlanInsightsArgs>(),
        )
        .with_title(culprit_core::t!("mcp.warehouse_plan_insights.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}

pub(super) fn compare_definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            WAREHOUSE_PLAN_COMPARE_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("mcp.warehouse_plan_compare.description")),
            schema_for_type::<WarehousePlanCompareArgs>(),
        )
        .with_title(culprit_core::t!("mcp.warehouse_plan_compare.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}

fn extract_verified_plan(
    report: &WarehouseArtifactReport,
    target_ref: &DiagnosticEvidenceRef,
) -> Result<(Plan, String, Vec<String>)> {
    let target_key = target_ref.key()?;
    for record in &report.records {
        let matches_record = record.reference.key()? == target_key
            || record.linked_from.iter().any(|from| from.key().ok().as_deref() == Some(target_key.as_str()));
        if !matches_record {
            continue;
        }
        for artifact in &record.artifacts {
            if artifact.kind == DefinitionContentKind::Plan
                && let Some(plan) = &artifact.plan
            {
                return Ok(((**plan).clone(), record.collected_until.clone(), artifact.limitations.clone()));
            }
        }
    }
    Err(Error::not_found(culprit_core::t!("mcp.warehouse_plan.not_found")))
}

impl CulpritMcp {
    pub(super) async fn warehouse_plan_insights(
        &self,
        value: Value,
        cancellation: Option<CollectionCancellation>,
    ) -> Result<Value> {
        let delegation = self.warehouse_delegation(WAREHOUSE_PLAN_INSIGHTS_TOOL)?;
        let args: WarehousePlanInsightsArgs = arguments(value)?;
        let reference: DiagnosticEvidenceRef = arguments(args.reference)?;
        let request = WarehouseArtifactQuery {
            resource: args.resource,
            references: vec![reference.clone()],
            full_payloads: true,
            expected_sql_sha256: None,
            text: WarehouseArtifactTextRead::Complete,
        };
        request.validate()?;
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = delegation.reader()?.artifacts(&request, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if report.query != request {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let (plan, collected_until, limitations) = extract_verified_plan(&report, &reference)?;
        let mut insights_view = crate::view::plan_insights(&plan);
        insights_view["collected_until"] = Value::String(collected_until);
        insights_view["qualifications"] = json!(report.qualifications);
        insights_view["limitations"] = json!(limitations);
        insights_view["gaps_available"] = json!(report.gaps.len());
        let output = super::response::bounded_tool_result(insights_view)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(output)
    }

    pub(super) async fn warehouse_plan_compare(
        &self,
        value: Value,
        cancellation: Option<CollectionCancellation>,
    ) -> Result<Value> {
        let delegation = self.warehouse_delegation(WAREHOUSE_PLAN_COMPARE_TOOL)?;
        let args: WarehousePlanCompareArgs = arguments(value)?;
        let ref_a: DiagnosticEvidenceRef = arguments(args.plan_a)?;
        let ref_b: DiagnosticEvidenceRef = arguments(args.plan_b)?;
        let label_a = args.label_a.unwrap_or_else(|| "Plan A".into());
        let label_b = args.label_b.unwrap_or_else(|| "Plan B".into());
        if label_a.len() > 128
            || label_b.len() > 128
            || label_a.chars().any(char::is_control)
            || label_b.chars().any(char::is_control)
        {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }

        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;

        let req_a = WarehouseArtifactQuery {
            resource: args.resource.clone(),
            references: vec![ref_a.clone()],
            full_payloads: true,
            expected_sql_sha256: None,
            text: WarehouseArtifactTextRead::Complete,
        };
        let req_b = WarehouseArtifactQuery {
            resource: args.resource,
            references: vec![ref_b.clone()],
            full_payloads: true,
            expected_sql_sha256: None,
            text: WarehouseArtifactTextRead::Complete,
        };
        req_a.validate()?;
        req_b.validate()?;

        let reader = delegation.reader()?;
        let rep_a = reader.artifacts(&req_a, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if rep_a.query != req_a {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let rep_b = reader.artifacts(&req_b, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if rep_b.query != req_b {
            return Err(DeliveryProblem::DestinationChanged.error());
        }

        let (plan_a, collected_a, _) = extract_verified_plan(&rep_a, &ref_a)?;
        let (plan_b, collected_b, _) = extract_verified_plan(&rep_b, &ref_b)?;
        if plan_a.engine != plan_b.engine {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse_plan.engine_mismatch")));
        }
        let diff = plan_diff_named(&plan_a, &label_a, &plan_b, &label_b);
        let changes: Vec<Value> = diff
            .changes
            .iter()
            .take(32)
            .map(|change| {
                json!({
                    "kind": change.kind,
                    "title": change.title,
                    "detail": change.detail,
                    "node_a": change.a,
                    "node_b": change.b,
                    "weight": change.weight,
                })
            })
            .collect();
        let output = json!({
            "engine": plan_b.engine,
            "headlines": diff.headlines,
            "same_shape": diff.same_shape,
            "cost_a": diff.cost_a,
            "cost_b": diff.cost_b,
            "cost_ratio": diff.cost_ratio,
            "time_a_ms": diff.time_a_ms,
            "time_b_ms": diff.time_b_ms,
            "scenarios": diff.scenarios,
            "collected_until_a": collected_a,
            "collected_until_b": collected_b,
            "changes_total": diff.changes.len(),
            "changes": changes,
            "plan_b_insights": crate::view::plan_insights(&plan_b)["insights"],
            "qualifications": rep_b.qualifications,
        });
        let output = super::response::bounded_tool_result(output)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::incidents::tests::{Host, body, report, zero_work};
    use super::*;
    use crate::WarehouseDelegation;
    use culprit_core::{
        NodeKind, Report, WarehouseArtifactRecord, WarehouseArtifactScope, WarehouseArtifactText,
        WarehouseDefinitionArtifact, WarehouseFleetFreshness, WarehouseFleetReport, WarehouseFleetRow,
        WindowNativeClock, telemetry::DefinitionPayload,
    };
    use culprit_engine::{
        WarehouseArtifactsTask, WarehouseFleetReader, WarehouseFleetTask, WarehouseIncidentReader,
        WarehouseIncidentTask, WarehouseResourcesTask, WarehouseWindowTask,
    };
    use std::sync::{Arc, Mutex};

    struct MockWarehouse {
        artifacts: Mutex<Vec<WarehouseArtifactReport>>,
        fleet_report: WarehouseFleetReport,
    }

    impl WarehouseFleetReader for MockWarehouse {
        fn fleet<'a>(
            &'a self,
            request: &'a culprit_core::WarehouseFleetRequest,
            _: &'a CollectionCancellation,
        ) -> WarehouseFleetTask<'a> {
            let mut out = self.fleet_report.clone();
            out.request = request.clone();
            Box::pin(async move { Ok(out) })
        }
    }

    impl WarehouseIncidentReader for MockWarehouse {
        fn resources<'a>(
            &'a self,
            _: &'a culprit_core::PublishedResourceRequest,
            _: &'a CollectionCancellation,
        ) -> WarehouseResourcesTask<'a> {
            Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
        }
        fn window<'a>(
            &'a self,
            _: &'a culprit_core::WarehouseWindowQuery,
            _: &'a CollectionCancellation,
        ) -> WarehouseWindowTask<'a> {
            Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
        }
        fn incident<'a>(
            &'a self,
            _: &'a culprit_core::PublishedResourceSelector,
            _: &'a str,
            _: &'a CollectionCancellation,
        ) -> WarehouseIncidentTask<'a> {
            Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
        }
        fn artifacts<'a>(
            &'a self,
            query: &'a WarehouseArtifactQuery,
            _: &'a CollectionCancellation,
        ) -> WarehouseArtifactsTask<'a> {
            let mut next = self.artifacts.lock().unwrap().remove(0);
            next.query = query.clone();
            Box::pin(async move { Ok(next) })
        }
    }

    fn demo_plans() -> (Plan, Plan) {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/mssql-demo.report.json");
        let demo: Report = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let mut plan_a = demo.snapshot.workload.unwrap().queries.into_iter().find_map(|q| q.plan).unwrap();
        plan_a.root.kind = NodeKind::Seek;
        plan_a.root.op = "Index Seek".into();
        plan_a.root.object = Some("dbo.Orders".into());
        plan_a.root.subtree_cost = 0.5;
        plan_a.root.self_cost = 0.5;
        plan_a.root.est_rows = 10.0;
        plan_a.total_cost = 0.5;

        let mut plan_b = plan_a.clone();
        plan_b.root.kind = NodeKind::Scan;
        plan_b.root.op = "Clustered Index Scan".into();
        plan_b.root.subtree_cost = 48.0;
        plan_b.root.self_cost = 48.0;
        plan_b.root.est_rows = 250_000.0;
        plan_b.root.predicate = Some("CONVERT_IMPLICIT(nvarchar(50), [CustomerCode], 0) = @P1".into());
        plan_b.total_cost = 48.0;
        (plan_a, plan_b)
    }

    fn artifact_report(host: &Host, reference: DiagnosticEvidenceRef, plan: Plan) -> WarehouseArtifactReport {
        let payload = DefinitionPayload::Available {
            text: "<ShowPlanXML/>".into(),
            sha256: "a".repeat(64),
            redaction_version: 1,
        };
        WarehouseArtifactReport {
            query: WarehouseArtifactQuery {
                resource: host.selector(),
                references: vec![reference.clone()],
                full_payloads: true,
                expected_sql_sha256: None,
                text: WarehouseArtifactTextRead::Complete,
            },
            records: vec![WarehouseArtifactRecord {
                reference,
                engine: culprit_core::Engine::Mssql,
                scope: WarehouseArtifactScope::Database { database: host.selector().database, label: "ExactDB".into() },
                source: "mssql.query-store".into(),
                native_clock: WindowNativeClock::Unknown,
                collected_from: "2026-11-25T11:00:00Z".into(),
                collected_until: "2026-11-25T12:00:00Z".into(),
                artifacts: vec![WarehouseDefinitionArtifact {
                    location: "/plan".into(),
                    kind: DefinitionContentKind::Plan,
                    label: "Execution plan".into(),
                    payload: WarehouseArtifactText::Complete { payload },
                    matches_requested_sql: false,
                    plan: Some(Box::new(plan)),
                    limitations: vec![],
                }],
                linked_from: vec![],
            }],
            gaps: vec![],
            qualifications: vec!["original scope".into()],
            archive_work: zero_work(),
        }
    }

    #[tokio::test]
    async fn fleet_attention_and_warehouse_plan_insights_and_compare_operate_source_free_within_64kib() {
        let host = Host::new();
        let ref_a = report(&host.database()).incidents[0].affected_queries[0].evidence[0].clone();
        let ref_b = ref_a.clone();
        let (plan_seek, plan_scan) = demo_plans();

        let fleet_report = WarehouseFleetReport {
            request: culprit_core::WarehouseFleetRequest { as_of: "2026-11-25T12:05:00Z".into(), ..Default::default() },
            summary: culprit_core::WarehouseFleetSummary {
                evaluated_resources: 1,
                attention_resources: 1,
                active_incidents: 1,
                high_finding_resources: 1,
                regression_resources: 1,
                stale_or_partial_resources: 0,
            },
            rows: vec![WarehouseFleetRow {
                resource_id: culprit_core::telemetry::digest(b"r1"),
                selector: host.selector(),
                engine: culprit_core::Engine::Mssql,
                database_name: "ExactDB".into(),
                connection_alias: Some("orders-prod".into()),
                environment: Some("production".into()),
                shared_group: None,
                freshness: WarehouseFleetFreshness::Observed,
                latest_collected_from: "2026-11-25T12:00:00Z".into(),
                latest_collected_until: "2026-11-25T12:01:00Z".into(),
                latest_observed_from: Some("2026-11-25T11:55:00Z".into()),
                latest_observed_until: Some("2026-11-25T12:00:00Z".into()),
                active_incidents: 1,
                uncertain_incidents: 0,
                high_findings: 2,
                actionable_findings: 3,
                recent_regressions: 1,
                recent_changes: 1,
                input_read_complete: true,
                limitations: vec![],
                telemetry: None,
            }],
            next: None,
            discovery_complete: true,
            gaps: vec![],
            candidate_rows: 1,
            archive_work: zero_work(),
            qualifications: vec![],
            collector_targets: None,
        };

        let mock = Arc::new(MockWarehouse {
            artifacts: Mutex::new(vec![
                artifact_report(&host, ref_b.clone(), plan_scan.clone()),
                artifact_report(&host, ref_a.clone(), plan_seek),
                artifact_report(&host, ref_b.clone(), plan_scan),
            ]),
            fleet_report,
        });
        let delegation = Arc::new(WarehouseDelegation::read_only(mock.clone()).with_fleet(mock));
        let server = host.server.clone().with_warehouse(delegation.clone());

        // 1. fleet_attention
        let fleet_res = server
            .handle(
                "agent-test",
                super::super::fleet::FLEET_ATTENTION_TOOL,
                json!({ "as_of": "2026-11-25T12:05:00Z" }).as_object().unwrap().clone(),
            )
            .await;
        assert_ne!(fleet_res.is_error, Some(true));
        let fleet_json = body(&fleet_res);
        assert_eq!(fleet_json["rows"][0]["connection_alias"], "orders-prod");
        assert_eq!(fleet_json["rows"][0]["active_incidents"], 1);
        assert_eq!(fleet_json["rows"][0]["needs_attention"], true);

        // 2. warehouse_plan_insights
        let ins_res = server
            .handle(
                "agent-test",
                WAREHOUSE_PLAN_INSIGHTS_TOOL,
                json!({ "resource": host.selector(), "reference": ref_b }).as_object().unwrap().clone(),
            )
            .await;
        assert_ne!(ins_res.is_error, Some(true));
        let ins_json = body(&ins_res);
        assert_eq!(ins_json["engine"], "mssql");
        assert!(ins_json["tree"].as_str().unwrap().contains("Clustered Index Scan"));

        // 3. warehouse_plan_compare
        let cmp_res = server
            .handle(
                "agent-test",
                WAREHOUSE_PLAN_COMPARE_TOOL,
                json!({
                    "resource": host.selector(),
                    "plan_a": ref_a,
                    "plan_b": ref_b,
                    "label_a": "Plan 68",
                    "label_b": "Plan 69"
                })
                .as_object()
                .unwrap()
                .clone(),
            )
            .await;
        assert_ne!(cmp_res.is_error, Some(true));
        let cmp_json = body(&cmp_res);
        assert!(cmp_json["cost_b"].as_f64().unwrap() > cmp_json["cost_a"].as_f64().unwrap());
        assert!(!cmp_json["changes"].as_array().unwrap().is_empty());

        // 4. Monotonic revocation denies all new warehouse tools immediately.
        delegation.revoke();
        let denied = server
            .handle(
                "agent-test",
                super::super::fleet::FLEET_ATTENTION_TOOL,
                json!({ "as_of": "2026-11-25T12:05:00Z" }).as_object().unwrap().clone(),
            )
            .await;
        assert_eq!(denied.is_error, Some(true));
        host.assert_source_free();
    }
}
