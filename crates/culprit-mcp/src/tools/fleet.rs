//! Source-free estate-wide triage across archived databases (`fleet_attention`).
use super::{
    warehouse::{arguments, check},
    *,
};
use culprit_core::{
    DEFAULT_WAREHOUSE_FLEET_MAX_AGE_SECONDS, Engine, MAX_WAREHOUSE_FLEET_PAGE_ROWS, PublishedResourceWindow,
    WarehouseFleetCursor, WarehouseFleetFilter, WarehouseFleetRequest, WarehouseFleetSort, format,
    telemetry::DeliveryProblem,
};
use culprit_read::cancellation::CollectionCancellation;

pub(crate) const FLEET_ATTENTION_TOOL: &str = "fleet_attention";

fn optional_fleet_cursor_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"anyOf":[{"type":"null"},{"type":"object","additionalProperties":false,
        "required":["selection_key","resource_id"],"properties":{
            "selection_key":{"type":"string","pattern":"^[a-f0-9]{64}$"},
            "resource_id":{"type":"string","pattern":"^[a-f0-9]{64}$"}}}]})
}

fn optional_engine_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"anyOf":[{"type":"null"},{"type":"string","enum":["mssql","postgres","yugabyte","mysql","sqlite"]}]})
}

fn optional_sort_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"anyOf":[{"type":"null"},{"type":"string","enum":["attention","resource"]}]})
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FleetAttentionArgs {
    /// UTC cutoff timestamp (RFC 3339). Omit on the first page; reuse the returned `as_of` when paging with `after`.
    as_of: Option<String>,
    #[schemars(schema_with = "super::warehouse::optional_window_schema")]
    window: Option<PublishedResourceWindow>,
    /// Maximum staleness age in seconds before a resource is flagged stale (default 1800).
    max_age_seconds: Option<u32>,
    /// Filter to a specific environment label (e.g. `production`, `staging`).
    environment: Option<String>,
    #[schemars(schema_with = "optional_engine_schema")]
    engine: Option<Engine>,
    /// Return only databases with active/uncertain incidents, high findings, regressions, or collector staleness (default true).
    attention_only: Option<bool>,
    /// Filter to a shared-capacity group (Elastic Pool, Managed Instance, or RDS cluster label).
    shared_group: Option<String>,
    #[schemars(schema_with = "optional_sort_schema")]
    sort: Option<WarehouseFleetSort>,
    /// Resources per page (1 to 24, default 24).
    page_rows: Option<u32>,
    #[schemars(schema_with = "optional_fleet_cursor_schema")]
    after: Option<WarehouseFleetCursor>,
}

pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            FLEET_ATTENTION_TOOL,
            format!(
                "{}{DATA_NOTE}",
                culprit_core::t!(
                    "mcp.fleet_attention.description",
                    limit = format::count(f64::from(MAX_WAREHOUSE_FLEET_PAGE_ROWS))
                )
            ),
            schema_for_type::<FleetAttentionArgs>(),
        )
        .with_title(culprit_core::t!("mcp.fleet_attention.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}

impl CulpritMcp {
    pub(super) async fn fleet_attention(
        &self,
        value: Value,
        cancellation: Option<CollectionCancellation>,
    ) -> Result<Value> {
        let delegation = self.warehouse_delegation(FLEET_ATTENTION_TOOL)?;
        let args: FleetAttentionArgs = arguments(value)?;
        let page_rows = args.page_rows.unwrap_or(MAX_WAREHOUSE_FLEET_PAGE_ROWS);
        if !(1..=MAX_WAREHOUSE_FLEET_PAGE_ROWS).contains(&page_rows) || (args.after.is_some() && args.as_of.is_none()) {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }
        let request = WarehouseFleetRequest {
            as_of: args.as_of.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            window: args.window,
            max_age_seconds: args.max_age_seconds.unwrap_or(DEFAULT_WAREHOUSE_FLEET_MAX_AGE_SECONDS),
            filter: WarehouseFleetFilter {
                environment: args.environment,
                engine: args.engine,
                attention_only: args.attention_only.unwrap_or(true),
                shared_group: args.shared_group,
            },
            sort: args.sort.unwrap_or_default(),
            page_rows,
            after: args.after,
        };
        if request.as_of.len() > 64 || request.window.as_ref().is_some_and(|w| w.from.len() > 64 || w.until.len() > 64)
        {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }
        request.validate()?;
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = delegation.fleet()?.fleet(&request, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if report.request != request || report.rows.len() > page_rows as usize {
            return Err(DeliveryProblem::Integrity.error());
        }
        let rows: Vec<Value> = report
            .rows
            .iter()
            .map(|row| {
                json!({
                    "selector": row.selector,
                    "engine": row.engine,
                    "database_name": format::escaped_preview(&row.database_name, 256).text,
                    "connection_alias": row.connection_alias.as_deref().map(|v| format::escaped_preview(v, 256).text),
                    "environment": row.environment.as_deref().map(|v| format::escaped_preview(v, 128).text),
                    "shared_group": row.shared_group.as_ref().map(|g| json!({
                        "kind": g.member_kind,
                        "label": format::escaped_preview(&g.label, 256).text,
                    })),
                    "freshness": row.freshness,
                    "needs_attention": row.needs_attention(),
                    "active_incidents": row.active_incidents,
                    "uncertain_incidents": row.uncertain_incidents,
                    "high_findings": row.high_findings,
                    "actionable_findings": row.actionable_findings,
                    "recent_regressions": row.recent_regressions,
                    "latest_collected_until": row.latest_collected_until,
                    "latest_observed_until": row.latest_observed_until,
                })
            })
            .collect();
        let output = json!({
            "as_of": report.request.as_of,
            "window": report.request.window,
            "summary": report.summary,
            "rows": rows,
            "next": report.next,
            "discovery_complete": report.discovery_complete,
            "gaps_available": report.gaps.len(),
            "qualifications": report.qualifications,
        });
        let output = super::response::bounded_tool_result(output)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(output)
    }
}
