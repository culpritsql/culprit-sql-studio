//! Same delegated archive action as incidents/resources; no native connection, rule or alternate reader.
use super::{
    warehouse::{arguments, check},
    *,
};
use culprit_core::{
    PublishedResourceCursor, PublishedResourceSelector, PublishedResourceWindow, WarehouseWindowQuery,
    WarehouseWindowView, telemetry::DeliveryProblem,
};
use culprit_read::cancellation::CollectionCancellation;
mod presentation;
pub(crate) const WINDOW_TOOL: &str = "window";
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum View {
    PublishedAnswers,
    CompatibleWorkload,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WindowArgs {
    #[schemars(schema_with = "super::warehouse::resource_schema")]
    resource: PublishedResourceSelector,
    from: String,
    until: String,
    as_of: Option<String>,
    view: Option<View>,
    page_rows: Option<u32>,
    #[schemars(schema_with = "super::warehouse::optional_cursor_schema")]
    after: Option<PublishedResourceCursor>,
}
pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            WINDOW_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("mcp.window.description")),
            schema_for_type::<WindowArgs>(),
        )
        .with_title(culprit_core::t!("mcp.window.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}
impl CulpritMcp {
    pub(super) async fn warehouse_window(
        &self,
        value: Value,
        cancellation: Option<CollectionCancellation>,
    ) -> Result<Value> {
        let delegation = self.warehouse_delegation(WINDOW_TOOL)?;
        let args: WindowArgs = arguments(value)?;
        if args.after.is_some() && args.as_of.is_none() {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }
        let request = WarehouseWindowQuery {
            resource: args.resource,
            window: PublishedResourceWindow { from: args.from, until: args.until },
            as_of: args.as_of.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            view: match args.view.unwrap_or(View::PublishedAnswers) {
                View::PublishedAnswers => WarehouseWindowView::PublishedAnswers,
                View::CompatibleWorkload => WarehouseWindowView::CompatibleWorkload,
            },
            page_rows: args.page_rows.unwrap_or(culprit_core::DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS),
            after: args.after,
        };
        request.validate()?;
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = delegation.reader()?.window(&request, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if report.request != request
            || report.published.len() > request.page_rows as usize
                && request.view == WarehouseWindowView::PublishedAnswers
            || report.incidents.database != request.resource.database
        {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let value = presentation::value(&report)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(value)
    }
}
#[cfg(test)]
mod tests;
