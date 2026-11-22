//! Bounded observed-resource lookup delegates to the same configured evidence reader and cumulative action owner.
use super::warehouse::{arguments, check};
use super::*;
use culprit_core::{
    PublishedResourceCursor, PublishedResourceRequest, PublishedResourceWindow, telemetry::DeliveryProblem,
};
use culprit_read::cancellation::CollectionCancellation;
mod presentation;

pub(crate) const RESOURCES_TOOL: &str = "resources";
// A presentation page tightens the evidence owner's existing 32-row source-free ceiling, without adding work.
const PAGE_ROWS: u32 = 8;
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ResourcesArgs {
    // UTC acquisition cutoff. Omit on the first page to fix it once; reuse the returned as_of on later pages.
    as_of: Option<String>,
    #[schemars(schema_with = "super::warehouse::optional_window_schema")]
    window: Option<PublishedResourceWindow>,
    // Copy the opaque next cursor unchanged. It is a continuation hint, never read authority.
    #[schemars(schema_with = "super::warehouse::optional_cursor_schema")]
    after: Option<PublishedResourceCursor>,
    // At most 8 observed resources per presentation page.
    page_rows: Option<u32>,
}
pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            RESOURCES_TOOL,
            format!(
                "{}{DATA_NOTE}",
                culprit_core::t!(
                    "mcp.resources.description",
                    limit = culprit_core::format::count(f64::from(PAGE_ROWS))
                )
            ),
            schema_for_type::<ResourcesArgs>(),
        )
        .with_title(culprit_core::t!("mcp.resources.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}
impl CulpritMcp {
    pub(super) async fn resources(&self, value: Value, cancellation: Option<CollectionCancellation>) -> Result<Value> {
        let delegation = self.warehouse_delegation(RESOURCES_TOOL)?;
        let args: ResourcesArgs = arguments(value)?;
        let page_rows = args.page_rows.unwrap_or(PAGE_ROWS);
        if !(1..=PAGE_ROWS).contains(&page_rows) || args.after.is_some() && args.as_of.is_none() {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }
        let request = PublishedResourceRequest {
            as_of: args.as_of.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            window: args.window,
            after: args.after,
            page_rows,
        };
        if request.as_of.len() > 64
            || request.window.as_ref().is_some_and(|window| window.from.len() > 64 || window.until.len() > 64)
        {
            return Err(Error::invalid(culprit_core::t!("mcp.warehouse.invalid_arguments")));
        }
        request.validate()?;
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = delegation.reader()?.resources(&request, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if report.as_of != request.as_of
            || report.window != request.window
            || report.resources.len() > page_rows as usize
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        let value = presentation::value(&report)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(value)
    }
}
