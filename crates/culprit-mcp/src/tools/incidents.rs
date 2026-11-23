//! An explicitly delegated archive selector serves incident lookup; no saved native connection is consulted.
use super::warehouse::{arguments, check};
use super::*;
use culprit_core::{
    PublishedResourceSelector,
    telemetry::{DeliveryProblem, ObservationBounds},
};
use culprit_read::cancellation::CollectionCancellation;
pub(super) mod presentation;

pub(crate) const INCIDENT_TOOL: &str = "incident";
const MAX_AROUND_BYTES: usize = 64;
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentArgs {
    // Copy the selector of an observed resource returned by resources. Do not construct native connection data.
    #[schemars(schema_with = "super::warehouse::resource_schema")]
    resource: PublishedResourceSelector,
    // An RFC3339 time around which the archive reader selects candidate original incidents.
    around: String,
}
pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            INCIDENT_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("mcp.incident.description")),
            schema_for_type::<IncidentArgs>(),
        )
        .with_title(culprit_core::t!("mcp.incident.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}
impl CulpritMcp {
    pub(super) async fn incident(&self, value: Value, cancellation: Option<CollectionCancellation>) -> Result<Value> {
        let delegation = self.warehouse_delegation(INCIDENT_TOOL)?;
        let args: IncidentArgs = arguments(value)?;
        args.resource.validate()?;
        if args.around.len() > MAX_AROUND_BYTES || ObservationBounds::new(&args.around, &args.around).is_none() {
            return Err(Error::invalid(culprit_core::t!("mcp.incident.around_invalid")));
        }
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = delegation.reader()?.incident(&args.resource, &args.around, &cancellation).await?;
        check(&cancellation)?;
        delegation.check()?;
        if report.database != args.resource.database {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let value = presentation::value(&report)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(value)
    }
}
#[cfg(test)]
pub(in crate::tools) mod tests;
