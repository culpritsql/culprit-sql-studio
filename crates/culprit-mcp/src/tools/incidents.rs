//! One explicitly installed archive capability serves database/time lookup; no native provider is consulted.
use super::*;
use culprit_core::{
    IncidentDatabaseSelector,
    telemetry::{CollectionStop, DeliveryProblem, ObservationBounds},
};
use culprit_engine::saved_incident_database;
use culprit_read::cancellation::CollectionCancellation;
mod presentation;

pub(crate) const INCIDENT_TOOL: &str = "incident";
const MAX_AROUND_BYTES: usize = 64;
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentArgs {
    /// A saved connection exposed by list_connections.
    connection: String,
    /// An RFC3339 time around which the archive reader selects candidate original incidents.
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
        let reader = self.incident_reader.as_ref().ok_or_else(|| unknown_tool(INCIDENT_TOOL))?;
        let args: IncidentArgs = args(value)?;
        let database = saved_incident_database(&self.allowed(&args.connection)?)?;
        if args.around.len() > MAX_AROUND_BYTES || ObservationBounds::new(&args.around, &args.around).is_none() {
            return Err(Error::invalid(culprit_core::t!("mcp.incident.around_invalid")));
        }
        let cancellation = cancellation.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
        check(&cancellation)?;
        let report = reader.incident(&database, &args.around, &cancellation).await?;
        check(&cancellation)?;
        // A revoked delegation or changed saved route cannot label a prior read as the current route's result.
        let current = saved_incident_database(&self.allowed(&args.connection)?)?;
        if current != database || report.database != IncidentDatabaseSelector::from_route(&database)? {
            return Err(DeliveryProblem::DestinationChanged.error());
        }
        let value = presentation::value(&report)?;
        check(&cancellation)?;
        Ok(value)
    }
}
fn check(cancellation: &CollectionCancellation) -> Result<()> {
    if cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
}
#[cfg(test)]
mod tests;
