//! Delegated archive detail; typed continuations preserve full SHA and never turn a slice into a whole plan.
use super::{
    warehouse::{arguments, check},
    *,
};
use culprit_core::{
    MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS, WarehouseArtifactQuery, WarehouseArtifactTextRead, telemetry::DeliveryProblem,
};
use culprit_read::cancellation::CollectionCancellation;
pub(crate) const ARTIFACTS_TOOL: &str = "artifacts";
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArtifactArgs {
    #[schemars(schema_with = "super::warehouse::resource_schema")]
    resource: culprit_core::PublishedResourceSelector,
    /// Exact original references copied from the selected incident query, finding or change.
    references: Vec<Value>,
    /// Explicit authorization to read whole immutable native recovery bodies, even when returning one chunk.
    full_payloads: bool,
    expected_sql_sha256: Option<String>,
    /// Copy an inventory/chunk continuation returned by this tool; do not construct a location or offset.
    continuation: Option<Value>,
}
pub(super) fn definition() -> Tool {
    culprit_core::i18n::scope("en", || {
        Tool::new(
            ARTIFACTS_TOOL,
            format!("{}{DATA_NOTE}", culprit_core::t!("mcp.artifact.description")),
            schema_for_type::<ArtifactArgs>(),
        )
        .with_title(culprit_core::t!("mcp.artifact.title"))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
    })
}
impl CulpritMcp {
    pub(super) async fn warehouse_artifacts(
        &self,
        value: Value,
        cancellation: Option<CollectionCancellation>,
    ) -> Result<Value> {
        let delegation = self.warehouse_delegation(ARTIFACTS_TOOL)?;
        let args: ArtifactArgs = arguments(value)?;
        let references = args.references.into_iter().map(arguments).collect::<Result<Vec<_>>>()?;
        let request = WarehouseArtifactQuery {
            resource: args.resource,
            references,
            full_payloads: args.full_payloads,
            expected_sql_sha256: args.expected_sql_sha256,
            text: match args.continuation {
                Some(cursor) => WarehouseArtifactTextRead::Chunk { cursor: arguments(cursor)? },
                None => WarehouseArtifactTextRead::Inventory { chunk_chars: MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS },
            },
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
        // Do not trim a chunk, lie about its end, parse it as a plan, or refill one action to fill a response.
        for record in &report.records {
            for artifact in &record.artifacts {
                let valid = artifact.plan.is_none()
                    && matches!(
                        (&request.text, &artifact.payload),
                        (
                            WarehouseArtifactTextRead::Inventory { .. },
                            culprit_core::WarehouseArtifactText::Metadata { .. },
                        ) | (
                            WarehouseArtifactTextRead::Inventory { .. },
                            culprit_core::WarehouseArtifactText::Complete {
                                payload: culprit_core::telemetry::DefinitionPayload::Unavailable { .. },
                            },
                        ) | (
                            WarehouseArtifactTextRead::Chunk { .. },
                            culprit_core::WarehouseArtifactText::Chunk { .. },
                        )
                    );
                if !valid {
                    return Err(DeliveryProblem::Integrity.error());
                }
            }
        }
        if !super::response::admits(&report)? {
            return Err(DeliveryProblem::Capacity.error());
        }
        let output = serde_json::to_value(report).map_err(|_| DeliveryProblem::Integrity.error())?;
        let output = super::response::bounded_tool_result(output)?;
        check(&cancellation)?;
        delegation.check()?;
        Ok(output)
    }
}
#[cfg(test)]
mod tests;
