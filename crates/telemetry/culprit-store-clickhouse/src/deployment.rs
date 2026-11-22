//! Explicit offline DDL. Normal collection never provisions or modifies warehouse schema.
use crate::eligibility;
use crate::{
    catalog::SchemaCatalog,
    schema::{Schema, quote},
};
use culprit_core::Result;
use culprit_evidence::analytical::semantics::stream_semantics;
use culprit_evidence::analytical::semantics::{CurrentScope, EligibilityScope};
use serde::Serialize;

#[derive(Clone, Copy, Default)]
pub enum TableLayout {
    #[default]
    SingleNode,
    Replicated,
}
#[derive(Serialize)]
pub struct DeploymentAssets {
    pub schema_digest: String,
    pub commands: Vec<String>,
}
impl DeploymentAssets {
    pub fn new() -> Result<Self> {
        Self::with_layout(TableLayout::SingleNode)
    }
    pub fn with_layout(layout: TableLayout) -> Result<Self> {
        let catalog = SchemaCatalog::new()?;
        let mut commands = Vec::new();
        for (_, schema) in catalog.schemas() {
            commands.push(create_table(schema, layout, false));
        }
        commands.push(create_table(catalog.publication(), layout, true));
        commands.push(eligibility::published(&catalog, EligibilityScope::Resource));
        commands.push(heads(false));
        commands.push(heads(true));
        for (stream, schema) in catalog.schemas() {
            commands.push(eligibility::eligible(schema, EligibilityScope::Resource));
            let semantics = stream_semantics(stream)?;
            commands.push(eligibility::rows(schema, EligibilityScope::Resource, semantics.latest_revision));
            if let Some(scope) = semantics.current {
                let (head, keys) = match scope {
                    CurrentScope::Activity => ("CulpritActivityHeadsV1", "RangeId"),
                    CurrentScope::NativeSource => ("CulpritSnapshotHeadsV1", "RangeId, NativeSource"),
                };
                commands.push(
                    include_str!("sql/current.sql")
                        .replace("$TABLE", schema.table())
                        .replace("$COLUMNS", &schema.column_names())
                        .replace("$HEAD", head)
                        .replace("$KEYS", keys),
                );
            }
        }
        Ok(Self { schema_digest: catalog.digest().into(), commands })
    }
    pub fn script(&self) -> String {
        format!("{};\n", self.commands.join(";\n\n"))
    }
}
fn create_table(schema: &Schema, layout: TableLayout, publication: bool) -> String {
    let mut columns = schema
        .columns()
        .iter()
        .map(|column| format!("{} {}", quote(&column.name), Schema::column_type(column)))
        .collect::<Vec<_>>();
    if publication {
        // Only completion rows need a second sort order. Data is looked up by published range IDs through its primary key.
        columns.push("PROJECTION ByResourceTime (SELECT * ORDER BY (ContractId, RecoveryStoreId, ResourceId, CollectedUntil, RangeId))".into());
    }
    let engine = match layout {
        TableLayout::SingleNode => "MergeTree",
        TableLayout::Replicated => "ReplicatedMergeTree",
    };
    let keys = if publication { "RangeId" } else { "RangeId, BatchId, RowOrdinal" };
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE = {engine} PARTITION BY toYYYYMM(CollectedUntil) ORDER BY ({keys}) SETTINGS fsync_after_insert=1, fsync_part_directory=1",
        quote(schema.table()),
        columns.join(",\n")
    )
}
fn heads(snapshot: bool) -> String {
    let (name, selection, expand, complete, keys) = if snapshot {
        (
            "CulpritSnapshotHeadsV1",
            "JSONExtractString(SourceDescription,'observation','kind')='snapshot'",
            "ARRAY JOIN JSONExtractArrayRaw(SourceDescription,'observation','data','sources') AS Coverage",
            "AND JSONExtractBool(Coverage,'complete_scope') AND JSONExtractString(Coverage,'state') IN ('collected','empty')",
            "LogicalSourceId, Dataset, NativeSource",
        )
    } else {
        (
            "CulpritActivityHeadsV1",
            "JSONExtractString(SourceDescription,'observation','kind')='activity' AND JSONExtractBool(SourceDescription,'observation','data','coverage','complete_scope') AND JSONExtractString(SourceDescription,'observation','data','coverage','state') IN ('collected','empty')",
            "",
            "",
            "LogicalSourceId, Dataset",
        )
    };
    include_str!("sql/heads.sql")
        .replace("$FUNCTION", name)
        .replace("$SELECTION", selection)
        .replace("$EXPAND", expand)
        .replace("$COMPLETE", complete)
        .replace("$KEYS", keys)
        .replace("$SOURCE", if snapshot { "JSONExtractString(Coverage,'source')" } else { "''" })
        .replace("$ARGS", &eligibility::arguments(EligibilityScope::Resource))
}
#[cfg(test)]
mod tests;
