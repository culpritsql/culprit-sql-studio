//! Authenticated catalogs are acquisition-time context. They never reconstruct a historical execution schema.
use super::*;
use culprit_core::{
    ColumnInfo, ConstraintIssue, ForeignKey, IndexStat, Setting, StatisticsInfo, TableStat, telemetry::*,
};
mod partition_scope;
mod projection;
mod relational;
pub(crate) mod selection;
pub(crate) mod settings;
mod subjects;

#[derive(Default)]
pub(crate) struct CatalogContext {
    tables: Vec<TableStat>,
    indexes: Vec<IndexStat>,
    columns: Vec<ColumnInfo>,
    statistics: Vec<StatisticsInfo>,
    settings: Vec<Setting>,
    foreign_keys: Vec<ForeignKey>,
    constraints: Vec<ConstraintIssue>,
    table_positions: std::collections::BTreeMap<(u32, u32), usize>,
    table_options: Vec<relational::TableOption>,
    projected_datasets: Vec<SnapshotDataset>,
    sources: Vec<SourceOutcome>,
    inputs: Vec<u16>,
    issues: Vec<culprit_core::DerivedContextIssue>,
    settings_complete: bool,
    provenance: super::CatalogProvenance,
}
impl CatalogContext {
    pub fn from_originals(primary: &dyn OriginalProjection, originals: &[&dyn OriginalProjection]) -> Result<Self> {
        let selection::CatalogSelection { originals: mut heads, mut issues } = selection::heads(primary, originals)?;
        heads.retain(|(_, original)| {
            let complete = complete(*original)
                && (selection::dataset(*original) != Some(SnapshotDataset::Partitions)
                    || partition_scope::complete(*original));
            if !complete && !issues.contains(&culprit_core::DerivedContextIssue::PartialCatalog) {
                issues.push(culprit_core::DerivedContextIssue::PartialCatalog);
            }
            complete
        });
        if !subjects::coherent(&heads) {
            heads.retain(|(_, original)| selection::dataset(*original) == Some(SnapshotDataset::Settings));
            issues.push(culprit_core::DerivedContextIssue::ChangedCatalogSubject);
        }
        let mut context = Self { issues, ..Default::default() };
        for (index, original) in heads {
            context.project(index, original)?;
        }
        context.finish_relational_projection();
        if !context.inputs.is_empty() {
            context.issue(culprit_core::DerivedContextIssue::AcquisitionCatalogOnly);
        }
        Ok(context)
    }
    pub fn from_catalog_primary(
        primary: &dyn OriginalProjection,
        originals: &[&dyn OriginalProjection],
    ) -> Result<Self> {
        let selection::CatalogSelection { originals: mut heads, mut issues } =
            selection::heads_including_primary(primary, originals)?;
        heads.retain(|(_, original)| {
            let complete = complete(*original)
                && (selection::dataset(*original) != Some(SnapshotDataset::Partitions)
                    || partition_scope::complete(*original));
            if !complete && !issues.contains(&culprit_core::DerivedContextIssue::PartialCatalog) {
                issues.push(culprit_core::DerivedContextIssue::PartialCatalog);
            }
            complete
        });
        if !subjects::coherent(&heads) {
            heads.retain(|(_, original)| selection::dataset(*original) == Some(SnapshotDataset::Settings));
            issues.push(culprit_core::DerivedContextIssue::ChangedCatalogSubject);
        }
        let mut context = Self { issues, ..Default::default() };
        for (index, original) in heads {
            context.project(index, original)?;
        }
        context.finish_relational_projection();
        Ok(context)
    }
    pub fn inputs(&self) -> &[u16] {
        &self.inputs
    }
    pub fn projected_datasets(&self) -> &[SnapshotDataset] {
        &self.projected_datasets
    }
    pub fn issues(&self) -> &[culprit_core::DerivedContextIssue] {
        &self.issues
    }
    pub fn settings_complete(&self) -> bool {
        self.settings_complete
    }
    pub(crate) fn provenance(&self) -> &super::CatalogProvenance {
        &self.provenance
    }
    pub(crate) fn apply(&self, snapshot: &mut Snapshot) {
        snapshot.tables = self.tables.clone();
        snapshot.indexes = self.indexes.clone();
        snapshot.columns = self.columns.clone();
        snapshot.statistics = self.statistics.clone();
        snapshot.settings = self.settings.clone();
        snapshot.foreign_keys = self.foreign_keys.clone();
        snapshot.constraints = self.constraints.clone();
        for source in &self.sources {
            if let Some(existing) = snapshot.sources.iter_mut().find(|existing| existing.source == source.source) {
                *existing = source.clone();
            }
        }
        // These remain acquisition-time points with their independent clocks. Retained object metadata
        // lacks module bodies/UTC DDL times and full current server/schema prerequisites remain absent.
        // No TableStats capability or fresh executable advice is granted by these projections.
    }
    pub(crate) fn issue(&mut self, issue: culprit_core::DerivedContextIssue) {
        if !self.issues.contains(&issue) {
            self.issues.push(issue);
        }
    }
}

/// Successful delivery alone does not establish a complete native catalog population.
pub(crate) fn complete(original: &dyn OriginalProjection) -> bool {
    let mut native_keys = std::collections::BTreeSet::new();
    original.is_read_complete()
        && original
            .records()
            .iter()
            .all(|row| serde_json::to_vec(&row.row().record.native_identity()).is_ok_and(|key| native_keys.insert(key)))
        && original.source().observation.snapshot().is_some_and(|native| {
            native.is_valid()
                && native.sources.iter().any(|source| {
                    source.complete_scope && matches!(source.state, SourceState::Collected | SourceState::Empty)
                })
                && original.records().len() == native.records as usize
                && native.sources.iter().all(|source| {
                    source.complete_scope && matches!(source.state, SourceState::Collected | SourceState::Empty)
                        || source.state == SourceState::NotApplicable
                })
        })
}
