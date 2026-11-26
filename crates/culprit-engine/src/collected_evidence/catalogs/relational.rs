//! Project native relationships/options only after the original complete-scope and generation checks.
use super::*;
use culprit_core::{DerivedContextIssue as Issue, LocalizedText, source_id};

pub(super) struct TableOption {
    parent: MssqlObjectIdentity,
    option: String,
}
impl CatalogContext {
    pub(super) fn project_relation(
        &mut self,
        dataset: SnapshotDataset,
        fact: &ObjectDefinitionFact,
        input: u16,
        position: usize,
    ) -> bool {
        match (dataset, fact) {
            (SnapshotDataset::ForeignKeys, ObjectDefinitionFact::MssqlForeignKey(key)) => {
                if let Some(issue) = key.constraint_issue() {
                    super::super::record_provenance(
                        &mut self.provenance,
                        culprit_core::CatalogObjectRef::Constraint {
                            schema: issue.schema.clone(),
                            table: issue.table.clone(),
                            name: issue.name.clone(),
                        },
                        input,
                        position,
                    );
                    self.constraints.push(issue);
                }
                let fk = key.as_ref().clone().into_foreign_key();
                super::super::record_provenance(
                    &mut self.provenance,
                    culprit_core::CatalogObjectRef::ForeignKey {
                        schema: fk.schema.clone(),
                        table: fk.table.clone(),
                        name: fk.name.clone(),
                    },
                    input,
                    position,
                );
                self.foreign_keys.push(fk);
                true
            }
            (SnapshotDataset::CheckConstraints, ObjectDefinitionFact::MssqlCheckConstraint(check)) => {
                if let Some(issue) = check.constraint_issue() {
                    super::super::record_provenance(
                        &mut self.provenance,
                        culprit_core::CatalogObjectRef::Constraint {
                            schema: issue.schema.clone(),
                            table: issue.table.clone(),
                            name: issue.name.clone(),
                        },
                        input,
                        position,
                    );
                    self.constraints.push(issue);
                }
                true
            }
            (SnapshotDataset::Triggers, ObjectDefinitionFact::MssqlDmlTrigger(trigger)) => {
                if !trigger.event_metadata_complete() {
                    self.issue(Issue::PartialCatalog);
                    return false;
                }
                if let Some(option) = trigger.table_storage_option() {
                    self.table_options.push(TableOption { parent: trigger.parent.clone(), option });
                }
                true
            }
            (SnapshotDataset::Partitions, ObjectDefinitionFact::MssqlPartitionBoundary(boundary)) => {
                if boundary.index_id > 1 || boundary.boundary_id.is_none() {
                    return true;
                }
                let Some(CatalogExpression { payload: DefinitionPayload::Available { text, .. }, .. }) =
                    &boundary.display_value
                else {
                    self.issue(Issue::PartialCatalog);
                    return false;
                };
                let Some(option) = boundary.display_storage_option(Some(text)) else {
                    return false;
                };
                self.table_options.push(TableOption { parent: boundary.table.clone(), option });
                true
            }
            // This metadata supplies exact subject kind/generation for the shared coherence guard only.
            // It supplies neither SQL module definitions nor a UTC create/alter event.
            (SnapshotDataset::Objects, ObjectDefinitionFact::MssqlObject(_)) => true,
            _ => false,
        }
    }
    pub(super) fn finish_relational_projection(&mut self) {
        let options = std::mem::take(&mut self.table_options);
        for option in options {
            if let Some(index) = self.table_positions.get(&(option.parent.database_id, option.parent.object_id)) {
                self.tables[*index]
                    .storage
                    .get_or_insert_with(culprit_core::TableStorage::default)
                    .options
                    .push(option.option);
            } else {
                self.issue(Issue::PartialCatalog);
            }
        }
        let assessed = self.projected_datasets.contains(&SnapshotDataset::ForeignKeys)
            && self.projected_datasets.contains(&SnapshotDataset::CheckConstraints);
        let any = self
            .sources
            .iter()
            .any(|source| matches!(source.source.as_str(), source_id::FOREIGN_KEYS | source_id::CHECK_CONSTRAINTS));
        if any {
            self.sources.push(SourceOutcome {
                source: source_id::CONSTRAINTS.into(),
                state: if !assessed {
                    SourceState::Partial
                } else if self.constraints.is_empty() {
                    SourceState::Empty
                } else {
                    SourceState::Collected
                },
                reason: (!assessed)
                    .then(|| LocalizedText::new(culprit_core::message!("view.synthesis.context.constraint_scope"))),
            });
        }
    }
}
