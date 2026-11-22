use super::*;
use culprit_core::{DerivedContextIssue as Issue, IndexFilterFacts};

impl CatalogContext {
    pub(super) fn project(&mut self, input: u16, original: &dyn OriginalProjection) -> Result<()> {
        let native = original.source().observation.snapshot().ok_or_else(invalid)?;
        if !complete(original) {
            self.issue(Issue::PartialCatalog);
            return Ok(());
        }
        let mut projected = true;
        for row in original.records() {
            match (native.dataset, &row.row().record) {
                (
                    SnapshotDataset::Tables,
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::MssqlTable(table),
                        ..
                    }),
                ) => {
                    if let Some(projected) = table.as_ref().clone().into_table_stat() {
                        self.table_positions
                            .insert((table.identity.database_id, table.identity.object_id), self.tables.len());
                        self.tables.push(projected);
                    } else {
                        projected = false;
                    }
                }
                (
                    SnapshotDataset::Indexes,
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::MssqlIndex(index),
                        ..
                    }),
                ) => {
                    if index.parent_kind == MssqlIndexParentKind::IndexedView {
                        continue;
                    }
                    let (filter, facts) = match &index.filter {
                        None => (None, None),
                        Some(CatalogExpression {
                            native_sha256: Some(fingerprint),
                            payload: DefinitionPayload::Available { text, .. },
                        }) => (
                            Some(text.clone()),
                            Some(IndexFilterFacts {
                                fingerprint: fingerprint.clone(),
                                matching_queries: vec![],
                                equalities: None,
                            }),
                        ),
                        Some(_) => {
                            projected = false;
                            continue;
                        }
                    };
                    let presentation = MssqlIndexPresentation { filter, filter_facts: facts, last_read: None };
                    if let Some(index) = index.as_ref().clone().into_index_stat(presentation) {
                        self.indexes.push(index);
                    } else {
                        projected = false;
                    }
                }
                (
                    SnapshotDataset::Columns,
                    TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::MssqlColumn(column),
                        ..
                    }),
                ) => {
                    let default = match &column.default {
                        None => None,
                        Some(CatalogExpression { payload: DefinitionPayload::Available { text, .. }, .. }) => {
                            Some(text.clone())
                        }
                        Some(_) => {
                            projected = false;
                            continue;
                        }
                    };
                    self.columns.push(column.as_ref().clone().into_column_info(default));
                }
                (
                    SnapshotDataset::Statistics,
                    TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::MssqlStatistics(statistic),
                        ..
                    }),
                ) => {
                    self.statistics.push(statistic.as_ref().clone().into_statistics_info(None));
                }
                (SnapshotDataset::Settings, TelemetryRecord::Setting(setting)) => {
                    match settings::project(original, setting) {
                        Ok(setting) => self.settings.push(setting),
                        Err(issue) => {
                            self.issue(issue);
                            projected = false;
                        }
                    }
                }
                (dataset, TelemetryRecord::ObjectDefinition(row)) if self.project_relation(dataset, &row.fact) => {}
                _ => {
                    projected = false;
                }
            }
        }
        if !projected {
            self.issue(Issue::PartialCatalog);
        }
        self.settings_complete = native.dataset == SnapshotDataset::Settings && projected || self.settings_complete;
        if projected {
            self.projected_datasets.push(native.dataset);
        }
        self.inputs.push(input);
        for coverage in &native.sources {
            let mut outcome = crate::collected_resources::read_outcome(coverage, coverage.records as usize, true)?;
            if !projected && matches!(outcome.state, SourceState::Collected | SourceState::Empty) {
                outcome.state = SourceState::Partial;
                outcome.reason = Some(culprit_core::LocalizedText::new(culprit_core::message!(
                    "view.synthesis.context.catalog_projection"
                )));
            }
            self.sources.push(outcome);
        }
        Ok(())
    }
}
