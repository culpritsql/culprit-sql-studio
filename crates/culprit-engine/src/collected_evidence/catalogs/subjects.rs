//! Numeric object generations and exact local ticks establish agreement; display names never repair it.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
type ObjectId = (u32, u32);
struct Subject<'a> {
    identity: &'a MssqlObjectIdentity,
    schema: &'a str,
    name: &'a str,
    modified: Option<&'a str>,
    native_type: &'a str,
}
#[derive(PartialEq, Eq)]
struct Parent<'a> {
    id: Option<u32>,
    schema: Option<&'a str>,
    name: Option<&'a str>,
}
#[derive(Default)]
struct Subjects<'a> {
    objects: BTreeMap<ObjectId, Subject<'a>>,
    names: BTreeMap<(String, String), ObjectId>,
    tables: BTreeSet<ObjectId>,
    columns: BTreeMap<(u32, u32, u32), &'a str>,
    parents: BTreeMap<ObjectId, Parent<'a>>,
    databases: BTreeSet<u32>,
}
pub(super) fn coherent(originals: &[(u16, &dyn OriginalProjection)]) -> bool {
    let mut subjects = Subjects::default();
    for (_, original) in originals {
        for row in original.records() {
            if !subjects.observe(&row.row().record) {
                return false;
            }
        }
    }
    let have_tables = has(originals, SnapshotDataset::Tables);
    let have_columns = has(originals, SnapshotDataset::Columns);
    subjects.databases.len() <= 1
        && (!have_tables
            || subjects
                .objects
                .iter()
                .all(|(key, subject)| subject.native_type != "U" || subjects.tables.contains(key)))
        && (!have_columns
            || originals
                .iter()
                .all(|(_, original)| original.records().iter().all(|row| subjects.columns_match(&row.row().record))))
}
fn has(originals: &[(u16, &dyn OriginalProjection)], dataset: SnapshotDataset) -> bool {
    originals.iter().any(|(_, original)| selection::dataset(*original) == Some(dataset))
}
impl<'a> Subjects<'a> {
    fn observe(&mut self, record: &'a TelemetryRecord) -> bool {
        match record {
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlTable(table), ..
            }) => {
                self.tables.insert((table.identity.database_id, table.identity.object_id))
                    && self.insert(Subject {
                        identity: &table.identity,
                        schema: &table.schema,
                        name: &table.name,
                        modified: Some(&table.modified_without_offset),
                        native_type: "U",
                    })
            }
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlIndex(index), ..
            }) => self.insert(Subject {
                identity: &index.parent_object,
                schema: &index.parent_schema,
                name: &index.parent_name,
                modified: Some(&index.parent_modified_without_offset),
                native_type: match index.parent_kind {
                    MssqlIndexParentKind::Table => "U",
                    MssqlIndexParentKind::IndexedView => "V",
                },
            }),
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlStatistics(statistic),
                ..
            }) => self.insert(Subject {
                identity: &statistic.table,
                schema: &statistic.schema,
                name: &statistic.table_name,
                modified: None,
                native_type: "U",
            }),
            TelemetryRecord::ObjectDefinition(row) => self.definition(&row.fact),
            _ => true,
        }
    }
    fn definition(&mut self, fact: &'a ObjectDefinitionFact) -> bool {
        match fact {
            ObjectDefinitionFact::MssqlColumn(column) => {
                self.insert(Subject {
                    identity: &column.table,
                    schema: &column.schema,
                    name: &column.table_name,
                    modified: Some(&column.table_modified_without_offset),
                    native_type: "U",
                }) && self
                    .columns
                    .insert((column.table.database_id, column.table.object_id, column.column_id), &column.name)
                    .is_none()
            }
            ObjectDefinitionFact::MssqlForeignKey(key) => self.foreign_key(key),
            ObjectDefinitionFact::MssqlCheckConstraint(check) => self.check(check),
            ObjectDefinitionFact::MssqlDmlTrigger(trigger) => self.trigger(trigger),
            ObjectDefinitionFact::MssqlObject(object) => self.object(object),
            ObjectDefinitionFact::MssqlPartitionBoundary(boundary) => self.insert(Subject {
                identity: &boundary.table,
                schema: &boundary.schema,
                name: &boundary.table_name,
                modified: None,
                native_type: "U",
            }),
            _ => true,
        }
    }
    fn foreign_key(&mut self, key: &'a MssqlForeignKey) -> bool {
        self.insert(Subject {
            identity: &key.identity,
            schema: &key.schema,
            name: &key.name,
            modified: Some(&key.modified_without_offset),
            native_type: "F",
        }) && self.insert(Subject {
            identity: &key.parent,
            schema: &key.parent_schema,
            name: &key.parent_table,
            modified: None,
            native_type: "U",
        }) && self.insert(Subject {
            identity: &key.referenced,
            schema: &key.referenced_schema,
            name: &key.referenced_table,
            modified: None,
            native_type: "U",
        })
    }
    fn check(&mut self, check: &'a MssqlCheckConstraint) -> bool {
        self.insert(Subject {
            identity: &check.identity,
            schema: &check.schema,
            name: &check.name,
            modified: Some(&check.modified_without_offset),
            native_type: "C",
        }) && self.insert(Subject {
            identity: &check.parent,
            schema: &check.parent_schema,
            name: &check.parent_table,
            modified: None,
            native_type: "U",
        })
    }
    fn trigger(&mut self, trigger: &'a MssqlDmlTrigger) -> bool {
        self.insert(Subject {
            identity: &trigger.identity,
            schema: &trigger.schema,
            name: &trigger.name,
            modified: Some(&trigger.modified_without_offset),
            native_type: &trigger.native_type,
        }) && self.insert(Subject {
            identity: &trigger.parent,
            schema: &trigger.parent_schema,
            name: &trigger.parent_name,
            modified: None,
            native_type: &trigger.parent_type,
        }) && self.parent(
            &trigger.identity,
            Parent {
                id: Some(trigger.parent.object_id),
                schema: Some(&trigger.parent_schema),
                name: Some(&trigger.parent_name),
            },
        )
    }
    fn object(&mut self, object: &'a MssqlCatalogObject) -> bool {
        self.insert(Subject {
            identity: &object.identity,
            schema: &object.schema,
            name: &object.name,
            modified: Some(&object.modified_without_offset),
            native_type: &object.native_type,
        }) && self.parent(
            &object.identity,
            Parent {
                id: object.parent_object_id,
                schema: object.parent_schema.as_deref(),
                name: object.parent_name.as_deref(),
            },
        )
    }
    fn insert(&mut self, subject: Subject<'a>) -> bool {
        let identity = subject.identity;
        let key = (identity.database_id, identity.object_id);
        self.databases.insert(identity.database_id);
        let name = (subject.schema.to_ascii_lowercase(), subject.name.to_ascii_lowercase());
        if self.names.insert(name, key).is_some_and(|known| known != key) {
            return false;
        }
        if let Some(existing) = self.objects.get_mut(&key) {
            if !same(existing, &subject) {
                return false;
            }
            // A weak parent/statistics reference must never erase a later observed modification witness.
            if existing.modified.is_none() {
                existing.modified = subject.modified;
            }
            true
        } else {
            self.objects.insert(key, subject);
            true
        }
    }
    fn parent(&mut self, identity: &MssqlObjectIdentity, parent: Parent<'a>) -> bool {
        let key = (identity.database_id, identity.object_id);
        if let Some(existing) = self.parents.get_mut(&key) {
            if existing.id != parent.id
                || existing.schema.zip(parent.schema).is_some_and(|(a, b)| a != b)
                || existing.name.zip(parent.name).is_some_and(|(a, b)| a != b)
            {
                return false;
            }
            existing.schema = existing.schema.or(parent.schema);
            existing.name = existing.name.or(parent.name);
            true
        } else {
            self.parents.insert(key, parent);
            true
        }
    }
    fn columns_match(&self, record: &TelemetryRecord) -> bool {
        match record {
            TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::MssqlIndex(index), ..
            }) => {
                index.parent_kind == MssqlIndexParentKind::IndexedView
                    || index
                        .columns
                        .iter()
                        .all(|column| self.column(&index.parent_object, column.column_id, Some(&column.name)))
            }
            TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                fact: ObjectDefinitionFact::MssqlForeignKey(key),
                ..
            }) => key.columns.iter().all(|column| {
                self.column(&key.parent, column.parent_column_id, Some(&column.parent_column))
                    && self.column(&key.referenced, column.referenced_column_id, Some(&column.referenced_column))
            }),
            TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                fact: ObjectDefinitionFact::MssqlCheckConstraint(check),
                ..
            }) => check.parent_column_id == 0 || self.column(&check.parent, check.parent_column_id, None),
            _ => true,
        }
    }
    fn column(&self, parent: &MssqlObjectIdentity, id: u32, name: Option<&str>) -> bool {
        self.columns
            .get(&(parent.database_id, parent.object_id, id))
            .is_some_and(|known| name.is_none_or(|name| name == *known))
    }
}
fn same(left: &Subject<'_>, right: &Subject<'_>) -> bool {
    left.native_type == right.native_type
        && left.schema == right.schema
        && left.name == right.name
        && local(&left.identity.created_without_offset)
            .zip(local(&right.identity.created_without_offset))
            .is_some_and(|(a, b)| a == b)
        && match (left.modified, right.modified) {
            (Some(a), Some(b)) => local(a).zip(local(b)).is_some_and(|(a, b)| a == b),
            _ => true,
        }
}
pub(super) fn local(value: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok()
}
