//! Manufactured typed original catalogs for canonical reader/projector controls; not native/provider qualification.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MssqlSchemaCatalogCase {
    Exact,
    PartialForeignKeys,
    ChangedParent,
    UnavailableCheckText,
    UnknownTriggerEvents,
    MissingPartitionBoundary,
    AmbiguousPartitionLayout,
    ConflictingFunctionAcrossIndexes,
    ChangedObjectKind,
    DisabledTrigger,
    NonbasePartition,
    TrustedConstraints,
    ReplicationConstraints,
}
pub fn mssql_schema_catalog_evidence(
    case: MssqlSchemaCatalogCase,
) -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    let (template, rows) = table_full_text_evidence(TableFullTextCase::Enabled);
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::MssqlTable(table), .. }) =
        &rows[0]
    else {
        panic!()
    };
    let mut facts = CatalogFacts::new(table);
    facts.apply(case);
    facts
        .into_rows()
        .into_iter()
        .map(|(dataset, name, records)| frame(&template, case, dataset, name, records))
        .collect()
}
struct CatalogFacts {
    foreign_key: MssqlForeignKey,
    check: MssqlCheckConstraint,
    trigger: MssqlDmlTrigger,
    objects: Vec<MssqlCatalogObject>,
    boundaries: Vec<MssqlPartitionBoundary>,
}
impl CatalogFacts {
    fn new(table: &MssqlTableSnapshot) -> Self {
        let trigger = trigger(table);
        Self {
            foreign_key: foreign_key(table),
            check: check_constraint(table),
            objects: objects(table, &trigger),
            trigger,
            boundaries: vec![partition_boundary(table)],
        }
    }
    fn apply(&mut self, case: MssqlSchemaCatalogCase) {
        match case {
            MssqlSchemaCatalogCase::ChangedParent => {
                self.foreign_key.parent.created_without_offset = "2020-01-01T09:00:00.007".into()
            }
            MssqlSchemaCatalogCase::UnavailableCheckText => {
                self.check.definition = CatalogExpression {
                    native_sha256: None,
                    payload: DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized },
                }
            }
            MssqlSchemaCatalogCase::UnknownTriggerEvents => self.trigger.update_event = None,
            MssqlSchemaCatalogCase::ChangedObjectKind => self.objects[0].native_type = "V".into(),
            MssqlSchemaCatalogCase::DisabledTrigger => self.trigger.disabled = true,
            MssqlSchemaCatalogCase::NonbasePartition => {
                self.boundaries[0].index_id = 2;
                self.boundaries[0].index_name = Some("ix_Documents".into());
            }
            MssqlSchemaCatalogCase::TrustedConstraints => {
                self.foreign_key.disabled = false;
                self.foreign_key.trusted = true;
                self.check.trusted = true;
            }
            MssqlSchemaCatalogCase::ReplicationConstraints => {
                self.foreign_key.not_for_replication = true;
                self.check.not_for_replication = true;
            }
            MssqlSchemaCatalogCase::MissingPartitionBoundary => self.boundaries[0].fanout = 3,
            MssqlSchemaCatalogCase::ConflictingFunctionAcrossIndexes => {
                let mut next = self.boundaries[0].clone();
                next.index_id = 2;
                next.index_name = Some("ix_Documents".into());
                next.function_modified_without_offset = "2020-01-01T09:00:00.203".into();
                self.boundaries.push(next);
            }
            MssqlSchemaCatalogCase::AmbiguousPartitionLayout => {
                self.boundaries[0].fanout = 3;
                let mut next = self.boundaries[0].clone();
                next.boundary_id = Some(2);
                next.function_modified_without_offset = "2020-01-01T09:00:00.203".into();
                self.boundaries.push(next);
            }
            _ => {}
        }
    }
    fn into_rows(self) -> Vec<(SnapshotDataset, &'static str, Vec<TelemetryRecord>)> {
        let record = |source: &str, fact| {
            TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord::new(source.into(), fact).unwrap())
        };
        vec![
            (
                SnapshotDataset::ForeignKeys,
                culprit_core::source_id::FOREIGN_KEYS,
                vec![record(
                    culprit_core::source_id::FOREIGN_KEYS,
                    ObjectDefinitionFact::MssqlForeignKey(Box::new(self.foreign_key)),
                )],
            ),
            (
                SnapshotDataset::CheckConstraints,
                culprit_core::source_id::CHECK_CONSTRAINTS,
                vec![record(
                    culprit_core::source_id::CHECK_CONSTRAINTS,
                    ObjectDefinitionFact::MssqlCheckConstraint(Box::new(self.check)),
                )],
            ),
            (
                SnapshotDataset::Triggers,
                culprit_core::source_id::TRIGGERS,
                vec![record(
                    culprit_core::source_id::TRIGGERS,
                    ObjectDefinitionFact::MssqlDmlTrigger(Box::new(self.trigger)),
                )],
            ),
            (
                SnapshotDataset::Objects,
                culprit_core::source_id::OBJECTS,
                self.objects
                    .into_iter()
                    .map(|object| {
                        record(culprit_core::source_id::OBJECTS, ObjectDefinitionFact::MssqlObject(Box::new(object)))
                    })
                    .collect(),
            ),
            (
                SnapshotDataset::Partitions,
                culprit_core::source_id::PARTITIONS,
                self.boundaries
                    .into_iter()
                    .map(|value| {
                        record(
                            culprit_core::source_id::PARTITIONS,
                            ObjectDefinitionFact::MssqlPartitionBoundary(Box::new(value)),
                        )
                    })
                    .collect(),
            ),
        ]
    }
}
fn frame(
    template: &CollectionDescription,
    case: MssqlSchemaCatalogCase,
    dataset: SnapshotDataset,
    name: &str,
    records: Vec<TelemetryRecord>,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mut source = template.clone();
    source.source = format!("controlled-{name}-catalog");
    let complete = !matches!(
        (case, dataset),
        (MssqlSchemaCatalogCase::PartialForeignKeys, SnapshotDataset::ForeignKeys)
            | (MssqlSchemaCatalogCase::UnknownTriggerEvents, SnapshotDataset::Triggers)
    );
    source.observation = SourceObservation::Snapshot(StateSnapshotDescription {
        dataset,
        observed_from: "2020-01-01T10:00:00Z".into(),
        observed_until: "2020-01-01T10:00:01Z".into(),
        records: records.len() as u32,
        sources: vec![StateSourceCoverage::observed(name.into(), records.len() as u32, complete)],
    });
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}
fn foreign_key(table: &MssqlTableSnapshot) -> MssqlForeignKey {
    MssqlForeignKey {
        identity: identity(101, "2020-01-01T09:00:00.010"),
        schema: "dbo".into(),
        name: "FK_Documents_Self".into(),
        modified_without_offset: "2020-01-01T09:00:00.020".into(),
        parent: table.identity.clone(),
        parent_schema: table.schema.clone(),
        parent_table: table.name.clone(),
        referenced: table.identity.clone(),
        referenced_schema: table.schema.clone(),
        referenced_table: table.name.clone(),
        columns: vec![MssqlForeignKeyColumn {
            ordinal: 1,
            parent_column_id: 1,
            parent_column: "status".into(),
            referenced_column_id: 1,
            referenced_column: "status".into(),
        }],
        delete_action: MssqlForeignKeyAction::Cascade,
        update_action: MssqlForeignKeyAction::NoAction,
        disabled: true,
        trusted: false,
        not_for_replication: false,
        system_named: false,
    }
}

fn check_constraint(table: &MssqlTableSnapshot) -> MssqlCheckConstraint {
    MssqlCheckConstraint {
        identity: identity(102, "2020-01-01T09:00:00.030"),
        schema: "dbo".into(),
        name: "CK_Documents_Status".into(),
        modified_without_offset: "2020-01-01T09:00:00.040".into(),
        parent: table.identity.clone(),
        parent_schema: table.schema.clone(),
        parent_table: table.name.clone(),
        parent_column_id: 0,
        definition: CatalogExpression::sql(
            "CONVERT(nvarchar(20),[status]) <> N'private-schema-canary'",
            culprit_core::Engine::Mssql,
            MAX_CATALOG_EXPRESSION_BYTES,
        ),
        disabled: false,
        trusted: false,
        not_for_replication: false,
        system_named: false,
        uses_database_collation: false,
    }
}

fn trigger(table: &MssqlTableSnapshot) -> MssqlDmlTrigger {
    MssqlDmlTrigger {
        identity: identity(104, "2020-01-01T09:00:00.050"),
        parent: table.identity.clone(),
        schema: "dbo".into(),
        name: "Audit:Documents".into(),
        parent_schema: table.schema.clone(),
        parent_name: table.name.clone(),
        parent_type: "U".into(),
        native_type: "TR".into(),
        modified_without_offset: "2020-01-01T09:00:00.060".into(),
        disabled: false,
        instead_of: false,
        not_for_replication: false,
        insert_event: Some(true),
        delete_event: Some(false),
        update_event: Some(true),
    }
}

fn partition_boundary(table: &MssqlTableSnapshot) -> MssqlPartitionBoundary {
    MssqlPartitionBoundary {
        table: table.identity.clone(),
        schema: table.schema.clone(),
        table_name: table.name.clone(),
        index_id: 1,
        index_name: Some("cx_Documents".into()),
        index_disabled: false,
        index_hypothetical: false,
        scheme_id: 2,
        scheme_name: "PS_Documents".into(),
        function_id: 3,
        function_name: "PF_Documents".into(),
        function_created_without_offset: "2020-01-01T09:00:00.100".into(),
        function_modified_without_offset: "2020-01-01T09:00:00.200".into(),
        fanout: 2,
        boundary_on_right: true,
        parameter_id: 1,
        parameter_type_schema: "sys".into(),
        parameter_type: "date".into(),
        parameter_system_type_id: 40,
        parameter_user_type_id: 40,
        parameter_maximum_bytes: 3,
        parameter_precision: 10,
        parameter_scale: 0,
        parameter_collation: None,
        boundary_id: Some(1),
        value_is_null: Some(false),
        native_value_type: Some("date".into()),
        native_value_sha256: Some(digest(b"manufactured-native-partition-binary")),
        display_value: Some(CatalogExpression::partition_bound(
            "N'2020-02-01'",
            culprit_core::Engine::Mssql,
            MAX_CATALOG_EXPRESSION_BYTES,
        )),
    }
}

fn objects(table: &MssqlTableSnapshot, trigger: &MssqlDmlTrigger) -> Vec<MssqlCatalogObject> {
    vec![
        MssqlCatalogObject {
            identity: table.identity.clone(),
            schema: table.schema.clone(),
            name: table.name.clone(),
            native_type: "U".into(),
            modified_without_offset: table.modified_without_offset.clone(),
            parent_object_id: None,
            parent_schema: None,
            parent_name: None,
        },
        MssqlCatalogObject {
            identity: trigger.identity.clone(),
            schema: trigger.schema.clone(),
            name: trigger.name.clone(),
            native_type: "TR".into(),
            modified_without_offset: trigger.modified_without_offset.clone(),
            parent_object_id: Some(table.identity.object_id),
            parent_schema: Some(table.schema.clone()),
            parent_name: Some(table.name.clone()),
        },
        MssqlCatalogObject {
            identity: identity(105, "2020-01-01T09:00:00.070"),
            schema: "dbo".into(),
            name: "LookupDocuments".into(),
            native_type: "P".into(),
            modified_without_offset: "2020-01-01T09:00:00.080".into(),
            parent_object_id: None,
            parent_schema: None,
            parent_name: None,
        },
    ]
}

fn identity(object_id: u32, created: &str) -> MssqlObjectIdentity {
    MssqlObjectIdentity { database_id: 7, object_id, created_without_offset: created.into() }
}
