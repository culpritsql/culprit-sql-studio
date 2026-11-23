use super::*;
use crate::{DatabaseIdentity, telemetry::*};

const NATIVE_SOURCE: &str = "native-statements";

fn source(engine: Engine, nested: bool, complete: bool) -> CollectionDescription {
    CollectionDescription {
        target: DatabaseIdentity::server(engine, "shop", "fixture.invalid", 5432, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("native-cluster"),
                native: NativeIdentity::new("native-member"),
            }),
            epoch: Some("native-process-a".into()),
        },
        source: if nested { "native-statement-definitions" } else { "native-query-history" }.into(),
        continuity: SourceContinuity::Unverified,
        observation: if nested {
            SourceObservation::Snapshot(StateSnapshotDescription {
                dataset: SnapshotDataset::StatementDefinitions,
                observed_from: "2020-01-01T12:00:00Z".into(),
                observed_until: "2020-01-01T12:00:01Z".into(),
                records: 1,
                sources: vec![StateSourceCoverage::observed(NATIVE_SOURCE.into(), 1, complete)],
            })
        } else {
            SourceObservation::QueryHistory(QueryHistoryDescription {
                acquisition_owner: None,
                population_binding: QueryHistoryPopulationBinding::Unknown,
                population_catalog: None,
                waits: None,
                interval: TelemetryInterval {
                    native_id: "native-interval".into(),
                    from: "2020-01-01T11:00:00Z".into(),
                    until: "2020-01-01T12:00:00Z".into(),
                },
                dimensions: Default::default(),
                native_properties: Default::default(),
            })
        },
    }
}
fn identity() -> NativeQueryIdentity {
    NativeQueryIdentity {
        query_id: "42".into(),
        qualifiers: [("userid".into(), "10".into()), ("generation".into(), "native-entry-a".into())].into(),
    }
}
fn definition(source: &CollectionDescription, sql: DefinitionPayload) -> QueryDefinition {
    let identity = identity();
    QueryDefinition {
        id: identity.definition_id(source).unwrap(),
        identity,
        origin: StatementOrigin::Unknown,
        monitoring_template_id: None,
        variant_family: None,
        context: Default::default(),
        context_available: false,
        source_text_bytes: None,
        sql,
    }
}
fn record(definition: QueryDefinition, nested: bool) -> TelemetryRecord {
    if nested {
        TelemetryRecord::ObjectDefinition(
            ObjectDefinitionRecord::new(NATIVE_SOURCE.into(), ObjectDefinitionFact::Statement(Box::new(definition)))
                .unwrap(),
        )
    } else {
        TelemetryRecord::QueryDefinition(definition)
    }
}
fn capture() -> CaptureIdentity {
    CaptureIdentity { observation_id: "controlled-native-definition-1".into(), revision: None }
}
fn retained(row: &TelemetryRecord) -> &QueryDefinition {
    match row {
        TelemetryRecord::QueryDefinition(definition) => definition,
        TelemetryRecord::ObjectDefinition(wrapper) => {
            let ObjectDefinitionFact::Statement(definition) = &wrapper.fact else { panic!() };
            definition
        }
        _ => panic!(),
    }
}
#[test]
fn top_level_and_nested_valid_definitions_keep_their_own_original_ids_and_payload_availability() {
    for engine in [Engine::Postgres, Engine::Mysql] {
        for nested in [false, true] {
            for complete in [false, true] {
                let source = source(engine, nested, complete);
                assert!(source.partition_is_consistent());
                for sql in [
                    DefinitionPayload::sql("SELECT value FROM items WHERE id = 9264", engine),
                    DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing },
                    DefinitionPayload::Unavailable { reason: PayloadUnavailable::Oversized },
                ] {
                    let definition = definition(&source, sql);
                    let original = definition.clone();
                    let exported = ExportedRecord::new(&source, &capture(), record(definition, nested)).unwrap();
                    assert_eq!(retained(&exported.record), &original);
                    assert!(original.matches_source(&source).unwrap());
                    let restored: ExportedRecord =
                        serde_json::from_slice(&serde_json::to_vec(&exported).unwrap()).unwrap();
                    assert_eq!(ExportedRecord::new(&source, &capture(), restored.record).unwrap(), exported);
                }
            }
        }
    }
}
#[test]
fn copied_inner_ids_and_foreign_sources_fail_even_with_a_fresh_valid_wrapper_hash() {
    for nested in [false, true] {
        for fault in ["source", "database", "member", "epoch", "identity", "copied-id"] {
            let original = source(Engine::Postgres, nested, true);
            let mut selected = original.clone();
            let mut definition = definition(&original, DefinitionPayload::sql("SELECT 1", Engine::Postgres));
            match fault {
                "source" => selected.source = "another-definition-source".into(),
                "database" => {
                    let CollectionTarget::Database(database) = &mut selected.target else { panic!() };
                    database.database = "foreign".into();
                }
                "member" => {
                    let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut selected.partition else {
                        panic!()
                    };
                    member.native.id = "foreign-member".into();
                }
                "epoch" => {
                    let SourcePartition::InstanceLocal { epoch, .. } = &mut selected.partition else { panic!() };
                    *epoch = Some("foreign-process".into());
                }
                "identity" => {
                    definition.identity.qualifiers.insert("userid".into(), "11".into());
                }
                "copied-id" => {
                    let mut foreign = original.clone();
                    foreign.source = "native-statement-counters".into();
                    definition.id = definition.identity.definition_id(&foreign).unwrap();
                }
                _ => unreachable!(),
            }
            let record = record(definition.clone(), nested);
            assert!(record.is_valid(), "self-consistent content hashes are not source authorization");
            assert!(!definition.matches_source(&selected).unwrap(), "{nested}/{fault}");
            let error = ExportedRecord::new(&selected, &capture(), record).unwrap_err();
            assert_eq!(CollectionStop::from_error(&error), CollectionStop::InvalidPage, "{nested}/{fault}");
        }
    }
}
#[test]
fn missing_owner_in_partial_sources_cannot_be_repaired_by_a_known_definition_or_unavailable_payload() {
    for nested in [false, true] {
        for missing in ["member", "epoch"] {
            let original = source(Engine::Postgres, nested, false);
            let definition =
                definition(&original, DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing });
            let mut changed = original.clone();
            let SourcePartition::InstanceLocal { member, epoch } = &mut changed.partition else { panic!() };
            if missing == "member" {
                *member = None
            } else {
                *epoch = None
            };
            if nested {
                assert!(changed.partition_is_consistent(), "partial coverage retains uncertainty");
            }
            assert!(definition.matches_source(&changed).is_err());
            assert_eq!(
                CollectionStop::from_error(
                    &ExportedRecord::new(&changed, &capture(), record(definition, nested)).unwrap_err()
                ),
                CollectionStop::InvalidPage
            );
        }
    }
}
