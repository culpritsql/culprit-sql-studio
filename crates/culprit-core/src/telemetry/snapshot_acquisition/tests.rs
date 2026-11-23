use super::*;
use crate::{DatabaseIdentity, Engine, MeasurementScope};

fn execution(writable: bool) -> NativeReadContext {
    NativeReadContext {
        target: DatabaseIdentity::server(Engine::Postgres, "shop", "private-route.invalid", 5432, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("42").qualified("kind", "postgres-system-identifier"),
                native: NativeIdentity::new("127.0.0.1").qualified("port", "5432"),
            }),
            epoch: Some("private-database-process-epoch".into()),
        },
        observed_from: "2026-11-01T00:00:00Z".into(),
        observed_until: "2026-11-01T00:00:00.010Z".into(),
        database_writable: Some(writable),
    }
}
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
        budget: CollectionBudget::default(),
    }
}
fn source(writable: bool) -> CollectionDescription {
    let execution = execution(writable);
    CollectionDescription {
        target: execution.target.clone(),
        partition: SourcePartition::Target,
        source: "postgres-settings".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::PagedSnapshotAcquisition {
            snapshot: PagedSnapshotDescription {
                dataset: SnapshotDataset::Settings,
                observed_from: execution.observed_from.clone(),
                sources: vec!["pg_settings".into()],
            },
            execution: Box::new(execution),
            settings_context: Some(NativeSettingsContext {
                format: NativeSettingsFormat::PostgresSettings,
                version: "180006".into(),
            }),
        },
    }
}
fn completion(writable: bool) -> SourceCompletion {
    let mut execution = execution(writable);
    execution.observed_until = "2026-11-01T00:00:00.030Z".into();
    SourceCompletion::SnapshotAcquisition {
        snapshot: StateSnapshotDescription {
            dataset: SnapshotDataset::Settings,
            observed_from: execution.observed_from.clone(),
            observed_until: execution.observed_until.clone(),
            records: 0,
            sources: vec![StateSourceCoverage::collected("pg_settings".into(), 0)],
        },
        execution: Box::new(execution),
        settings_context: Some(NativeSettingsContext {
            format: NativeSettingsFormat::PostgresSettings,
            version: "180006".into(),
        }),
    }
}
#[test]
fn ordinary_snapshot_retains_its_actual_read_owner_without_rebinding_the_data_namespace() {
    for writable in [true, false] {
        let source = source(writable);
        assert!(source.matches_request(&request()));
        assert!(!source.observation.covers_request());
        assert!(source.observation.snapshot_acquisition_context().is_none());
        let completed = source.completed(completion(writable)).unwrap().unwrap();
        assert!(completed.matches_request(&request()));
        assert!(completed.observation.covers_request());
        assert_eq!(completed.target, source.target);
        assert_eq!(completed.partition, SourcePartition::Target);
        assert!(completed.target.database().unwrap().incarnation.is_none());
        let actual = completed.observation.snapshot_acquisition_context().unwrap();
        assert!(actual.has_qualified_local_owner());
        assert_eq!(actual.database_writable, Some(writable));
        assert!(matches!(&actual.partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) }));
        let data_context = NativeReadContext::from_snapshot(&completed).unwrap();
        assert_eq!(data_context.partition, SourcePartition::Target);
        assert_eq!(data_context.database_writable, None, "the global accessor cannot borrow acquisition role");
        assert!(!data_context.has_qualified_local_owner());
    }
}
#[test]
fn a_typed_final_acquisition_is_required_and_cannot_repair_an_ordinary_or_unknown_initial_header() {
    let original = source(true);
    assert!(original.completed(SourceCompletion::Verified).is_err());
    let SourceCompletion::SnapshotAcquisition { snapshot, execution, .. } = completion(true) else { panic!() };
    assert!(original.completed(SourceCompletion::Snapshot(snapshot.clone())).is_err());
    assert!(
        original
            .completed(SourceCompletion::LocalResource { snapshot: snapshot.clone(), execution: execution.clone() })
            .is_err()
    );
    let mut missing = original.clone();
    missing.observation = SourceObservation::PagedSnapshot(PagedSnapshotDescription {
        dataset: SnapshotDataset::Settings,
        observed_from: snapshot.observed_from,
        sources: vec!["pg_settings".into()],
    });
    assert!(missing.completed(completion(true)).is_err());
    assert_eq!(original.completed(SourceCompletion::Changed).unwrap(), None);
}
#[test]
fn only_the_declared_ordinary_settings_shape_and_exact_native_target_can_use_this_carrier() {
    for case in ["dataset", "source-target", "data-partition", "owner", "epoch", "role", "member-kind", "bounds"] {
        let mut changed = source(true);
        let SourceObservation::PagedSnapshotAcquisition { snapshot, execution, .. } = &mut changed.observation else {
            panic!()
        };
        match case {
            "dataset" => snapshot.dataset = SnapshotDataset::StatementCounters,
            "source-target" => {
                let CollectionTarget::Database(database) = &mut changed.target else { panic!() };
                database.database = "foreign".into();
            }
            "data-partition" => changed.partition = execution.partition.clone(),
            "owner" => {
                let SourcePartition::InstanceLocal { member, .. } = &mut execution.partition else { panic!() };
                *member = None;
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut execution.partition else { panic!() };
                *epoch = None;
            }
            "role" => execution.database_writable = None,
            "member-kind" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut execution.partition else {
                    panic!()
                };
                member.kind = SharedResourceKind::Node;
            }
            "bounds" => snapshot.observed_from = "2026-10-31T23:59:59Z".into(),
            _ => unreachable!(),
        }
        assert!(!changed.partition_is_consistent(), "{case}");
        assert!(!changed.matches_request(&request()), "{case}");
        assert!(changed.completed(completion(true)).is_err(), "{case}");
    }
}
#[test]
fn final_foreign_member_process_database_role_or_regressed_bounds_never_closes_the_original() {
    for case in ["member", "epoch", "database", "role", "start", "end", "shape"] {
        let mut changed = completion(true);
        let SourceCompletion::SnapshotAcquisition { snapshot, execution, .. } = &mut changed else { panic!() };
        match case {
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut execution.partition else {
                    panic!()
                };
                member.native.id = "127.0.0.2".into();
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut execution.partition else { panic!() };
                *epoch = Some("other-process".into());
            }
            "database" => {
                let CollectionTarget::Database(database) = &mut execution.target else { panic!() };
                database.database = "other".into();
            }
            "role" => execution.database_writable = Some(false),
            "start" => execution.observed_from = "2026-11-01T00:00:00.005Z".into(),
            "end" => execution.observed_until = "2026-11-01T00:00:00.005Z".into(),
            "shape" => snapshot.dataset = SnapshotDataset::StatementDefinitions,
            _ => unreachable!(),
        }
        assert!(source(true).completed(changed).is_err(), "{case}");
    }
}
#[test]
fn final_original_serde_preserves_native_context_and_public_or_pending_shapes_do_not_restore_it() {
    let completed = source(true).completed(completion(true)).unwrap().unwrap();
    let restored: CollectionDescription = serde_json::from_slice(&serde_json::to_vec(&completed).unwrap()).unwrap();
    assert_eq!(restored, completed);
    assert!(restored.partition_is_consistent());
    let native = restored.observation.snapshot_acquisition_context().unwrap();
    assert_eq!(native.partition, execution(true).partition);
    assert!(source(true).observation.snapshot_acquisition_context().is_none());
    let public = serde_json::json!({"kind":"snapshot-acquisition-context","data":{"snapshot":restored.observation.snapshot(),"acquisition_key":digest(b"opaque"),"database_writable":true}});
    assert!(serde_json::from_value::<SourceObservation>(public).is_err());
}
#[test]
fn settings_rows_stay_in_the_same_declared_stream_and_scope() {
    let mut completion = completion(true);
    let SourceCompletion::SnapshotAcquisition { snapshot, .. } = &mut completion else { panic!() };
    snapshot.records = 1;
    snapshot.sources[0] = StateSourceCoverage::collected("pg_settings".into(), 1);
    let original = source(true);
    let final_source = original.completed(completion).unwrap().unwrap();
    let record = TelemetryRecord::Setting(SettingObservation {
        source: "pg_settings".into(),
        namespace: "pg_catalog.pg_settings".into(),
        name: "work_mem".into(),
        scope: MeasurementScope::Session,
        nature: SettingNature::Configuration,
        value: None,
        value_in_use: Some("4096".into()),
        value_for_secondary: None,
        unit: Some("kB".into()),
        native_context: Some("user".into()),
        native_source: Some("session".into()),
        pending_restart: Some(false),
        native_is_default: None,
        default_value: None,
        default_source: SettingDefaultSource::Unknown,
    });
    assert!(record.for_export(&original).is_ok());
    assert!(record.for_export(&final_source).is_ok());
    assert!(!final_source.observation.accepts_snapshot_record(TelemetryStream::OperationalSamples, "pg_settings"));
    assert!(!final_source.observation.accepts_snapshot_record(TelemetryStream::SettingObservations, "foreign"));
}

#[test]
fn counter_acquisition_uses_the_exact_member_population_without_a_global_database_incarnation() {
    let mut source = source(false);
    let SourceObservation::PagedSnapshotAcquisition { snapshot, execution, settings_context } = &mut source.observation
    else {
        panic!()
    };
    snapshot.dataset = SnapshotDataset::StatementCounters;
    *settings_context = None;
    source.partition = execution.partition.clone();
    let mut final_read = completion(false);
    let SourceCompletion::SnapshotAcquisition { snapshot, settings_context, .. } = &mut final_read else { panic!() };
    snapshot.dataset = SnapshotDataset::StatementCounters;
    *settings_context = None;
    let final_source = source.completed(final_read).unwrap().unwrap();
    assert!(final_source.partition_is_consistent());
    assert_eq!(source.partition, final_source.partition);
    assert!(final_source.target.database().unwrap().incarnation.is_none());
    assert_eq!(final_source.observation.snapshot_acquisition_context().unwrap().database_writable, Some(false));
    for partition in [SourcePartition::Target, SourcePartition::InstanceLocal { member: None, epoch: None }] {
        let mut changed = source.clone();
        changed.partition = partition;
        assert!(!changed.partition_is_consistent());
    }
}

#[test]
fn settings_decoder_metadata_is_required_originally_and_exact_on_the_final_native_bound() {
    let original = source(true);
    assert!(original.observation.snapshot_settings_context().is_none(), "pending is not final original metadata");
    for case in ["missing", "version", "format"] {
        let mut final_read = completion(true);
        let SourceCompletion::SnapshotAcquisition { settings_context, .. } = &mut final_read else { panic!() };
        match case {
            "missing" => *settings_context = None,
            "version" => settings_context.as_mut().unwrap().version = "180007".into(),
            "format" => {
                *settings_context = Some(NativeSettingsContext {
                    format: NativeSettingsFormat::MysqlGlobalVariables,
                    version: "8.4.6".into(),
                })
            }
            _ => unreachable!(),
        }
        assert!(original.completed(final_read).is_err(), "{case}");
    }
    let completed = original.completed(completion(true)).unwrap().unwrap();
    assert_eq!(completed.observation.snapshot_settings_context().unwrap().version, "180006");
    let mut old = serde_json::to_value(&completed.observation).unwrap();
    old["data"].as_object_mut().unwrap().remove("settings_context").unwrap();
    assert!(serde_json::from_value::<SourceObservation>(old).is_err(), "no legacy omitted-field default");
}
