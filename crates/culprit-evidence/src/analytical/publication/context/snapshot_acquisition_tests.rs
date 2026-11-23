use super::*;

#[test]
fn ordinary_snapshot_public_context_cannot_restore_private_acquisition_authority() {
    let context = NativeReadContext {
        target: culprit_core::DatabaseIdentity::server(
            culprit_core::Engine::Postgres,
            "private-db-canary",
            "private-route.invalid",
            5432,
            None,
        )
        .into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: culprit_core::Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("private-cluster-canary"),
                native: NativeIdentity::new("private-member-canary"),
            }),
            epoch: Some("private-process-canary".into()),
        },
        observed_from: "2026-11-01T00:00:00Z".into(),
        observed_until: "2026-11-01T00:00:01Z".into(),
        database_writable: Some(false),
    };
    let original = SourceObservation::SnapshotAcquisition {
        snapshot: StateSnapshotDescription {
            dataset: SnapshotDataset::Settings,
            observed_from: context.observed_from.clone(),
            observed_until: context.observed_until.clone(),
            records: 0,
            sources: vec![StateSourceCoverage::collected("pg_settings".into(), 0)],
        },
        execution: Box::new(context.clone()),
        settings_context: Some(NativeSettingsContext {
            format: NativeSettingsFormat::PostgresSettings,
            version: "180006".into(),
        }),
    };
    assert!(original.covers_request());
    let raw = serde_json::to_vec(&original).unwrap();
    let public = observation(&original).unwrap();
    assert_eq!(public["kind"], "snapshot-acquisition-context");
    assert_eq!(public["data"]["snapshot"], serde_json::to_value(original.snapshot().unwrap()).unwrap());
    assert_eq!(public["data"]["database_writable"], false);
    assert_eq!(public["data"]["settings_context"]["format"], "postgres-settings");
    assert_eq!(public["data"]["settings_context"]["version"], "180006");
    assert_eq!(public["data"]["acquisition_key"], digest(&serde_json::to_vec(&context).unwrap()));
    for private in [
        "private-db-canary",
        "private-route.invalid",
        "private-cluster-canary",
        "private-member-canary",
        "private-process-canary",
    ] {
        assert!(!public.to_string().contains(private));
    }
    assert!(serde_json::from_value::<SourceObservation>(public).is_err());
    let restored: SourceObservation = serde_json::from_slice(&raw).unwrap();
    assert_eq!(restored.snapshot_acquisition_context(), Some(&context));
    assert_eq!(serde_json::to_vec(&original).unwrap(), raw, "curation cannot alter retained originals");
    let pending = SourceObservation::PagedSnapshotAcquisition {
        snapshot: PagedSnapshotDescription {
            dataset: SnapshotDataset::Settings,
            observed_from: context.observed_from.clone(),
            sources: vec!["pg_settings".into()],
        },
        execution: Box::new(context),
        settings_context: Some(NativeSettingsContext {
            format: NativeSettingsFormat::PostgresSettings,
            version: "180006".into(),
        }),
    };
    assert!(observation(&pending).is_err());
    assert!(pending.snapshot_acquisition_context().is_none());
}
