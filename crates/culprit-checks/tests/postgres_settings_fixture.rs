//! Explicit controlled Settings facts shared by pure rule and wording tests.
use culprit_core::{Engine, Snapshot};

/// These are explicit controlled native facts for the existing pure rule tests, never provider admission.
pub(crate) fn record_settings_acquisition(snapshot: &mut Snapshot, writable: bool) {
    use culprit_core::{DatabaseIdentity, MeasurementScope, telemetry::*};
    assert_eq!(snapshot.server.engine, Engine::Postgres);
    let database =
        DatabaseIdentity::server(Engine::Postgres, &snapshot.server.database, "settings-fixture.invalid", 5432, None);
    let context = NativeReadContext {
        target: database.clone().into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("config-settings-system"),
                native: NativeIdentity::new("config-settings-member"),
            }),
            epoch: Some("config-settings-db-process".into()),
        },
        observed_from: "2026-11-23T10:00:00Z".into(),
        observed_until: "2026-11-23T10:00:01Z".into(),
        database_writable: Some(writable),
    };
    let observations = snapshot
        .settings
        .iter()
        .filter(|setting| matches!(setting.name.as_str(), "autovacuum" | "autovacuum_vacuum_scale_factor"))
        .map(|setting| SettingObservation {
            source: "pg_settings".into(),
            namespace: "pg_catalog.pg_settings".into(),
            name: setting.name.clone(),
            scope: MeasurementScope::Server,
            nature: SettingNature::Configuration,
            value: None,
            value_in_use: Some(setting.value.clone()),
            value_for_secondary: None,
            unit: None,
            native_context: Some("sighup".into()),
            native_source: Some("configuration file".into()),
            pending_restart: Some(false),
            native_is_default: None,
            default_value: None,
            default_source: SettingDefaultSource::Unknown,
        })
        .collect();
    snapshot.identity = Some(database);
    snapshot.operations.postgres_settings = Some(Box::new(PostgresSettingsRead {
        settings_context: NativeSettingsContext {
            format: NativeSettingsFormat::PostgresSettings,
            version: "180006".into(),
        },
        observations,
        population_complete: true,
        acquisition: PostgresSettingsAcquisition::Completed { context: Box::new(context) },
    }));
    assert!(snapshot.operations.postgres_settings.as_ref().unwrap().is_valid());
}
