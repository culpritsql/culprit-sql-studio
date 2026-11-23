use super::*;

fn database() -> DatabaseIdentity {
    DatabaseIdentity::server(Engine::Postgres, "settings-owner", "fixture.invalid", 5432, None)
}
fn context(writable: bool) -> NativeReadContext {
    NativeReadContext {
        target: database().into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("settings-system").qualified("kind", "postgres-system-identifier"),
                native: NativeIdentity::new("settings-member").qualified("port", "5432"),
            }),
            epoch: Some("settings-db-process-generation".into()),
        },
        observed_from: "2026-11-23T10:00:00Z".into(),
        observed_until: "2026-11-23T10:00:01Z".into(),
        database_writable: Some(writable),
    }
}
fn observed(name: &str, value: Option<&str>) -> SettingObservation {
    SettingObservation {
        source: "pg_settings".into(),
        namespace: "pg_catalog.pg_settings".into(),
        name: name.into(),
        scope: MeasurementScope::Server,
        nature: SettingNature::Configuration,
        value: None,
        value_in_use: value.map(str::to_owned),
        value_for_secondary: None,
        unit: None,
        native_context: Some("sighup".into()),
        native_source: Some("configuration file".into()),
        pending_restart: Some(false),
        native_is_default: None,
        default_value: Some("on".into()),
        default_source: SettingDefaultSource::NativeValue,
    }
}
fn original(writable: bool) -> PostgresSettingsRead {
    PostgresSettingsRead {
        settings_context: NativeSettingsContext {
            format: NativeSettingsFormat::PostgresSettings,
            version: "180006".into(),
        },
        observations: vec![observed("autovacuum", Some("off"))],
        population_complete: true,
        acquisition: PostgresSettingsAcquisition::Completed { context: Box::new(context(writable)) },
    }
}

#[test]
fn primary_and_recovery_share_raw_values_but_have_distinct_maintenance_applicability() {
    for writable in [true, false] {
        let original = original(writable);
        assert!(original.is_valid());
        let bytes = serde_json::to_vec(&original).unwrap();
        let restored: PostgresSettingsRead = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(restored, original);
        let advice = restored.configuration(Some(&database()));
        assert_eq!(advice.setting("autovacuum").is_some(), writable);
        assert_eq!(restored.observations[0].value_in_use.as_deref(), Some("off"));
        assert_eq!(restored.observations[0].default_value.as_deref(), Some("on"));
        assert_eq!(advice.limitations.contains(&PostgresSettingsAdviceLimit::RecoveryMaintenanceInactive), !writable);
    }
}

#[test]
fn unknown_changed_and_foreign_acquisition_cannot_borrow_the_flat_value_or_a_primary_probe() {
    let mut unknown = original(true);
    let mut before = context(true);
    before.database_writable = None;
    unknown.acquisition = PostgresSettingsAcquisition::Unqualified {
        before: Some(Box::new(before)),
        after: None,
        reason: PostgresSettingsAcquisitionGap::ContextUnavailable,
    };
    let mut changed = original(true);
    changed.acquisition = PostgresSettingsAcquisition::Unqualified {
        before: Some(Box::new(context(true))),
        after: Some(Box::new(context(false))),
        reason: PostgresSettingsAcquisitionGap::SourceChanged,
    };
    for read in [unknown, changed] {
        assert!(read.is_valid());
        assert!(read.configuration(Some(&database())).setting("autovacuum").is_none());
        assert_eq!(read.observations[0].value_in_use.as_deref(), Some("off"));
    }
    let mut wrong_format = original(true);
    wrong_format.settings_context =
        NativeSettingsContext { format: NativeSettingsFormat::MysqlGlobalVariables, version: "8.4.8".into() };
    assert!(!wrong_format.is_valid());
    assert!(wrong_format.configuration(Some(&database())).settings.is_empty());
    let mut malformed_version = original(true);
    malformed_version.settings_context.version = "18.6".into();
    assert!(!malformed_version.is_valid(), "PG context retains numeric server_version_num, not a display label");
    let mut missing_context = serde_json::to_value(original(true)).unwrap();
    missing_context.as_object_mut().unwrap().remove("settings_context");
    assert!(serde_json::from_value::<PostgresSettingsRead>(missing_context).is_err());
    let foreign = DatabaseIdentity::server(Engine::Postgres, "another-db", "fixture.invalid", 5432, None);
    assert!(original(true).configuration(Some(&foreign)).setting("autovacuum").is_none());
    assert!(original(true).configuration(None).setting("autovacuum").is_none());
}

#[test]
fn partial_missing_session_and_duplicate_native_values_remain_explicit_without_erasing_the_originals() {
    let mut read = original(true);
    read.population_complete = false;
    read.observations.push(observed("shared_buffers", None));
    let mut session = observed("work_mem", Some("4096"));
    session.scope = MeasurementScope::Session;
    session.unit = Some("kB".into());
    read.observations.push(session);
    let advice = read.configuration(Some(&database()));
    assert!(advice.setting("autovacuum").is_some(), "a positive observed fact survives partial roster coverage");
    assert!(!advice.is_complete());
    for issue in [
        PostgresSettingsAdviceLimit::PartialPopulation,
        PostgresSettingsAdviceLimit::UnavailableValue,
        PostgresSettingsAdviceLimit::SessionValue,
    ] {
        assert!(advice.limitations.contains(&issue));
    }
    assert_eq!(read.observations.len(), 3);
    let mut empty = original(true);
    empty.observations.clear();
    assert!(empty.is_valid());
    assert!(
        !empty.configuration(Some(&database())).is_complete(),
        "EOF of an empty selected roster is not healthy configuration"
    );
    read.observations.push(read.observations[0].clone());
    assert!(!read.is_valid());
    assert!(read.configuration(Some(&database())).settings.is_empty());
}

#[test]
fn sharing_withholds_native_cohort_and_owner_without_relabeling_display_values_as_authority() {
    let mut snapshot: Snapshot =
        serde_json::from_str(include_str!("../../../../../../../fixtures/postgres-demo.json")).unwrap();
    snapshot.identity = Some(database());
    snapshot.operations.postgres_settings = Some(Box::new(original(true)));
    snapshot.settings.retain(|setting| setting.name != "autovacuum");
    snapshot.settings.push(Setting {
        name: "autovacuum".into(),
        value: "off".into(),
        default_value: None,
        scope: "server".into(),
    });
    assert!(snapshot.postgres_configuration().unwrap().setting("autovacuum").is_some());
    let shared = crate::redact::redact_snapshot(&snapshot);
    let read = shared.operations.postgres_settings.as_ref().unwrap();
    assert!(read.observations.is_empty());
    assert!(matches!(
        read.acquisition,
        PostgresSettingsAcquisition::Unqualified { reason: PostgresSettingsAcquisitionGap::SharingWithheld, .. }
    ));
    assert!(shared.postgres_configuration().unwrap().setting("autovacuum").is_none());
    assert_eq!(shared.setting("autovacuum").unwrap().value, "off");
    let json = serde_json::to_string(&shared).unwrap();
    for private in ["settings-system", "settings-member", "settings-db-process-generation", "fixture.invalid"] {
        assert!(!json.contains(private), "private original acquisition must be withdrawn");
    }
    assert_eq!(snapshot.operations.postgres_settings.as_ref().unwrap().observations.len(), 1);
}
