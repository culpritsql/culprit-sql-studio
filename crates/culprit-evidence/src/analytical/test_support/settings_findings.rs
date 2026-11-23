//! Controlled native Settings originals, shared across pure/Gold and physical canonical reader controls.
use super::*;
use culprit_core::{DatabaseIdentity, Engine, MeasurementScope};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingsFindingCase {
    Disabled,
    Enabled,
    NullValue,
    SessionOnly,
    Partial,
    Empty,
    ReadOnly,
    UnknownNature,
}
impl SettingsFindingCase {
    pub const ALL: [Self; 8] = [
        Self::Disabled,
        Self::Enabled,
        Self::NullValue,
        Self::SessionOnly,
        Self::Partial,
        Self::Empty,
        Self::ReadOnly,
        Self::UnknownNature,
    ];
}
pub fn settings_finding_evidence(case: SettingsFindingCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let target: CollectionTarget =
        DatabaseIdentity::server(Engine::Postgres, "native-settings", "fixture.invalid", 5432, None).into();
    let from = "2020-01-01T12:00:00Z";
    let until = "2020-01-01T12:00:01Z";
    let mut records = Vec::new();
    if case != SettingsFindingCase::Empty {
        records.push(TelemetryRecord::Setting(SettingObservation {
            source: "pg_settings".into(),
            namespace: "pg_catalog.pg_settings".into(),
            name: "autovacuum".into(),
            scope: if case == SettingsFindingCase::SessionOnly {
                MeasurementScope::Session
            } else {
                MeasurementScope::Server
            },
            nature: if case == SettingsFindingCase::UnknownNature {
                SettingNature::Unknown
            } else {
                SettingNature::Configuration
            },
            value: None,
            value_in_use: if case == SettingsFindingCase::NullValue {
                None
            } else {
                Some(
                    match case {
                        SettingsFindingCase::Enabled => "on",
                        _ => "off",
                    }
                    .into(),
                )
            },
            value_for_secondary: None,
            unit: None,
            native_context: Some("sighup".into()),
            native_source: Some("configuration file".into()),
            pending_restart: Some(false),
            native_is_default: Some(false),
            default_value: Some("on".into()),
            default_source: SettingDefaultSource::NativeValue,
        }));
    }
    let source = CollectionDescription {
        target: target.clone(),
        partition: SourcePartition::Target,
        source: "postgres-settings".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SnapshotAcquisition {
            settings_context: Some(NativeSettingsContext {
                format: NativeSettingsFormat::PostgresSettings,
                version: "180000".into(),
            }),
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::Settings,
                observed_from: from.into(),
                observed_until: until.into(),
                records: records.len() as u32,
                sources: vec![StateSourceCoverage::observed(
                    "pg_settings".into(),
                    records.len() as u32,
                    case != SettingsFindingCase::Partial,
                )],
            },
            execution: Box::new(NativeReadContext {
                target,
                partition: SourcePartition::InstanceLocal {
                    member: Some(SharedResourceIdentity {
                        engine: Engine::Postgres,
                        kind: SharedResourceKind::Instance,
                        namespace: NativeIdentity::new("native-pg-system"),
                        native: NativeIdentity::new("native-pg-member"),
                    }),
                    epoch: Some(postgres_activity_epoch("7", "2019-01-01T00:00:00Z", 180000)),
                },
                observed_from: from.into(),
                observed_until: until.into(),
                database_writable: Some(case != SettingsFindingCase::ReadOnly),
            }),
        },
    };
    (source, records)
}
