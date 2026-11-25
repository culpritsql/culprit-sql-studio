//! Controlled original SHOW/global settings facts; canonical readers still supply all original proof.
use super::*;
use culprit_core::{DatabaseIdentity, Engine, MeasurementScope};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GlobalSettingsFindingCase {
    Disabled,
    Enabled,
    SlowLogDisabled,
    NullValue,
    SessionOnly,
    Partial,
    Empty,
    ReadOnly,
    UnknownNature,
    WrongUnit,
    WrongDefault,
}
impl GlobalSettingsFindingCase {
    pub const ALL: [Self; 11] = [
        Self::Disabled,
        Self::Enabled,
        Self::SlowLogDisabled,
        Self::NullValue,
        Self::SessionOnly,
        Self::Partial,
        Self::Empty,
        Self::ReadOnly,
        Self::UnknownNature,
        Self::WrongUnit,
        Self::WrongDefault,
    ];
}
pub fn global_settings_finding_evidence(
    format: NativeSettingsFormat,
    case: GlobalSettingsFindingCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    assert!(matches!(
        format,
        NativeSettingsFormat::MysqlGlobalVariables | NativeSettingsFormat::MariaDbGlobalVariables
    ));
    let target: CollectionTarget =
        DatabaseIdentity::server(Engine::Mysql, "native-settings", "fixture.invalid", 3306, None).into();
    let mut records = vec![];
    if case != GlobalSettingsFindingCase::Empty {
        let mut setting = format
            .global_variable(
                "performance_schema",
                if case == GlobalSettingsFindingCase::NullValue {
                    None
                } else {
                    Some(
                        if matches!(
                            case,
                            GlobalSettingsFindingCase::Enabled | GlobalSettingsFindingCase::SlowLogDisabled
                        ) {
                            "ON"
                        } else {
                            "OFF"
                        }
                        .into(),
                    )
                },
            )
            .unwrap();
        match case {
            GlobalSettingsFindingCase::SessionOnly => setting.scope = MeasurementScope::Session,
            GlobalSettingsFindingCase::UnknownNature => setting.nature = SettingNature::Unknown,
            GlobalSettingsFindingCase::WrongUnit => setting.unit = Some("B".into()),
            GlobalSettingsFindingCase::WrongDefault => setting.native_is_default = Some(true),
            _ => {}
        }
        records.push(TelemetryRecord::Setting(setting));
        for (name, value) in [
            ("slow_query_log", if case == GlobalSettingsFindingCase::SlowLogDisabled { "OFF" } else { "ON" }),
            ("long_query_time", "0.2"),
            ("log_output", "FILE"),
        ] {
            records.push(TelemetryRecord::Setting(format.global_variable(name, Some(value.into())).unwrap()));
        }
    }
    let member = match format {
        NativeSettingsFormat::MysqlGlobalVariables => SharedResourceIdentity {
            engine: Engine::Mysql,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new("mysql-server")
                .qualified("server_uuid", "f54a461c-0001-4000-8000-000000000001"),
            native: NativeIdentity::new("f54a461c-0001-4000-8000-000000000001"),
        },
        NativeSettingsFormat::MariaDbGlobalVariables => SharedResourceIdentity {
            engine: Engine::Mysql,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new(MARIADB_SERVER_UID_NAMESPACE),
            native: NativeIdentity::new("q83hHHfky00d9QAi5bwmy+JVlfE="),
        },
        NativeSettingsFormat::PostgresSettings
        | NativeSettingsFormat::MssqlSettings
        | NativeSettingsFormat::AzureSqlManagedInstanceSettings => unreachable!(),
    };
    let source = CollectionDescription {
        target: target.clone(),
        partition: SourcePartition::Target,
        source: "mysql-global-variables".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SnapshotAcquisition {
            settings_context: Some(NativeSettingsContext {
                format,
                version: if format == NativeSettingsFormat::MysqlGlobalVariables { "8.4.6" } else { "11.8.9-MariaDB" }
                    .into(),
            }),
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::Settings,
                observed_from: "2020-01-01T12:00:00Z".into(),
                observed_until: "2020-01-01T12:00:01Z".into(),
                records: records.len() as u32,
                sources: vec![StateSourceCoverage::observed(
                    "global-variables".into(),
                    records.len() as u32,
                    case != GlobalSettingsFindingCase::Partial,
                )],
            },
            execution: Box::new(NativeReadContext {
                target,
                partition: SourcePartition::InstanceLocal {
                    member: Some(member),
                    epoch: Some(digest(b"mysql-native-process-v1|9900")),
                },
                database_writable: Some(case != GlobalSettingsFindingCase::ReadOnly),
                observed_from: "2020-01-01T12:00:00Z".into(),
                observed_until: "2020-01-01T12:00:01Z".into(),
            }),
        },
    };
    assert!(source.partition_is_consistent());
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}
