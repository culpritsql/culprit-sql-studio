use super::*;

#[test]
fn shared_native_global_projection_keeps_units_defaults_product_variants_and_original_numeric_text() {
    for (format, expected) in [
        (NativeSettingsFormat::MysqlGlobalVariables, "262144"),
        (NativeSettingsFormat::MariaDbGlobalVariables, "2097152"),
    ] {
        let observed = format.global_variable("sort_buffer_size", Some("9007199254740993".into())).unwrap();
        assert_eq!(observed.unit.as_deref(), Some("B"));
        assert_eq!(observed.default_source, SettingDefaultSource::EngineSpec);
        assert_eq!(observed.default_value.as_deref(), Some(expected));
        assert_eq!(observed.native_is_default, None, "compiled defaults are not native default verdicts");
        let projected = format.project_global_variable(&observed).unwrap();
        assert_eq!(projected.value, "9007199254740993");
        assert_eq!(projected.default_value.as_deref(), Some(expected));
        assert_eq!(projected.scope, "server");
    }
    let mysql = NativeSettingsFormat::MysqlGlobalVariables;
    assert_eq!(mysql.global_variable("max_execution_time", Some("0".into())).unwrap().unit.as_deref(), Some("ms"));
    let dynamic = mysql.global_variable("temptable_max_ram", Some("1073741824".into())).unwrap();
    assert_eq!(dynamic.default_source, SettingDefaultSource::Unknown);
    assert!(dynamic.default_value.is_none());
    assert!(
        NativeSettingsFormat::MariaDbGlobalVariables.global_variable("max_execution_time", Some("0".into())).is_none()
    );
    assert!(NativeSettingsFormat::PostgresSettings.global_variable_names().is_none());
}
#[test]
fn missing_values_and_foreign_scope_units_nature_or_default_claims_never_become_configuration() {
    let format = NativeSettingsFormat::MysqlGlobalVariables;
    let missing = format.global_variable("slow_query_log", None).unwrap();
    assert!(missing.is_valid());
    assert!(format.project_global_variable(&missing).is_none(), "no fallback to compiled default or empty string");
    let original = format.global_variable("innodb_buffer_pool_size", Some("134217728".into())).unwrap();
    for case in ["scope", "unit", "nature", "namespace", "source", "default", "native-default"] {
        let mut changed = original.clone();
        match case {
            "scope" => changed.scope = MeasurementScope::Session,
            "unit" => changed.unit = Some("ms".into()),
            "nature" => changed.nature = SettingNature::Unknown,
            "namespace" => changed.namespace = "foreign".into(),
            "source" => changed.source = "foreign".into(),
            "default" => changed.default_value = Some("1".into()),
            "native-default" => changed.native_is_default = Some(true),
            _ => unreachable!(),
        }
        assert!(format.project_global_variable(&changed).is_none(), "{case}");
    }
    for private in ["password", "primary_conninfo", "init_connect"] {
        assert!(format.global_variable(private, Some("private".into())).is_none());
    }
}
