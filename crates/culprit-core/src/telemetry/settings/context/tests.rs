use super::*;

fn server(engine: Engine) -> ServerInfo {
    ServerInfo {
        engine,
        product: "unknown".into(),
        version: "unknown".into(),
        edition: None,
        cloud: None,
        platform: None,
        database: "selected-original".into(),
        database_size_bytes: None,
        started_at: None,
        read_only_session: true,
        capabilities: vec![],
        missing: vec![],
    }
}
#[test]
fn native_formats_preserve_actual_products_versions_and_do_not_fill_unobserved_server_facts() {
    for (format, raw, engine, product, display) in [
        (
            NativeSettingsFormat::PostgresSettings,
            "180006",
            Engine::Postgres,
            DatabaseProduct::Postgres,
            "PostgreSQL 18.6",
        ),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4.6", Engine::Mysql, DatabaseProduct::Mysql, "MySQL 8.4.6"),
        (
            NativeSettingsFormat::MariaDbGlobalVariables,
            "11.8.9-MariaDB-ubu2404",
            Engine::Mysql,
            DatabaseProduct::Mariadb,
            "MariaDB 11.8.9",
        ),
        (
            NativeSettingsFormat::MariaDbGlobalVariables,
            "5.5.5-11.8.3-MariaDB",
            Engine::Mysql,
            DatabaseProduct::Mariadb,
            "MariaDB 11.8.3",
        ),
        (NativeSettingsFormat::MssqlSettings, "16", Engine::Mssql, DatabaseProduct::Mssql, "SQL Server 16.0"),
        (
            NativeSettingsFormat::AzureSqlManagedInstanceSettings,
            "12",
            Engine::Mssql,
            DatabaseProduct::Mssql,
            "Azure SQL Managed Instance",
        ),
    ] {
        let context = NativeSettingsContext { format, version: raw.into() };
        assert!(context.is_valid_for(engine));
        let mut original = server(engine);
        assert!(context.apply_server_metadata(&mut original));
        assert_eq!(original.database_product(), Some(product));
        assert_eq!(original.product, display);
        assert_eq!(original.database, "selected-original");
        assert!(original.started_at.is_none() && original.edition.is_none() && original.platform.is_none());
        assert!(
            original.read_only_session,
            "native metadata does not replace collector session policy or prove a role"
        );
        let restored: NativeSettingsContext = serde_json::from_slice(&serde_json::to_vec(&context).unwrap()).unwrap();
        assert_eq!(context, restored);
    }
}
#[test]
fn unknown_mixed_unbounded_or_control_character_versions_do_not_choose_a_decoder() {
    for (format, raw) in [
        (NativeSettingsFormat::PostgresSettings, "18.6"),
        (NativeSettingsFormat::PostgresSettings, "0180006"),
        (NativeSettingsFormat::PostgresSettings, "0"),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4"),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4.6-MariaDB"),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4.6-mariadb"),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4.6\nsecret"),
        (NativeSettingsFormat::MysqlGlobalVariables, "8.4.6'"),
        (NativeSettingsFormat::MariaDbGlobalVariables, "11.8.9"),
        (NativeSettingsFormat::MariaDbGlobalVariables, "11.8.9-MariaDB-"),
        (NativeSettingsFormat::MariaDbGlobalVariables, "11.8.9-MariaDB x"),
        (NativeSettingsFormat::MssqlSettings, "12"),
        (NativeSettingsFormat::MssqlSettings, "16.0"),
        (NativeSettingsFormat::AzureSqlManagedInstanceSettings, "11"),
    ] {
        assert!(!NativeSettingsContext { format, version: raw.into() }.is_valid(), "{raw:?}");
    }
    assert!(
        !NativeSettingsContext { format: NativeSettingsFormat::MysqlGlobalVariables, version: "x".repeat(129) }
            .is_valid()
    );
    let context = NativeSettingsContext { format: NativeSettingsFormat::MysqlGlobalVariables, version: "8.4.6".into() };
    let mut wrong = server(Engine::Postgres);
    assert!(!context.apply_server_metadata(&mut wrong));
    assert_eq!(wrong.product, "unknown");
    assert!(
        serde_json::from_value::<NativeSettingsContext>(serde_json::json!({"format":"unknown","version":"8.4.6"}))
            .is_err()
    );
}
