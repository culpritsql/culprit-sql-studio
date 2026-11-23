//! One nonce-owned PostgreSQL fixture actor/deadline owner, reused by provider and Engine native acceptance.
//! This explicit test feature is absent from ordinary provider/desktop/MCP dependency graphs.
pub(crate) mod actor;
pub mod case;
mod maintenance;
mod readable_secondary;
use crate::{
    connect::Connector,
    read::{NativeRead, QueryRead, ReadSession},
};
use culprit_core::{ConnectionSpec, Engine, telemetry::*};
pub use maintenance::{
    AutomaticCase, MaintenanceFixture, RelationQueueCase, RelationQueueRole, TransactionMetadataRole,
};
pub use readable_secondary::ReadableSecondaryFixture;

pub fn connection_spec() -> ConnectionSpec {
    let database = std::env::var("CULPRIT_PREVIEW_PG_DATABASE").unwrap();
    assert!(matches!(database.as_str(), "culprit_smoke" | "culprit_timescale"));
    ConnectionSpec {
        engine: Some(Engine::Postgres),
        host: "127.0.0.1".into(),
        port: Some(std::env::var("CULPRIT_PREVIEW_PG_PORT").unwrap().parse().unwrap()),
        database: Some(database),
        user: Some("postgres".into()),
        password: Some(std::env::var("CULPRIT_PREVIEW_PG_PASSWORD").unwrap()),
        options: [("sslmode".into(), "disable".into())].into(),
        ..Default::default()
    }
}
pub fn owner() -> String {
    let owner = std::env::var("CULPRIT_SMOKE_OWNER").expect("explicit owned fixture nonce");
    assert!(owner.len() == 24 && owner.bytes().all(|value| value.is_ascii_hexdigit() && !value.is_ascii_uppercase()));
    owner
}

pub async fn specification(database: &str, budget: CollectionBudget) -> ConnectionSpec {
    let specification = connection_spec();
    assert_eq!(specification.database.as_deref(), Some(database));
    assert!(specification.port.is_some_and(|port| port >= 1024 && port != 5432));
    let owner = owner();
    let mut reader = ReadSession::new(Connector::new(&specification).unwrap(), budget);
    let result = reader
        .query("SELECT current_database() AS database,pg_catalog.current_setting('cluster_name') AS marker", 1)
        .await;
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let rows = result.expect("bounded owned fixture admission");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].text("database").as_deref(), Some(database));
    // Existing supervisor-owned PostgreSQL and Timescale fixtures share this native startup marker.
    assert_eq!(rows[0].text("marker"), Some(format!("culprit_owned_{owner}")));
    let metrics = reader.metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!(metrics.active_tasks, 0);
    assert_eq!(metrics.untracked_tasks, 0);
    assert_eq!(metrics.templates.iter().map(|template| template.completed_tasks).sum::<u32>(), reader.queries());
    assert_eq!(reader.queries(), 2, "one native session setup and one nonce ownership SELECT");
    specification
}

/// Receipt output is fixture-only and contains no connection specification, password or raw SQL.
pub fn write_automatic_receipt(case: &str, name: &str, value: &serde_json::Value) -> std::io::Result<()> {
    assert!(matches!(
        case,
        "builtin-safety"
            | "builtin-recovery"
            | "relation-queue-small"
            | "relation-queue-medium"
            | "relation-queue-parallel"
            | "transaction-metadata"
            | "readable-secondary"
            | "readable-secondary-endpoint"
    ));
    assert!(name.ends_with(".json") && !name.contains('/') && !name.contains('\\'));
    let directory = automatic_directory(case)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(directory.join(name))?;
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?)?;
    file.sync_all()
}

pub fn automatic_directory(case: &str) -> std::io::Result<std::path::PathBuf> {
    assert!(matches!(
        case,
        "builtin-safety"
            | "builtin-recovery"
            | "relation-queue-small"
            | "relation-queue-medium"
            | "relation-queue-parallel"
            | "transaction-metadata"
            | "readable-secondary"
            | "readable-secondary-endpoint"
    ));
    let root = std::path::PathBuf::from(std::env::var("CULPRIT_NATIVE_SAFETY_RECEIPT_DIR").map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "explicit private native safety receipt directory required")
    })?);
    let directory = root.join(case);
    std::fs::create_dir_all(&directory)?;
    Ok(directory)
}
