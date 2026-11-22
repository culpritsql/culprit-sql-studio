use super::*;
use culprit_auth::{Memory, SecretStore};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
fn settings() -> WarehouseConnectionSettings {
    WarehouseConnectionSettings {
        pipeline: "pipeline".into(),
        shard: "shard-0".into(),
        warehouse: WarehouseEndpoint::Clickhouse {
            endpoint: "https://warehouse.example".into(),
            database: "telemetry".into(),
            username: "archive_reader".into(),
            credential_name: "warehouse-read".into(),
        },
        recovery: WarehouseRecovery::AzureBlob {
            account: "ownedarchive".into(),
            container: "originals".into(),
            prefix: "pipeline/shard".into(),
            authentication: WarehouseAzureProfile { name: "recovery-read".into(), tenant: None },
        },
    }
}
fn input(name: &str) -> SaveWarehouseConnection {
    SaveWarehouseConnection {
        name: name.into(),
        settings: settings(),
        password: Some("private-warehouse-password".into()),
    }
}
#[derive(Default)]
struct Secrets {
    memory: Memory,
    fail_delete: AtomicBool,
    fail_set: AtomicBool,
    reads: AtomicUsize,
    accounts: Mutex<Vec<String>>,
    break_metadata_on_set: Mutex<Option<std::path::PathBuf>>,
    break_metadata_on_delete: Mutex<Option<std::path::PathBuf>>,
}
impl SecretStore for Secrets {
    fn get(&self, account: &str) -> Result<Option<String>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.memory.get(account)
    }
    fn set(&self, account: &str, secret: &str) -> Result<()> {
        self.accounts.lock().unwrap().push(account.into());
        if self.fail_set.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorCode::PermissionDenied, "owned keychain write refusal"));
        }
        self.memory.set(account, secret)?;
        if let Some(path) = self.break_metadata_on_set.lock().unwrap().take() {
            std::fs::create_dir(path).unwrap();
        }
        Ok(())
    }
    fn delete(&self, account: &str) -> Result<()> {
        if self.fail_delete.load(Ordering::SeqCst) {
            Err(Error::new(ErrorCode::PermissionDenied, "owned keychain refusal"))
        } else {
            self.memory.delete(account)?;
            if let Some(path) = self.break_metadata_on_delete.lock().unwrap().take() {
                // Preserve the old inode, but make atomic publication fail at the actual destination.
                std::fs::rename(&path, path.with_extension("retained")).unwrap();
                std::fs::create_dir(path).unwrap();
            }
            Ok(())
        }
    }
}
#[test]
#[cfg(all(feature = "warehouse-clickhouse", feature = "evidence-azure", feature = "auth-azure"))]
fn archive_metadata_has_no_secret_and_never_enters_native_connection_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let saved = ws.add_warehouse_connection(input("warehouse")).unwrap();
    assert!(ws.connections().unwrap().is_empty());
    assert_eq!(ws.warehouse_connections().unwrap().len(), 1);
    assert_eq!(secrets.reads.load(Ordering::SeqCst), 0, "listing never prompts the keychain");
    let stored = std::fs::read_to_string(dir.path().join("warehouse-connections.toml")).unwrap();
    assert!(!stored.contains("private-warehouse-password"));
    assert!(stored.contains("archive_reader"));
    assert!(!serde_json::to_string(&saved).unwrap().contains("private-warehouse-password"));
    let account = secrets.accounts.lock().unwrap()[0].clone();
    assert!(account.starts_with("warehouse-password:"));
    assert_ne!(account, "warehouse");
    assert_eq!(secrets.get(&account).unwrap().as_deref(), Some("private-warehouse-password"));
    let old = saved.reference;
    ws.remove_warehouse_connection(&old).unwrap();
    assert!(secrets.get(&account).unwrap().is_none());
    let replacement = ws.add_warehouse_connection(input("warehouse")).unwrap();
    assert_ne!(old.revision, replacement.reference.revision);
    assert!(ws.remove_warehouse_connection(&old).is_err());
    assert_eq!(ws.warehouse_connections().unwrap().len(), 1);
}
#[test]
#[cfg(all(feature = "warehouse-clickhouse", feature = "evidence-azure", feature = "auth-azure"))]
fn a_refused_keychain_delete_retains_metadata_and_duplicate_names_cannot_replace_secret_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let saved = ws.add_warehouse_connection(input("warehouse")).unwrap();
    assert!(ws.add_warehouse_connection(input("warehouse")).is_err());
    assert_eq!(secrets.accounts.lock().unwrap().len(), 1);
    secrets.fail_delete.store(true, Ordering::SeqCst);
    assert!(ws.remove_warehouse_connection(&saved.reference).is_err());
    assert_eq!(ws.warehouse_connections().unwrap()[0].reference, saved.reference);
    secrets.fail_delete.store(false, Ordering::SeqCst);
    ws.remove_warehouse_connection(&saved.reference).unwrap();
    assert!(ws.warehouse_connections().unwrap().is_empty());
}
#[test]
#[cfg(all(feature = "warehouse-clickhouse", feature = "evidence-azure", feature = "auth-azure"))]
fn invalid_routes_crossed_profiles_and_embedded_credentials_refuse_before_keychain_writes() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    for endpoint in [
        "https://user:private@warehouse.example",
        "https://warehouse.example?password=private",
        "http://remote.example",
    ] {
        let mut input = input("warehouse");
        let WarehouseEndpoint::Clickhouse { endpoint: target, .. } = &mut input.settings.warehouse else { panic!() };
        *target = endpoint.into();
        let error = ws.add_warehouse_connection(input).unwrap_err();
        assert!(!format!("{error:?}").contains("private"));
    }
    let mut crossed = input("warehouse");
    let WarehouseRecovery::AzureBlob { authentication, .. } = &mut crossed.settings.recovery else { panic!() };
    authentication.name = "warehouse-read".into();
    assert!(ws.add_warehouse_connection(crossed).is_err());
    assert!(secrets.accounts.lock().unwrap().is_empty());
    assert!(ws.warehouse_connections().unwrap().is_empty());
}
#[tokio::test]
#[cfg(all(feature = "warehouse-clickhouse", feature = "evidence-azure", feature = "auth-azure"))]
async fn cancelled_or_stale_saved_archive_opens_no_native_source_or_warehouse_credential() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let saved = ws.add_warehouse_connection(input("warehouse")).unwrap();
    let cancelled = CollectionCancellation::default();
    cancelled.cancel();
    assert!(ws.warehouse_resources(&saved.reference, &PublishedResourceRequest::default(), &cancelled).await.is_err());
    assert_eq!(secrets.reads.load(Ordering::SeqCst), 0, "pre-cancelled reads never access the keychain");
    ws.remove_warehouse_connection(&saved.reference).unwrap();
    let explicit_removal_reads = secrets.reads.load(Ordering::SeqCst);
    assert_eq!(explicit_removal_reads, 1, "only removal retains its own scoped password for commit compensation");
    ws.add_warehouse_connection(input("warehouse")).unwrap();
    assert!(
        ws.warehouse_resources(
            &saved.reference,
            &PublishedResourceRequest::default(),
            &CollectionCancellation::default()
        )
        .await
        .is_err()
    );
    let current = ws.warehouse_connections().unwrap().remove(0);
    let file = dir.path().join("warehouse-connections.toml");
    let changed =
        std::fs::read_to_string(&file).unwrap().replace("https://warehouse.example", "https://other-warehouse.example");
    std::fs::write(&file, changed).unwrap();
    assert!(
        ws.warehouse_resources(
            &current.reference,
            &PublishedResourceRequest::default(),
            &CollectionCancellation::default()
        )
        .await
        .is_err()
    );
    assert_eq!(
        secrets.reads.load(Ordering::SeqCst),
        explicit_removal_reads,
        "stale and edited read selections add no credential work beyond explicit removal"
    );
}

mod reads;

#[test]
fn metadata_commit_failure_removes_only_the_new_archive_password_without_publishing_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    secrets.memory.set("native-connection", "native-secret").unwrap();
    *secrets.break_metadata_on_set.lock().unwrap() = Some(dir.path().join("warehouse-connections.toml"));
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let error = ws.add_warehouse_connection(input("warehouse")).unwrap_err();
    assert!(!format!("{error:?}").contains("private-warehouse-password"));
    let account = secrets.accounts.lock().unwrap()[0].clone();
    assert!(secrets.get(&account).unwrap().is_none());
    assert_eq!(secrets.get("native-connection").unwrap().as_deref(), Some("native-secret"));
}
#[test]
#[cfg(feature = "warehouse-adx")]
fn azure_roles_retain_distinct_tenants_and_cannot_persist_a_password_or_collapse_named_profiles() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let mut settings = settings();
    settings.warehouse = WarehouseEndpoint::Adx {
        endpoint: "https://owned.westeurope.kusto.windows.net".into(),
        database: "telemetry".into(),
        authentication: WarehouseAzureProfile {
            name: "warehouse-read".into(),
            tenant: Some("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb".into()),
        },
    };
    let WarehouseRecovery::AzureBlob { authentication, .. } = &mut settings.recovery else { panic!() };
    authentication.tenant = Some("cccccccc-1111-2222-3333-dddddddddddd".into());
    assert!(
        ws.add_warehouse_connection(SaveWarehouseConnection {
            name: "bad-password".into(),
            settings: settings.clone(),
            password: Some("private-input".into())
        })
        .is_err()
    );
    let saved = ws
        .add_warehouse_connection(SaveWarehouseConnection {
            name: "adx".into(),
            settings: settings.clone(),
            password: None,
        })
        .unwrap();
    assert_eq!(saved.settings, settings);
    assert!(!saved.password_saved);
    assert!(secrets.accounts.lock().unwrap().is_empty());
    assert_eq!(secrets.reads.load(Ordering::SeqCst), 0);
}

fn restore_metadata_after_destination_fault(path: &std::path::Path, expected: &[u8]) {
    let retained = path.with_extension("retained");
    assert_eq!(std::fs::read(&retained).unwrap(), expected, "the failed removal never changed the original metadata");
    std::fs::remove_dir(path).unwrap();
    std::fs::rename(retained, path).unwrap();
}
#[test]
fn failed_removal_metadata_commit_restores_only_the_same_archive_password_and_original_revision() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    secrets.memory.set("native-connection", "native-secret").unwrap();
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let saved = ws.add_warehouse_connection(input("archive")).unwrap();
    let account = secrets.accounts.lock().unwrap()[0].clone();
    let metadata = dir.path().join("warehouse-connections.toml");
    let original = std::fs::read(&metadata).unwrap();
    *secrets.break_metadata_on_delete.lock().unwrap() = Some(metadata.clone());
    let error = ws.remove_warehouse_connection(&saved.reference).unwrap_err();
    assert!(error.hint.is_none(), "successful compensation preserves the original metadata error");
    assert_eq!(secrets.accounts.lock().unwrap().as_slice(), &[account.clone(), account.clone()]);
    assert_eq!(secrets.get(&account).unwrap().as_deref(), Some("private-warehouse-password"));
    assert_eq!(secrets.get("native-connection").unwrap().as_deref(), Some("native-secret"));
    assert!(!serde_json::to_string(&error).unwrap().contains("private-warehouse-password"));
    restore_metadata_after_destination_fault(&metadata, &original);
    assert_eq!(ws.warehouse_connections().unwrap()[0].reference, saved.reference);
    ws.remove_warehouse_connection(&saved.reference).unwrap();
    assert!(ws.warehouse_connections().unwrap().is_empty());
}
#[test]
fn refused_removal_password_restoration_preserves_the_commit_error_and_explicit_recovery_gap() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Arc::new(Secrets::default());
    let ws = Workspace::open_in(dir.path()).with_secrets(secrets.clone());
    let saved = ws.add_warehouse_connection(input("archive")).unwrap();
    let account = secrets.accounts.lock().unwrap()[0].clone();
    let metadata = dir.path().join("warehouse-connections.toml");
    let original = std::fs::read(&metadata).unwrap();
    *secrets.break_metadata_on_delete.lock().unwrap() = Some(metadata.clone());
    secrets.fail_set.store(true, Ordering::SeqCst);
    let error = ws.remove_warehouse_connection(&saved.reference).unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert_eq!(error.hint.as_deref(), Some(culprit_core::t!("warehouse.connection.secret_restore_failed").as_str()));
    assert!(secrets.get(&account).unwrap().is_none(), "failed compensation must not pretend the secret exists");
    assert!(!serde_json::to_string(&error).unwrap().contains("private-warehouse-password"));
    restore_metadata_after_destination_fault(&metadata, &original);
    assert_eq!(ws.warehouse_connections().unwrap()[0].reference, saved.reference);
    // Missing was already observed before this retry. Removal remains possible without inventing a password.
    secrets.fail_set.store(false, Ordering::SeqCst);
    ws.remove_warehouse_connection(&saved.reference).unwrap();
    assert!(ws.warehouse_connections().unwrap().is_empty());
}
