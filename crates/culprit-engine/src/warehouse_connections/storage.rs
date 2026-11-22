//! Atomic metadata and keychain lifecycle for one archive revision. Stored values contain no secret bytes.
use super::*;
use crate::private_fs;
use culprit_core::telemetry::{digest, valid_digest};
use serde::{Deserialize, Serialize};
use std::{fs, io::Read};
use zeroize::Zeroizing;
const FORMAT: u32 = 1;
const MAX_CONNECTIONS: usize = 64;
const MAX_METADATA_BYTES: u64 = 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    format: u32,
    connections: BTreeMap<String, Stored>,
}
impl File {
    fn current(&self, reference: &WarehouseConnectionRef) -> Result<&Stored> {
        crate::store::validate_name(&reference.name)?;
        if !valid_digest(&reference.revision) {
            return Err(invalid());
        }
        let stored = self.connections.get(&reference.name).ok_or_else(changed)?;
        if stored.reference(&reference.name)? != *reference {
            return Err(changed());
        }
        Ok(stored)
    }
}
impl Default for File {
    fn default() -> Self {
        Self { format: FORMAT, connections: BTreeMap::new() }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stored {
    id: String,
    pub settings: WarehouseConnectionSettings,
    pub password_account: Option<String>,
}
impl Stored {
    fn reference(&self, name: &str) -> Result<WarehouseConnectionRef> {
        Ok(WarehouseConnectionRef {
            name: name.into(),
            revision: digest(
                &serde_json::to_vec(&("warehouse-connection-v1", &self.id, &self.settings, &self.password_account))
                    .map_err(|_| invalid())?,
            ),
        })
    }
    fn info(&self, name: &str) -> Result<WarehouseConnectionInfo> {
        Ok(WarehouseConnectionInfo {
            reference: self.reference(name)?,
            settings: self.settings.clone(),
            password_saved: self.password_account.is_some(),
        })
    }
    fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.id.len() > 128 || self.id.chars().any(char::is_control) {
            return Err(invalid());
        }
        let expected_account = match &self.settings.warehouse {
            WarehouseEndpoint::Clickhouse { .. } => Some(format!("warehouse-password:{}", self.id)),
            _ => None,
        };
        if self.password_account != expected_account {
            return Err(invalid());
        }
        credentials::validate_settings(&self.settings)
    }
}
#[derive(Clone)]
pub(super) struct Storage {
    file: PathBuf,
}
impl Storage {
    pub fn new(config: PathBuf) -> Self {
        Self { file: config.join("warehouse-connections.toml") }
    }
    fn load(&self) -> Result<File> {
        let file = match fs::File::open(&self.file) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(File::default()),
            Err(_) => return Err(invalid()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_METADATA_BYTES + 1).read_to_end(&mut bytes).map_err(|_| invalid())?;
        if bytes.len() as u64 > MAX_METADATA_BYTES {
            return Err(capacity());
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
        let file: File = toml::from_str(text).map_err(|_| invalid())?;
        if file.format != FORMAT || file.connections.len() > MAX_CONNECTIONS {
            return Err(invalid());
        }
        for (name, stored) in &file.connections {
            crate::store::validate_name(name)?;
            stored.validate()?;
        }
        Ok(file)
    }
    fn save(&self, file: &File) -> Result<()> {
        if let Some(parent) = self.file.parent() {
            private_fs::create_dir(parent).map_err(|_| invalid())?;
        }
        let bytes = toml::to_string(file).map_err(|_| invalid())?;
        if bytes.len() as u64 > MAX_METADATA_BYTES {
            return Err(capacity());
        }
        private_fs::write_atomic(&self.file, bytes.as_bytes()).map_err(|_| invalid())
    }
    pub fn list(&self) -> Result<Vec<WarehouseConnectionInfo>> {
        self.load()?.connections.into_iter().map(|(name, stored)| stored.info(&name)).collect()
    }
    pub fn current(&self, reference: &WarehouseConnectionRef) -> Result<Stored> {
        self.load()?.current(reference).cloned()
    }
    pub fn add(&self, input: SaveWarehouseConnection, secrets: &dyn SecretStore) -> Result<WarehouseConnectionInfo> {
        crate::store::validate_name(&input.name)?;
        credentials::validate_settings(&input.settings)?;
        let password = input.password.map(Zeroizing::new);
        match &input.settings.warehouse {
            WarehouseEndpoint::Clickhouse { .. }
                if password.as_ref().is_none_or(|password| {
                    password.is_empty() || password.len() > 32 * 1024 || password.chars().any(char::is_control)
                }) =>
            {
                return Err(invalid());
            }
            WarehouseEndpoint::Adx { .. } if password.is_some() => return Err(invalid()),
            _ => {}
        }
        let _lock = private_fs::lock(&self.file).map_err(|_| invalid())?;
        let mut file = self.load()?;
        if file.connections.contains_key(&input.name) {
            return Err(Error::invalid(culprit_core::t!("engine.store.exists", name = &input.name)));
        }
        if file.connections.len() >= MAX_CONNECTIONS {
            return Err(capacity());
        }
        let id = culprit_read::durable_fs::random_id()?;
        let stored = Stored {
            password_account: password.as_ref().map(|_| format!("warehouse-password:{id}")),
            id,
            settings: input.settings,
        };
        stored.validate()?;
        let info = stored.info(&input.name)?;
        if let Some(account) = &stored.password_account {
            secrets.set(account, password.as_ref().ok_or_else(invalid)?.as_str())?;
        }
        file.connections.insert(input.name, stored.clone());
        if let Err(error) = self.save(&file) {
            if let Some(account) = &stored.password_account
                && secrets.delete(account).is_err()
            {
                return Err(error.with_hint(culprit_core::t!("warehouse.connection.secret_rollback_failed")));
            }
            return Err(error);
        }
        Ok(info)
    }
    pub fn remove(&self, reference: &WarehouseConnectionRef, secrets: &dyn SecretStore) -> Result<()> {
        let _lock = private_fs::lock(&self.file).map_err(|_| invalid())?;
        let mut file = self.load()?;
        let stored = file.current(reference)?.clone();
        let password = match &stored.password_account {
            Some(account) => {
                let previous = secrets.get(account)?.map(Zeroizing::new);
                secrets.delete(account)?;
                previous
            }
            None => None,
        };
        file.connections.remove(&reference.name);
        if let Err(error) = self.save(&file) {
            if let (Some(account), Some(password)) = (&stored.password_account, &password)
                && secrets.set(account, password.as_str()).is_err()
            {
                return Err(error.with_hint(culprit_core::t!("warehouse.connection.secret_restore_failed")));
            }
            return Err(error);
        }
        Ok(())
    }
}
fn changed() -> Error {
    DeliveryProblem::DestinationChanged.error()
}
