use culprit_core::{Error, Result, t};
use culprit_read::durable_fs;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Mutex,
};

const MAX_STATE_FILE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TRACKED_KEYS: usize = 16_384;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EpisodeAlertStatus {
    Triggered,
    Resolved,
    SuppressedBackfill,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackedEpisode {
    pub dedup_key: String,
    pub status: EpisodeAlertStatus,
    pub first_observed_at: String,
    pub updated_at: String,
    pub resource_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub headline: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackedFinding {
    pub dedup_key: String,
    pub consecutive_windows: u32,
    pub last_window_until: String,
    pub alerted: bool,
    pub resource_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub headline: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackedAvailability {
    pub dedup_key: String,
    pub degraded: bool,
    pub last_state: String,
    pub updated_at: String,
    pub resource_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeduplicationState {
    #[serde(default)]
    pub episodes: BTreeMap<String, TrackedEpisode>,
    #[serde(default)]
    pub findings: BTreeMap<String, TrackedFinding>,
    #[serde(default)]
    pub regressions: BTreeMap<String, String>,
    #[serde(default)]
    pub availability: BTreeMap<String, TrackedAvailability>,
}

impl DeduplicationState {
    /// Enforce bounded state growth by evicting the oldest entries if capacity is reached.
    pub fn enforce_bounds(&mut self) {
        trim_map(&mut self.episodes, MAX_TRACKED_KEYS, |entry| entry.updated_at.clone());
        trim_map(&mut self.findings, MAX_TRACKED_KEYS, |entry| entry.last_window_until.clone());
        trim_map(&mut self.regressions, MAX_TRACKED_KEYS, Clone::clone);
        trim_map(&mut self.availability, MAX_TRACKED_KEYS, |entry| entry.updated_at.clone());
    }
}

fn trim_map<V>(map: &mut BTreeMap<String, V>, limit: usize, key_time: impl Fn(&V) -> String) {
    while map.len() > limit {
        let oldest = map
            .iter()
            .min_by(|(ka, va), (kb, vb)| key_time(va).cmp(&key_time(vb)).then_with(|| ka.cmp(kb)))
            .map(|(k, _)| k.clone());
        if let Some(key) = oldest {
            map.remove(&key);
        } else {
            break;
        }
    }
}

pub trait DeduplicationStore: Send + Sync {
    fn load(&self) -> Result<DeduplicationState>;
    fn save(&self, state: &DeduplicationState) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryDeduplicationStore {
    state: Mutex<DeduplicationState>,
}

impl MemoryDeduplicationStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeduplicationStore for MemoryDeduplicationStore {
    fn load(&self) -> Result<DeduplicationState> {
        Ok(self.state.lock().map_err(|_| Error::internal(t!("notify.store.failed")))?.clone())
    }

    fn save(&self, state: &DeduplicationState) -> Result<()> {
        let mut next = state.clone();
        next.enforce_bounds();
        *self.state.lock().map_err(|_| Error::internal(t!("notify.store.failed")))? = next;
        Ok(())
    }
}

/// Atomic file-backed deduplication journal guarded by an exclusive lock file (`culprit_read::durable_fs`).
pub struct DurableFileDeduplicationStore {
    state_path: PathBuf,
    _owner: durable_fs::ExclusiveFile,
}

impl DurableFileDeduplicationStore {
    pub fn open(directory: &Path) -> Result<Self> {
        if !directory.exists() {
            durable_fs::create_directory(directory).map_err(|_| Error::internal(t!("notify.store.failed")))?;
        }
        let lock_path = directory.join("notify-dedup.lock");
        if !lock_path.exists() {
            let _ = durable_fs::create_file(&lock_path);
        }
        let owner = durable_fs::own_file(&lock_path).map_err(|err| match err {
            durable_fs::OwnershipError::Contended => Error::invalid(t!("notify.store.contended")),
            _ => Error::internal(t!("notify.store.failed")),
        })?;
        Ok(Self { state_path: directory.join("notify-dedup.json"), _owner: owner })
    }
}

impl DeduplicationStore for DurableFileDeduplicationStore {
    fn load(&self) -> Result<DeduplicationState> {
        if !self.state_path.exists() {
            return Ok(DeduplicationState::default());
        }
        let file =
            durable_fs::regular_file(&self.state_path).map_err(|_| Error::internal(t!("notify.store.failed")))?;
        let mut bytes = Vec::new();
        file.take((MAX_STATE_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::internal(t!("notify.store.failed")))?;
        if bytes.len() > MAX_STATE_FILE_BYTES {
            return Err(Error::internal(t!("notify.store.failed")));
        }
        serde_json::from_slice(&bytes).map_err(|_| Error::internal(t!("notify.store.failed")))
    }

    fn save(&self, state: &DeduplicationState) -> Result<()> {
        let mut next = state.clone();
        next.enforce_bounds();
        let bytes = durable_fs::json_bytes(&next, MAX_STATE_FILE_BYTES)
            .map_err(|_| Error::internal(t!("notify.store.failed")))?;
        let _ = std::fs::remove_file(self.state_path.with_file_name(".notify-dedup.json.new"));
        durable_fs::publish(&self.state_path, &bytes).map_err(|_| Error::internal(t!("notify.store.failed")))
    }
}
