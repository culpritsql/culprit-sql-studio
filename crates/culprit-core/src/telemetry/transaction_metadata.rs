//! Original native transaction metadata keeps cache state, shared counters and retention sources independent.
use super::*;
use std::collections::BTreeSet;

pub const POSTGRES_TRANSACTION_METADATA_SOURCE: &str = "postgres-transaction-metadata";
pub const MAX_TRANSACTION_METADATA_BACKENDS: usize = 32;
pub const MAX_TRANSACTION_METADATA_RETAINERS: usize = 32;

/// Native wait-family semantics supplied by the provider, never inferred from arbitrary lock counts or SQL.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum TransactionMetadataWait {
    Subtransaction,
    MultiXact,
}
impl TransactionMetadataWait {
    pub fn from_class(class: ActivityWaitClass) -> Option<Self> {
        match class {
            ActivityWaitClass::SubtransactionMetadata => Some(Self::Subtransaction),
            ActivityWaitClass::MultiTransactionMetadata => Some(Self::MultiXact),
            _ => None,
        }
    }
    pub const fn class(self) -> ActivityWaitClass {
        match self {
            Self::Subtransaction => ActivityWaitClass::SubtransactionMetadata,
            Self::MultiXact => ActivityWaitClass::MultiTransactionMetadata,
        }
    }
}

/// Backend-ID is a native statistics index, distinct from the PID/session generation carried by actor.
/// Overflow describes the top-level transaction's cache, not the statement that generated its children.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresSubtransactionCache {
    pub backend_id: u32,
    pub actor: ActivityObservation,
    #[serde(deserialize_with = "Option::deserialize")]
    pub transaction_started_at: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub cached_subxids: Option<u32>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub overflowed: Option<bool>,
}
impl PostgresSubtransactionCache {
    pub fn is_valid(&self) -> bool {
        self.backend_id <= i32::MAX as u32
            && self.cached_subxids.is_none_or(|count| count <= i32::MAX as u32)
            && self.actor.is_valid()
            && self.actor.has_session_generation_authority()
            && self
                .actor
                .key
                .session
                .id
                .parse::<u32>()
                .is_ok_and(|pid| pid > 0 && pid <= i32::MAX as u32 && pid.to_string() == self.actor.key.session.id)
            && matches!(self.actor.scope, ActivityScopeBasis::SessionDatabase | ActivityScopeBasis::RequestDatabase)
            && canonical_time(&self.actor.key.session.generation).is_ok()
            && self.actor.key.request.as_ref().is_none_or(|request| canonical_time(&request.generation).is_ok())
            && matches!(self.actor.session_context.as_deref().and_then(|context| context.native.as_deref()),
                Some(SessionNativeContext::Postgres(native)) if native.backend_type.as_deref() == Some("client backend"))
            && self.cached_subxids.is_some() == self.overflowed.is_some()
            && self.transaction_started_at.as_deref().is_none_or(|at| canonical_time(at).is_ok())
            && self.actor.key.transaction.as_deref().is_none_or(|at| Some(at) == self.transaction_started_at.as_deref())
            && (!self.actor.idle || !self.actor.statement.is_available())
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PostgresSlruKind {
    Subtransaction,
    MultiXactMember,
    MultiXactOffset,
}
/// A cluster-shared point of cumulative operations. stats_reset is independent of the process epoch.
/// These totals are neither an interval rate nor I/O owned by any observed request.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresSlruCounters {
    pub kind: PostgresSlruKind,
    pub native_name: String,
    pub observed_at: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub stats_reset: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocks_zeroed: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocks_hit: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocks_read: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocks_written: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub blocks_exists: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub flushes: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub truncates: Option<String>,
}
impl PostgresSlruCounters {
    pub fn is_valid(&self) -> bool {
        let name_matches = match self.kind {
            PostgresSlruKind::Subtransaction => matches!(self.native_name.as_str(), "Subtrans" | "subtransaction"),
            PostgresSlruKind::MultiXactMember => {
                matches!(self.native_name.as_str(), "MultiXactMember" | "multixact_member")
            }
            PostgresSlruKind::MultiXactOffset => {
                matches!(self.native_name.as_str(), "MultiXactOffset" | "multixact_offset")
            }
        };
        name_matches
            && canonical_time(&self.observed_at).is_ok()
            && self.stats_reset.as_deref().is_none_or(|reset| canonical_time(reset).is_ok())
            && [
                &self.blocks_zeroed,
                &self.blocks_hit,
                &self.blocks_read,
                &self.blocks_written,
                &self.blocks_exists,
                &self.flushes,
                &self.truncates,
            ]
            .into_iter()
            .flatten()
            .all(|value| value.parse::<i64>().is_ok_and(|parsed| parsed >= 0 && parsed.to_string() == *value))
    }
}

/// One selected native read. The bounded backend roster is confined to the selected database; it cannot
/// establish absence of overflow elsewhere in the cluster, or identify the exact cause of an SLRU wait.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresTransactionMetadata {
    pub observed_at: String,
    pub database_oid: String,
    pub database: String,
    pub focus: ActivityKey,
    pub request_end: BlockingRequestState,
    pub caches: Vec<PostgresSubtransactionCache>,
    pub cache_rows: u32,
    pub cache_limited: bool,
    pub slrus: Vec<PostgresSlruCounters>,
    pub retention: RetentionSnapshot,
    pub retainer_rows: u32,
    pub retainer_limited: bool,
}
impl PostgresTransactionMetadata {
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(invalid());
        }
        serde_json::to_vec(&("postgres-transaction-metadata-point-v1", &self.focus, &self.observed_at))
            .map(|bytes| digest(&bytes))
            .map_err(|_| invalid())
    }
    pub fn is_valid(&self) -> bool {
        let mut backends = BTreeSet::new();
        let mut sessions = BTreeSet::new();
        let mut slrus = BTreeSet::new();
        canonical_time(&self.observed_at).is_ok()
            && self.database_oid.parse::<u32>().is_ok_and(|oid| oid > 0 && oid.to_string() == self.database_oid)
            && valid_key(&self.database)
            && self.focus.is_valid() && self.focus.request.is_some()
            && matches!(self.request_end, BlockingRequestState::SameStatement | BlockingRequestState::Changed | BlockingRequestState::Missing)
            && self.caches.len() <= MAX_TRANSACTION_METADATA_BACKENDS
            && self.cache_rows as usize >= self.caches.len()
            && self.cache_rows as usize <= MAX_TRANSACTION_METADATA_BACKENDS + 1
            && self.cache_limited == (self.cache_rows as usize > self.caches.len())
            && self.caches.iter().all(|row| row.is_valid()
                && (row.actor.key == self.focus || row.overflowed == Some(true))
                && backends.insert(row.backend_id)
                && sessions.insert(&row.actor.key.session)
                && row.actor.database_context_is_consistent(&self.database)
                && matches!(row.actor.session_context.as_deref().and_then(|context| context.native.as_deref()),
                    Some(SessionNativeContext::Postgres(native)) if native.database_oid.as_deref() == Some(self.database_oid.as_str())))
            && self.caches.iter().any(|row| row.actor.key == self.focus)
            && self.slrus.len() <= 3
            && self.slrus.iter().all(|row| row.is_valid() && slrus.insert(row.kind)
)
            && self.retention.is_valid()
            && self.retention.holders.len() <= MAX_TRANSACTION_METADATA_RETAINERS
            && self.retainer_rows as usize >= self.retention.holders.len()
            && self.retainer_rows as usize <= MAX_TRANSACTION_METADATA_RETAINERS + 1
            && self.retainer_limited == (self.retainer_rows as usize > self.retention.holders.len())
            && self.retention.holders.iter().all(|holder| holder.fits_database_capture(&self.database))
    }
    /// Complete only within this documented selected profile, never a cluster-wide culprit census.
    pub fn observation_is_complete(&self) -> bool {
        self.is_valid()
            && !self.cache_limited
            && !self.retainer_limited
            && self.slrus.len() == 3
            && self.caches.iter().all(|cache| cache.overflowed.is_some())
            && self.request_end == BlockingRequestState::SameStatement
            && self
                .retention
                .sources
                .iter()
                .all(|source| matches!(source.state, crate::SourceState::Collected | crate::SourceState::Empty))
    }
    pub fn fits_source_scope(&self, source: &CollectionDescription) -> bool {
        let SourceObservation::SourceRead { snapshot, execution, .. } = &source.observation else { return false };
        let Some(database) = source.target.database() else { return false };
        self.is_valid()
            && source.target.engine() == crate::Engine::Postgres
            && snapshot.dataset == SnapshotDataset::TransactionMetadata
            && snapshot
                .coverage_state()
                .is_ok_and(|state| state != crate::SourceState::Collected || self.observation_is_complete())
            && database.database == self.database
            && execution.target == source.target
            && source.partition == SourcePartition::Target
            && execution.has_qualified_local_owner()
            && execution.database_writable.is_some()
            && ObservationBounds::new(&execution.observed_from, &execution.observed_until).is_some_and(|bounds| {
                bounds.contains(&self.observed_at)
                    && self.slrus.iter().all(|row| bounds.contains(&row.observed_at))
                    && self.retention.holders.iter().all(|holder| bounds.contains(&holder.observed_at))
            })
    }
    pub fn redact(&mut self, engine: crate::Engine) {
        for row in &mut self.caches {
            row.actor.redact(engine);
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct TransactionMetadataReport {
    pub mechanism: TransactionMetadataWait,
    pub source_state: crate::SourceState,
    pub summary: String,
    pub investigation: String,
    pub verify: String,
    pub observed_overflows: Vec<NativeActivityIdentity>,
    pub slru_changes: Vec<PostgresSlruChange>,
    pub observed_xmin_sources: Vec<RetentionObservation>,
    pub limitations: Vec<String>,
}

/// One cluster-shared counter family, with the exact independent endpoint receipts and native clocks.
/// It does not identify the request or transaction responsible for these operations.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum PostgresSlruChange {
    Observed {
        kind: PostgresSlruKind,
        native_name: String,
        scope: crate::MeasurementScope,
        before: Box<DiagnosticEvidenceRef>,
        after: Box<DiagnosticEvidenceRef>,
        before_context: Box<NativeReadContext>,
        after_context: Box<NativeReadContext>,
        observed_from: String,
        observed_until: String,
        stats_reset: String,
        before_state: crate::SourceState,
        after_state: crate::SourceState,
        counters: Vec<PostgresSlruCounterChange>,
    },
    Unavailable {
        kind: PostgresSlruKind,
        reason: PostgresSlruChangeGap,
    },
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PostgresSlruCounterChange {
    /// Documented native pg_stat_slru field, never a query or SAVEPOINT count.
    pub native_name: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub before: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub after: Option<String>,
    /// None retains missing/decreased fields separately from observed zero changes.
    #[serde(deserialize_with = "Option::deserialize")]
    pub change: Option<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PostgresSlruChangeGap {
    MissingPoint,
    AmbiguousPoint,
    UnreadBody,
    IncompatibleOwner,
    RoleChanged,
    UnorderedClocks,
    UnknownReset,
    ResetChanged,
    ResetClock,
}

fn invalid() -> Error {
    CollectionStop::InvalidPage.error()
}

#[cfg(test)]
pub(super) mod tests;
