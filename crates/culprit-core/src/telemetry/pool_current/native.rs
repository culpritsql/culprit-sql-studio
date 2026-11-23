//! A current pool anchor proves a bounded native population, never a portable compute/pool incarnation.
//! Native counter scope is retained: an engine-instance clerk/scheduler is not a selected database allocation.
use super::*;
use std::collections::BTreeSet;

pub const MSSQL_POOL_OPERATIONAL_SOURCE: &str = "azure-pool-operational-state";
pub const MAX_MSSQL_POOL_MEMBERS: usize = 500;
/// Leaves headroom within the existing 128 KiB local manifest ceiling; no metadata limit is widened.
pub const MAX_MSSQL_POOL_SCOPE_BYTES: usize = 64 * 1024;
pub const MSSQL_POOL_OPERATIONAL_SOURCES: [&str; 5] = [
    crate::source_id::MEMORY_CLERKS,
    crate::source_id::SCHEDULERS,
    crate::source_id::PERFORMANCE_COUNTERS,
    crate::source_id::FILE_LATENCY,
    crate::source_id::TEMP_CAPACITY,
];

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlPoolNativeMember {
    /// Native resource-governance ID, not the logical-server catalog ID.
    pub database_id: u32,
    pub database_name: String,
    pub identity: NativeIdentity,
    pub workload_group_id: u32,
}
impl MssqlPoolNativeMember {
    fn is_valid_for(&self, anchor: &MssqlPoolCurrentPoint) -> bool {
        self.database_id > 0
            && self.database_id <= i32::MAX as u32
            && self.workload_group_id > 0
            && self.workload_group_id <= i32::MAX as u32
            && valid_name(&self.database_name)
            && self.identity.id == anchor.member.id
            && self.identity.is_valid()
            && self.identity.qualifiers.len() == 2
            && ["logical_database_guid", "physical_database_guid"].into_iter().all(|name| {
                self.identity.qualifiers.get(name).is_some_and(|value| {
                    replication::native_guid(value) && value.bytes().any(|byte| byte != b'0' && byte != b'-')
                })
            })
    }
}
fn valid_name(value: &str) -> bool {
    !value.trim().is_empty() && value.encode_utf16().count() <= 128 && !value.chars().any(char::is_control)
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlPoolNativeLimitation {
    UnqualifiedTier,
    VisibilityUnavailable,
    ProcessEpochUnavailable,
    ComputeMemberUnavailable,
    MemberRosterUnavailable,
    MemberRosterLimit,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MssqlPoolNativeScope {
    Unavailable {
        reason: MssqlPoolNativeLimitation,
    },
    Observed {
        /// Opaque local startup facts from the existing native process decoder. Not UTC or a stable member UUID.
        process_epoch: String,
        #[serde(deserialize_with = "Option::deserialize")]
        anchor_replica_id: Option<String>,
        members: Vec<MssqlPoolNativeMember>,
    },
}
impl MssqlPoolNativeScope {
    pub fn is_valid_for(&self, anchor: &MssqlPoolCurrentPoint) -> bool {
        if !anchor.is_valid_for_operational() {
            return false;
        }
        match self {
            Self::Unavailable { reason } => match reason {
                MssqlPoolNativeLimitation::UnqualifiedTier => {
                    operational_qualification(anchor) == MssqlPoolScopeQualification::UnqualifiedTier
                }
                MssqlPoolNativeLimitation::VisibilityUnavailable => {
                    operational_qualification(anchor) == MssqlPoolScopeQualification::VisibilityUnavailable
                }
                MssqlPoolNativeLimitation::ComputeMemberUnavailable => {
                    operational_qualification(anchor) == MssqlPoolScopeQualification::Qualified
                        && anchor.native_tier.as_deref() == Some("Hyperscale")
                }
                MssqlPoolNativeLimitation::ProcessEpochUnavailable
                | MssqlPoolNativeLimitation::MemberRosterUnavailable
                | MssqlPoolNativeLimitation::MemberRosterLimit => {
                    operational_qualification(anchor) == MssqlPoolScopeQualification::Qualified
                }
            },
            Self::Observed { process_epoch, anchor_replica_id, members } => {
                if operational_qualification(anchor) != MssqlPoolScopeQualification::Qualified
                    || !valid_digest(process_epoch)
                    || anchor_replica_id.as_deref().is_some_and(|id| {
                        !replication::native_guid(id) || id.bytes().all(|byte| byte == b'0' || byte == b'-')
                    })
                    || anchor.native_tier.as_deref() == Some("Hyperscale") && anchor_replica_id.is_none()
                    || members.is_empty()
                    || members.len() > MAX_MSSQL_POOL_MEMBERS
                {
                    return false;
                }
                let mut ids = BTreeSet::new();
                let mut groups = BTreeSet::new();
                let mut logical = BTreeSet::new();
                let mut physical = BTreeSet::new();
                members.iter().all(|member| {
                    member.is_valid_for(anchor)
                        && ids.insert(member.database_id)
                        && groups.insert(member.workload_group_id)
                        && logical.insert(member.identity.qualifiers.get("logical_database_guid"))
                        && physical.insert(member.identity.qualifiers.get("physical_database_guid"))
                }) && members.iter().any(|member| {
                    member.database_id == anchor.database_id
                        && member.database_name == anchor.database_name
                        && member.identity == anchor.member
                })
            }
        }
    }
    pub fn key(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|_| CollectionStop::InvalidPage.error())?;
        if bytes.len() > MAX_MSSQL_POOL_SCOPE_BYTES {
            return Err(CollectionStop::ByteLimit.error());
        }
        Ok(digest(&bytes))
    }
    pub fn observed(&self) -> Option<(&str, &[MssqlPoolNativeMember])> {
        match self {
            Self::Observed { process_epoch, members, .. } => Some((process_epoch, members)),
            Self::Unavailable { .. } => None,
        }
    }
}
pub fn operational_qualification(anchor: &MssqlPoolCurrentPoint) -> MssqlPoolScopeQualification {
    if !matches!(
        anchor.native_tier.as_deref(),
        Some("General Purpose" | "Business Critical" | "Basic" | "Standard" | "Premium" | "Hyperscale")
    ) {
        MssqlPoolScopeQualification::UnqualifiedTier
    } else if anchor.server_performance_state != Some(true) {
        MssqlPoolScopeQualification::VisibilityUnavailable
    } else {
        MssqlPoolScopeQualification::Qualified
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlPoolNativeAuthority {
    pub before: MssqlPoolNativeScope,
    /// Exact final scope digest; the bounded member roster is retained once, never duplicated in the header.
    pub after: String,
}
impl MssqlPoolNativeAuthority {
    pub fn is_valid_for(&self, anchor: &MssqlPoolCurrentPoint) -> bool {
        self.before.is_valid_for(anchor)
            && valid_digest(&self.after)
            && self.before.key().is_ok_and(|before| before == self.after)
    }
    pub fn accepts_sources(&self, sources: &[StateSourceCoverage]) -> bool {
        let mut names = BTreeSet::new();
        sources.len() == MSSQL_POOL_OPERATIONAL_SOURCES.len()
            && sources.iter().all(|source| {
                source.is_valid()
                    && MSSQL_POOL_OPERATIONAL_SOURCES.contains(&source.source.as_str())
                    && names.insert(source.source.as_str())
                    && (self.before.observed().is_some()
                        || source.records == 0
                            && source.state == crate::SourceState::Unavailable
                            && !source.complete_scope)
            })
    }
}
impl MssqlPoolCurrentAuthority {
    pub fn accepts_operational(&self, snapshot: &StateSnapshotDescription, row: &OperationalObservation) -> bool {
        let Some(native) = self.native.as_deref() else {
            return false;
        };
        let Some((_, members)) = native.before.observed() else {
            return false;
        };
        if !self.matches_snapshot(snapshot) || !row.is_valid() {
            return false;
        }
        match (&row.fact, row.source.as_str()) {
            (OperationalFact::CounterSet(sample), crate::source_id::MEMORY_CLERKS) => {
                sample.scope == crate::MeasurementScope::Server
                    && sample.identity.qualifiers.get("native_object").map(String::as_str)
                        == Some("sys.dm_os_memory_clerks")
            }
            (OperationalFact::CounterSet(sample), crate::source_id::SCHEDULERS) => {
                sample.scope == crate::MeasurementScope::Server
                    && sample.identity.qualifiers.get("native_object").map(String::as_str)
                        == Some(MSSQL_SCHEDULER_OBJECT)
            }
            (OperationalFact::CounterSet(sample), crate::source_id::PERFORMANCE_COUNTERS) => {
                crate::Engine::Mssql
                    .spec()
                    .performance_counters()
                    .is_some_and(|profile| profile.validate(sample).is_ok())
                    && (!resource_governor_candidate(sample) || self.accepts_governor_sample(sample))
            }
            (OperationalFact::Space(space), crate::source_id::TEMP_CAPACITY) => {
                space.population == StoragePopulation::SharedTempdbDataFiles
                    && space.kind == crate::FileKind::Temp
                    && space.scope == crate::FileScope::Server
                    && space.database.is_none()
                    && space.native_name.is_none()
                    && ObservationBounds::new(&snapshot.observed_from, &snapshot.observed_until)
                        .is_some_and(|bounds| bounds.contains(&space.observed_at))
            }
            (OperationalFact::FileIo(file), crate::source_id::FILE_LATENCY) => {
                (file.scope == crate::FileScope::Database
                    && members.iter().any(|member| {
                        file.native_database_id == member.database_id.to_string()
                            && file.database.as_deref().is_none_or(|name| name == member.database_name)
                    })
                    || self.before.native_tier.as_deref() == Some("Hyperscale")
                        && file.scope == crate::FileScope::Server
                        && file.native_database_id == "0"
                        && file.context.read == crate::IoAccounting::CachePage
                        && file.context.replica_id.is_none())
                    && file.reset == crate::ResetMarker::Unknown
            }
            _ => false,
        }
    }
    pub fn accepts_governor_sample(&self, sample: &NativeCounterSet) -> bool {
        let Some(native) = self.native.as_deref() else {
            return false;
        };
        let Some((epoch, _)) = native.before.observed() else {
            return false;
        };
        let properties =
            if resource_governor_counter_set(sample) { &sample.identity.qualifiers } else { &sample.properties };
        properties
            .get("resource_governor_elastic_pool_id")
            .is_some_and(|id| self.before.pool.telemetry_id().is_ok_and(|expected| expected == *id))
            && properties
                .get("resource_governor_anchor_id")
                .is_some_and(|id| self.before.member.key().is_ok_and(|expected| expected == *id))
            && properties.get("resource_governor_pool_process_epoch").map(String::as_str) == Some(epoch)
            && sample.properties.get(PERFORMANCE_COUNTER_CLOCK_PROPERTY).is_some_and(|at| {
                ObservationBounds::new(&self.before.observed_at, &self.after.observed_at)
                    .is_some_and(|bounds| bounds.contains(at))
            })
    }
}

/// Same native read projection for an opt-in ordinary scan and a completed collector original. No capture is invented.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlPoolOperationalRead {
    pub source: CollectionDescription,
    pub observations: Vec<OperationalObservation>,
}
impl MssqlPoolOperationalRead {
    pub fn is_valid(&self) -> bool {
        let Ok(Some(authority)) = mssql_pool_current_authority(&self.source) else {
            return false;
        };
        let Some(snapshot) = self.source.observation.snapshot() else {
            return false;
        };
        if authority.native.is_none() || self.observations.len() != snapshot.records as usize {
            return false;
        }
        let mut keys = BTreeSet::new();
        self.observations
            .iter()
            .all(|row| authority.accepts_operational(snapshot, row) && keys.insert((&row.source, &row.key)))
            && snapshot.sources.iter().all(|source| {
                self.observations.iter().filter(|row| row.source == source.source).count() == source.records as usize
            })
    }
}

pub fn mssql_pool_operational_candidate(source: &CollectionDescription) -> bool {
    matches!(&source.target, CollectionTarget::Shared(pool) if pool.engine == crate::Engine::Mssql && pool.kind == SharedResourceKind::Pool)
        && source.observation.snapshot().is_some_and(|snapshot| snapshot.dataset == SnapshotDataset::Operational)
}
