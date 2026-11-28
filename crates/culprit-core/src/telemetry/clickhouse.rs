//! Node-local ClickHouse system table records. Source queries omit raw SQL, users and free-form errors.
use super::*;
mod incident;
mod validation;
mod workload;
pub use workload::ClickHousePopulation;
mod sharing;
#[cfg(test)]
mod tests;
pub use incident::*;

/// Only these per-execution counters are exported, when ClickHouse actually records them.
pub const CLICKHOUSE_PROFILE_EVENTS: &[&str] = &[
    "UserTimeMicroseconds",
    "SystemTimeMicroseconds",
    "OSCPUWaitMicroseconds",
    "OSIOWaitMicroseconds",
    "SelectedParts",
    "SelectedMarks",
    "ExternalAggregationWritePart",
    "ExternalSortWritePart",
    "DiskReadElapsedMicroseconds",
    "NetworkReceiveElapsedMicroseconds",
];

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind", content = "observation")]
#[ts(export)]
pub enum ClickHouseObservation {
    Failure(ClickHouseFailure),
    Execution(ClickHouseExecution),
    PartEvent(ClickHousePartEvent),
    QueryFamily(ClickHouseQueryFamily),
    Parts(ClickHouseParts),
    Merge(ClickHouseMerge),
    Mutation(ClickHouseMutation),
    Replica(ClickHouseReplica),
    ReplicationTask(ClickHouseReplicationTask),
    DistributedQueue(ClickHouseDistributedQueue),
    AsyncInsert(ClickHouseAsyncInsert),
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseFailure {
    pub node: String,
    pub database: String,
    pub query_id: String,
    pub initial_query_id: String,
    pub family: String,
    pub started_us: i64,
    pub ended_us: i64,
    pub outcome: String,
    pub exception_code: u32,
    pub exception_name: String,
    pub query_duration_ms: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub memory_usage: u64,
    pub tables: Vec<String>,
    pub total_tables: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub profile: std::collections::BTreeMap<String, u64>,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseParts {
    pub node: String,
    pub database: String,
    pub table: String,
    pub partition_id: String,
    pub active_parts: u64,
    pub total_rows: u64,
    pub bytes_on_disk: u64,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseMerge {
    pub node: String,
    pub database: String,
    pub table: String,
    pub partition_id: String,
    pub result_part_name: String,
    pub elapsed: f64,
    pub progress: f64,
    pub num_parts: u64,
    pub is_mutation: u8,
    pub rows_read: u64,
    pub rows_written: u64,
    pub memory_usage: u64,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseMutation {
    pub node: String,
    pub database: String,
    pub table: String,
    pub mutation_id: String,
    pub created_seconds: u64,
    pub parts_to_do: u64,
    pub latest_failed_part: String,
    pub last_failure_seconds: u64,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseReplica {
    pub node: String,
    pub database: String,
    pub table: String,
    pub replica_name: String,
    pub is_readonly: u8,
    pub is_session_expired: u8,
    pub queue_size: u64,
    pub oldest_seconds: u64,
    pub absolute_delay: u64,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseReplicationTask {
    pub node: String,
    pub database: String,
    pub table: String,
    pub replica_name: String,
    pub node_name: String,
    #[serde(rename = "type")]
    pub task_type: String,
    pub num_tries: u64,
    pub num_postponed: u64,
    pub created_seconds: u64,
    pub attempted_seconds: u64,
    pub has_exception: u8,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseAsyncInsert {
    pub node: String,
    pub database: String,
    pub table: String,
    pub query_id: String,
    pub flush_query_id: Option<String>,
    pub status: String,
    pub queued_us: i64,
    pub flushed_us: Option<i64>,
    pub bytes: u64,
}

pub const CLICKHOUSE_PART_CONTEXT_US: i64 = 5 * 60 * 1_000_000;
pub const CLICKHOUSE_OPERATIONAL_SOURCE: &str = "clickhouse-system-tables-v1";
impl ClickHouseObservation {
    pub fn source_name(&self) -> &'static str {
        match self {
            Self::Execution(_) => "system.query_log.execution",
            Self::PartEvent(_) => "system.part_log",
            Self::QueryFamily(_) => "system.query_log.families",
            Self::Failure(_) => "system.query_log.exceptions",
            Self::Parts(_) => "system.parts",
            Self::Merge(_) => "system.merges",
            Self::Mutation(_) => "system.mutations",
            Self::Replica(_) => "system.replicas",
            Self::ReplicationTask(_) => "system.replication_queue",
            Self::AsyncInsert(_) => "system.asynchronous_insert_log",
            Self::DistributedQueue(_) => "system.distribution_queue",
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ClickHouseRequestIdentity {
    pub node: String,
    pub database: String,
    pub query_id: String,
    pub started_us: i64,
    pub ended_us: i64,
}
impl ClickHouseRequestIdentity {
    pub fn identity(&self) -> NativeIdentity {
        NativeIdentity::new("clickhouse-execution-v1")
            .qualified("node", &self.node)
            .qualified("database", &self.database)
            .qualified("query_id", &self.query_id)
            .qualified("start", self.started_us.to_string())
            .qualified("end", self.ended_us.to_string())
    }
    pub fn is_valid(&self) -> bool {
        self.identity().is_valid()
            && self.started_us > 0
            && self.ended_us >= self.started_us
            && DateTime::<Utc>::from_timestamp_micros(self.ended_us).is_some()
    }
}
impl ClickHouseFailure {
    pub fn request(&self) -> ClickHouseRequestIdentity {
        ClickHouseRequestIdentity {
            node: self.node.clone(),
            database: self.database.clone(),
            query_id: self.query_id.clone(),
            started_us: self.started_us,
            ended_us: self.ended_us,
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseExecution {
    pub focus: ClickHouseRequestIdentity,
    pub family: String,
    pub outcome: String,
    pub exception_code: u32,
    pub query_duration_ms: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub memory_usage: u64,
    pub profile: std::collections::BTreeMap<String, u64>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHousePartEvent {
    pub node: String,
    pub database: String,
    pub table: String,
    pub partition_id: String,
    pub part_name: String,
    pub query_id: String,
    pub event: String,
    pub occurred_us: i64,
    pub duration_ms: u64,
    pub rows: u64,
    pub size_in_bytes: u64,
    pub error: u32,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseQueryFamily {
    pub node: String,
    pub database: String,
    pub family: String,
    pub from: String,
    pub until: String,
    pub population: ClickHousePopulation,
    pub first_seen: String,
    pub last_seen: String,
    pub sample: DefinitionPayload,
    pub executions: u64,
    pub failures: u64,
    pub elapsed_ms: u64,
    pub max_ms: u64,
    pub rows_read: u64,
    pub bytes_read: u64,
    pub rows_returned: u64,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ClickHouseInvestigation {
    pub target: crate::DatabaseIdentity,
    pub request: ClickHouseRequestIdentity,
    /// Part activity is independent context. The source accepts a table only when the original exception named it.
    pub table: Option<String>,
}
impl ClickHouseInvestigation {
    pub fn key(&self) -> Result<String> {
        serde_json::to_vec(self).map(|value| digest(&value)).map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub fn is_valid(&self) -> bool {
        self.target.engine == crate::Engine::Clickhouse
            && self.target.incarnation.as_deref().is_some_and(valid_key)
            && self.target.database == self.request.database
            && matches!(&self.target.location, crate::DatabaseLocation::Server {instance:Some(node),..} if node==&self.request.node)
            && self.request.is_valid()
            && self.table.as_deref().is_none_or(valid_key)
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct ClickHouseDistributedQueue {
    pub node: String,
    pub database: String,
    pub table: String,
    pub queue_id: String,
    pub is_blocked: u8,
    pub error_count: u64,
    pub data_files: u64,
    pub data_compressed_bytes: u64,
    pub last_error_seconds: u64,
}
