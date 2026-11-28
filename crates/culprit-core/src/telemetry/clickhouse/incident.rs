//! A positive native verdict may produce a finding and an archived incident. Queue size alone is context.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ClickHouseIncident {
    MemoryLimit,
    PartsLimit,
    ConcurrencyLimit,
    Timeout,
    DiskSpace,
    Keeper,
    RejectedQuery,
    QueryLimit,
    ReplicaUnavailable,
    NetworkFailure,
    ReadOnlyWrite,
    MutationFailure,
    ReplicaReadOnly,
    ReplicaSessionExpired,
    ReplicationRetry,
    DistributedSendBlocked,
    ReplicaDelay,
    AsyncInsertFailure,
}
pub const CLICKHOUSE_INCIDENT_SOURCES: &[&str] = &[
    "system.query_log.exceptions",
    "system.mutations",
    "system.replicas",
    "system.replication_queue",
    "system.distribution_queue",
    "system.asynchronous_insert_log",
];
const REPLICA_DELAY_SECONDS: u64 = 300;
impl ClickHouseObservation {
    /// Render the recorded entity and native clock/counters for both live and archived findings.
    pub fn summary(&self) -> String {
        if let Self::Failure(row) = self {
            return crate::t!(
                "provider.clickhouse.query_summary",
                node = &row.node,
                database = &row.database,
                error = &row.exception_name,
                request = &row.query_id,
                duration = crate::format::duration(row.query_duration_ms as f64),
                rows = crate::format::count(row.read_rows as f64)
            );
        }
        let identity = self.identity();
        let label = |key: &str| identity.qualifiers.get(key).map(String::as_str).unwrap_or_default();
        let record = ["mutation_id", "node_name", "replica_name", "query_id", "queue"]
            .iter()
            .find_map(|key| identity.qualifiers.get(*key))
            .map(String::as_str)
            .unwrap_or_default();
        crate::t!(
            "provider.clickhouse.table_summary",
            node = label("node"),
            database = label("database"),
            table = label("table"),
            record = record
        )
    }
    pub fn incident(&self) -> Option<ClickHouseIncident> {
        use ClickHouseIncident::*;
        if !self.is_valid() {
            return None;
        }
        Some(match self {
            Self::Failure(row) => match row.exception_name.as_str() {
                "TOO_MANY_ROWS" | "TOO_MANY_BYTES" | "QUOTA_EXCEEDED" | "TOO_SLOW" => QueryLimit,
                "NO_ACTIVE_REPLICAS" | "TOO_FEW_LIVE_REPLICAS" | "ALL_REPLICAS_ARE_STALE" => ReplicaUnavailable,
                "NETWORK_ERROR" | "ALL_CONNECTION_TRIES_FAILED" | "CANNOT_READ_FROM_SOCKET" => NetworkFailure,
                "READONLY" | "TABLE_IS_READ_ONLY" => ReadOnlyWrite,
                "MEMORY_LIMIT_EXCEEDED" | "MEMORY_LIMIT_EXCEEDED_FOR_USER" => MemoryLimit,
                "TOO_MANY_PARTS" => PartsLimit,
                "TOO_MANY_SIMULTANEOUS_QUERIES" | "TOO_MANY_SIMULTANEOUS_QUERIES_FOR_USER" => ConcurrencyLimit,
                "TIMEOUT_EXCEEDED" | "SOCKET_TIMEOUT" => Timeout,
                "NOT_ENOUGH_SPACE" | "CANNOT_RESERVE" => DiskSpace,
                "KEEPER_EXCEPTION" | "NO_ZOOKEEPER" => Keeper,
                "QUERY_WAS_CANCELLED" | "QUERY_WAS_CANCELLED_BY_CLIENT" => return None,
                _ => RejectedQuery,
            },
            Self::Mutation(row) if row.last_failure_seconds > 0 && !row.latest_failed_part.is_empty() => {
                MutationFailure
            }
            Self::Replica(row) if row.is_session_expired == 1 => ReplicaSessionExpired,
            Self::Replica(row) if row.is_readonly == 1 => ReplicaReadOnly,
            Self::ReplicationTask(row) if row.has_exception == 1 && row.num_tries > 0 => ReplicationRetry,
            Self::DistributedQueue(row) if row.is_blocked == 1 && row.data_files > 0 => DistributedSendBlocked,
            Self::Replica(row) if row.queue_size > 0 && row.absolute_delay >= REPLICA_DELAY_SECONDS => ReplicaDelay,
            Self::AsyncInsert(row) if matches!(row.status.as_str(), "ParsingError" | "FlushError") => {
                AsyncInsertFailure
            }
            _ => return None,
        })
    }
}
impl ClickHouseIncident {
    pub fn title(self) -> String {
        match self {
            Self::MemoryLimit => crate::t!("provider.clickhouse.incident.memorylimit.title"),
            Self::PartsLimit => crate::t!("provider.clickhouse.incident.partslimit.title"),
            Self::ConcurrencyLimit => crate::t!("provider.clickhouse.incident.concurrencylimit.title"),
            Self::Timeout => crate::t!("provider.clickhouse.incident.timeout.title"),
            Self::DiskSpace => crate::t!("provider.clickhouse.incident.diskspace.title"),
            Self::Keeper => crate::t!("provider.clickhouse.incident.keeper.title"),
            Self::QueryLimit => crate::t!("provider.clickhouse.incident.querylimit.title"),
            Self::ReplicaUnavailable => crate::t!("provider.clickhouse.incident.replicaunavailable.title"),
            Self::NetworkFailure => crate::t!("provider.clickhouse.incident.networkfailure.title"),
            Self::ReadOnlyWrite => crate::t!("provider.clickhouse.incident.readonlywrite.title"),
            Self::RejectedQuery => crate::t!("provider.clickhouse.incident.rejectedquery.title"),
            Self::MutationFailure => crate::t!("provider.clickhouse.incident.mutationfailure.title"),
            Self::ReplicaReadOnly => crate::t!("provider.clickhouse.incident.replicareadonly.title"),
            Self::ReplicaSessionExpired => crate::t!("provider.clickhouse.incident.replicasessionexpired.title"),
            Self::ReplicationRetry => crate::t!("provider.clickhouse.incident.replicationretry.title"),
            Self::DistributedSendBlocked => crate::t!("provider.clickhouse.incident.distributed.title"),
            Self::ReplicaDelay => crate::t!("provider.clickhouse.incident.lag.title"),
            Self::AsyncInsertFailure => crate::t!("provider.clickhouse.incident.asyncinsertfailure.title"),
        }
    }
    pub fn followup(self) -> String {
        match self {
            Self::MemoryLimit => crate::t!("provider.clickhouse.incident.memorylimit.followup"),
            Self::PartsLimit => crate::t!("provider.clickhouse.incident.partslimit.followup"),
            Self::ConcurrencyLimit => crate::t!("provider.clickhouse.incident.concurrencylimit.followup"),
            Self::Timeout => crate::t!("provider.clickhouse.incident.timeout.followup"),
            Self::DiskSpace => crate::t!("provider.clickhouse.incident.diskspace.followup"),
            Self::Keeper => crate::t!("provider.clickhouse.incident.keeper.followup"),
            Self::QueryLimit => crate::t!("provider.clickhouse.incident.querylimit.followup"),
            Self::ReplicaUnavailable => crate::t!("provider.clickhouse.incident.replicaunavailable.followup"),
            Self::NetworkFailure => crate::t!("provider.clickhouse.incident.networkfailure.followup"),
            Self::ReadOnlyWrite => crate::t!("provider.clickhouse.incident.readonlywrite.followup"),
            Self::RejectedQuery => crate::t!("provider.clickhouse.incident.rejectedquery.followup"),
            Self::MutationFailure => crate::t!("provider.clickhouse.incident.mutationfailure.followup"),
            Self::ReplicaReadOnly => crate::t!("provider.clickhouse.incident.replicareadonly.followup"),
            Self::ReplicaSessionExpired => crate::t!("provider.clickhouse.incident.replicasessionexpired.followup"),
            Self::ReplicationRetry => crate::t!("provider.clickhouse.incident.replicationretry.followup"),
            Self::DistributedSendBlocked => crate::t!("provider.clickhouse.incident.distributed.followup"),
            Self::ReplicaDelay => crate::t!("provider.clickhouse.incident.lag.followup"),
            Self::AsyncInsertFailure => crate::t!("provider.clickhouse.incident.asyncinsertfailure.followup"),
        }
    }
}
