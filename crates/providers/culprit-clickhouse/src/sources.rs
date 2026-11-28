//! Coverage is reported per native table, including optional logs.
use culprit_core::{Capability, Source, key};
const fn source(name: &'static str) -> Source {
    Source { name, reads: key!("provider.clickhouse.detail"), delivers: &[] }
}
pub const TABLES: Source = Source {
    name: culprit_core::source_id::TABLES,
    reads: key!("provider.clickhouse.detail"),
    delivers: &[Capability::TableStats],
};
pub const COLUMNS: Source =
    Source { name: culprit_core::source_id::COLUMNS, reads: key!("provider.clickhouse.detail"), delivers: &[] };
pub const TIMELINE: Source = source("system.query_log.timeline");
pub const WORKLOAD: Source = Source {
    name: culprit_core::source_id::WORKLOAD,
    reads: key!("provider.clickhouse.history"),
    delivers: &[Capability::ClickHouseQueryLog],
};
pub const LIVE: Source =
    Source { name: "system.processes", reads: key!("provider.clickhouse.live"), delivers: &[Capability::Sessions] };
pub const FAILURES: Source = source("system.query_log.exceptions");
pub const PARTS: Source = source("system.parts");
pub const MERGES: Source = source("system.merges");
pub const MUTATIONS: Source = source("system.mutations");
pub const REPLICAS: Source = source("system.replicas");
pub const REPLICATION_QUEUE: Source = source("system.replication_queue");
pub const DISTRIBUTION_QUEUE: Source = source("system.distribution_queue");
pub const ASYNC_INSERTS: Source = source("system.asynchronous_insert_log");
pub const ALL: &[Source] = &[
    TABLES,
    COLUMNS,
    WORKLOAD,
    TIMELINE,
    LIVE,
    FAILURES,
    PARTS,
    MERGES,
    MUTATIONS,
    REPLICAS,
    REPLICATION_QUEUE,
    DISTRIBUTION_QUEUE,
    ASYNC_INSERTS,
];
