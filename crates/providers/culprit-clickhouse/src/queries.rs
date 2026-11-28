//! Source statements use ClickHouse typed parameters. Values never become SQL syntax.
pub(crate) const IDENTITY: &str = include_str!("sql/identity.sql");
pub(crate) const TIMELINE: &str = include_str!("sql/timeline.sql");
pub(crate) const WORKLOAD: &str = include_str!("sql/workload.sql");
pub(crate) const LIVE: &str = include_str!("sql/live.sql");
pub(crate) const FAILURES: &str = include_str!("sql/failures.sql");
pub(crate) const ACTIVE_PARTS: &str = include_str!("sql/active_parts.sql");
pub(crate) const MERGES: &str = include_str!("sql/merges.sql");
pub(crate) const MUTATIONS: &str = include_str!("sql/mutations.sql");
pub(crate) const REPLICAS: &str = include_str!("sql/replicas.sql");
pub(crate) const REPLICATION_QUEUE: &str = include_str!("sql/replication_queue.sql");
pub(crate) const ASYNC_INSERTS: &str = include_str!("sql/async_inserts.sql");
pub(crate) const EXECUTION: &str = include_str!("sql/execution.sql");
pub(crate) const PART_HISTORY: &str = include_str!("sql/part_history.sql");
pub(crate) const TABLES: &str = include_str!("sql/tables.sql");
pub(crate) const COLUMNS: &str = include_str!("sql/columns.sql");
pub(crate) const DISTRIBUTION_QUEUE: &str = include_str!("sql/distribution_queue.sql");

pub(crate) fn profile_events() -> (&'static str, String) {
    ("profile_events", culprit_core::telemetry::CLICKHOUSE_PROFILE_EVENTS.join(","))
}
