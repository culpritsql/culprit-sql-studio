//! Executable conformance verification for all 59 Azure SQL Database Watcher routes, 13 Database Hub
//! Microsoft SQL tables (169 fields), Hub PostgreSQL native vs externally measured boundaries, and
//! CollectorHealth readiness/freshness safety invariants. Runs in normal `cargo test` without I/O.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

struct WatcherDatasetSpec {
    id: &'static str,
    scope: &'static str,
    dataset: &'static str,
    recipe_id: &'static str,
    subject: RecipeSubject,
    source_name: &'static str,
    stream: TelemetryStream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HubFieldDisposition {
    /// Native database/instance measurement or identity present in Culprit's canonical wire/details projection.
    NativeProjected(&'static str),
    /// Collector-envelope timestamp/host/instance identity carried on every canonical warehouse row.
    Envelope(&'static str),
    /// Derived deterministically by Culprit's pure reader/view without fabricating missing inputs.
    PureDerived(&'static str),
}

struct HubTableSpec {
    table: &'static str,
    stream: TelemetryStream,
    fields: &'static [(&'static str, HubFieldDisposition)],
}

const WATCHER_59: &[WatcherDatasetSpec] = &[
    // 23 Database datasets (D01-D23)
    WatcherDatasetSpec {
        id: "D01",
        scope: "database",
        dataset: "Active sessions",
        recipe_id: "mssql.activity",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::OPEN_TRANSACTIONS,
        stream: TelemetryStream::ActivityObservations,
    },
    WatcherDatasetSpec {
        id: "D02",
        scope: "database",
        dataset: "Backup history",
        recipe_id: "mssql.backup-history",
        subject: RecipeSubject::Database,
        source_name: MSSQL_BACKUP_HISTORY_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D03",
        scope: "database",
        dataset: "Change processing",
        recipe_id: "mssql.cdc",
        subject: RecipeSubject::Database,
        source_name: MSSQL_CDC_SCANS_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D04",
        scope: "database",
        dataset: "Change processing errors",
        recipe_id: "mssql.change-feed",
        subject: RecipeSubject::Database,
        source_name: MSSQL_CHANGE_FEED_ERRORS_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D05",
        scope: "database",
        dataset: "Connectivity",
        recipe_id: "mssql.activity",
        subject: RecipeSubject::Database,
        source_name: COLLECTOR_CONNECTION_SOURCE,
        stream: TelemetryStream::CollectorConnections,
    },
    WatcherDatasetSpec {
        id: "D06",
        scope: "database",
        dataset: "Geo-replicas",
        recipe_id: "mssql.replication",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::REPLICATION,
        stream: TelemetryStream::ReplicationSamples,
    },
    WatcherDatasetSpec {
        id: "D07",
        scope: "database",
        dataset: "Index metadata",
        recipe_id: "mssql.indexes",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::INDEXES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D08",
        scope: "database",
        dataset: "Memory utilization",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::MEMORY_CLERKS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D09",
        scope: "database",
        dataset: "Missing indexes",
        recipe_id: "mssql.missing-indexes",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::MISSING_INDEXES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D10",
        scope: "database",
        dataset: "Out-of-memory events",
        recipe_id: "mssql.database-native-failures",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::MEMORY_FAILURES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D11",
        scope: "database",
        dataset: "Performance counters, common",
        recipe_id: "mssql.database-performance-counters",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D12",
        scope: "database",
        dataset: "Performance counters, detailed",
        recipe_id: "mssql.database-performance-counters",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D13",
        scope: "database",
        dataset: "Properties",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: DATABASE_PROPERTIES_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D14",
        scope: "database",
        dataset: "Query runtime statistics",
        recipe_id: "mssql.query-history",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::QUERY_STORE,
        stream: TelemetryStream::QueryRuntime,
    },
    WatcherDatasetSpec {
        id: "D15",
        scope: "database",
        dataset: "Query wait statistics",
        recipe_id: "mssql.query-history",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::QUERY_WAITS,
        stream: TelemetryStream::QueryWaits,
    },
    WatcherDatasetSpec {
        id: "D16",
        scope: "database",
        dataset: "Replicas",
        recipe_id: "mssql.replication",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::REPLICATION,
        stream: TelemetryStream::ReplicationSamples,
    },
    WatcherDatasetSpec {
        id: "D17",
        scope: "database",
        dataset: "Resource utilization",
        recipe_id: "mssql.resource-history",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::RESOURCES,
        stream: TelemetryStream::ResourceSamples,
    },
    WatcherDatasetSpec {
        id: "D18",
        scope: "database",
        dataset: "Session statistics",
        recipe_id: "mssql.activity",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::LOGINS,
        stream: TelemetryStream::ActivityObservations,
    },
    WatcherDatasetSpec {
        id: "D19",
        scope: "database",
        dataset: "SOS schedulers",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::SCHEDULERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D20",
        scope: "database",
        dataset: "Storage IO",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::FILE_LATENCY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D21",
        scope: "database",
        dataset: "Storage utilization",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::FILE_CAPACITY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D22",
        scope: "database",
        dataset: "Table metadata",
        recipe_id: "mssql.tables",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::TABLES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "D23",
        scope: "database",
        dataset: "Wait statistics",
        recipe_id: "mssql.wait-counters",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::WAITS,
        stream: TelemetryStream::WaitCounterSamples,
    },
    // 10 Elastic-pool datasets (P01-P10)
    WatcherDatasetSpec {
        id: "P01",
        scope: "elastic-pool",
        dataset: "Memory utilization",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::MEMORY_CLERKS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P02",
        scope: "elastic-pool",
        dataset: "Out-of-memory events",
        recipe_id: "mssql.memory-failures",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::MEMORY_FAILURES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P03",
        scope: "elastic-pool",
        dataset: "Performance counters, common",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P04",
        scope: "elastic-pool",
        dataset: "Performance counters, detailed",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P05",
        scope: "elastic-pool",
        dataset: "Properties",
        recipe_id: "mssql.resource-pool-settings",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: MSSQL_POOL_SETTINGS_SOURCE,
        stream: TelemetryStream::SettingObservations,
    },
    WatcherDatasetSpec {
        id: "P06",
        scope: "elastic-pool",
        dataset: "Resource utilization",
        recipe_id: "mssql.pool-resource-history",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::POOL_RESOURCES,
        stream: TelemetryStream::ResourceSamples,
    },
    WatcherDatasetSpec {
        id: "P07",
        scope: "elastic-pool",
        dataset: "SOS schedulers",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::SCHEDULERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P08",
        scope: "elastic-pool",
        dataset: "Storage IO",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::FILE_LATENCY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P09",
        scope: "elastic-pool",
        dataset: "Storage utilization",
        recipe_id: "mssql.pool-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::TEMP_CAPACITY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "P10",
        scope: "elastic-pool",
        dataset: "Wait statistics",
        recipe_id: "mssql.pool-wait-counters",
        subject: RecipeSubject::Shared(SharedResourceKind::Pool),
        source_name: culprit_core::source_id::WAITS,
        stream: TelemetryStream::WaitCounterSamples,
    },
    // 26 Managed Instance datasets (MI01-MI26)
    WatcherDatasetSpec {
        id: "MI01",
        scope: "managed-instance",
        dataset: "Active sessions",
        recipe_id: "mssql.instance-activity",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::OPEN_TRANSACTIONS,
        stream: TelemetryStream::ActivityObservations,
    },
    WatcherDatasetSpec {
        id: "MI02",
        scope: "managed-instance",
        dataset: "Backup history",
        recipe_id: "mssql.backup-history",
        subject: RecipeSubject::Database,
        source_name: MSSQL_BACKUP_HISTORY_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI03",
        scope: "managed-instance",
        dataset: "Change processing",
        recipe_id: "mssql.cdc",
        subject: RecipeSubject::Database,
        source_name: MSSQL_CDC_SCANS_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI04",
        scope: "managed-instance",
        dataset: "Change processing errors",
        recipe_id: "mssql.cdc",
        subject: RecipeSubject::Database,
        source_name: MSSQL_CDC_ERRORS_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI05",
        scope: "managed-instance",
        dataset: "Connectivity",
        recipe_id: "mssql.instance-activity",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: COLLECTOR_CONNECTION_SOURCE,
        stream: TelemetryStream::CollectorConnections,
    },
    WatcherDatasetSpec {
        id: "MI06",
        scope: "managed-instance",
        dataset: "Database geo-replicas",
        recipe_id: "mssql.replication",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::REPLICATION,
        stream: TelemetryStream::ReplicationSamples,
    },
    WatcherDatasetSpec {
        id: "MI07",
        scope: "managed-instance",
        dataset: "Database properties",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: DATABASE_PROPERTIES_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI08",
        scope: "managed-instance",
        dataset: "Database replicas",
        recipe_id: "mssql.replication",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::REPLICATION,
        stream: TelemetryStream::ReplicationSamples,
    },
    WatcherDatasetSpec {
        id: "MI09",
        scope: "managed-instance",
        dataset: "Database storage utilization",
        recipe_id: "mssql.operational",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::FILE_CAPACITY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI10",
        scope: "managed-instance",
        dataset: "Index metadata",
        recipe_id: "mssql.indexes",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::INDEXES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI11",
        scope: "managed-instance",
        dataset: "Instance properties",
        recipe_id: "mssql.managed-instance-resource-history",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: MSSQL_MANAGED_INSTANCE_RESOURCES_SOURCE,
        stream: TelemetryStream::ResourceSamples,
    },
    WatcherDatasetSpec {
        id: "MI12",
        scope: "managed-instance",
        dataset: "Memory utilization",
        recipe_id: "mssql.shared-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::MEMORY_CLERKS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI13",
        scope: "managed-instance",
        dataset: "Missing indexes",
        recipe_id: "mssql.missing-indexes",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::MISSING_INDEXES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI14",
        scope: "managed-instance",
        dataset: "Out-of-memory events",
        recipe_id: "mssql.memory-failures",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::MEMORY_FAILURES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI15",
        scope: "managed-instance",
        dataset: "Performance counters, common",
        recipe_id: "mssql.shared-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI16",
        scope: "managed-instance",
        dataset: "Performance counters, detailed",
        recipe_id: "mssql.shared-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::PERFORMANCE_COUNTERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI17",
        scope: "managed-instance",
        dataset: "Query runtime statistics",
        recipe_id: "mssql.query-history",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::QUERY_STORE,
        stream: TelemetryStream::QueryRuntime,
    },
    WatcherDatasetSpec {
        id: "MI18",
        scope: "managed-instance",
        dataset: "Query wait statistics",
        recipe_id: "mssql.query-history",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::QUERY_WAITS,
        stream: TelemetryStream::QueryWaits,
    },
    WatcherDatasetSpec {
        id: "MI19",
        scope: "managed-instance",
        dataset: "Resource utilization",
        recipe_id: "mssql.managed-instance-resource-history",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: MSSQL_MANAGED_INSTANCE_RESOURCES_SOURCE,
        stream: TelemetryStream::ResourceSamples,
    },
    WatcherDatasetSpec {
        id: "MI20",
        scope: "managed-instance",
        dataset: "Session statistics",
        recipe_id: "mssql.instance-activity",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::LOGINS,
        stream: TelemetryStream::ActivityObservations,
    },
    WatcherDatasetSpec {
        id: "MI21",
        scope: "managed-instance",
        dataset: "SOS schedulers",
        recipe_id: "mssql.shared-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::SCHEDULERS,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI22",
        scope: "managed-instance",
        dataset: "SQL Agent job history",
        recipe_id: "mssql.agent-jobs",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: MSSQL_AGENT_HISTORY_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI23",
        scope: "managed-instance",
        dataset: "SQL Agent job state",
        recipe_id: "mssql.agent-jobs",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: MSSQL_AGENT_JOBS_SOURCE,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI24",
        scope: "managed-instance",
        dataset: "Storage IO",
        recipe_id: "mssql.shared-operational",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::FILE_LATENCY,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI25",
        scope: "managed-instance",
        dataset: "Table metadata",
        recipe_id: "mssql.tables",
        subject: RecipeSubject::Database,
        source_name: culprit_core::source_id::TABLES,
        stream: TelemetryStream::OperationalSamples,
    },
    WatcherDatasetSpec {
        id: "MI26",
        scope: "managed-instance",
        dataset: "Wait statistics",
        recipe_id: "mssql.instance-wait-counters",
        subject: RecipeSubject::Shared(SharedResourceKind::Instance),
        source_name: culprit_core::source_id::WAITS,
        stream: TelemetryStream::WaitCounterSamples,
    },
];

use HubFieldDisposition::*;

const HUB_13_SQL_TABLES: &[HubTableSpec] = &[
    HubTableSpec {
        table: "SqlServerCPUUtilization",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("process_sample_time_utc", NativeProjected("/Details/properties/observed_at")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("avg_cpu_percent", PureDerived("process_cpu_interval::avg_engine_cpu_percent")),
            ("sql_process_cpu_percent", NativeProjected("/Details/properties/process_utilization")),
            ("idle_cpu_percent", NativeProjected("/Details/properties/system_idle")),
            ("other_process_cpu_percent", PureDerived("process_cpu_interval::other_process_percent_when_valid")),
        ],
    },
    HubTableSpec {
        table: "SqlServerWaitStats",
        stream: TelemetryStream::WaitCounterSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("wait_type", NativeProjected("/WaitType")),
            ("waiting_tasks_count", NativeProjected("/WaitingTasks")),
            ("wait_time_ms", NativeProjected("/WaitTimeMs")),
            ("max_wait_time_ms", NativeProjected("/MaxWaitTimeMs")),
            ("signal_wait_time_ms", NativeProjected("/SignalWaitTimeMs")),
            ("resource_wait_time_ms", PureDerived("WaitCounter::resource_wait_time_ms")),
        ],
    },
    HubTableSpec {
        table: "SqlServerMemoryUtilization",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("memory_clerk_type", NativeProjected("/Details/properties/type")),
            ("memory_clerk_name", NativeProjected("/Details/properties/name")),
            ("memory_size_mb", NativeProjected("/Details/counters/1/value")),
        ],
    },
    HubTableSpec {
        table: "SqlServerPerformanceCountersCommon",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", PureDerived("SourceObservation::DatabasePerformanceCounters::native_database")),
            ("database_name", PureDerived("SourceObservation::DatabasePerformanceCounters::database")),
            ("object_name", NativeProjected("/Details/counters/0/native_object")),
            ("counter_name", NativeProjected("/Details/counters/0/native_name")),
            ("instance_name", NativeProjected("/Details/counters/0/native_instance")),
            ("cntr_type", NativeProjected("/Details/counters/0/native_type")),
            ("cntr_value", NativeProjected("/Details/counters/0/value")),
        ],
    },
    HubTableSpec {
        table: "SqlServerPerformanceCountersDetailed",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", PureDerived("SourceObservation::DatabasePerformanceCounters::native_database")),
            ("database_name", PureDerived("SourceObservation::DatabasePerformanceCounters::database")),
            ("object_name", NativeProjected("/Details/counters/0/native_object")),
            ("counter_name", NativeProjected("/Details/counters/0/native_name")),
            ("instance_name", NativeProjected("/Details/counters/0/native_instance")),
            ("cntr_type", NativeProjected("/Details/counters/0/native_type")),
            ("cntr_value", NativeProjected("/Details/counters/0/value")),
        ],
    },
    HubTableSpec {
        table: "SqlServerStorageIO",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", NativeProjected("/Details/native_database_id")),
            ("database_name", NativeProjected("/Details/database")),
            ("file_id", NativeProjected("/Details/file_id")),
            ("file_type", NativeProjected("/Details/kind")),
            ("file_size_mb", NativeProjected("/Details/logical_size")),
            ("file_max_size_mb", NativeProjected("/Details/maximum_size")),
            ("size_on_disk_bytes", NativeProjected("/Details/size_on_disk_bytes")),
            ("num_of_reads", NativeProjected("/Details/reads")),
            ("num_of_bytes_read", NativeProjected("/Details/read_bytes")),
            ("io_stall_read_ms", NativeProjected("/Details/read_ms")),
            ("io_stall_queued_read_ms", NativeProjected("/Details/queued_read_ms")),
            ("num_of_writes", NativeProjected("/Details/writes")),
            ("num_of_bytes_written", NativeProjected("/Details/write_bytes")),
            ("io_stall_write_ms", NativeProjected("/Details/write_ms")),
            ("io_stall_queued_write_ms", NativeProjected("/Details/queued_write_ms")),
        ],
    },
    HubTableSpec {
        table: "SqlServerDatabaseStorageUtilization",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", NativeProjected("/Details/observed_at")),
            ("collection_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", NativeProjected("/Details/native_database_id")),
            ("database_name", NativeProjected("/Details/database")),
            ("is_primary_replica", PureDerived("DatabasePropertiesObservation::local_replica_role")),
            ("count_data_files", PureDerived("StorageFileSummary::data_file_count")),
            ("count_log_files", PureDerived("StorageFileSummary::log_file_count")),
            ("data_size_allocated_mb", PureDerived("StorageFileSummary::data_allocated_bytes")),
            ("data_size_used_mb", PureDerived("StorageFileSummary::data_used_bytes")),
            ("log_size_allocated_mb", PureDerived("OperationalSpace::total_bytes")),
            ("log_size_used_mb", PureDerived("OperationalSpace::used_bytes")),
            ("persistent_version_store_size_mb", PureDerived("MssqlPersistentVersionStore::off_row_kibibytes")),
            ("online_index_version_store_size_mb", PureDerived("MssqlPersistentVersionStore::online_index_kibibytes")),
        ],
    },
    HubTableSpec {
        table: "SqlServerDatabaseProperties",
        stream: TelemetryStream::OperationalSamples,
        fields: &[
            ("sample_time_utc", NativeProjected("/Details/observed_at")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", NativeProjected("/Details/catalog_database_id")),
            ("database_name", NativeProjected("/Details/database")),
            ("create_date", NativeProjected("/Details/catalog_created_without_offset")),
            ("state_desc", NativeProjected("/Details/catalog_state/description")),
            ("user_access_desc", NativeProjected("/Details/user_access/description")),
            ("updateability", NativeProjected("/Details/updateability")),
            ("is_read_only", NativeProjected("/Details/catalog_read_only")),
            ("is_in_standby", NativeProjected("/Details/in_standby")),
            ("is_broker_enabled", NativeProjected("/Details/broker_enabled")),
            ("collation_name", NativeProjected("/Details/collation")),
            ("compatibility_level", NativeProjected("/Details/compatibility_level")),
            ("snapshot_isolation_state_desc", NativeProjected("/Details/snapshot_isolation/description")),
            ("is_read_committed_snapshot_on", NativeProjected("/Details/read_committed_snapshot")),
            ("is_auto_shrink_on", NativeProjected("/Details/auto_shrink")),
            ("is_auto_create_stats_on", NativeProjected("/Details/auto_create_statistics")),
            ("is_auto_update_stats_on", NativeProjected("/Details/auto_update_statistics")),
            ("is_auto_update_stats_async_on", NativeProjected("/Details/auto_update_statistics_async")),
            ("is_parameterization_forced", NativeProjected("/Details/forced_parameterization")),
            ("is_query_store_on", NativeProjected("/Details/query_store_enabled")),
            ("page_verify_option_desc", NativeProjected("/Details/page_verify/description")),
            ("recovery_model_desc", NativeProjected("/Details/recovery_model/description")),
            ("delayed_durability_desc", NativeProjected("/Details/delayed_durability/description")),
            ("containment_desc", NativeProjected("/Details/containment/description")),
            ("is_encrypted", NativeProjected("/Details/encrypted")),
            ("is_ledger_on", NativeProjected("/Details/ledger_enabled")),
            ("is_change_feed_enabled", NativeProjected("/Details/change_feed_enabled")),
            ("is_accelerated_database_recovery_on", NativeProjected("/Details/accelerated_recovery_enabled")),
            ("last_good_checkdb_time", NativeProjected("/Details/last_good_checkdb_without_offset")),
        ],
    },
    HubTableSpec {
        table: "SqlServerActiveSessions",
        stream: TelemetryStream::ActivityObservations,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("session_id", NativeProjected("/SessionId")),
            ("connection_id", PureDerived("MssqlSessionContext::request_connection_id")),
            ("database_id", PureDerived("MssqlSessionContext::request_database_id")),
            ("database_name", NativeProjected("/NativeDatabase")),
            ("session_status", NativeProjected("/NativeState")),
            ("open_transaction_count", NativeProjected("/OpenTransactions")),
            ("blocking_session_id", NativeProjected("/WaitOwners")),
        ],
    },
    HubTableSpec {
        table: "SqlServerClientConnections",
        stream: TelemetryStream::ActivityObservations,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", PureDerived("SessionClientDatabase::CurrentSession::native_id")),
            ("database_name", PureDerived("SessionClientDatabase::CurrentSession::name")),
            ("host_name", NativeProjected("/ClientHost")),
            ("client_app_name", NativeProjected("/ClientApplication")),
            ("client_interface_name", PureDerived("SessionContext::client_library")),
            ("login_name", NativeProjected("/ClientUser")),
            ("tds_protocol_version", PureDerived("MssqlSessionContext::tds_protocol_version")),
            ("session_count", PureDerived("SessionCensusClientGroup::sessions")),
        ],
    },
    HubTableSpec {
        table: "SqlServerAvailabilityGroupStates",
        stream: TelemetryStream::ReplicationSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("group_id", NativeProjected("/Details/group_id")),
            ("group_name", NativeProjected("/Details/group/name")),
            ("primary_replica", NativeProjected("/Details/group/primary_replica")),
            ("primary_recovery_health_desc", NativeProjected("/Details/group/native_primary_recovery_health")),
            ("secondary_recovery_health_desc", NativeProjected("/Details/group/native_secondary_recovery_health")),
            ("synchronization_health_desc", NativeProjected("/Details/group/native_synchronization_health")),
            ("automated_backup_preference_desc", NativeProjected("/Details/group/native_automated_backup_preference")),
            ("cluster_type_desc", NativeProjected("/Details/group/native_cluster_type")),
        ],
    },
    HubTableSpec {
        table: "SqlServerAvailabilityReplicaStates",
        stream: TelemetryStream::ReplicationSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("group_id", NativeProjected("/Details/group_id")),
            ("replica_id", NativeProjected("/Details/replica_id")),
            ("replica_server_name", NativeProjected("/Details/native_name")),
            ("is_local", NativeProjected("/Details/is_local")),
            ("role_desc", NativeProjected("/Details/replica/native_role")),
            ("operational_state_desc", NativeProjected("/Details/replica/native_operational_state")),
            ("connected_state_desc", NativeProjected("/Details/replica/native_connected_state")),
            ("recovery_health_desc", NativeProjected("/Details/replica/native_recovery_health")),
            ("synchronization_health_desc", NativeProjected("/Details/replica/native_synchronization_health")),
        ],
    },
    HubTableSpec {
        table: "SqlServerDatabaseReplicaStates",
        stream: TelemetryStream::ReplicationSamples,
        fields: &[
            ("sample_time_utc", Envelope("/CollectedUntil")),
            ("machine_name", Envelope("/ResourceId")),
            ("sql_server_instance_name", Envelope("/SourceId")),
            ("database_id", NativeProjected("/Details/database_id")),
            ("database_name", NativeProjected("/Details/database_name")),
            ("group_id", NativeProjected("/Details/group_id")),
            ("replica_id", NativeProjected("/Details/replica_id")),
            ("group_database_id", NativeProjected("/Details/group_database_id")),
            ("is_local", NativeProjected("/Details/is_local")),
            ("is_primary_replica", NativeProjected("/Details/is_primary")),
            ("synchronization_state_desc", NativeProjected("/Details/native_state")),
            ("synchronization_health_desc", NativeProjected("/Details/native_synchronization_health")),
            ("is_suspended", NativeProjected("/Details/is_suspended")),
            ("suspend_reason_desc", NativeProjected("/Details/native_suspend_reason")),
            ("log_send_queue_size_kb", NativeProjected("/Details/log_send_queue_kb")),
            ("log_send_rate", NativeProjected("/Details/log_send_rate_kb_per_second")),
            ("redo_queue_size_kb", NativeProjected("/Details/redo_queue_kb")),
            ("redo_rate", NativeProjected("/Details/redo_rate_kb_per_second")),
        ],
    },
];

async fn project_wire_rows(
    catalog: &LogicalCatalog,
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    sequence: u64,
) -> Vec<(TelemetryStream, Value)> {
    let recovery = corpus::Recovery::capture("conformance-verify", catalog.digest(), source, records, sequence);
    recovery.manifest.validate().unwrap();
    let mut out = Vec::new();
    for batch in &recovery.manifest.batches {
        let schema = catalog.get(batch.stream).unwrap();
        let envelope = super::Envelope::new(&recovery.manifest, &batch.batch_id).unwrap();
        let mut reader = RecoveryRecords::open(&recovery, batch).await.unwrap();
        let mut ordinal = 0;
        while let Some(record) = reader.next().await.unwrap() {
            ordinal += 1;
            let prepared = envelope.input(ordinal, &record).unwrap();
            let projected = schema.project(&prepared).unwrap();
            let _wire: Value = serde_json::from_slice(&schema.encode(&projected).unwrap()).unwrap();
            let logical = schema
                .fields()
                .iter()
                .zip(&projected)
                .map(|(field, value)| (field.name.to_owned(), value.clone()))
                .collect::<serde_json::Map<_, _>>();
            out.push((batch.stream, Value::Object(logical)));
        }
        assert_eq!(ordinal, batch.rows.len());
    }
    out
}

#[tokio::test]
async fn watcher_59_datasets_map_to_registered_recipes_sources_and_canonical_projections() {
    assert_eq!(WATCHER_59.len(), 59, "exact 23 database + 10 pool + 26 MI Watcher dataset routes");
    assert_eq!(WATCHER_59.iter().filter(|spec| spec.scope == "database").count(), 23);
    assert_eq!(WATCHER_59.iter().filter(|spec| spec.scope == "elastic-pool").count(), 10);
    assert_eq!(WATCHER_59.iter().filter(|spec| spec.scope == "managed-instance").count(), 26);

    let ids: BTreeSet<&str> = WATCHER_59.iter().map(|spec| spec.id).collect();
    assert_eq!(ids.len(), 59, "every Watcher dataset ID must be unique");

    let catalog = LogicalCatalog::new().unwrap();
    let recipes: BTreeMap<String, RecipeDescriptor> =
        culprit_mssql::telemetry_recipes().into_iter().map(|recipe| (recipe.id.clone(), recipe)).collect();
    let declared_sources: BTreeSet<&str> =
        culprit_mssql::sources::ALL.iter().map(|source| source.name).chain([COLLECTOR_CONNECTION_SOURCE]).collect();

    for spec in WATCHER_59 {
        let recipe =
            recipes.get(spec.recipe_id).unwrap_or_else(|| panic!("missing recipe {} for {}", spec.recipe_id, spec.id));
        assert!(
            recipe.subjects.contains(&spec.subject),
            "Watcher {} ({}) requires subject {:?} on recipe {}, found {:?}",
            spec.id,
            spec.dataset,
            spec.subject,
            spec.recipe_id,
            recipe.subjects
        );
        assert!(
            declared_sources.contains(spec.source_name),
            "Watcher {} ({}) references undeclared source {}",
            spec.id,
            spec.dataset,
            spec.source_name
        );
        assert!(catalog.get(spec.stream).is_ok(), "missing analytical stream {:?} for {}", spec.stream, spec.id);
        assert!(
            !matches!(recipe.admission, RecipeAdmission::Measured(_)),
            "MSSQL recipe {} must not advertise measured cost receipts",
            spec.recipe_id
        );
        let builtin = matches!(
            spec.recipe_id,
            "mssql.activity"
                | "mssql.blocking"
                | "mssql.instance-wait-counters"
                | "mssql.memory-failures"
                | "mssql.operational"
                | "mssql.readable-secondary-recovery"
                | "mssql.settings"
                | "mssql.shared-operational"
                | "mssql.wait-counters"
        );
        assert_eq!(
            matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)),
            builtin,
            "unexpected admission {:?} on MSSQL recipe {}",
            recipe.admission,
            spec.recipe_id
        );
    }

    // Regression (Plan §2): Culprit must never query Azure SQL's discouraged internal `dl` XE session.
    let events_src = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../providers/culprit-mssql/src/deadlocks.rs"));
    assert!(!events_src.contains("fn_xe_telemetry_blob_target_read_file"));
}

#[tokio::test]
async fn hub_13_sql_tables_169_fields_and_hub_postgres_boundaries_are_verified() {
    assert_eq!(HUB_13_SQL_TABLES.len(), 13, "exact 13 documented Hub Microsoft SQL tables");
    let total_fields: usize = HUB_13_SQL_TABLES.iter().map(|table| table.fields.len()).sum();
    assert_eq!(total_fields, 169, "exact 169 documented Hub Microsoft SQL fields across 13 tables");

    let catalog = LogicalCatalog::new().unwrap();
    let profile = Engine::Mssql.spec().performance_counters().unwrap();
    assert_eq!(profile.common_fields.len(), 20, "Common performance counter inventory");
    assert_eq!(profile.detailed_fields.len(), 12, "Detailed performance counter inventory");
    assert_eq!(profile.investigation_fields.len(), 7, "Resource Governor counter inventory");

    // Project canonical corpus records for all 13 Hub SQL tables and verify every projected/envelope pointer exists!
    let mut table_wires: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    let [(_, cpu_rows_a), (cpu_src, cpu_rows_b)] =
        corpus::process_cpu_interval_evidence(corpus::ProcessCpuIntervalCase::Exact);
    let _ = cpu_rows_a;
    let cpu_projected = project_wire_rows(&catalog, cpu_src, cpu_rows_b, 1).await;
    table_wires.insert("SqlServerCPUUtilization", cpu_projected.into_iter().map(|(_, w)| w).collect());

    let (wait_src, wait_rows) = corpus::wait_counter_evidence(corpus::WaitCounterCase::Exact);
    let TelemetryRecord::WaitCounter(wait_sample) = &wait_rows[0] else { panic!() };
    assert!(wait_sample.resource_wait_time_ms().is_some());
    table_wires.insert(
        "SqlServerWaitStats",
        project_wire_rows(&catalog, wait_src, wait_rows, 2).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (clerk_src, clerk_rows) = corpus::memory_clerk_evidence(corpus::MemoryClerkCase::SharedInstance);
    table_wires.insert(
        "SqlServerMemoryUtilization",
        project_wire_rows(&catalog, clerk_src, clerk_rows, 3).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (perf_src, perf_rows) = corpus::performance_counter_evidence(corpus::PerformanceCounterCase::Exact);
    let perf_wires: Vec<Value> =
        project_wire_rows(&catalog, perf_src, perf_rows, 4).await.into_iter().map(|(_, w)| w).collect();
    table_wires.insert("SqlServerPerformanceCountersCommon", perf_wires.clone());
    table_wires.insert("SqlServerPerformanceCountersDetailed", perf_wires);

    let (io_src, io_rows) = corpus::file_io_evidence_cases()
        .into_iter()
        .find(|(src, rows)| src.target.engine() == Engine::Mssql && !rows.is_empty())
        .unwrap();
    table_wires.insert(
        "SqlServerStorageIO",
        project_wire_rows(&catalog, io_src, io_rows, 5).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (storage_src, storage_rows) = corpus::storage_evidence(corpus::StorageCase::FileCensus);
    table_wires.insert(
        "SqlServerDatabaseStorageUtilization",
        project_wire_rows(&catalog, storage_src, storage_rows, 6).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (prop_src, prop_rows) = corpus::database_properties_evidence(corpus::DatabasePropertiesCase::Exact);
    table_wires.insert(
        "SqlServerDatabaseProperties",
        project_wire_rows(&catalog, prop_src, prop_rows, 7).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (act_src, act_rows) = corpus::activity_evidence_cases()
        .into_iter()
        .find(|(src, rows)| src.target.engine() == Engine::Mssql && !rows.is_empty())
        .unwrap();
    table_wires.insert(
        "SqlServerActiveSessions",
        project_wire_rows(&catalog, act_src, act_rows, 8).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (client_src, client_rows) = corpus::recent_client_evidence(corpus::RecentClientCase::IdlePool);
    let frame_rows: Vec<ActivityFrameRow<'_>> = client_rows
        .iter()
        .map(|row| {
            let TelemetryRecord::Activity(observation) = row else { panic!() };
            ActivityFrameRow { observation, reference: None }
        })
        .collect();
    let frame = ActivityFrameEvidence::new(&client_src, &frame_rows, true).unwrap();
    let census = culprit_checks::session_census::explain_frame(&frame).unwrap();
    assert_eq!(census.recent_clients.groups.entries.len(), 1);
    table_wires.insert(
        "SqlServerClientConnections",
        project_wire_rows(&catalog, client_src, client_rows, 9).await.into_iter().map(|(_, w)| w).collect(),
    );

    let (ag_src, ag_rows) = corpus::replication_evidence(corpus::ReplicationCase::MssqlAvailabilityGroup);
    let ag_wires: Vec<Value> =
        project_wire_rows(&catalog, ag_src, ag_rows, 10).await.into_iter().map(|(_, w)| w).collect();
    table_wires.insert("SqlServerAvailabilityGroupStates", ag_wires.clone());
    table_wires.insert("SqlServerAvailabilityReplicaStates", ag_wires.clone());
    table_wires.insert("SqlServerDatabaseReplicaStates", ag_wires);

    for spec in HUB_13_SQL_TABLES {
        assert!(catalog.get(spec.stream).is_ok());
        let seen_fields: BTreeSet<&str> = spec.fields.iter().map(|(name, _)| *name).collect();
        assert_eq!(seen_fields.len(), spec.fields.len(), "duplicate field in {}", spec.table);
        let wires =
            table_wires.get(spec.table).unwrap_or_else(|| panic!("missing corpus wire rows for {}", spec.table));
        assert!(!wires.is_empty());
        for &(field_name, disposition) in spec.fields {
            match disposition {
                Envelope(pointer) | NativeProjected(pointer) => {
                    assert!(
                        wires.iter().any(|row| row.pointer(pointer).is_some()),
                        "Hub SQL {}.{} expected wire JSON pointer {} in {:?}",
                        spec.table,
                        field_name,
                        pointer,
                        wires
                    );
                }
                PureDerived(owner) => assert!(!owner.is_empty()),
            }
        }
    }

    // Linux SystemIdle = 0 must never fabricate other-process CPU utilization (Plan §2 / §4).
    assert_eq!(corpus::ProcessCpuIntervalCase::LinuxIdle.expected_history_other_percent(), None);
    assert_eq!(corpus::ProcessCpuIntervalCase::Exact.expected_history_other_percent(), Some(40));

    // Hub PostgreSQL native vs externally measured host/cloud boundaries & explicit Cosmos deferral (Plan §1.4, §12).
    #[cfg(feature = "postgres")]
    {
        let pg_sources: BTreeSet<&str> = culprit_postgres::sources::ALL.iter().map(|s| s.name).collect();
        for required_native in [
            culprit_core::source_id::SESSIONS,
            culprit_core::source_id::DATABASE_STATS,
            culprit_core::source_id::FILE_LATENCY,
            culprit_core::source_id::SHARED_MEMORY_ALLOCATIONS,
            culprit_core::source_id::REPLICATION,
            culprit_core::source_id::STANDBY_CONFLICTS,
            culprit_core::source_id::MULTIXACTS,
            culprit_core::source_id::WORKLOAD,
        ] {
            assert!(pg_sources.contains(required_native), "missing native PG source {required_native}");
        }
        for external_only in ["host-cpu", "host-memory", "backup-storage", "network-egress"] {
            assert!(
                !pg_sources.contains(external_only),
                "database-only PG provider must not pretend to measure external host/cloud metric {external_only}"
            );
        }
        let pg_recipes: BTreeSet<String> = culprit_postgres::telemetry_recipes().into_iter().map(|r| r.id).collect();
        for required_recipe in [
            "postgres.activity",
            "postgres.statement-counters",
            "postgres.operational",
            "postgres.instance-operational",
            "postgres.replication",
            "postgres.settings",
        ] {
            assert!(pg_recipes.contains(required_recipe), "missing PG recipe {required_recipe}");
        }
    }
    assert!(
        !Engine::ALL.iter().any(|engine| format!("{engine:?}").to_ascii_lowercase().contains("cosmos")),
        "Cosmos remains explicitly deferred rather than spoofed through SQL branches"
    );
}

#[test]
fn collector_health_exposes_independent_freshness_stages_and_never_clears_unconfirmed_source_debt() {
    use culprit_collector::health::CollectorHealth;
    let inventory = FleetInventory {
        targets: vec![FleetTarget {
            id: "orders".into(),
            connection: "orders".into(),
            subject: CollectionSubject::Database,
            shard: "alpha".into(),
            admission_group: "orders".into(),
            schedules: vec![FleetSchedule {
                id: "activity".into(),
                every_seconds: 300,
                jitter_seconds: 0,
                collection: ScheduledCollection::Activity { policy: ActivityPolicy::default() },
                budget: CollectionBudget::default(),
            }],
        }],
    };
    let health = CollectorHealth::new(&inventory, "alpha").unwrap();
    health.initialized().unwrap();
    health
        .collection(&FleetCollectionEvent {
            target: "orders".into(),
            connection: "orders".into(),
            schedule: "activity".into(),
            work: FleetWork::Capture,
            status: CollectionStatus::Complete,
            native_coverage: Some(culprit_core::SourceState::Collected),
            capture: None,
            error_code: None,
            gap: None,
        })
        .unwrap();
    health
        .archive_backlog(DeliveryBacklog { pending: 1, quarantined: 0, oldest_pending_collection_ms: Some(1_000) })
        .unwrap();
    health.backlog(DeliveryBacklog { pending: 2, quarantined: 0, oldest_pending_collection_ms: Some(2_000) }).unwrap();
    health
        .outbox(&OutboxStatus {
            outbox_id: "a".repeat(32),
            limits: OutboxLimits::default(),
            charged_bytes: 512,
            captures: 1,
            ready: 1,
            unfinished: 0,
            connection_controls_pending: 0,
            source_debts: 0,
            unconfirmed_sources: 0,
            entries: vec![],
            next_cursor: None,
        })
        .unwrap();
    let healthy = health.reply().unwrap();
    assert!(healthy.ready);
    let metrics = String::from_utf8(healthy.metrics).unwrap();
    for independent_metric in [
        "culprit_collector_schedule_last_completed_operation_age_seconds",
        "culprit_collector_schedule_last_native_observation_age_seconds",
        "culprit_collector_schedule_native_coverage",
        "culprit_collector_archive_pending_ranges 1",
        "culprit_collector_delivery_pending_ranges 2",
        "culprit_collector_outbox_unconfirmed_sources 0",
        "culprit_collector_source_fenced 0",
        "culprit_collector_ready 1",
    ] {
        assert!(metrics.contains(independent_metric), "missing metric {independent_metric} in:\n{metrics}");
    }

    // Unconfirmed native source work immediately marks readiness false and remains fenced without timer auto-clear.
    health
        .outbox(&OutboxStatus {
            outbox_id: "a".repeat(32),
            limits: OutboxLimits::default(),
            charged_bytes: 512,
            captures: 1,
            ready: 1,
            unfinished: 0,
            connection_controls_pending: 0,
            source_debts: 1,
            unconfirmed_sources: 1,
            entries: vec![],
            next_cursor: None,
        })
        .unwrap();
    assert!(!health.ready().unwrap());
    health.source_paused().unwrap();
    let fenced = health.reply().unwrap();
    assert!(!fenced.ready);
    let fenced_metrics = String::from_utf8(fenced.metrics).unwrap();
    assert!(fenced_metrics.contains("culprit_collector_source_fenced 1"));
    assert!(fenced_metrics.contains("culprit_collector_outbox_unconfirmed_sources 1"));
    assert!(fenced_metrics.contains("culprit_collector_ready 0"));
}
