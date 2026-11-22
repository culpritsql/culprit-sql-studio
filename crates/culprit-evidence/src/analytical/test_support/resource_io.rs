//! Controlled source facts exercise real capture, sealed Outbox restore and committed archive preparation.
//! No SQL is executed and no native support/cost qualification is claimed.
use super::*;
pub fn resource_io_evidence(accounting: DataIoAccounting) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (manifest, row) = fixture();
    let mut native = row.record;
    let TelemetryRecord::ResourceSample(point) = &mut native else { unreachable!() };
    point.data_io_accounting = accounting;
    point.metrics[0].key = "data-io".into();
    point.metrics[0].value = Some(99.0);
    (manifest.source, vec![native])
}

/// Controlled transport facts, not a claim that the MSSQL provider admits secondary resource export.
/// Its existing primary-only native policy is tested independently in the provider.
pub fn resource_role_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    use MssqlResourceReplicaRole::*;
    use ResourceLimitBasis::{Absolute, DatabaseLimit, InstanceLimit, PoolLimit};
    use ResourceStatistic::{Maximum, Mean, Point};
    use ResourceUnit::{Cores, DatabaseTransactionUnits, Percent};
    use culprit_core::MeasurementScope::{Database, Server};
    [None, Some(Primary), Some(HighAvailabilitySecondary), Some(GeoReplicationForwarder), Some(NamedReplica)]
        .into_iter()
        .map(|role| {
            let (source, mut rows) = resource_io_evidence(DataIoAccounting::DataFiles);
            let TelemetryRecord::ResourceSample(point) = &mut rows[0] else { unreachable!() };
            point.replica_id = None;
            point.replica_role = role.map(NativeResourceReplicaRole::MssqlDbResourceStats);
            point.metrics = [
                ("cpu", Database, Percent, Mean, Some(15_000), DatabaseLimit, Some(0.0)),
                ("data-io", Database, Percent, Mean, Some(15_000), DatabaseLimit, Some(20.0)),
                ("log-write", Database, Percent, Mean, Some(15_000), DatabaseLimit, Some(90.0)),
                ("memory", Database, Percent, Mean, Some(15_000), DatabaseLimit, None),
                ("workers", Database, Percent, Maximum, Some(15_000), DatabaseLimit, Some(55.0)),
                ("sessions", Database, Percent, Maximum, Some(15_000), DatabaseLimit, Some(30.0)),
                ("xtp-storage", Database, Percent, Point, None, PoolLimit, Some(0.0)),
                ("cpu", Server, Percent, Mean, Some(60_000), InstanceLimit, Some(70.0)),
                ("memory", Server, Percent, Mean, None, InstanceLimit, Some(40.0)),
                ("dtu-limit", Database, DatabaseTransactionUnits, Point, None, Absolute, None),
                ("cpu-limit", Database, Cores, Point, None, Absolute, Some(10.0)),
            ]
            .into_iter()
            .map(|(key, scope, unit, statistic, period_ms, limit_basis, value)| ResourceMetricSample {
                key: key.into(),
                scope,
                unit,
                statistic,
                period_ms,
                limit_basis,
                value,
            })
            .collect();
            (source, rows)
        })
        .collect()
}

pub fn rbio_wait_evidence() -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (manifest, _) = fixture();
    let names = [
        "RBIO_RG_STORAGE",
        "RBIO_RG_REPLICA",
        "RBIO_RG_LOCALDESTAGE",
        "RBIO_RG_DESTAGE",
        "RBIO_RG_GEOREPLICA",
        "RBIO_RG_STORAGE_CHECKPOINT",
        "RBIO_RG_MIGRATION_TARGET",
        "FUTURE_NATIVE_WAIT",
    ];
    let rows: Vec<_> = names
        .into_iter()
        .map(|name| {
            TelemetryRecord::WaitCounter(WaitCounterObservation {
                mechanism: culprit_core::Engine::Mssql.spec().native_wait_mechanism(name),
                source: culprit_core::source_id::WAITS.into(),
                wait_type: name.into(),
                scope: culprit_core::MeasurementScope::Database,
                reset: culprit_core::ResetMarker::Unknown,
                waiting_tasks_count: "9007199254740993".into(),
                wait_time_ms: "9007199254740993".into(),
                signal_wait_time_ms: "0".into(),
                max_wait_time_ms: "7".into(),
            })
        })
        .collect();
    let source = CollectionDescription {
        source: "wait-counters".into(),
        observation: SourceObservation::Snapshot(StateSnapshotDescription {
            dataset: SnapshotDataset::WaitCounters,
            observed_from: "2020-01-01T12:00:00Z".into(),
            observed_until: "2020-01-01T12:00:01Z".into(),
            records: rows.len() as u32,
            sources: vec![StateSourceCoverage::collected(culprit_core::source_id::WAITS.into(), rows.len() as u32)],
        }),
        ..manifest.source
    };
    (source, rows)
}
