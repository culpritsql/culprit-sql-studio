use crate::{
    ClickHouseProvider as Provider,
    health::Read,
    identity::{Identity, instant},
    sources,
    workload::{self, Window},
};
use culprit_core::{Capability, Coverage, Result, SNAPSHOT_FORMAT, Snapshot, SnapshotOptions, message};
use std::time::Instant;

pub(crate) async fn snapshot(provider: &Provider, connection: &str, options: &SnapshotOptions) -> Result<Snapshot> {
    options.require_default_workload_population()?;
    let client = provider.client.session()?;
    let started = Instant::now();
    let identity = Identity::read(&client).await?;
    let taken = identity.clock()?;
    let window = Window::ending(taken, options.window_in_minutes())?;
    let mut coverage = Coverage::default();
    let (tables, columns) = crate::catalog::read(&client, &identity, &mut coverage).await;
    provider.require_settled()?;
    let mut workload = coverage.read(&sources::WORKLOAD, workload::read(&client, &identity, options, &window).await);
    if let Some(data) = &mut workload {
        coverage.read(&sources::TIMELINE, crate::timeline::attach(&client, &identity, &window, data).await);
        coverage.delivered(Capability::ClickHouseQueryLog);
    }
    coverage.note(sources::WORKLOAD.name, message!("provider.clickhouse.scope"));
    let mut operations = culprit_core::operations::OperationalStats::default();
    for &read in Read::ALL {
        let source = read.source();
        if let Some(capture) = coverage.read(source, read.capture(&client, &identity, &window, 128).await) {
            coverage.completed(source, capture.rows.len());
            if !capture.complete {
                coverage.partial(source, message!("provider.clickhouse.truncated"));
            }
            operations.clickhouse.extend(capture.rows);
        }
        provider.require_settled()?;
    }
    let target = identity.target(&provider.host, provider.port);
    if Identity::read(&client).await?.target(&provider.host, provider.port) != target {
        return Err(culprit_core::telemetry::CollectionStop::SourceChanged.error());
    }
    let mut snapshot = Snapshot {
        format: SNAPSHOT_FORMAT,
        id: instant(taken).replace(['-', ':'], ""),
        taken_at: instant(taken),
        connection: connection.into(),
        environment: provider.environment,
        duration_ms: started.elapsed().as_secs_f64() * 1000.0,
        server: identity.server(),
        options: Some(options.clone()),
        workload,
        operations,
        runtime_populations: None,
        tables,
        indexes: vec![],
        columns,
        foreign_keys: vec![],
        missing_indexes: vec![],
        statistics: vec![],
        waits: vec![],
        resources: vec![],
        settings: vec![],
        notes: vec![],
        pressure: None,
        query_families: vec![],
        plan_cache: None,
        tuning: None,
        deadlocks: None,
        constraints: vec![],
        histograms: None,
        escalations_before: None,
        compilations_before: None,
        contention_before: None,
        evictions_before: None,
        sizes_before: None,
        sources: vec![],
        identity: None,
    };
    coverage.append_to(&mut snapshot);
    snapshot.bind_identity(target);
    Ok(snapshot)
}
