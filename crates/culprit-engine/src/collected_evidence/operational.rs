//! Database-scoped Operational, Cleanup, PlanCacheSummary, WaitCounters, and DeadlockHistory captures feed pure checks without a connection.
use super::*;
use culprit_core::{
    CleanupHolder, Deadlock, DeadlockReport, MeasurementScope, OpenTransaction, PlanCacheUse, Pressure, Report,
    TransactionActivity, WaitStat, operations::SpaceUsage, telemetry::*,
};
use std::collections::BTreeMap;

pub(crate) struct RetainedOperationalAnalysis {
    pub report: Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
}

pub(crate) fn supports_operational(original: &dyn OriginalProjection) -> bool {
    original.source().target.database().is_some()
        && (original.source().observation.events().is_some()
            || original.source().observation.snapshot().is_some_and(|snapshot| {
                matches!(
                    snapshot.dataset,
                    SnapshotDataset::Operational
                        | SnapshotDataset::PlanCacheSummary
                        | SnapshotDataset::Cleanup
                        | SnapshotDataset::WaitCounters
                )
            }))
}

pub(crate) fn analyze_operational_original(
    original: &dyn OriginalProjection,
    catalogs: &catalogs::CatalogContext,
    pipeline: &culprit_checks::Pipeline,
) -> Result<RetainedOperationalAnalysis> {
    let source = original.source();
    let database = source.target.database().ok_or_else(invalid)?.clone();
    if !supports_operational(original) || !source.partition_is_consistent() || !original.original().is_valid() {
        return Err(invalid());
    }
    let taken_at = if let Some(header) = source.observation.snapshot() {
        if !header.is_valid() || original.records().len() > header.records as usize {
            return Err(invalid());
        }
        header.observed_until.clone()
    } else if let SourceObservation::RetainedEvents(events) = &source.observation {
        events.observed_at.clone()
    } else {
        return Err(invalid());
    };
    let mut limitations = vec![
        limit(None, CollectedAnalysisLimit::MissingCurrentServerMetadata),
        limit(None, CollectedAnalysisLimit::MissingSchema),
        limit(None, CollectedAnalysisLimit::MissingSettings),
        limit(None, CollectedAnalysisLimit::MissingResourceMeasurements),
    ];
    let analysis_key = original.original().key()?;
    let policy = AnalysisPolicy {
        analysis_key: &analysis_key,
        pipeline,
        origin: culprit_core::AnalysisOrigin::RetainedInput,
        catalogs,
    };
    let mut snapshot = super::snapshot(original, database, None, !original.is_read_complete(), &policy);
    snapshot.taken_at = taken_at;
    catalogs.apply(&mut snapshot);
    if catalogs.settings_complete() {
        limitations.retain(|limitation| limitation.reason != CollectedAnalysisLimit::MissingSettings);
    }

    if let Some(header) = source.observation.snapshot() {
        match header.dataset {
            SnapshotDataset::Operational => populate_operational(original, &mut snapshot)?,
            SnapshotDataset::PlanCacheSummary => populate_plan_cache(original, &mut snapshot)?,
            SnapshotDataset::Cleanup => populate_cleanup(original, &mut snapshot)?,
            SnapshotDataset::WaitCounters => populate_wait_counters(original, &mut snapshot)?,
            _ => return Err(invalid()),
        }
    } else if let SourceObservation::RetainedEvents(events) = &source.observation {
        populate_deadlocks(original, events, &mut snapshot)?;
    } else {
        return Err(invalid());
    }

    let report =
        run_retained_snapshot(snapshot, pipeline, culprit_core::AnalysisOrigin::RetainedInput, &mut limitations);
    Ok(RetainedOperationalAnalysis { report, limitations })
}

fn populate_operational(original: &dyn OriginalProjection, snapshot: &mut Snapshot) -> Result<()> {
    let read = original.operational_read()?;
    if read
        .description
        .observation
        .snapshot()
        .is_some_and(|header| header.sources.iter().any(|entry| StorageRead::recognizes(&entry.source)))
    {
        let storage = StorageRead::from_source(&read.description, read.observations.clone(), read.read_complete)?;
        let mut spaces = Vec::new();
        for row in &storage.observations {
            let OperationalFact::Space(native) = &row.fact else { continue };
            let (Some(total), Some(used)) = (
                native.total_bytes.as_deref().and_then(|v| v.parse::<u64>().ok()),
                native.used_bytes.as_deref().and_then(|v| v.parse::<u64>().ok()),
            ) else {
                continue;
            };
            let limit_bytes = native
                .limit_bytes
                .as_deref()
                .map(str::parse::<u64>)
                .transpose()
                .map_err(|_| invalid())?
                .map(|v| v as f64);
            let name = match native.kind {
                culprit_core::FileKind::Log => "Transaction log".into(),
                culprit_core::FileKind::Temp => "Tempdb".into(),
                _ => native.database.clone().unwrap_or_else(|| "Data files".into()),
            };
            spaces.push(SpaceUsage {
                origin: Some(StoragePointRef { source: row.source.clone(), key: row.key.clone() }),
                name,
                kind: native.kind,
                scope: native.scope,
                total_bytes: total as f64,
                used_bytes: used as f64,
                parts: Vec::new(),
                growth_enabled: native.growth_enabled,
                limit_bytes,
                reuse_wait: native.reuse_wait.clone(),
            });
        }
        for coverage in &storage.sources {
            set_source_state(snapshot, &coverage.source, coverage.state, original.is_read_complete());
        }
        snapshot.operations.spaces = spaces;
        snapshot.operations.storage = Some(Box::new(storage));
    }
    apply_mssql_cleanup_facts(&read.observations, snapshot);
    Ok(())
}

fn populate_plan_cache(original: &dyn OriginalProjection, snapshot: &mut Snapshot) -> Result<()> {
    let read = original.operational_read()?;
    let mut gauges = BTreeMap::<&str, u64>::new();
    for row in &read.observations {
        if row.source != source_id::PLAN_CACHE_USE {
            continue;
        }
        let OperationalFact::Counter(sample) = &row.fact else { return Err(invalid()) };
        let value = sample.value.integer().ok_or_else(invalid)?;
        if gauges.insert(row.key.as_str(), value).is_some() {
            return Err(invalid());
        }
    }
    if let (Some(&total_plans), Some(&total_bytes), Some(&adhoc_plans), Some(&adhoc_bytes)) = (
        gauges.get("plan-cache-total-entries"),
        gauges.get("plan-cache-total-bytes"),
        gauges.get("plan-cache-adhoc-lookup-one-entries"),
        gauges.get("plan-cache-adhoc-lookup-one-bytes"),
    ) {
        if adhoc_plans > total_plans || adhoc_bytes > total_bytes {
            return Err(invalid());
        }
        snapshot.plan_cache = Some(PlanCacheUse {
            total_plans: total_plans as f64,
            total_bytes: total_bytes as f64,
            single_use_plans: adhoc_plans as f64,
            single_use_bytes: adhoc_bytes as f64,
        });
    }
    if let Some(coverage) = read.source_coverage(source_id::PLAN_CACHE_USE) {
        set_source_state(snapshot, source_id::PLAN_CACHE_USE, coverage.state, original.is_read_complete());
    }
    Ok(())
}

fn populate_cleanup(original: &dyn OriginalProjection, snapshot: &mut Snapshot) -> Result<()> {
    let read = original.operational_read()?;
    for name in [source_id::OPEN_TRANSACTIONS, source_id::VERSION_STORE, source_id::PERSISTENT_VERSION_STORE] {
        if let Some(coverage) = read.source_coverage(name) {
            set_source_state(snapshot, name, coverage.state, original.is_read_complete());
        }
    }
    apply_mssql_cleanup_facts(&read.observations, snapshot);
    Ok(())
}

fn populate_wait_counters(original: &dyn OriginalProjection, snapshot: &mut Snapshot) -> Result<()> {
    let mut rows = Vec::with_capacity(original.records().len());
    for row in original.records() {
        let TelemetryRecord::WaitCounter(obs) = &row.row().record else { return Err(invalid()) };
        rows.push(obs.clone());
    }
    let read = WaitCounterSnapshot::from_source(original.source(), rows, original.is_read_complete())?;
    if !read.is_valid_for(snapshot.server.engine) {
        return Err(invalid());
    }
    for coverage in &read.sources {
        set_source_state(snapshot, &coverage.source, coverage.state, original.is_read_complete());
    }
    let mut waits = Vec::new();
    for obs in &read.observations {
        if obs.scope != MeasurementScope::Database || obs.wait_type.starts_with("SLEEP_") {
            continue;
        }
        let wait_ms = obs.wait_time_ms.parse::<u64>().map_err(|_| invalid())? as f64;
        if wait_ms <= 0.0 {
            continue;
        }
        let tasks = obs.waiting_tasks_count.parse::<u64>().map_err(|_| invalid())? as f64;
        let category = if let Some(mechanism) = obs.mechanism {
            mechanism.class().category()
        } else if matches!(
            obs.wait_type.as_str(),
            "WAIT_ON_SYNC_STATISTICS_REFRESH" | "RESOURCE_SEMAPHORE_QUERY_COMPILE"
        ) {
            "Compilation"
        } else {
            "Other"
        };
        waits.push(WaitStat {
            mechanism: obs.mechanism,
            category: category.into(),
            wait_type: Some(obs.wait_type.clone()),
            wait_ms,
            tasks: (tasks > 0.0).then_some(tasks),
        });
    }
    waits.sort_by(|a, b| b.wait_ms.total_cmp(&a.wait_ms).then_with(|| a.wait_type.cmp(&b.wait_type)));
    snapshot.waits = waits;
    snapshot.pressure.get_or_insert_with(Pressure::default).wait_counters = Some(Box::new(read));
    Ok(())
}

fn populate_deadlocks(
    original: &dyn OriginalProjection,
    events: &EventHistoryDescription,
    snapshot: &mut Snapshot,
) -> Result<()> {
    let from = chrono::DateTime::parse_from_rfc3339(&events.from).map_err(|_| invalid())?;
    let until = chrono::DateTime::parse_from_rfc3339(&events.until).map_err(|_| invalid())?;
    let seconds = (until - from).num_seconds();
    if seconds <= 0 {
        return Err(invalid());
    }
    let since_hours = (seconds as f64 / 3600.0).max(1.0 / 3600.0);
    let mut deadlocks = Vec::new();
    for row in original.records() {
        let TelemetryRecord::Deadlock(event) = &row.row().record else { return Err(invalid()) };
        if !event.is_valid() {
            return Err(invalid());
        }
        let mut graph = Deadlock::new(
            &original.source().source,
            Some(event.at.clone()),
            event.processes.clone(),
            event.resources.clone(),
        );
        graph.redact(snapshot.server.engine);
        deadlocks.push(graph);
    }
    deadlocks.sort_by(|a, b| b.at.cmp(&a.at));
    let state = if deadlocks.is_empty() { SourceState::Empty } else { SourceState::Collected };
    set_source_state(snapshot, source_id::DEADLOCKS, state, original.is_read_complete());
    snapshot.deadlocks = Some(DeadlockReport {
        scope: Some(MeasurementScope::Database),
        deadlocks,
        source: original.source().source.clone(),
        since_hours,
        recent_only: !events.history_complete,
        ..Default::default()
    });
    Ok(())
}

fn apply_mssql_cleanup_facts(observations: &[OperationalObservation], snapshot: &mut Snapshot) {
    if snapshot.server.engine != culprit_core::Engine::Mssql {
        return;
    }
    let version_store_bytes = observations.iter().find_map(|row| match &row.fact {
        OperationalFact::Counter(sample)
            if row.source == source_id::VERSION_STORE && sample.unit == CounterUnit::Kibibytes =>
        {
            sample.value.integer().map(|kib| (kib as f64) * 1024.0)
        }
        _ => None,
    });
    let oldest = observations.iter().find_map(|row| match &row.fact {
        OperationalFact::MssqlCleanupHorizon(horizon) => {
            let candidate = horizon.oldest_confirmed.as_ref()?;
            let seconds = candidate.age_seconds.as_deref()?.parse::<u64>().ok()? as f64;
            let wrote = candidate
                .log_record_count
                .as_deref()
                .and_then(|count| count.parse::<u64>().ok())
                .is_some_and(|count| count > 0);
            let snapshot_reader = candidate.row_version_participant || candidate.snapshot_isolation == Some(true);
            Some(OpenTransaction {
                catalog_only: false,
                session: candidate.session.id.clone(),
                seconds,
                program: candidate.program.clone(),
                activity: if snapshot_reader && !wrote {
                    TransactionActivity::Snapshot
                } else {
                    TransactionActivity::Reported
                },
                state: candidate.native_status.clone(),
                held_bytes: version_store_bytes,
                holder: CleanupHolder::Session,
                xid_age: None,
                wrote: Some(wrote),
                database: Some(snapshot.server.database.clone()),
            })
        }
        _ => None,
    });
    if oldest.is_some() {
        let pressure = snapshot.pressure.get_or_insert_with(Pressure::default);
        pressure.oldest_transaction = oldest;
    }
}

fn set_source_state(snapshot: &mut Snapshot, name: &str, state: SourceState, complete: bool) {
    if let Some(outcome) = snapshot.sources.iter_mut().find(|entry| entry.source == name) {
        outcome.state = if !complete && matches!(state, SourceState::Collected | SourceState::Empty) {
            SourceState::Partial
        } else {
            state
        };
        if matches!(outcome.state, SourceState::Collected | SourceState::Empty) {
            outcome.reason = None;
        }
    }
}
