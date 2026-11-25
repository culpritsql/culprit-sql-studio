//! Database-scoped Operational, Cleanup, and PlanCacheSummary captures feed pure checks without a connection.
use super::*;
use culprit_core::{
    CleanupHolder, OpenTransaction, PlanCacheUse, Pressure, Report, TransactionActivity, operations::SpaceUsage,
    telemetry::*,
};
use std::collections::BTreeMap;

pub(crate) struct RetainedOperationalAnalysis {
    pub report: Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
}

pub(crate) fn supports_operational(original: &dyn OriginalProjection) -> bool {
    original.source().target.database().is_some()
        && original.source().observation.snapshot().is_some_and(|snapshot| {
            matches!(
                snapshot.dataset,
                SnapshotDataset::Operational | SnapshotDataset::PlanCacheSummary | SnapshotDataset::Cleanup
            )
        })
}

pub(crate) fn analyze_operational_original(
    original: &dyn OriginalProjection,
    catalogs: &catalogs::CatalogContext,
    pipeline: &culprit_checks::Pipeline,
) -> Result<RetainedOperationalAnalysis> {
    let source = original.source();
    let database = source.target.database().ok_or_else(invalid)?.clone();
    let header = source.observation.snapshot().ok_or_else(invalid)?;
    if !supports_operational(original)
        || !source.partition_is_consistent()
        || !header.is_valid()
        || !original.original().is_valid()
        || original.records().len() > header.records as usize
    {
        return Err(invalid());
    }
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
    snapshot.taken_at = header.observed_until.clone();
    catalogs.apply(&mut snapshot);
    if catalogs.settings_complete() {
        limitations.retain(|limitation| limitation.reason != CollectedAnalysisLimit::MissingSettings);
    }

    match header.dataset {
        SnapshotDataset::Operational => populate_operational(original, &mut snapshot)?,
        SnapshotDataset::PlanCacheSummary => populate_plan_cache(original, &mut snapshot)?,
        SnapshotDataset::Cleanup => populate_cleanup(original, &mut snapshot)?,
        _ => return Err(invalid()),
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
