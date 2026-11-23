//! Guarded PostgreSQL statement-counter points join same-owner SQL definitions by exact native entry identity.
//! Counter-only captures and unqualified scopes remain explicitly limited without inventing intervals or origins.
use super::*;
use culprit_core::{
    DatabaseIdentity, Engine, QueryStat, ResetMarker, WaitStat, Workload, WorkloadKind, t, telemetry::*,
};
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum StatementCounterOutcome {
    CounterOnly { limitations: Vec<CollectedAnalysisLimit> },
    Evaluated(Box<EvaluatedStatementCounters>),
}

pub(crate) struct EvaluatedStatementCounters {
    pub report: culprit_core::Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
    pub query_evidence: BTreeMap<String, StatementFindingEvidence>,
}

#[derive(Clone)]
pub(crate) struct StatementFindingEvidence {
    pub identity: NativeQueryIdentity,
    pub definition_ref: DiagnosticEvidenceRef,
    pub sql: DefinitionPayload,
    pub inputs: Vec<DiagnosticEvidenceRef>,
}

pub(crate) fn analyze_statement_counters(
    primary: &dyn OriginalProjection,
    originals: &[&dyn OriginalProjection],
    pipeline: &culprit_checks::Pipeline,
    catalogs: &catalogs::CatalogContext,
) -> Result<StatementCounterOutcome> {
    let (primary_snapshot, primary_rows) = counter_snapshot(primary)?;
    culprit_checks::statement_work::explain(&primary_snapshot, primary.is_read_complete())?;
    let counter_only = counter_only_limitations(&primary_snapshot, primary.is_read_complete())?;

    let CollectionTarget::Database(database) = &primary.source().target else {
        return Ok(StatementCounterOutcome::CounterOnly { limitations: counter_only });
    };
    let Some(primary_ctx) = primary.source().observation.snapshot_acquisition_context() else {
        return Ok(StatementCounterOutcome::CounterOnly { limitations: counter_only });
    };
    if database.engine != Engine::Postgres
        || !primary_ctx.has_qualified_local_owner()
        || primary_ctx.database_writable.is_none()
        || !primary.is_read_complete()
        || !primary_snapshot.coverage.complete_scope
        || !all_entries_guarded(&primary_rows)
    {
        return Ok(StatementCounterOutcome::CounterOnly { limitations: counter_only });
    }
    let Some((defs, defs_map)) = select_definitions(primary, primary_ctx, originals)? else {
        return Ok(StatementCounterOutcome::CounterOnly { limitations: counter_only });
    };
    let delta = select_delta_window(primary, primary_ctx, &primary_rows, &defs_map, originals)?;
    evaluate_population(
        primary,
        primary_ctx,
        database.clone(),
        &primary_rows,
        defs,
        &defs_map,
        delta,
        pipeline,
        catalogs,
    )
    .map(|evaluated| StatementCounterOutcome::Evaluated(Box::new(evaluated)))
}

struct CounterRow<'a> {
    position: usize,
    record_id: Option<String>,
    sample: &'a StatementCounterSample,
}

fn counter_snapshot(original: &dyn OriginalProjection) -> Result<(StatementCounterSnapshot, Vec<CounterRow<'_>>)> {
    if !original
        .source()
        .observation
        .snapshot()
        .is_some_and(|header| header.dataset == SnapshotDataset::StatementCounters)
        || !original.original().is_valid()
    {
        return Err(invalid());
    }
    let mut observations = Vec::with_capacity(original.records().len());
    let mut rows = Vec::with_capacity(original.records().len());
    for (position, row) in original.records().iter().enumerate() {
        let exported = row.row();
        let TelemetryRecord::Operational(observation) = &exported.record else { return Err(invalid()) };
        let OperationalFact::StatementCounters(sample) = &observation.fact else { return Err(invalid()) };
        observations.push(observation.clone());
        rows.push(CounterRow { position, record_id: exported.record_id.clone(), sample });
    }
    let snapshot = StatementCounterSnapshot::from_source(original.source(), observations)?;
    if !snapshot.is_valid_read(original.is_read_complete()) {
        return Err(invalid());
    }
    Ok((snapshot, rows))
}

fn counter_only_limitations(
    snapshot: &StatementCounterSnapshot,
    read_complete: bool,
) -> Result<Vec<CollectedAnalysisLimit>> {
    let mut limitations = vec![
        CollectedAnalysisLimit::MissingQueryDefinition,
        CollectedAnalysisLimit::UnknownStatementOrigin,
        CollectedAnalysisLimit::RuntimeDenominatorUnavailable,
        CollectedAnalysisLimit::ExecutionPopulationUnknown,
    ];
    if !read_complete || !snapshot.coverage.complete_scope {
        limitations.push(CollectedAnalysisLimit::RuntimeBounds);
    }
    for row in &snapshot.observations {
        let OperationalFact::StatementCounters(sample) = &row.fact else { return Err(invalid()) };
        if StatementQueryMetrics::from_sample(sample)?.is_none() {
            let reason = precision_or_duration_reason(sample);
            if !limitations.contains(&reason) {
                limitations.push(reason);
            }
        }
    }
    Ok(limitations)
}

fn precision_or_duration_reason(sample: &StatementCounterSample) -> CollectedAnalysisLimit {
    if sample.totals.iter().any(|counter| {
        matches!(counter.key, RuntimeMetricKey::Executions | RuntimeMetricKey::Duration)
            && counter.total.exact_real().is_none()
    }) {
        CollectedAnalysisLimit::CounterPrecision
    } else {
        CollectedAnalysisLimit::DurationUnavailable
    }
}

fn all_entries_guarded(rows: &[CounterRow<'_>]) -> bool {
    rows.iter().all(|row| {
        let sample = row.sample;
        let Some(raw_since) = sample.identity.qualifiers.get("stats_since") else { return false };
        let Ok(since) = canonical_time(raw_since) else { return false };
        let Some(raw_started) = sample.identity.qualifiers.get("postmaster_started_at") else { return false };
        let Ok(started) = canonical_time(raw_started) else { return false };
        sample.member.is_none()
            && sample.scope == culprit_core::MeasurementScope::Database
            && raw_since == &since
            && raw_started == &started
            && sample.reset == ResetMarker::At(since.clone())
            && ObservationBounds::new(&since, &sample.observed_at).is_some()
            && sample.identity.qualifiers.get("dbid").is_some_and(|db| db.parse::<u32>().is_ok_and(|id| id > 0))
            && sample.identity.qualifiers.get("userid").is_some_and(|u| u.parse::<u32>().is_ok_and(|id| id > 0))
            && matches!(sample.identity.qualifiers.get("toplevel").map(String::as_str), Some("true" | "false"))
    })
}

type EntryKey = (String, BTreeMap<String, String>);

fn entry_key(identity: &NativeQueryIdentity) -> EntryKey {
    (identity.query_id.clone(), identity.qualifiers.clone())
}

type DefinitionMap<'a> = BTreeMap<EntryKey, (usize, &'a QueryDefinition)>;

fn select_definitions<'a>(
    primary: &dyn OriginalProjection,
    primary_ctx: &NativeReadContext,
    originals: &[&'a dyn OriginalProjection],
) -> Result<Option<(&'a dyn OriginalProjection, DefinitionMap<'a>)>> {
    let primary_until = validation::time(&primary_ctx.observed_until)?;
    let mut before: Option<(chrono::DateTime<chrono::FixedOffset>, &'a dyn OriginalProjection, bool)> = None;
    let mut after: Option<(chrono::DateTime<chrono::FixedOffset>, &'a dyn OriginalProjection, bool)> = None;
    for &candidate in originals.iter().skip(1) {
        if candidate.capture() == primary.capture() {
            continue;
        }
        let Some(header) = candidate.source().observation.snapshot() else { continue };
        if header.dataset != SnapshotDataset::StatementDefinitions {
            continue;
        }
        let Some(ctx) = candidate.source().observation.snapshot_acquisition_context() else { continue };
        if !ctx.same_local_database_owner(primary_ctx)
            || ctx.database_writable != primary_ctx.database_writable
            || !candidate.is_read_complete()
            || header.sources.len() != 1
            || !header.sources[0].complete_scope
        {
            continue;
        }
        let until = validation::time(&ctx.observed_until)?;
        if until <= primary_until {
            match &mut before {
                Some((best, _, tied)) if until == *best => *tied = true,
                Some((best, _, _)) if until > *best => before = Some((until, candidate, false)),
                Some(_) => (),
                None => before = Some((until, candidate, false)),
            }
        } else {
            let from = validation::time(&ctx.observed_from)?;
            match &mut after {
                Some((best, _, tied)) if from == *best => *tied = true,
                Some((best, _, _)) if from < *best => after = Some((from, candidate, false)),
                Some(_) => (),
                None => after = Some((from, candidate, false)),
            }
        }
    }
    let selected = match before {
        Some((_, _, true)) => return Ok(None),
        Some((_, selected, false)) => selected,
        None => match after {
            Some((_, selected, false)) => selected,
            _ => return Ok(None),
        },
    };
    let mut map = BTreeMap::new();
    for (position, row) in selected.records().iter().enumerate() {
        let TelemetryRecord::ObjectDefinition(record) = &row.row().record else { return Err(invalid()) };
        let ObjectDefinitionFact::Statement(definition) = &record.fact else { return Err(invalid()) };
        if !definition.is_valid() || definition.id != definition.identity.definition_id(selected.source())? {
            return Err(invalid());
        }
        if map.insert(entry_key(&definition.identity), (position, definition.as_ref())).is_some() {
            return Err(invalid());
        }
    }
    Ok(Some((selected, map)))
}

struct DeltaWindow {
    from: String,
    until: String,
    hours: f64,
    samples: BTreeMap<EntryKey, (StatementCounterSample, Option<DiagnosticEvidenceRef>)>,
}

fn select_delta_window<'a>(
    primary: &dyn OriginalProjection,
    primary_ctx: &NativeReadContext,
    primary_rows: &[CounterRow<'_>],
    defs_map: &DefinitionMap<'a>,
    originals: &[&'a dyn OriginalProjection],
) -> Result<Option<DeltaWindow>> {
    let primary_from = validation::time(&primary_ctx.observed_from)?;
    let mut best: Option<(chrono::DateTime<chrono::FixedOffset>, &'a dyn OriginalProjection, bool)> = None;
    for &candidate in originals.iter().skip(1) {
        if candidate.capture() == primary.capture() {
            continue;
        }
        let Some(header) = candidate.source().observation.snapshot() else { continue };
        if header.dataset != SnapshotDataset::StatementCounters {
            continue;
        }
        let Some(ctx) = candidate.source().observation.snapshot_acquisition_context() else { continue };
        if !ctx.same_local_database_owner(primary_ctx)
            || ctx.database_writable != primary_ctx.database_writable
            || !candidate.is_read_complete()
            || header.sources.len() != 1
            || !header.sources[0].complete_scope
        {
            continue;
        }
        let Ok(until) = validation::time(&ctx.observed_until) else { continue };
        if until > primary_from {
            continue;
        }
        match &mut best {
            Some((current, _, tied)) if until == *current => *tied = true,
            Some((current, _, _)) if until > *current => best = Some((until, candidate, false)),
            Some(_) => (),
            None => best = Some((until, candidate, false)),
        }
    }
    let Some((_, earlier, false)) = best else { return Ok(None) };
    let Some(earlier_ctx) = earlier.source().observation.snapshot_acquisition_context() else { return Ok(None) };
    let Ok(hours) = validation::hours(&earlier_ctx.observed_from, &primary_ctx.observed_until) else {
        return Ok(None);
    };
    let (earlier_snapshot, earlier_rows) = counter_snapshot(earlier)?;
    if !earlier_snapshot.coverage.complete_scope || !all_entries_guarded(&earlier_rows) {
        return Ok(None);
    }
    let mut earlier_top = BTreeMap::new();
    for row in &earlier_rows {
        if is_top_level(row.sample) {
            earlier_top.insert(entry_key(&row.sample.identity), (row.position, row.sample));
        }
    }
    let primary_top: BTreeMap<EntryKey, &StatementCounterSample> = primary_rows
        .iter()
        .filter(|row| is_top_level(row.sample))
        .map(|row| (entry_key(&row.sample.identity), row.sample))
        .collect();

    // Every non-excluded top-level entry from the earlier point must survive with its identical reset marker;
    // otherwise eviction or reset lost an unknown share of the interval denominator.
    for (id, (_, sample)) in &earlier_top {
        let excluded = defs_map.get(id).is_some_and(|(_, def)| {
            matches!(
                def.origin,
                StatementOrigin::Culprit | StatementOrigin::CulpritMonitoring | StatementOrigin::EngineInternal
            )
        });
        if excluded {
            continue;
        }
        if !primary_top.contains_key(id) && non_zero_executions(sample) {
            return Ok(None);
        }
    }
    let earlier_from = validation::time(&earlier_ctx.observed_from)?;
    let mut samples = BTreeMap::new();
    for row in primary_rows.iter().filter(|row| is_top_level(row.sample)) {
        let key = entry_key(&row.sample.identity);
        if let Some(&(earlier_pos, before)) = earlier_top.get(&key) {
            let Some(differenced) = difference_sample(before, row.sample) else {
                return Ok(None);
            };
            samples.insert(key, (differenced, Some(earlier.reference(earlier_pos)?)));
        } else {
            let Some(since) = row.sample.identity.qualifiers.get("stats_since") else { return Ok(None) };
            if validation::time(since)? < earlier_from {
                return Ok(None);
            }
            samples.insert(key, (row.sample.clone(), None));
        }
    }
    Ok(Some(DeltaWindow {
        from: canonical_time(&earlier_ctx.observed_from)?,
        until: canonical_time(&primary_ctx.observed_until)?,
        hours,
        samples,
    }))
}

fn non_zero_executions(sample: &StatementCounterSample) -> bool {
    sample
        .totals
        .iter()
        .find(|counter| counter.key == RuntimeMetricKey::Executions)
        .and_then(|counter| match &counter.total {
            MeasurementValue::Integer(v) => v.parse::<u128>().ok(),
            MeasurementValue::Real(_) => None,
        })
        .is_none_or(|calls| calls > 0)
}

fn difference_sample(
    before: &StatementCounterSample,
    after: &StatementCounterSample,
) -> Option<StatementCounterSample> {
    if before.identity != after.identity || before.reset != after.reset {
        return None;
    }
    let before_map: BTreeMap<&RuntimeMetricKey, &StatementCounter> =
        before.totals.iter().map(|counter| (&counter.key, counter)).collect();
    let mut totals = Vec::with_capacity(after.totals.len());
    for current in &after.totals {
        let Some(prev) = before_map.get(&current.key) else {
            totals.push(current.clone());
            continue;
        };
        if prev.unit != current.unit {
            return None;
        }
        let total = match (&prev.total, &current.total) {
            (MeasurementValue::Integer(a), MeasurementValue::Integer(b)) => {
                let a = a.parse::<u128>().ok()?;
                let b = b.parse::<u128>().ok()?;
                MeasurementValue::Integer(b.checked_sub(a)?.to_string())
            }
            (MeasurementValue::Real(a), MeasurementValue::Real(b)) => {
                let diff = *b - *a;
                if !diff.is_finite() || diff < 0.0 {
                    return None;
                }
                MeasurementValue::Real(diff)
            }
            _ => return None,
        };
        totals.push(StatementCounter { key: current.key.clone(), unit: current.unit, total });
    }
    let distributions = if !non_zero_executions(before) { after.distributions.clone() } else { vec![] };
    let sample = StatementCounterSample {
        member: after.member.clone(),
        scope: after.scope,
        identity: after.identity.clone(),
        observed_at: after.observed_at.clone(),
        reset: after.reset.clone(),
        totals,
        distributions,
    };
    sample.is_valid().then_some(sample)
}

fn is_top_level(sample: &StatementCounterSample) -> bool {
    sample.identity.qualifiers.get("toplevel").map(String::as_str) == Some("true")
}

#[allow(clippy::too_many_arguments)]
fn evaluate_population<'a>(
    primary: &dyn OriginalProjection,
    primary_ctx: &NativeReadContext,
    database: DatabaseIdentity,
    primary_rows: &[CounterRow<'_>],
    defs: &dyn OriginalProjection,
    defs_map: &DefinitionMap<'a>,
    delta: Option<DeltaWindow>,
    pipeline: &culprit_checks::Pipeline,
    catalogs: &catalogs::CatalogContext,
) -> Result<EvaluatedStatementCounters> {
    let mut limitations = vec![
        limit(None, CollectedAnalysisLimit::MissingCurrentServerMetadata),
        limit(None, CollectedAnalysisLimit::MissingSchema),
        limit(None, CollectedAnalysisLimit::MissingSettings),
        limit(None, CollectedAnalysisLimit::MissingResourceMeasurements),
    ];
    let mut partial = false;
    let mut total_executions = 0u128;
    let mut total_time_ms = Some(0.0f64);
    let mut denominator_ok = true;
    let mut active_identities = 0usize;
    let mut queries = Vec::new();
    let mut query_evidence = BTreeMap::new();

    for row in primary_rows.iter().filter(|row| is_top_level(row.sample)) {
        let key = entry_key(&row.sample.identity);
        let joined = defs_map.get(&key);
        if joined.is_some_and(|(_, def)| {
            matches!(
                def.origin,
                StatementOrigin::Culprit | StatementOrigin::CulpritMonitoring | StatementOrigin::EngineInternal
            )
        }) {
            continue;
        }
        let (effective_sample, earlier_ref) = match &delta {
            Some(window) => {
                let (sample, earlier_ref) = window.samples.get(&key).ok_or_else(invalid)?;
                (sample, earlier_ref.clone())
            }
            None => (row.sample, None),
        };
        let Some(metrics) = StatementQueryMetrics::from_sample(effective_sample)? else {
            limitations.push(limit(row.record_id.clone(), precision_or_duration_reason(effective_sample)));
            denominator_ok = false;
            partial = true;
            record_definition_gap(&mut limitations, row.record_id.clone(), joined.map(|(_, def)| *def));
            continue;
        };
        let Some(calls) = exact_calls(effective_sample) else {
            limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::CounterPrecision));
            denominator_ok = false;
            partial = true;
            continue;
        };
        if let Some(sum) = total_executions.checked_add(u128::from(calls)) {
            total_executions = sum;
        } else {
            denominator_ok = false;
        }
        total_time_ms = total_time_ms.and_then(|acc| {
            let next = acc + metrics.total_time_ms;
            next.is_finite().then_some(next)
        });
        if calls > 0 {
            active_identities += 1;
        }
        let Some((def_pos, def)) = joined else {
            limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::MissingQueryDefinition));
            limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::UnknownStatementOrigin));
            partial = true;
            continue;
        };
        if def.origin != StatementOrigin::Application {
            limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::UnknownStatementOrigin));
            if !def.sql.is_available() {
                limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::QueryTextUnavailable));
            }
            partial = true;
            continue;
        }
        let DefinitionPayload::Available { text, .. } = &def.sql else {
            limitations.push(limit(row.record_id.clone(), CollectedAnalysisLimit::QueryTextUnavailable));
            partial = true;
            continue;
        };
        if calls == 0 {
            continue;
        }
        let id = primary.source().scoped_id("collected-statement-entry-v1", &effective_sample.identity)?;
        let query = build_query_stat(id.clone(), text.clone(), metrics, effective_sample)?;
        let definition_ref = defs.reference(*def_pos)?;
        let mut inputs = Vec::new();
        let mut seen = BTreeSet::new();
        for reference in
            [Some(primary.reference(row.position)?), Some(definition_ref.clone()), earlier_ref].into_iter().flatten()
        {
            if seen.insert(reference.key()?) {
                inputs.push(reference);
            }
        }
        query_evidence.insert(
            id,
            StatementFindingEvidence { identity: def.identity.clone(), definition_ref, sql: def.sql.clone(), inputs },
        );
        queries.push(query);
    }

    let workload = if queries.is_empty() {
        None
    } else if !denominator_ok
        || total_executions > u128::from(groups::MAX_EXACT_COUNT)
        || total_time_ms.is_none_or(|ms| ms <= 0.0)
    {
        limitations.push(limit(None, CollectedAnalysisLimit::RuntimeDenominatorUnavailable));
        None
    } else {
        let total_time_ms = total_time_ms.unwrap_or(0.0);
        let (windowed, window_start, window_end, window_hours) = match &delta {
            Some(window) => (true, Some(window.from.clone()), Some(window.until.clone()), window.hours),
            None => (false, None, Some(canonical_time(&primary_ctx.observed_until)?), 0.0),
        };
        Some(Workload {
            population: None,
            population_catalog: None,
            source: "pg_stat_statements".into(),
            kind: Some(WorkloadKind::StatementStats),
            windowed,
            log: None,
            window_start,
            window_end,
            window_hours,
            total_time_ms,
            total_cpu_ms: None,
            total_executions: total_executions as f64,
            query_count: active_identities as f64,
            queries,
            timeline: None,
            untracked_time_ms: None,
            hourly_time_ms: None,
            min_logged_ms: None,
            history_inputs: vec![],
            counter_set: None,
            source_label: None,
        })
    };

    let analysis_key = primary.original().key()?;
    let policy = AnalysisPolicy {
        analysis_key: &analysis_key,
        pipeline,
        origin: culprit_core::AnalysisOrigin::RetainedInput,
        catalogs,
    };
    let mut snapshot = super::snapshot(primary, database, workload, partial, &policy);
    catalogs.apply(&mut snapshot);
    if catalogs.settings_complete() {
        limitations.retain(|limitation| limitation.reason != CollectedAnalysisLimit::MissingSettings);
    }
    let report =
        run_retained_snapshot(snapshot, pipeline, culprit_core::AnalysisOrigin::RetainedInput, &mut limitations);
    Ok(EvaluatedStatementCounters { report, limitations, query_evidence })
}

fn record_definition_gap(
    limitations: &mut Vec<CollectedAnalysisLimitation>,
    record_id: Option<String>,
    def: Option<&QueryDefinition>,
) {
    match def {
        None => {
            limitations.push(limit(record_id.clone(), CollectedAnalysisLimit::MissingQueryDefinition));
            limitations.push(limit(record_id, CollectedAnalysisLimit::UnknownStatementOrigin));
        }
        Some(def) => {
            if def.origin == StatementOrigin::Unknown {
                limitations.push(limit(record_id.clone(), CollectedAnalysisLimit::UnknownStatementOrigin));
            }
            if !def.sql.is_available() {
                limitations.push(limit(record_id, CollectedAnalysisLimit::QueryTextUnavailable));
            }
        }
    }
}

fn exact_calls(sample: &StatementCounterSample) -> Option<u64> {
    let counter = sample.totals.iter().find(|counter| counter.key == RuntimeMetricKey::Executions)?;
    let MeasurementValue::Integer(value) = &counter.total else { return None };
    value.parse::<u64>().ok().filter(|calls| *calls <= groups::MAX_EXACT_COUNT)
}

fn build_query_stat(
    id: String,
    text: String,
    metrics: StatementQueryMetrics,
    sample: &StatementCounterSample,
) -> Result<QueryStat> {
    let mut query = metrics.into_query(id, text);
    query.native_identity = Some(sample.identity.clone());
    query.temporary_work = statement_temporary_work(sample, "pg_stat_statements")?.into_iter().next();
    let hits = page_counter(sample, "postgres.shared-block-hits");
    let reads = page_counter(sample, "postgres.shared-block-reads");
    if let Some(reads) = reads {
        query.physical_reads = Some(reads as f64);
        if let Some(hits) = hits
            && let Some(logical) = hits.checked_add(reads).filter(|sum| *sum <= groups::MAX_EXACT_COUNT)
        {
            query.logical_reads = Some(logical as f64);
        }
    }
    if let Some(wait_ms) = io_wait_ms(sample).filter(|ms| *ms > 0.0) {
        query.waits.push(WaitStat {
            mechanism: None,
            category: "IO".into(),
            wait_type: Some(t!("provider.postgres.wait.data_files")),
            wait_ms,
            tasks: None,
        });
    }
    Ok(query)
}

fn page_counter(sample: &StatementCounterSample, name: &str) -> Option<u64> {
    let key = RuntimeMetricKey::Native(name.into());
    let counter = sample.totals.iter().find(|counter| counter.key == key)?;
    if !matches!(counter.unit, RuntimeMetricUnit::Pages { .. }) {
        return None;
    }
    let MeasurementValue::Integer(value) = &counter.total else { return None };
    value.parse::<u64>().ok().filter(|pages| *pages <= groups::MAX_EXACT_COUNT)
}

fn io_wait_ms(sample: &StatementCounterSample) -> Option<f64> {
    let ms = |name: &str| -> Option<f64> {
        let key = RuntimeMetricKey::Native(name.into());
        let counter = sample.totals.iter().find(|counter| counter.key == key)?;
        counter.total.exact_real().and_then(|value| counter.unit.milliseconds(value))
    };
    let shared = match (ms("postgres.shared-io-read-time"), ms("postgres.shared-io-write-time")) {
        (None, None) => None,
        (read, write) => Some(read.unwrap_or(0.0) + write.unwrap_or(0.0)),
    };
    shared
        .or_else(|| match (ms("postgres.io-read-time"), ms("postgres.io-write-time")) {
            (None, None) => None,
            (read, write) => Some(read.unwrap_or(0.0) + write.unwrap_or(0.0)),
        })
        .filter(|total| total.is_finite())
}
