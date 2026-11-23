//! Merge prepared disjoint populations; the original row decoder, metric conversion and pipeline stay single-owned.
use super::*;
use culprit_core::{PeriodStats, PlanVariant, QueryStat, Spread, WaitStat};
mod native;

struct QueryParts {
    input: CollectedQueryInput,
    queries: Vec<QueryStat>,
}
pub(super) fn prepare(
    prepared: impl IntoIterator<Item = PreparedPopulation>,
    originals: &[&dyn OriginalProjection],
    observed: &PublishedResourceWindow,
) -> Result<PreparedPopulation> {
    let mut prepared = prepared.into_iter();
    let first = prepared.next().ok_or_else(invalid)?;
    let population = first.population.clone();
    let source = originals.first().ok_or_else(invalid)?.source();
    let native = native::NativeInputs::new(originals, &population)?;
    let mut queries = BTreeMap::<String, QueryParts>::new();
    let mut totals = None;
    let mut totals_complete = true;
    let mut workload = None;
    let mut workload_complete = true;
    let mut partial = false;
    let mut limitations = Vec::new();
    for prepared in std::iter::once(first).chain(prepared) {
        if prepared.population != population {
            return Err(invalid());
        }
        partial |= prepared.partial;
        for limitation in prepared.limitations {
            admit_limit(&mut limitations, limitation);
        }
        let known_empty = prepared.totals.as_ref().is_some_and(|totals| totals.executions == 0);
        workload_complete &= prepared.workload.is_some() || known_empty;
        match prepared.totals {
            Some(next) => match &mut totals {
                Some(total) => groups::CapturedTotals::add(total, next)?,
                None => totals = Some(next),
            },
            None => totals_complete = false,
        }
        let mut inputs =
            prepared.query_inputs.into_iter().map(|input| (input.query_id.clone(), input)).collect::<BTreeMap<_, _>>();
        if let Some(mut next) = prepared.workload {
            for mut query in std::mem::take(&mut next.queries) {
                let mut input = inputs.remove(&query.id).ok_or_else(invalid)?;
                let key = query_key(source, &population, &input)?;
                query.id = key.clone();
                input.query_id = key.clone();
                if let Some(group) = queries.get_mut(&key) {
                    if group.input.references.query != input.references.query {
                        return Err(invalid());
                    }
                    if group.input.references.plan != input.references.plan {
                        group.input.references.plan = None;
                    }
                    group.input.measurements.extend(input.measurements);
                    group.queries.push(query);
                } else {
                    queries.insert(key, QueryParts { input, queries: vec![query] });
                }
            }
            workload.get_or_insert(next);
        }
    }
    if !totals_complete {
        totals = None;
    }
    let mut projected = Vec::new();
    let mut inputs = Vec::new();
    for (id, group) in queries {
        if let Some(query) = query(&id, &group, &native, source, &mut limitations)? {
            projected.push(query);
        } else {
            partial = true;
        }
        inputs.push(group.input);
    }
    if !workload_complete {
        workload = None;
    }
    if let Some(total) = totals.as_ref() {
        if total.executions > u128::from(groups::MAX_EXACT_COUNT) || total.time_ms.is_none() {
            workload = None;
        }
    } else {
        workload = None;
    }
    if let Some(workload) = &mut workload {
        let total = totals.as_ref().ok_or_else(invalid)?;
        workload.queries = projected;
        workload.total_executions = total.executions as f64;
        workload.total_time_ms = total.time_ms.ok_or_else(invalid)?;
        workload.total_cpu_ms = total.cpu_ms;
        workload.query_count = total.queries.len() as f64;
        workload.window_start = Some(observed.from.clone());
        workload.window_end = Some(observed.until.clone());
        workload.window_hours = validation::hours(&observed.from, &observed.until)?;
    }
    if workload.is_none() && totals.as_ref().is_none_or(|total| total.executions > 0) {
        partial = true;
        admit_limit(&mut limitations, limit(None, CollectedAnalysisLimit::RuntimeDenominatorUnavailable));
    }
    Ok(PreparedPopulation { population, totals, query_inputs: inputs, workload, partial, limitations })
}
fn query_key(
    source: &CollectionDescription,
    population: &CollectedQueryPopulation,
    input: &CollectedQueryInput,
) -> Result<String> {
    let first = input.measurements.first().ok_or_else(invalid)?;
    if input.measurements.iter().any(|key| key.cohort != first.cohort) {
        return Err(invalid());
    }
    if first.cohort.is_none() && input.measurements.iter().any(|key| key.native.qualifiers != first.native.qualifiers) {
        return Err(invalid());
    }
    native_query_key(source, population, &input.references, first.cohort.as_ref(), &first.native.qualifiers)
}
fn native_query_key(
    source: &CollectionDescription,
    population: &CollectedQueryPopulation,
    references: &StatementReferences,
    cohort: Option<&NativeIdentity>,
    qualifiers: &BTreeMap<String, String>,
) -> Result<String> {
    match cohort {
        Some(cohort) => source.scoped_id("collected-window-cohort-v1", &(population, &references.query, cohort)),
        None => source.scoped_id("collected-window-query-v1", &(population, references, qualifiers)),
    }
}

fn query(
    id: &str,
    group: &QueryParts,
    native: &native::NativeInputs<'_>,
    source: &CollectionDescription,
    limitations: &mut Vec<CollectedAnalysisLimitation>,
) -> Result<Option<QueryStat>> {
    let Some(first) = group.queries.first() else { return Err(invalid()) };
    if group.queries.iter().any(|query| {
        query.native_identity != first.native_identity
            || query.text != first.text
            || query.variant_family != first.variant_family
    }) {
        return Err(invalid());
    }
    let Some(stats) = stats(group.queries.iter().map(|query| {
        (
            query.executions,
            query.total_time_ms,
            query.spread.as_ref().and_then(|spread| spread.min_ms),
            query.max_time_ms,
        )
    })) else {
        admit_limit(limitations, limit(Some(id.into()), CollectedAnalysisLimit::CounterPrecision));
        return Ok(None);
    };
    let mut query = first.clone();
    query.id = id.into();
    query.executions = stats.executions;
    query.total_time_ms = stats.total_ms;
    query.mean_time_ms = stats.mean_ms;
    query.max_time_ms = stats.max_ms;
    query.spread = Some(Spread { min_ms: stats.min_ms, ..Default::default() });
    query.cpu_time_ms = optional_sum(group.queries.iter().map(|query| query.cpu_time_ms));
    query.rows = optional_sum(group.queries.iter().map(|query| query.rows));
    query.first_seen = endpoint(group.queries.iter().map(|query| query.first_seen.as_deref()), true)?;
    query.last_seen = endpoint(group.queries.iter().map(|query| query.last_seen.as_deref()), false)?;
    if group.queries.len() > 1 {
        query.stddev_time_ms = None;
        admit_limit(limitations, limit(Some(id.into()), CollectedAnalysisLimit::MetricNotProjected));
    }
    query.temporary_work = runtime_temporary_space(native.runtime(id).iter().copied(), &source.source);
    query.plans = plans(&group.queries, limitations)?;
    query.plans.sort_by(|a, b| b.total_time_ms.total_cmp(&a.total_time_ms).then_with(|| a.id.cmp(&b.id)));
    let selected = query.plans.first();
    query.plan_id = selected.map(|plan| plan.id.clone());
    query.native_plan_identity = selected.and_then(|plan| plan.native_identity.clone());
    let definition = selected.and_then(|selected| native.plan(&selected.id));
    query.plan = groups::plan(definition, source.target.engine(), limitations)?;
    if let Some(history) = &mut query.plan_history {
        history.captured_plan_id = query.plan_id.clone();
        history.captured_plan = None;
    }
    merge_waits(&mut query, &group.queries)?;
    Ok(Some(query))
}
fn stats(parts: impl IntoIterator<Item = (f64, f64, Option<f64>, Option<f64>)>) -> Option<PeriodStats> {
    let mut total = PeriodStats::default();
    let mut count = 0u64;
    let mut complete_min = true;
    let mut complete_max = true;
    for (executions, time, min, max) in parts {
        if !executions.is_finite()
            || executions < 0.0
            || executions.fract() != 0.0
            || executions > groups::MAX_EXACT_COUNT as f64
            || !time.is_finite()
            || time < 0.0
        {
            return None;
        }
        count = count.checked_add(executions as u64).filter(|count| *count <= groups::MAX_EXACT_COUNT)?;
        total.add(&PeriodStats {
            executions,
            total_ms: time,
            mean_ms: if executions > 0.0 { time / executions } else { 0.0 },
            min_ms: min,
            max_ms: max,
        });
        complete_min &= min.is_some();
        complete_max &= max.is_some();
        if !total.total_ms.is_finite() {
            return None;
        }
    }
    total.executions = count as f64;
    if !complete_min {
        total.min_ms = None;
    }
    if !complete_max {
        total.max_ms = None;
    }
    Some(total)
}
fn plans(queries: &[QueryStat], limitations: &mut Vec<CollectedAnalysisLimitation>) -> Result<Vec<PlanVariant>> {
    let mut selected = BTreeMap::<&str, Vec<&PlanVariant>>::new();
    for query in queries {
        for plan in &query.plans {
            selected.entry(&plan.id).or_default().push(plan);
        }
    }
    let mut output = Vec::new();
    for (id, parts) in selected {
        let first = parts.first().ok_or_else(invalid)?;
        if parts.iter().any(|plan| plan.native_identity != first.native_identity) {
            return Err(invalid());
        }
        let Some(total) =
            stats(parts.iter().map(|plan| (plan.executions, plan.total_time_ms, plan.min_time_ms, plan.max_time_ms)))
        else {
            admit_limit(limitations, limit(Some(id.into()), CollectedAnalysisLimit::CounterPrecision));
            continue;
        };
        let mut plan = (**first).clone();
        plan.executions = total.executions;
        plan.total_time_ms = total.total_ms;
        plan.mean_time_ms = total.mean_ms;
        plan.min_time_ms = total.min_ms;
        plan.max_time_ms = total.max_ms;
        plan.cpu_time_ms = optional_sum(parts.iter().map(|plan| plan.cpu_time_ms));
        plan.logical_reads = optional_sum(parts.iter().map(|plan| plan.logical_reads));
        plan.first_seen = endpoint(parts.iter().map(|plan| plan.first_seen.as_deref()), true)?;
        plan.last_seen = endpoint(parts.iter().map(|plan| plan.last_seen.as_deref()), false)?;
        if parts.len() > 1 {
            plan.stddev_time_ms = None;
            plan.page_server_reads = None;
            admit_limit(limitations, limit(Some(id.into()), CollectedAnalysisLimit::MetricNotProjected));
        }
        if parts.iter().any(|part| part.compiled != first.compiled) {
            plan.compiled = None;
        }
        if parts.iter().any(|part| part.is_forced != first.is_forced) {
            plan.is_forced = None;
        }
        output.push(plan);
    }
    Ok(output)
}
fn merge_waits(query: &mut QueryStat, parts: &[QueryStat]) -> Result<()> {
    let mut waits = BTreeMap::<&'static str, WaitStat>::new();
    for part in parts {
        for wait in &part.waits {
            let key = culprit_core::waits::known_category_key(&wait.category).ok_or_else(invalid)?;
            if let Some(existing) = waits.get_mut(key) {
                existing.wait_ms = finite(existing.wait_ms, wait.wait_ms).ok_or_else(invalid)?;
            } else {
                waits.insert(key, wait.clone());
            }
        }
    }
    query.waits = waits.into_values().collect();
    query.waits.sort_by(|a, b| b.wait_ms.total_cmp(&a.wait_ms).then_with(|| a.category.cmp(&b.category)));
    if let Some(coverage) = &mut query.waits_coverage {
        coverage.complete &= parts.iter().all(|part| {
            part.waits_coverage.as_ref().is_some_and(|next| next.complete && next.classes == coverage.classes)
        });
    }
    Ok(())
}
fn optional_sum(values: impl IntoIterator<Item = Option<f64>>) -> Option<f64> {
    values.into_iter().try_fold(0.0, |sum, value| finite(sum, value?))
}
fn finite(a: f64, b: f64) -> Option<f64> {
    let sum = a + b;
    (a.is_finite() && b.is_finite() && sum.is_finite()).then_some(sum)
}
fn endpoint<'a>(values: impl IntoIterator<Item = Option<&'a str>>, minimum: bool) -> Result<Option<String>> {
    let mut selected = None;
    for value in values {
        let Some(value) = value else { return Ok(None) };
        let at = validation::time(value)?;
        if selected.as_ref().is_none_or(|(known, _)| if minimum { at < *known } else { at > *known }) {
            selected = Some((at, value.to_owned()));
        }
    }
    Ok(selected.map(|(_, value)| value))
}
fn admit_limit(limits: &mut Vec<CollectedAnalysisLimitation>, next: CollectedAnalysisLimitation) {
    if !limits.iter().any(|known| known.record_id == next.record_id && known.reason == next.reason) {
        limits.push(next);
    }
}
