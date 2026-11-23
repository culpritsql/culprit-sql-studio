//! Whole compatible native intervals feed the existing comparison rules. Missing query populations are not zero.
use super::*;
use culprit_core::changes::{CompareOptions, Period, PeriodData, PeriodStats, PlanPeriods, QueryPeriods};

const COMPARISON_RULES_VERSION: &str = "native-interval-comparison-1";

/// Each original population was projected once. The SAME merge owner composes each comparison period and then
/// the whole selected section; no second pipeline, source, row decoder or cross-population scan is introduced.
pub(super) fn prepare(
    mut parts: Vec<Part<'_>>,
    originals: &[&dyn OriginalProjection],
    observed: &PublishedResourceWindow,
    check: &dyn Fn() -> Result<()>,
) -> Result<(PreparedPopulation, CollectedWindowComparison)> {
    let mut comparison = qualification(&parts)?;
    if !comparison.gaps.is_empty() {
        return Ok((merge::prepare(parts.into_iter().map(|part| part.prepared), originals, observed)?, comparison));
    }
    let periods = PeriodSelection::new(&parts, observed, &mut comparison.gaps)?;
    let recent_parts = parts.split_off(periods.recent);
    let baseline_parts = parts.split_off(periods.baseline);
    comparison.baseline_inputs = baseline_parts.iter().map(|part| part.input.original.clone()).collect();
    comparison.recent_inputs = recent_parts.iter().map(|part| part.input.original.clone()).collect();
    check()?;
    let before = merge_parts(baseline_parts, &periods.before)?;
    let after = merge_parts(recent_parts, &periods.after)?;
    comparison.changes =
        changes(&before, &after, &periods, originals[0].source().target.engine(), &mut comparison.gaps)?;
    let mut populations = Vec::new();
    if !parts.is_empty() {
        let prior = PublishedResourceWindow { from: observed.from.clone(), until: periods.before.from.clone() };
        populations.push(merge_parts(parts, &prior)?);
    }
    populations.extend([before, after]);
    check()?;
    Ok((merge::prepare(populations, originals, observed)?, comparison))
}

fn qualification(parts: &[Part<'_>]) -> Result<CollectedWindowComparison> {
    let first = parts.first().ok_or_else(invalid)?;
    let mut output = CollectedWindowComparison {
        rules_version: COMPARISON_RULES_VERSION.into(),
        baseline_inputs: vec![],
        recent_inputs: vec![],
        changes: None,
        gaps: vec![],
    };
    if !selection::qualified(first.original) {
        output.gaps.push(CollectedComparisonGap::UnqualifiedScope);
    }
    if first.prepared.population == CollectedQueryPopulation::Unknown {
        output.gaps.push(CollectedComparisonGap::UnknownPopulation);
    }
    if parts.iter().any(|part| !part.input.read_complete) {
        output.gaps.push(CollectedComparisonGap::IncompleteOriginal);
    }
    if parts.len() < 2 {
        output.gaps.push(CollectedComparisonGap::TooFewIntervals);
    }
    Ok(output)
}

struct PeriodSelection {
    baseline: usize,
    recent: usize,
    before: PublishedResourceWindow,
    after: PublishedResourceWindow,
}
impl PeriodSelection {
    fn new(
        parts: &[Part<'_>],
        observed: &PublishedResourceWindow,
        gaps: &mut Vec<CollectedComparisonGap>,
    ) -> Result<Self> {
        let first = validation::time(&observed.from)?;
        let end = validation::time(&observed.until)?;
        let requested = crate::changes::resolve(
            &crate::changes::ChangeRequest::default(),
            (first.timestamp_millis() as f64 / 1000.0, end.timestamp_millis() as f64 / 1000.0),
        )?;
        let boundaries = parts.iter().map(|part| part.from).chain(std::iter::once(end)).collect::<Vec<_>>();
        let recent = nearest(&boundaries, 1..parts.len(), &requested.1.start)?;
        let baseline = nearest(&boundaries, 0..recent, &requested.0.start)?;
        let before = PublishedResourceWindow {
            from: canonical_time(&boundaries[baseline].to_rfc3339())?,
            until: canonical_time(&boundaries[recent].to_rfc3339())?,
        };
        let after = PublishedResourceWindow { from: before.until.clone(), until: observed.until.clone() };
        retain_boundary(&requested.0, &before, gaps)?;
        retain_boundary(&requested.1, &after, gaps)?;
        Ok(Self { baseline, recent, before, after })
    }
}
fn nearest(
    boundaries: &[chrono::DateTime<chrono::FixedOffset>],
    choices: std::ops::Range<usize>,
    requested: &str,
) -> Result<usize> {
    let requested = validation::time(requested)?;
    choices
        .min_by_key(|&index| {
            (boundaries[index].signed_duration_since(requested).num_milliseconds().unsigned_abs(), index)
        })
        .ok_or_else(invalid)
}
fn retain_boundary(
    requested: &Period,
    selected: &PublishedResourceWindow,
    gaps: &mut Vec<CollectedComparisonGap>,
) -> Result<()> {
    if validation::time(&requested.start)? != validation::time(&selected.from)?
        || validation::time(&requested.end)? != validation::time(&selected.until)?
    {
        gaps.push(CollectedComparisonGap::NativeBoundarySelected {
            requested: requested.clone(),
            selected: Period { start: selected.from.clone(), end: selected.until.clone() },
        });
    }
    Ok(())
}
fn merge_parts(parts: Vec<Part<'_>>, observed: &PublishedResourceWindow) -> Result<PreparedPopulation> {
    let originals = parts.iter().map(|part| part.original).collect::<Vec<_>>();
    merge::prepare(parts.into_iter().map(|part| part.prepared), &originals, observed)
}

fn changes(
    before: &PreparedPopulation,
    after: &PreparedPopulation,
    periods: &PeriodSelection,
    engine: Engine,
    gaps: &mut Vec<CollectedComparisonGap>,
) -> Result<Option<Changes>> {
    let (Some(baseline_total), Some(recent_total)) = (totals(before)?, totals(after)?) else {
        gaps.push(CollectedComparisonGap::MissingRuntimeDenominator);
        return Ok(None);
    };
    if before.partial || after.partial {
        gaps.push(CollectedComparisonGap::IncompleteQueryProjection);
    }
    let before_queries = queries(before);
    let after_queries = queries(after);
    let data = PeriodData {
        source: TimelineSource::NativeIntervals,
        population: match &before.population {
            CollectedQueryPopulation::Population { identity } => Some(identity.clone()),
            _ => None,
        },
        baseline: Period { start: periods.before.from.clone(), end: periods.before.until.clone() },
        recent: Period { start: periods.after.from.clone(), end: periods.after.until.clone() },
        baseline_total,
        recent_total,
        baseline_coverage: 1.0,
        queries: query_periods(&before_queries, &after_queries, gaps)?,
        ..Default::default()
    };
    Ok(Some(culprit_core::changes::compare("", engine, data, &CompareOptions::default())))
}
fn queries(prepared: &PreparedPopulation) -> BTreeMap<&str, &QueryStat> {
    prepared.workload.iter().flat_map(|workload| &workload.queries).map(|query| (query.id.as_str(), query)).collect()
}
fn totals(prepared: &PreparedPopulation) -> Result<Option<PeriodStats>> {
    let Some(total) = &prepared.totals else { return Ok(None) };
    let Some(time) = total.time_ms.filter(|_| total.executions <= u128::from(groups::MAX_EXACT_COUNT)) else {
        return Ok(None);
    };
    if total.executions > 0 && prepared.workload.is_none() {
        return Ok(None);
    }
    Ok(Some(stats(total.executions as f64, time, None, None)?))
}
fn query_periods(
    before: &BTreeMap<&str, &QueryStat>,
    after: &BTreeMap<&str, &QueryStat>,
    gaps: &mut Vec<CollectedComparisonGap>,
) -> Result<Vec<QueryPeriods>> {
    let keys = before.keys().chain(after.keys()).copied().collect::<std::collections::BTreeSet<_>>();
    let mut output = Vec::new();
    for id in keys {
        let earlier = before.get(id).copied();
        let later = after.get(id).copied();
        let basis = earlier.or(later).ok_or_else(invalid)?;
        if earlier.zip(later).is_some_and(|(a, b)| a.text != b.text || a.object_name != b.object_name) {
            return Err(invalid());
        }
        let mut query = QueryPeriods {
            query_id: id.into(),
            text: basis.text.clone(),
            object_name: basis.object_name.clone(),
            baseline: earlier.map(query_stats).transpose()?,
            recent: later.map(query_stats).transpose()?,
            ..Default::default()
        };
        if earlier.is_none() || later.is_none() {
            query.untracked = Some(culprit_core::t!("core.changes.reason.native_intervals"));
            gaps.push(CollectedComparisonGap::QueryMissingFromPeriod { query_id: id.into() });
        }
        if let Some((before, after)) = earlier.zip(later) {
            if before.plans.len() == 1
                && after.plans.len() == 1
                && before.plans[0].native_identity.is_some()
                && after.plans[0].native_identity.is_some()
            {
                add_plan(&mut query.plans, &before.plans[0], true)?;
                add_plan(&mut query.plans, &after.plans[0], false)?;
            } else {
                gaps.push(CollectedComparisonGap::PlanMixUnestablished { query_id: id.into() });
            }
        }
        output.push(query);
    }
    Ok(output)
}
fn query_stats(query: &QueryStat) -> Result<PeriodStats> {
    stats(
        query.executions,
        query.total_time_ms,
        query.spread.as_ref().and_then(|spread| spread.min_ms),
        query.max_time_ms,
    )
}
fn stats(executions: f64, total_ms: f64, min_ms: Option<f64>, max_ms: Option<f64>) -> Result<PeriodStats> {
    if !executions.is_finite()
        || executions < 0.0
        || executions.fract() != 0.0
        || executions > groups::MAX_EXACT_COUNT as f64
        || !total_ms.is_finite()
        || total_ms < 0.0
    {
        return Err(invalid());
    }
    Ok(PeriodStats { min_ms, max_ms, ..PeriodStats::new(executions, total_ms) })
}
fn add_plan(plans: &mut Vec<PlanPeriods>, plan: &PlanVariant, earlier: bool) -> Result<()> {
    let index = match plans.iter().position(|candidate| candidate.id == plan.id) {
        Some(index) => index,
        None => {
            plans.push(PlanPeriods { id: plan.id.clone(), ..Default::default() });
            plans.len() - 1
        }
    };
    let value = stats(plan.executions, plan.total_time_ms, plan.min_time_ms, plan.max_time_ms)?;
    if earlier {
        plans[index].baseline = Some(value);
    } else {
        plans[index].recent = Some(value);
        plans[index].first_recent = plan.first_seen.clone();
    }
    Ok(())
}
