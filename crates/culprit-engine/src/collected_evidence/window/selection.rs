//! Native identity and exact interval membership decide selection; arrival time never decides a cause or revision.
use super::*;
use std::collections::BTreeSet;

type DefinitionKey = (u8, String);
type EncodedDefinition = (DefinitionKey, Vec<u8>);

pub(super) fn qualified(original: &dyn OriginalProjection) -> bool {
    original.source().is_partition_qualified()
        && (original.source().target.database().is_some_and(|database| database.incarnation.is_some())
            || matches!(
                &original.source().partition,
                SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) }
            ))
}
pub(super) fn originals(
    originals: &[&dyn OriginalProjection],
    window: &PublishedResourceWindow,
    output: &mut CollectedQueryWindowAnalysis,
) -> Result<Vec<usize>> {
    let mut intervals = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    let mut seen = BTreeMap::new();
    for (index, &original) in originals.iter().enumerate() {
        if seen
            .insert(original.capture().observation_id.clone(), original.original())
            .is_some_and(|previous| previous != original.original())
        {
            return Err(DeliveryProblem::Conflict.error());
        }
        let Some(history) = original.source().query_history() else { continue };
        validation::capture(original)?;
        if !qualified_clock(&history.interval.from) || !qualified_clock(&history.interval.until) {
            return Err(invalid());
        }
        if validation::time(&history.interval.from)? >= validation::time(&window.until)?
            || validation::time(&history.interval.until)? <= validation::time(&window.from)?
        {
            continue;
        }
        if !qualified(original) {
            output.gaps.push(CollectedWindowGap::UnqualifiedScope { original: original.original().clone() });
        }
        if !original.is_read_complete() {
            output.gaps.push(CollectedWindowGap::IncompleteOriginal { original: original.original().clone() });
        }
        if validation::time(&history.interval.from)? < validation::time(&window.from)?
            || validation::time(&history.interval.until)? > validation::time(&window.until)?
        {
            output.gaps.push(CollectedWindowGap::NativeBoundaryExpanded { original: original.original().clone() });
        }
        let key = serde_json::to_vec(&(
            original.source().captured_range()?.scope_id,
            &history.interval.native_id,
            (!qualified(original)).then_some(original.original()),
        ))
        .map_err(|_| invalid())?;
        let group = intervals.entry(key).or_default();
        if !group.iter().any(|&prior| originals[prior].original() == original.original()) {
            group.push(index);
        }
    }
    let mut selected = Vec::new();
    for group in intervals.into_values() {
        select_revision(originals, &group, &mut selected, output)?;
    }
    Ok(selected)
}
fn select_revision(
    originals: &[&dyn OriginalProjection],
    group: &[usize],
    selected: &mut Vec<usize>,
    output: &mut CollectedQueryWindowAnalysis,
) -> Result<()> {
    let Some(&first) = group.first() else { return Err(invalid()) };
    if group.len() == 1 {
        selected.push(first);
        return Ok(());
    }
    let interval = originals[first].source().captured_range()?.range;
    let mut newest = first;
    let first_rank = rank(originals[first]);
    let compatible = group.iter().all(|&index| {
        let current = originals[index];
        current.source().captured_range().is_ok_and(|captured| captured.range == interval)
            && first_rank.as_ref().zip(rank(current)).is_some_and(|(first, next)| first.0 == next.0)
    });
    if compatible {
        let mut ranks = BTreeSet::new();
        for &index in group {
            let next = rank(originals[index]).ok_or_else(invalid)?;
            if !ranks.insert((next.1, next.2)) {
                ambiguous(originals, group, output);
                selected.extend(group);
                return Ok(());
            }
            let current = rank(originals[newest]).ok_or_else(invalid)?;
            if (next.1, next.2) > (current.1, current.2) {
                newest = index;
            }
        }
        selected.push(newest);
        for &index in group {
            if index != newest {
                output.superseded.push(CollectedWindowSupersession {
                    previous: originals[index].original().clone(),
                    selected: originals[newest].original().clone(),
                });
            }
        }
    } else {
        ambiguous(originals, group, output);
        selected.extend(group);
    }
    Ok(())
}
fn rank(original: &dyn OriginalProjection) -> Option<(&str, u64, u64)> {
    let revision = original.capture().revision.as_ref()?;
    Some((&revision.outbox_id, revision.owner_epoch.parse().ok()?, revision.sequence.parse().ok()?))
}
fn ambiguous(originals: &[&dyn OriginalProjection], group: &[usize], output: &mut CollectedQueryWindowAnalysis) {
    output.gaps.push(CollectedWindowGap::AmbiguousRevision {
        originals: group.iter().map(|&index| originals[index].original().clone()).collect(),
    });
}
fn qualified_clock(value: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

/// Contiguous nonoverlapping intervals can share one workload. Overlaps remain their own visible sections;
/// conflicting definitions cannot become a reconstructed stable plan/text merely because their IDs match.
pub(super) fn chains<'a>(
    parts: Vec<Part<'a>>,
    output: &mut CollectedQueryWindowAnalysis,
) -> Result<Vec<Vec<Part<'a>>>> {
    let mut chains = Vec::new();
    let mut current: Vec<Part<'a>> = Vec::new();
    let mut definitions = BTreeMap::new();
    for part in parts {
        let overlap = current
            .iter()
            .any(|previous| validation::time(&previous.input.interval.until).is_ok_and(|until| until > part.from));
        let conflict = definition_conflict(part.original, &definitions)?;
        let adjacent = current
            .last()
            .is_none_or(|previous| validation::time(&previous.input.interval.until).ok() == Some(part.from));
        if overlap || conflict || !adjacent {
            if overlap {
                let mut originals = current.iter().map(|part| part.input.original.clone()).collect::<Vec<_>>();
                originals.push(part.input.original.clone());
                output.gaps.push(CollectedWindowGap::OverlappingIntervals { originals });
            }
            if conflict {
                let mut originals = current.iter().map(|part| part.input.original.clone()).collect::<Vec<_>>();
                originals.push(part.input.original.clone());
                output.gaps.push(CollectedWindowGap::ConflictingDefinition { originals });
            }
            if !current.is_empty() {
                chains.push(std::mem::take(&mut current));
            }
            definitions.clear();
        }
        remember_definitions(part.original, &mut definitions)?;
        current.push(part);
        if overlap {
            chains.push(std::mem::take(&mut current));
            definitions.clear();
        }
    }
    if !current.is_empty() {
        chains.push(current);
    }
    Ok(chains)
}
fn definition_conflict(original: &dyn OriginalProjection, known: &BTreeMap<DefinitionKey, Vec<u8>>) -> Result<bool> {
    for (key, value) in definition_values(original)? {
        if known.get(&key).is_some_and(|previous| previous != &value) {
            return Ok(true);
        }
    }
    Ok(false)
}
fn remember_definitions(original: &dyn OriginalProjection, known: &mut BTreeMap<DefinitionKey, Vec<u8>>) -> Result<()> {
    known.extend(definition_values(original)?);
    Ok(())
}
fn definition_values(original: &dyn OriginalProjection) -> Result<Vec<EncodedDefinition>> {
    original
        .records()
        .iter()
        .filter_map(|row| match &row.row().record {
            TelemetryRecord::QueryDefinition(query) if query.sql.is_available() => {
                Some(((0, query.id.clone()), serde_json::to_vec(query)))
            }
            TelemetryRecord::PlanDefinition(plan) if plan.plan.is_available() => {
                Some(((1, plan.id.clone()), serde_json::to_vec(plan)))
            }
            _ => None,
        })
        .map(|(key, value)| value.map(|value| (key, value)).map_err(|_| invalid()))
        .collect()
}
