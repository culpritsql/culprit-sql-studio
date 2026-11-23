//! Whole compatible native intervals become one real workload through the existing preparation/rules owner.
//! Incompatible/unknown generations, overlapping intervals and ambiguous revisions never become one total.
use super::*;
use culprit_core::{telemetry::*, *};
use culprit_evidence::original_projection::OriginalProjection;
use std::{collections::BTreeMap, sync::Arc};
mod comparison;
mod merge;
mod selection;
const WINDOW_RULES_VERSION: &str = "native-window-2";

/// No source or archive I/O. The caller authenticates every capture using its one cumulative EvidenceSession.
/// This entry point never constructs a native capture/header or repairs a missing native identity.
pub fn analyze_window(
    captures: &[&EvidenceCapture],
    window: &PublishedResourceWindow,
) -> Result<CollectedQueryWindowAnalysis> {
    analyze_window_with(captures, window, &culprit_checks::Pipeline::builtin(), &|| Ok(()))
}

pub(crate) fn analyze_window_with(
    captures: &[&EvidenceCapture],
    window: &PublishedResourceWindow,
    pipeline: &culprit_checks::Pipeline,
    check: &dyn Fn() -> Result<()>,
) -> Result<CollectedQueryWindowAnalysis> {
    check()?;
    if !window.is_valid() {
        return Err(DeliveryProblem::InvalidContract.error());
    }
    if captures.len() > MAX_COLLECTED_WINDOW_INPUTS
        || captures
            .iter()
            .try_fold(0usize, |count, capture| count.checked_add(capture.records().len()))
            .is_none_or(|count| count > MAX_COLLECTED_WINDOW_ROWS)
    {
        return Err(DeliveryProblem::Capacity.error());
    }
    let mut remaining = MAX_COLLECTED_WINDOW_INPUT_BYTES;
    for capture in captures {
        check()?;
        admit_input(capture.source(), &mut remaining)?;
        for row in capture.records() {
            check()?;
            admit_input(row.row(), &mut remaining)?;
        }
        if capture.source().query_history().is_some() {
            validation::warehouse_capture(capture)?;
        }
    }
    let originals = captures.iter().map(|capture| capture.verified_original()).collect::<Result<Vec<_>>>()?;
    let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
    analyze_originals(&borrowed, window, pipeline, check)
}

fn analyze_originals(
    originals: &[&dyn OriginalProjection],
    window: &PublishedResourceWindow,
    pipeline: &culprit_checks::Pipeline,
    check: &dyn Fn() -> Result<()>,
) -> Result<CollectedQueryWindowAnalysis> {
    check()?;
    let mut output = CollectedQueryWindowAnalysis {
        rules_version: WINDOW_RULES_VERSION.into(),
        requested: window.clone(),
        sections: vec![],
        superseded: vec![],
        gaps: vec![],
    };
    let selected = selection::originals(originals, window, &mut output)?;
    let mut populations = BTreeMap::<Vec<u8>, Vec<Part<'_>>>::new();
    for index in selected {
        check()?;
        let original = originals[index];
        let history = original.source().query_history().ok_or_else(invalid)?;
        let catalog = Arc::new(catalogs::CatalogContext::from_originals(original, originals)?);
        let catalog_inputs = catalog
            .inputs()
            .iter()
            .map(|index| catalog_input(originals[usize::from(*index)]))
            .collect::<Result<Vec<_>>>()?;
        let definitions = validation::Definitions::new(original)?;
        let native = populations::PopulationIndex::new(original, None)?;
        for population in native.selected() {
            let qualified = selection::qualified(original) && *population != CollectedQueryPopulation::Unknown;
            if *population == CollectedQueryPopulation::Unknown {
                output.gaps.push(CollectedWindowGap::UnknownPopulation { original: original.original().clone() });
            }
            let key = serde_json::to_vec(&(
                original.source().captured_range()?.scope_id,
                population,
                &history.waits,
                &history.population_binding,
                &history.dimensions,
                &catalog_inputs,
                (!qualified).then_some(original.original()),
            ))
            .map_err(|_| invalid())?;
            let prepared = prepare_population(original, &definitions, population.clone(), native.rows(population)?)?;
            let input = CollectedWindowInput {
                original: original.original().clone(),
                interval: history.interval.clone(),
                runtime_rows: original.captured_rows(TelemetryStream::QueryRuntime),
                wait_rows: original.captured_rows(TelemetryStream::QueryWaits),
                read_complete: original.is_read_complete(),
                queries: prepared.query_inputs.clone(),
            };
            populations.entry(key).or_default().push(Part {
                original,
                prepared,
                input,
                catalog_inputs: catalog_inputs.clone(),
                catalog: catalog.clone(),
                from: validation::time(&history.interval.from)?,
            });
        }
    }
    for parts in populations.into_values() {
        let mut parts = parts;
        parts.sort_by_key(|part| part.from);
        for chain in selection::chains(parts, &mut output)? {
            check()?;
            let section = section(chain, pipeline, check)?;
            check()?;
            uncovered(&section, window, &mut output.gaps)?;
            output.sections.push(section);
        }
    }
    if output.sections.is_empty() {
        output.gaps.push(CollectedWindowGap::NoNativeIntervals);
    }
    check()?;
    culprit_core::serialization::json_size(&output, MAX_DERIVED_OUTPUT_BYTES).map_err(output_size_error)?;
    Ok(output)
}
struct Part<'a> {
    original: &'a dyn OriginalProjection,
    prepared: PreparedPopulation,
    input: CollectedWindowInput,
    catalog_inputs: Vec<DerivedInputReference>,
    catalog: Arc<catalogs::CatalogContext>,
    from: chrono::DateTime<chrono::FixedOffset>,
}
fn section(
    parts: Vec<Part<'_>>,
    pipeline: &culprit_checks::Pipeline,
    check: &dyn Fn() -> Result<()>,
) -> Result<CollectedWindowSection> {
    let first = parts.first().ok_or_else(invalid)?;
    let last = parts.last().ok_or_else(invalid)?;
    let original = first.original;
    let scope_id = original.source().captured_range()?.scope_id;
    let population = first.prepared.population.clone();
    let observed = PublishedResourceWindow {
        from: canonical_time(&first.input.interval.from)?,
        until: canonical_time(&last.input.interval.until)?,
    };
    let inputs = parts.iter().map(|part| part.input.clone()).collect::<Vec<_>>();
    let catalog_inputs = first.catalog_inputs.clone();
    let context_issues = first.catalog.issues().to_vec();
    let analysis_key = digest(
        &serde_json::to_vec(&(
            "collected-native-window-v1",
            &scope_id,
            &population,
            &observed,
            &inputs,
            &catalog_inputs,
        ))
        .map_err(|_| invalid())?,
    );
    let originals = parts.iter().map(|part| part.original).collect::<Vec<_>>();
    let catalog = parts[0].catalog.clone();
    let (prepared, comparison) = comparison::prepare(parts, &originals, &observed, check)?;
    let policy = AnalysisPolicy {
        analysis_key: &analysis_key,
        pipeline,
        origin: culprit_core::AnalysisOrigin::ArchivedTelemetry,
        catalogs: catalog.as_ref(),
    };
    let database = validation::capture(original)?;
    let analysis = finish_population(original, &database, prepared, &policy)?;
    let analysis_id = digest(
        &serde_json::to_vec(&(
            WINDOW_RULES_VERSION,
            &analysis_key,
            &analysis.provenance,
            &comparison.rules_version,
            culprit_evidence::analytical::prepared::PREPARATION_VERSION,
            culprit_evidence::analytical::projection::PROJECTION_NORMALIZATION_VERSION,
        ))
        .map_err(|_| invalid())?,
    );
    Ok(CollectedWindowSection {
        analysis_id,
        scope_id,
        native_scope_qualified: selection::qualified(original) && population != CollectedQueryPopulation::Unknown,
        population,
        observed,
        inputs,
        catalog_inputs,
        context_issues,
        analysis,
        comparison,
    })
}
fn catalog_input(original: &dyn OriginalProjection) -> Result<DerivedInputReference> {
    let snapshot = original.source().observation.snapshot().ok_or_else(invalid)?;
    Ok(DerivedInputReference {
        capture: original.original().clone(),
        role: DerivedInputRole::Catalog,
        catalog: Some(DerivedCatalogProvenance {
            observed_from: canonical_time(original.collected_from())?,
            observed_until: canonical_time(original.collected_until())?,
            native_time: Some(DerivedCatalogTime::Utc { at: canonical_time(&snapshot.observed_until)? }),
        }),
    })
}
fn uncovered(
    section: &CollectedWindowSection,
    window: &PublishedResourceWindow,
    gaps: &mut Vec<CollectedWindowGap>,
) -> Result<()> {
    for (from, until) in [(&window.from, &section.observed.from), (&section.observed.until, &window.until)] {
        if validation::time(from)? < validation::time(until)? {
            gaps.push(CollectedWindowGap::SectionDoesNotCoverWindow {
                analysis_id: section.analysis_id.clone(),
                scope_id: section.scope_id.clone(),
                population: section.population.clone(),
                window: PublishedResourceWindow { from: from.clone(), until: until.clone() },
            });
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests;

fn admit_input(value: &impl serde::Serialize, remaining: &mut usize) -> Result<()> {
    let bytes = culprit_core::serialization::json_size(value, *remaining).map_err(|error| match error {
        JsonSizeError::LimitExceeded => DeliveryProblem::Capacity.error(),
        JsonSizeError::Serialization => DeliveryProblem::Integrity.error(),
    })?;
    *remaining = remaining.checked_sub(bytes).ok_or_else(|| DeliveryProblem::Capacity.error())?;
    Ok(())
}
