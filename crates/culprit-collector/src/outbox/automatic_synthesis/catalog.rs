//! Bounded retained catalog heads are acquisition-time context, never proof of a historical executor or DDL continuity.
use super::*;
use crate::synthesis::MAX_SYNTHESIS_RETAINED_VERSIONS;
use rusqlite::{Connection, Transaction};

fn dataset(source: &CollectionDescription) -> Option<SnapshotDataset> {
    let dataset = source.observation.snapshot()?.dataset;
    matches!(
        dataset,
        SnapshotDataset::Tables
            | SnapshotDataset::Indexes
            | SnapshotDataset::Columns
            | SnapshotDataset::ForeignKeys
            | SnapshotDataset::CheckConstraints
            | SnapshotDataset::Objects
            | SnapshotDataset::Triggers
            | SnapshotDataset::Partitions
            | SnapshotDataset::Statistics
            | SnapshotDataset::Settings
    )
    .then_some(dataset)
}

fn family(dataset: SnapshotDataset) -> Result<String> {
    crate::synthesis::synthesis_metadata_key(&("synthesis-catalog-family-v1", dataset))
}

/// A route name/member process alone cannot prove a database incarnation across independent catalog captures.
/// Sources without a native database generation remain useful originals, with explicit missing catalog context.
fn scope(source: &CollectionDescription) -> Result<Option<String>> {
    let Some(database) = source.target.database() else { return Ok(None) };
    if database.incarnation.is_none() || !source.partition_is_consistent() || !source.partition.is_qualified() {
        return Ok(None);
    }
    crate::synthesis::synthesis_metadata_key(&("synthesis-catalog-scope-v1", &source.target, &source.partition))
        .map(Some)
}

pub(super) fn retain(
    tx: &Transaction<'_>,
    original: &DiagnosticCaptureRef,
    source: &CollectionDescription,
    from: &str,
    until: &str,
) -> Result<()> {
    let Some(dataset) = dataset(source) else {
        return Ok(());
    };
    let Some(scope) = scope(source)? else {
        return Ok(());
    };
    heads::retain(tx, original, source, &scope, &family(dataset)?, from, until)
}

/// Results are metadata hints only. The caller reopens every selected body through the same canonical reader,
/// then checks these exact native scope and acquisition bounds before providing it to the pure analyzer.
pub(super) fn select(conn: &Connection, primary: &SynthesisCapture) -> Result<CatalogSelection> {
    let Some(scope_key) = scope(primary.source())? else { return Ok(CatalogSelection::UnknownScope) };
    let as_of = canonical_time(primary.collected_from()).map_err(|_| integrity())?;
    let as_of = chrono::DateTime::parse_from_rfc3339(&as_of).map_err(|_| integrity())?;
    let candidates = heads::select(conn, &scope_key)?;
    let mut selected = Vec::new();
    let mut families = std::collections::BTreeMap::<String, u32>::new();
    for hint in candidates {
        let heads::Hint { capture, source, from, until, family: retained_family } = hint;
        if !capture.is_valid()
            || scope(&source)?.as_deref() != Some(&scope_key)
            || dataset(&source).map(family).transpose()?.as_deref() != Some(&retained_family)
            || source.captured_range()?.scope_id != capture.source_scope_id
            || ObservationBounds::new(&from, &until).is_none()
        {
            return Err(integrity());
        }
        let until_time = chrono::DateTime::parse_from_rfc3339(&until).map_err(|_| integrity())?;
        if until_time > as_of || capture.capture == primary.original().capture {
            continue;
        }
        let family = capture_journal::encode(&dataset(&source).ok_or_else(integrity)?)?;
        let count = families.entry(family).or_default();
        if *count >= MAX_SYNTHESIS_RETAINED_VERSIONS {
            continue;
        }
        *count += 1;
        selected.push(CatalogHint { capture, source, from, until, family: retained_family });
    }
    Ok(CatalogSelection::Known(selected))
}

pub(super) enum CatalogSelection {
    UnknownScope,
    Known(Vec<CatalogHint>),
}
pub(super) type CatalogHint = heads::Hint;

pub(super) fn matches_anchor(catalog: &SynthesisCapture, anchor: &SynthesisCapture) -> bool {
    if dataset(catalog.source()).is_none() {
        return false;
    }
    let (Ok(Some(left)), Ok(Some(right))) = (scope(catalog.source()), scope(anchor.source())) else { return false };
    let (Ok(until), Ok(as_of)) = (
        chrono::DateTime::parse_from_rfc3339(catalog.collected_until()),
        chrono::DateTime::parse_from_rfc3339(anchor.collected_from()),
    ) else {
        return false;
    };
    left == right && until <= as_of
}
