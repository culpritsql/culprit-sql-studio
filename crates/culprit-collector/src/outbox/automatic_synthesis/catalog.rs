//! Bounded retained catalog heads are acquisition-time context, never proof of a historical executor or DDL continuity.
use super::*;
use crate::synthesis::{MAX_SYNTHESIS_CATALOG_HEADS, MAX_SYNTHESIS_CATALOG_VERSIONS};
use rusqlite::{Connection, Transaction, params};

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
    let Some(dataset) = dataset(source) else { return Ok(()) };
    let Some(scope) = scope(source)? else { return Ok(()) };
    if ObservationBounds::new(from, until).is_none() {
        return Err(integrity());
    }
    let revision = original.capture.revision.as_ref().ok_or_else(integrity)?;
    let epoch = revision.owner_epoch.parse::<i64>().map_err(|_| integrity())?;
    let sequence = revision.sequence.parse::<i64>().map_err(|_| integrity())?;
    let key = original.key()?;
    tx.execute(
        "INSERT OR IGNORE INTO synthesis_catalog_heads VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            key,
            scope,
            capture_journal::encode(&dataset)?,
            original.capture.observation_id,
            capture_journal::encode(original)?,
            capture_journal::encode(source)?,
            from,
            until,
            epoch,
            sequence
        ],
    )
    .map_err(storage)?;
    tx.execute(
        "INSERT OR IGNORE INTO diagnostic_pins VALUES(?1,?2)",
        params![format!("synthesis:catalog:{key}"), original.capture.observation_id],
    )
    .map_err(storage)?;
    trim(tx, Some((&scope, dataset)), MAX_SYNTHESIS_CATALOG_VERSIONS)?;
    trim(tx, None, MAX_SYNTHESIS_CATALOG_HEADS)
}

fn trim(tx: &Transaction<'_>, selected: Option<(&str, SnapshotDataset)>, keep: u32) -> Result<()> {
    let removed: Vec<String> = match selected {
        Some((scope, dataset)) => {
            let mut query=tx.prepare("SELECT head_key FROM synthesis_catalog_heads WHERE scope_key=?1 AND dataset_json=?2 ORDER BY epoch DESC,sequence DESC LIMIT -1 OFFSET ?3").map_err(storage)?;
            query
                .query_map(params![scope, capture_journal::encode(&dataset)?, keep], |row| row.get(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<_>>()
                .map_err(storage)?
        }
        None => {
            let mut query = tx
                .prepare(
                    "SELECT head_key FROM synthesis_catalog_heads ORDER BY epoch DESC,sequence DESC LIMIT -1 OFFSET ?1",
                )
                .map_err(storage)?;
            query
                .query_map([keep], |row| row.get(0))
                .map_err(storage)?
                .collect::<rusqlite::Result<_>>()
                .map_err(storage)?
        }
    };
    for key in removed {
        tx.execute("DELETE FROM diagnostic_pins WHERE owner=?1", [format!("synthesis:catalog:{key}")])
            .map_err(storage)?;
        tx.execute("DELETE FROM synthesis_catalog_heads WHERE head_key=?1", [key]).map_err(storage)?;
    }
    Ok(())
}

pub(super) fn remove(tx: &Transaction<'_>, id: &str) -> Result<()> {
    let keys: Vec<String> = {
        let mut query = tx
            .prepare("SELECT head_key FROM synthesis_catalog_heads WHERE observation_id=?1 LIMIT ?2")
            .map_err(storage)?;
        query
            .query_map(params![id, MAX_SYNTHESIS_CATALOG_HEADS], |row| row.get(0))
            .map_err(storage)?
            .collect::<rusqlite::Result<_>>()
            .map_err(storage)?
    };
    for key in keys {
        tx.execute("DELETE FROM diagnostic_pins WHERE owner=?1", [format!("synthesis:catalog:{key}")])
            .map_err(storage)?;
        tx.execute("DELETE FROM synthesis_catalog_heads WHERE head_key=?1", [key]).map_err(storage)?;
    }
    Ok(())
}

/// Results are metadata hints only. The caller reopens every selected body through the same canonical reader,
/// then checks these exact native scope and acquisition bounds before providing it to the pure analyzer.
pub(super) fn select(conn: &Connection, primary: &SynthesisCapture) -> Result<CatalogSelection> {
    let Some(scope_key) = scope(primary.source())? else { return Ok(CatalogSelection::UnknownScope) };
    let as_of = canonical_time(primary.collected_from()).map_err(|_| integrity())?;
    let as_of = chrono::DateTime::parse_from_rfc3339(&as_of).map_err(|_| integrity())?;
    let mut query=conn.prepare("SELECT capture_json,source_json,collected_from,collected_until FROM synthesis_catalog_heads WHERE scope_key=?1 ORDER BY epoch DESC,sequence DESC LIMIT ?2").map_err(storage)?;
    let candidates = query
        .query_map(params![scope_key, MAX_SYNTHESIS_CATALOG_HEADS], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?))
        })
        .map_err(storage)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage)?;
    let mut selected = Vec::new();
    let mut families = std::collections::BTreeMap::<String, u32>::new();
    for (capture, source, from, until) in candidates {
        let capture: DiagnosticCaptureRef = capture_journal::decode(&capture)?;
        let source: CollectionDescription = capture_journal::decode(&source)?;
        if !capture.is_valid()
            || scope(&source)?.as_deref() != Some(&scope_key)
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
        if *count >= MAX_SYNTHESIS_CATALOG_VERSIONS {
            continue;
        }
        *count += 1;
        selected.push(CatalogHint { capture, source, from, until });
    }
    Ok(CatalogSelection::Known(selected))
}

pub(super) enum CatalogSelection {
    UnknownScope,
    Known(Vec<CatalogHint>),
}
pub(super) struct CatalogHint {
    pub capture: DiagnosticCaptureRef,
    pub source: CollectionDescription,
    pub from: String,
    pub until: String,
}
impl CatalogHint {
    pub fn matches(&self, capture: &SynthesisCapture) -> bool {
        self.capture == *capture.original()
            && self.source == *capture.source()
            && self.from == capture.collected_from()
            && self.until == capture.collected_until()
    }
}

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
