//! Published observations are historical archive metadata, never a configured fleet or current native authority.
use crate::{
    Engine, IncidentDatabaseSelector, Result,
    collected_analysis::CollectedEvidenceMetrics,
    telemetry::{DeliveryProblem, SourceContinuity, StateSourceCoverage, TelemetryCollection, valid_digest},
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const MAX_PUBLISHED_RESOURCE_PAGE_ROWS: u32 = 32;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PublishedResourceWindow {
    pub from: String,
    pub until: String,
}
impl PublishedResourceWindow {
    pub fn is_valid(&self) -> bool {
        match (utc(&self.from), utc(&self.until)) {
            (Some(from), Some(until)) => from < until,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PublishedResourcePosition {
    pub resource_id: String,
    pub source_id: String,
    pub range_id: String,
}
impl PublishedResourcePosition {
    pub fn is_valid(&self) -> bool {
        [&self.resource_id, &self.source_id, &self.range_id].into_iter().all(|value| valid_digest(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PublishedResourceCursor {
    /// Consistency binding to this contract, authenticated recovery namespace and fixed selection. Not authority.
    pub selection_key: String,
    pub position: PublishedResourcePosition,
}

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PublishedResourceRequest {
    /// Collector acquisition cutoff. It is not a native watermark or a warehouse snapshot isolation promise.
    pub as_of: String,
    pub window: Option<PublishedResourceWindow>,
    pub after: Option<PublishedResourceCursor>,
    pub page_rows: u32,
}
impl Default for PublishedResourceRequest {
    fn default() -> Self {
        Self {
            as_of: chrono::Utc::now().to_rfc3339(),
            window: None,
            after: None,
            page_rows: MAX_PUBLISHED_RESOURCE_PAGE_ROWS,
        }
    }
}
impl PublishedResourceRequest {
    pub fn validate(&self) -> Result<()> {
        if utc(&self.as_of).is_none()
            || self.window.as_ref().is_some_and(|window| !window.is_valid())
            || !(1..=MAX_PUBLISHED_RESOURCE_PAGE_ROWS).contains(&self.page_rows)
            || self
                .after
                .as_ref()
                .is_some_and(|cursor| !valid_digest(&cursor.selection_key) || !cursor.position.is_valid())
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }
    /// Both warehouse-rounded hints and immutable original clocks must independently satisfy this selection.
    pub fn accepts_acquisition(&self, from: &str, until: &str) -> bool {
        let (Some(from), Some(until), Some(as_of)) = (utc(from), utc(until), utc(&self.as_of)) else {
            return false;
        };
        from <= until
            && until <= as_of
            && self.window.as_ref().is_none_or(|window| {
                utc(&window.from).zip(utc(&window.until)).is_some_and(|(start, end)| from < end && until >= start)
            })
    }
}

#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum PublishedResourceTime {
    Utc {
        from: String,
        until: String,
    },
    /// Native time is not available as an explicitly offset-qualified UTC interval from this header.
    Unknown,
}

#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum PublishedResourceCoverage {
    /// Each original source keeps its own partial/denied/unavailable status; no health is derived here.
    Snapshot {
        sources: Vec<StateSourceCoverage>,
    },
    Activity {
        coverage: StateSourceCoverage,
    },
    Unknown,
}

/// This selector is copied from an authenticated discovery result. Its namespace is rechecked by the read owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct PublishedResourceSelector {
    pub namespace_id: String,
    pub database: IncidentDatabaseSelector,
}
impl PublishedResourceSelector {
    pub fn validate(&self) -> Result<()> {
        if !valid_digest(&self.namespace_id) || !self.database.is_valid() {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct PublishedDatabaseResource {
    /// Existing opaque keys preserve distinct incarnations/member scopes without disclosing their native proofs.
    pub position: PublishedResourcePosition,
    pub logical_source_id: String,
    pub selector: PublishedResourceSelector,
    pub engine: Engine,
    pub database_name: String,
    /// A capture does not necessarily retain a saved connection alias. Names never establish an owner.
    pub connection_alias: Option<String>,
    pub source_name: String,
    pub collection: TelemetryCollection,
    pub continuity: SourceContinuity,
    pub collected_from: String,
    pub collected_until: String,
    pub native_time: PublishedResourceTime,
    pub coverage: PublishedResourceCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum PublishedResourceGap {
    /// A candidate could not be reopened as a committed original. No name or source identity is exposed for it.
    OriginalUnavailable,
    NotPublished,
    AcquisitionClockBoundary,
    MoreCandidates,
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct PublishedResources {
    pub as_of: String,
    pub window: Option<PublishedResourceWindow>,
    pub resources: Vec<PublishedDatabaseResource>,
    pub next: Option<PublishedResourceCursor>,
    /// Only this bounded candidate page and its originals were checked; never complete configured-fleet coverage.
    pub discovery_complete: bool,
    pub gaps: Vec<PublishedResourceGap>,
    pub candidate_rows: u32,
    pub archive_work: CollectedEvidenceMetrics,
    /// Includes missing metadata GETs. Metadata bytes are also charged to archive_work.proof_bytes.
    pub recovery_metadata_reads: u32,
}

fn utc(value: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    if value.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(value).ok().filter(|at| at.offset().local_minus_utc() == 0)
}

#[cfg(test)]
mod tests;
