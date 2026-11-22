//! Bounded database/time discovery returns hints only; original manifests and rows remain the proof owner.
use super::{EvidenceReadBudget, EvidenceReadMetrics};
use culprit_core::{IncidentDatabaseSelector, Result, telemetry::*};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const MAX_INCIDENT_CANDIDATE_PAGE_ROWS: u32 = 32;
const MAX_INCIDENT_WINDOW_SECONDS: i64 = 24 * 60 * 60;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum IncidentCandidateSelection {
    Around { database: IncidentDatabaseSelector, from: String, until: String },
    Episode { database: IncidentDatabaseSelector, episode_id: String, controller_version: u32 },
}
impl IncidentCandidateSelection {
    pub fn database(&self) -> &IncidentDatabaseSelector {
        match self {
            Self::Around { database, .. } | Self::Episode { database, .. } => database,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if !self.database().is_valid() {
            return Err(invalid());
        }
        match self {
            Self::Around { from, until, .. } => {
                if !utc(from) || !utc(until) {
                    return Err(invalid());
                }
                let from = chrono::DateTime::parse_from_rfc3339(from).map_err(|_| invalid())?;
                let until = chrono::DateTime::parse_from_rfc3339(until).map_err(|_| invalid())?;
                let duration = until.signed_duration_since(from);
                if duration <= chrono::Duration::zero()
                    || duration > chrono::Duration::seconds(MAX_INCIDENT_WINDOW_SECONDS)
                {
                    return Err(invalid());
                }
            }
            Self::Episode { episode_id, controller_version, .. } => {
                if !valid_digest(episode_id) || *controller_version == 0 {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
}

/// Stable order within one selected request. The original timestamp is a candidate cutoff, never a native watermark.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IncidentCandidatePosition {
    pub episode_id: String,
    pub controller_version: u32,
    pub revision: String,
    pub range_id: String,
    pub batch_id: String,
    /// Warehouse receipt positions are1-based; the existing original-reference converter owns subtraction.
    pub row_ordinal: u32,
}
impl IncidentCandidatePosition {
    fn validate(&self) -> Result<()> {
        if !valid_digest(&self.episode_id)
            || !valid_digest(&self.range_id)
            || !valid_digest(&self.batch_id)
            || self.controller_version == 0
            || self.row_ordinal == 0
            || self.revision.parse::<u64>().ok().is_none_or(|number| number == 0 || number.to_string() != self.revision)
        {
            return Err(invalid());
        }
        Ok(())
    }
    /// Length then canonical digits preserves the full unsigned revision value without native signed narrowing.
    pub fn revision_order(&self) -> (usize, &str) {
        (self.revision.len(), &self.revision)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IncidentCandidateCursor {
    pub request_sha256: String,
    pub position: IncidentCandidatePosition,
}
#[derive(Clone, Debug)]
pub struct IncidentDiscoveryRequest {
    pub selection: IncidentCandidateSelection,
    pub recorded_before: String,
    pub after: Option<IncidentCandidateCursor>,
    pub page_rows: u32,
}
impl IncidentDiscoveryRequest {
    pub fn key(&self) -> Result<String> {
        Ok(digest(
            &serde_json::to_vec(&("incident-discovery-request-v1", &self.selection, &self.recorded_before))
                .map_err(|_| invalid())?,
        ))
    }
    pub fn validate(&self) -> Result<()> {
        self.selection.validate()?;
        if !utc(&self.recorded_before) || !(1..=MAX_INCIDENT_CANDIDATE_PAGE_ROWS).contains(&self.page_rows) {
            return Err(invalid());
        }
        if let Some(after) = &self.after {
            after.position.validate()?;
            if after.request_sha256 != self.key()? {
                return Err(invalid());
            }
        }
        Ok(())
    }
    pub fn within_budget(&self, budget: EvidenceReadBudget) -> Result<Self> {
        self.validate()?;
        budget.validate()?;
        let mut request = self.clone();
        request.page_rows = request.page_rows.min(budget.page_rows).min(budget.max_rows.saturating_sub(1));
        if request.page_rows == 0 {
            return Err(DeliveryProblem::Capacity.error());
        }
        request.validate_budget(budget)?;
        Ok(request)
    }
    pub fn validate_budget(&self, budget: EvidenceReadBudget) -> Result<()> {
        self.validate()?;
        budget.validate()?;
        if self.page_rows.checked_add(1).is_none_or(|rows| rows > budget.max_rows) || self.page_rows > budget.page_rows
        {
            return Err(DeliveryProblem::Capacity.error());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct IncidentCandidateHint {
    pub position: IncidentCandidatePosition,
    pub revision_id: String,
    pub database: IncidentDatabaseSelector,
    pub original: DiagnosticEvidenceRef,
    pub observed_from: String,
    pub observed_until: String,
    pub recorded_at: String,
    pub original_interval: Option<TelemetryInterval>,
}
impl IncidentCandidateHint {
    pub(super) fn from_values(values: &Map<String, Value>) -> Result<Self> {
        let original: DiagnosticEvidenceRef =
            serde_json::from_value(values.get("OriginalReference").ok_or_else(invalid)?.clone())
                .map_err(|_| invalid())?;
        let interval = match (
            optional(values, "OriginalIntervalId")?,
            optional(values, "OriginalIntervalFrom")?,
            optional(values, "OriginalIntervalUntil")?,
        ) {
            (None, None, None) => None,
            (Some(native_id), Some(from), Some(until)) => {
                Some(TelemetryInterval { native_id: native_id.into(), from: from.into(), until: until.into() })
            }
            _ => return Err(invalid()),
        };
        let position = IncidentCandidatePosition {
            episode_id: text(values, "EpisodeId")?.into(),
            controller_version: integer(values, "ControllerVersion")?,
            revision: text(values, "Revision")?.into(),
            range_id: text(values, "RangeId")?.into(),
            batch_id: text(values, "BatchId")?.into(),
            row_ordinal: integer(values, "RowOrdinal")?,
        };
        position.validate()?;
        let hint = Self {
            position,
            revision_id: text(values, "RevisionId")?.into(),
            database: IncidentDatabaseSelector::from_key(text(values, "DatabaseSelector")?)?,
            original,
            observed_from: text(values, "OriginalObservedFrom")?.into(),
            observed_until: text(values, "OriginalObservedUntil")?.into(),
            recorded_at: text(values, "RecordedAt")?.into(),
            original_interval: interval,
        };
        if !valid_digest(&hint.revision_id)
            || !hint.original.is_valid()
            || !utc(&hint.observed_from)
            || !utc(&hint.observed_until)
            || ObservationBounds::new(&hint.observed_from, &hint.observed_until).is_none()
            || !utc(&hint.recorded_at)
            || hint.original_interval.as_ref().is_some_and(|interval| {
                interval.native_id.trim().is_empty()
                    || !utc(&interval.from)
                    || !utc(&interval.until)
                    || ObservationBounds::new(&interval.from, &interval.until).is_none()
            })
        {
            return Err(invalid());
        }
        Ok(hint)
    }
}

pub struct IncidentDiscoveryPage {
    pub candidates: Vec<IncidentCandidateHint>,
    pub next: Option<IncidentCandidateCursor>,
    /// These are unverified candidate rows, deliberately separate from authenticated selected_rows.
    pub charged_candidate_rows: u32,
    pub metrics: EvidenceReadMetrics,
}

pub(super) fn page(
    request: &IncidentDiscoveryRequest,
    mut candidates: Vec<IncidentCandidateHint>,
    metrics: EvidenceReadMetrics,
) -> Result<IncidentDiscoveryPage> {
    request.validate()?;
    if candidates.len() > request.page_rows as usize + 1 {
        return Err(invalid());
    }
    let charged_candidate_rows = candidates.len() as u32;
    let mut previous = request.after.as_ref().map(|cursor| &cursor.position);
    for candidate in &candidates {
        if !matches_selection(request, candidate)?
            || candidate.database != *request.selection.database()
            || matches!(&request.selection, IncidentCandidateSelection::Episode {episode_id,controller_version,..}
                if candidate.position.episode_id != *episode_id || candidate.position.controller_version != *controller_version)
            || previous.is_some_and(|before| order(before, &candidate.position) != std::cmp::Ordering::Less)
        {
            return Err(invalid());
        }
        previous = Some(&candidate.position);
    }
    let next = if candidates.len() > request.page_rows as usize {
        candidates.pop();
        Some(IncidentCandidateCursor {
            request_sha256: request.key()?,
            position: candidates.last().ok_or_else(invalid)?.position.clone(),
        })
    } else {
        None
    };
    Ok(IncidentDiscoveryPage { candidates, next, charged_candidate_rows, metrics })
}
fn order(before: &IncidentCandidatePosition, after: &IncidentCandidatePosition) -> std::cmp::Ordering {
    (
        &before.episode_id,
        before.controller_version,
        before.revision_order(),
        &before.range_id,
        &before.batch_id,
        before.row_ordinal,
    )
        .cmp(&(
            &after.episode_id,
            after.controller_version,
            after.revision_order(),
            &after.range_id,
            &after.batch_id,
            after.row_ordinal,
        ))
}
fn text<'a>(values: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    values.get(key).and_then(Value::as_str).ok_or_else(invalid)
}
fn optional<'a>(values: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match values.get(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        _ => Err(invalid()),
    }
}
fn integer(values: &Map<String, Value>, key: &str) -> Result<u32> {
    values.get(key).and_then(Value::as_u64).and_then(|value| u32::try_from(value).ok()).ok_or_else(invalid)
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::InvalidContract.error()
}

fn matches_selection(request: &IncidentDiscoveryRequest, candidate: &IncidentCandidateHint) -> Result<bool> {
    request.accepts_original_clocks(
        &candidate.observed_from,
        &candidate.observed_until,
        &candidate.recorded_at,
        candidate.original_interval.as_ref(),
    )
}
impl IncidentDiscoveryRequest {
    /// Search projections may have less precision. The resolver rechecks the immutable original clocks here;
    /// no rounded warehouse timestamp substitutes for that original observation or establishes a causal edge.
    pub fn accepts_original_clocks(
        &self,
        observed_from: &str,
        observed_until: &str,
        recorded_at: &str,
        interval: Option<&TelemetryInterval>,
    ) -> Result<bool> {
        let at = |value: &str| chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid());
        if !utc(observed_from)
            || !utc(observed_until)
            || !utc(recorded_at)
            || ObservationBounds::new(observed_from, observed_until).is_none()
        {
            return Err(invalid());
        }
        if at(recorded_at)? > at(&self.recorded_before)? {
            return Ok(false);
        }
        let IncidentCandidateSelection::Around { from, until, .. } = &self.selection else {
            return Ok(true);
        };
        let from = at(from)?;
        let until = at(until)?;
        let overlaps = |start: &str, end: &str| -> Result<bool> { Ok(at(start)? < until && at(end)? >= from) };
        Ok(overlaps(observed_from, observed_until)?
            || (at(recorded_at)? >= from && at(recorded_at)? < until)
            || interval.map(|interval| overlaps(&interval.from, &interval.until)).transpose()?.unwrap_or(false))
    }
}

#[cfg(test)]
mod tests;

fn utc(value: &str) -> bool {
    ObservationBounds::new(value, value).is_some()
        && chrono::DateTime::parse_from_rfc3339(value).is_ok_and(|at| at.offset().local_minus_utc() == 0)
}
