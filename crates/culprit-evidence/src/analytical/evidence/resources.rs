//! Publication search yields opaque hints. Only the existing committed-manifest/publication verifier grants labels.
use super::{EvidenceReadBudget, EvidenceReadMetrics};
use crate::delivery::{contract::Contract, recovery::CommittedRange};
use culprit_core::{
    IncidentDatabaseSelector, PublishedResourceCursor, PublishedResourcePosition, PublishedResourceRequest, Result,
    telemetry::{DeliveryProblem, digest, valid_digest},
};
use serde_json::{Map, Value};

/// Opaque consistency identity of the selected read contract and the actually opened recovery namespace.
pub fn resource_namespace_id(contract: &Contract, recovery_store_id: &str) -> Result<String> {
    contract.validate()?;
    if !valid_digest(recovery_store_id) {
        return Err(invalid());
    }
    Ok(digest(
        &serde_json::to_vec(&("published-resource-namespace-v1", contract.id()?, recovery_store_id))
            .map_err(|_| invalid())?,
    ))
}

/// Candidate discovery policy only. Exact immutable native clocks are rechecked by the window resolver.
#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PublishedResourceClock {
    Acquisition,
    /// Late historical/derived captures and unknown native clocks remain candidates. No acquisition window
    /// may erase a native history interval, a retained catalog ancestor or an empty derived evaluation.
    NativeWindowCandidates,
}

/// Selection of existing published headers, never an inference from a provider/source display name.
#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PublishedResourceContent {
    All,
    DerivedDiagnosis,
}
impl PublishedResourceContent {
    pub fn observation_kind(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::DerivedDiagnosis => Some("derived-diagnosis"),
        }
    }
    pub fn accepts_source(self, source: &culprit_core::telemetry::CollectionDescription) -> bool {
        match self {
            Self::All => true,
            Self::DerivedDiagnosis => {
                matches!(&source.observation, culprit_core::telemetry::SourceObservation::DerivedDiagnosis(_))
            }
        }
    }
    pub fn accepts_publication(self, values: &Map<String, Value>) -> bool {
        self.observation_kind().is_none_or(|kind| {
            values
                .get("SourceDescription")
                .and_then(|source| source.pointer("/observation/kind"))
                .and_then(Value::as_str)
                == Some(kind)
        })
    }
}

#[derive(Clone, Debug)]
pub struct PublishedResourceQuery {
    pub selection: PublishedResourceRequest,
    pub recovery_store_id: String,
    pub database: Option<IncidentDatabaseSelector>,
    pub clock: PublishedResourceClock,
    pub content: PublishedResourceContent,
}
impl PublishedResourceQuery {
    pub fn key(&self, contract: &Contract) -> Result<String> {
        Ok(digest(
            &serde_json::to_vec(&(
                "published-resource-selection-v3",
                contract.id()?,
                &self.recovery_store_id,
                &self.selection.as_of,
                &self.selection.window,
                &self.database,
                self.clock,
                self.content,
            ))
            .map_err(|_| invalid())?,
        ))
    }
    pub fn validate(&self, contract: &Contract) -> Result<()> {
        contract.validate()?;
        self.selection.validate()?;
        if self.database.as_ref().is_some_and(|database| !database.is_valid())
            || self.clock == PublishedResourceClock::NativeWindowCandidates
                && (self.database.is_none() || self.selection.window.is_none())
            || !valid_digest(&self.recovery_store_id)
            || self
                .selection
                .after
                .as_ref()
                .is_some_and(|cursor| self.key(contract).ok().as_ref() != Some(&cursor.selection_key))
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn accepts(&self, hint: &PublishedResourceHint) -> bool {
        let mut cutoff = self.selection.clone();
        cutoff.window = None;
        if !cutoff.accepts_acquisition(&hint.collected_from, &hint.collected_until) {
            return false;
        }
        match self.clock {
            PublishedResourceClock::Acquisition => {
                self.selection.accepts_acquisition(&hint.collected_from, &hint.collected_until)
            }
            PublishedResourceClock::NativeWindowCandidates => match (&hint.interval, &self.selection.window) {
                (Some(interval), Some(window)) => interval.is_valid() && overlaps(interval, window),
                (None, Some(_)) => true,
                _ => false,
            },
        }
    }
    pub fn within_budget(&self, contract: &Contract, budget: EvidenceReadBudget) -> Result<Self> {
        self.validate(contract)?;
        budget.validate()?;
        let mut request = self.clone();
        request.selection.page_rows =
            request.selection.page_rows.min(budget.page_rows).min(budget.max_rows.saturating_sub(1));
        if request.selection.page_rows == 0 {
            return Err(DeliveryProblem::Capacity.error());
        }
        Ok(request)
    }
}

#[derive(Clone, Debug)]
pub struct PublishedResourceHint {
    pub database: Option<IncidentDatabaseSelector>,
    pub interval: Option<culprit_core::PublishedResourceWindow>,
    pub position: PublishedResourcePosition,
    pub logical_source_id: String,
    pub manifest_sha256: String,
    pub collected_from: String,
    pub collected_until: String,
}
impl PublishedResourceHint {
    pub(super) fn from_values(
        contract: &Contract,
        request: &PublishedResourceQuery,
        values: &Map<String, Value>,
    ) -> Result<Self> {
        let interval = match (nullable_text(values, "IntervalFrom")?, nullable_text(values, "IntervalUntil")?) {
            (None, None) => None,
            (Some(from), Some(until)) => {
                Some(culprit_core::PublishedResourceWindow { from: from.into(), until: until.into() })
            }
            _ => return Err(invalid()),
        };
        let hint = Self {
            database: nullable_text(values, "DatabaseSelector")?.map(IncidentDatabaseSelector::from_key).transpose()?,
            interval,
            position: PublishedResourcePosition {
                resource_id: text(values, "ResourceId")?.into(),
                source_id: text(values, "SourceId")?.into(),
                range_id: text(values, "RangeId")?.into(),
            },
            logical_source_id: text(values, "LogicalSourceId")?.into(),
            manifest_sha256: text(values, "ManifestSha256")?.into(),
            collected_from: text(values, "CollectedFrom")?.into(),
            collected_until: text(values, "CollectedUntil")?.into(),
        };
        if text(values, "ContractId")? != contract.id()?
            || text(values, "SchemaDigest")? != contract.schema_digest()
            || text(values, "RecoveryStoreId")? != request.recovery_store_id
            || text(values, "ResourceKind")? != "database"
            || !hint.position.is_valid()
            || !valid_digest(&hint.logical_source_id)
            || !valid_digest(&hint.manifest_sha256)
            || hint.interval.as_ref().is_some_and(|interval| !interval.is_valid())
            || !request.accepts(&hint)
            || !request.content.accepts_publication(values)
            || request.database.as_ref().is_some_and(|database| hint.database.as_ref() != Some(database))
        {
            return Err(invalid());
        }
        Ok(hint)
    }
    /// Candidate fields must agree with independently reopened immutable metadata. This comparison does not replace
    /// read_metadata's exact canonical publication proof or authorize any current database operation.
    pub fn matches_original(&self, range: &CommittedRange) -> Result<bool> {
        let manifest = range.manifest();
        Ok(manifest.source.target.database().is_some()
            && self.database
                == manifest.source.target.database().map(IncidentDatabaseSelector::from_route).transpose()?
            && self.position.range_id == manifest.range_id
            && self.manifest_sha256 == range.manifest_digest()
            && self.position.resource_id == manifest.source.target.telemetry_id()?
            && self.position.source_id == manifest.source.captured_range()?.scope_id
            && self.logical_source_id == manifest.source.logical_source_id()?)
    }
}

pub struct ResourceDiscoveryPage {
    pub candidates: Vec<PublishedResourceHint>,
    pub next: Option<PublishedResourceCursor>,
    pub charged_candidate_rows: u32,
    pub metrics: EvidenceReadMetrics,
}
pub(super) fn page(
    contract: &Contract,
    request: &PublishedResourceQuery,
    mut candidates: Vec<PublishedResourceHint>,
    metrics: EvidenceReadMetrics,
) -> Result<ResourceDiscoveryPage> {
    request.validate(contract)?;
    if candidates.len() > request.selection.page_rows as usize + 1 {
        return Err(invalid());
    }
    let charged_candidate_rows = candidates.len() as u32;
    let mut previous = request.selection.after.as_ref().map(|cursor| &cursor.position);
    for candidate in &candidates {
        if previous.is_some_and(|position| position >= &candidate.position) {
            return Err(invalid());
        }
        previous = Some(&candidate.position);
    }
    let next = if candidates.len() > request.selection.page_rows as usize {
        candidates.pop();
        Some(PublishedResourceCursor {
            selection_key: request.key(contract)?,
            position: candidates.last().ok_or_else(invalid)?.position.clone(),
        })
    } else {
        None
    };
    Ok(ResourceDiscoveryPage { candidates, next, charged_candidate_rows, metrics })
}

fn overlaps(a: &culprit_core::PublishedResourceWindow, b: &culprit_core::PublishedResourceWindow) -> bool {
    let at = |value: &str| chrono::DateTime::parse_from_rfc3339(value).ok();
    at(&a.from)
        .zip(at(&a.until))
        .zip(at(&b.from).zip(at(&b.until)))
        .is_some_and(|((start, end), (from, until))| start <= until && end >= from)
}
fn nullable_text<'a>(values: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match values.get(key) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(invalid()),
    }
}

fn text<'a>(values: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    values.get(key).and_then(Value::as_str).ok_or_else(invalid)
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

#[cfg(test)]
mod tests;
