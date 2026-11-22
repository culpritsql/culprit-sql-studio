//! Database/time is candidate discovery only. Every relationship comes from an authenticated immutable receipt.
use super::{EvidenceService, EvidenceSession};
use crate::collected_resources::references;
use culprit_checks::incident_history::{HistoryRevision, explain};
use culprit_core::{telemetry::*, *};
use culprit_evidence::analytical::evidence::*;
use culprit_evidence::delivery::contract::Contract;
use culprit_evidence::delivery::evidence::range_id_for;
use culprit_read::cancellation::CollectionCancellation;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
mod ancestry;
mod named;
use ancestry::Ancestry;

const INCIDENT_AROUND_SECONDS: i64 = 5 * 60;
const MAX_INCIDENT_EPISODES: usize = 8;
const MAX_INCIDENT_CAPTURES: usize = 32;
const MAX_INCIDENT_CANDIDATES: usize = 256;

pub struct IncidentRequest {
    pub database: IncidentDatabaseSelector,
    pub from: String,
    pub until: String,
    pub recorded_before: String,
    pub budget: EvidenceReadBudget,
}
impl IncidentRequest {
    /// Incident ancestry includes bounded private original recovery when a public projection cannot represent it.
    /// This is an archive-only read policy, independent of source acquisition or native cost approval.
    pub fn default_budget() -> EvidenceReadBudget {
        EvidenceReadBudget { recovery_payloads: Some(RecoveryPayloadBudget::default()), ..Default::default() }
    }

    /// A configured route selects candidates without opening a database or borrowing its present identity.
    pub fn around(database: &DatabaseIdentity, at: &str, budget: EvidenceReadBudget) -> Result<Self> {
        Self::around_selector(IncidentDatabaseSelector::from_route(database)?, at, budget)
    }
    fn around_selector(database: IncidentDatabaseSelector, at: &str, budget: EvidenceReadBudget) -> Result<Self> {
        let at = around_clock(at)?;
        let radius = chrono::Duration::seconds(INCIDENT_AROUND_SECONDS);
        let request = Self {
            database,
            from: at.checked_sub_signed(radius).ok_or_else(invalid)?.to_rfc3339(),
            until: at.checked_add_signed(radius).ok_or_else(invalid)?.to_rfc3339(),
            recorded_before: chrono::Utc::now().to_rfc3339(),
            budget,
        };
        request.discovery(None, None).within_budget(budget)?;
        Ok(request)
    }
    fn discovery(
        &self,
        episode: Option<(&str, u32)>,
        after: Option<IncidentCandidateCursor>,
    ) -> IncidentDiscoveryRequest {
        let selection = match episode {
            Some((episode_id, controller_version)) => IncidentCandidateSelection::Episode {
                database: self.database.clone(),
                episode_id: episode_id.into(),
                controller_version,
            },
            None => IncidentCandidateSelection::Around {
                database: self.database.clone(),
                from: self.from.clone(),
                until: self.until.clone(),
            },
        };
        IncidentDiscoveryRequest {
            selection,
            recorded_before: self.recorded_before.clone(),
            after,
            page_rows: MAX_INCIDENT_CANDIDATE_PAGE_ROWS,
        }
    }
}
impl EvidenceService {
    /// The selector comes from resource discovery; the same action authenticates its actual namespace before
    /// candidate queries, then keeps the remaining budget/deadline for the existing ancestry assembler.
    pub async fn resource_incident(
        &self,
        selector: &PublishedResourceSelector,
        around: &str,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<IncidentReadReport> {
        selector.validate()?;
        let request = IncidentRequest::around_selector(selector.database.clone(), around, budget)?;
        complete_resource(self.session(budget, cancellation)?, &self.contract, selector, request).await
    }

    pub async fn incident(
        &self,
        request: &IncidentRequest,
        cancellation: &CollectionCancellation,
    ) -> Result<IncidentReadReport> {
        request.discovery(None, None).within_budget(request.budget)?;
        Assembly::new(self.session(request.budget, cancellation)?, &self.contract).read(request).await
    }
}

fn around_clock(at: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    if at.len() > 64 || ObservationBounds::new(at, at).is_none() {
        return Err(invalid());
    }
    chrono::DateTime::parse_from_rfc3339(at).map(|at| at.with_timezone(&chrono::Utc)).map_err(|_| invalid())
}

async fn complete_resource<'a>(
    mut session: EvidenceSession<'a>,
    contract: &'a Contract,
    selector: &PublishedResourceSelector,
    request: IncidentRequest,
) -> Result<IncidentReadReport> {
    session.validate_resource_selector(selector).await?;
    Box::pin(Assembly::new(session, contract).read(&request)).await
}

struct SelectedRevision {
    row: DiagnosticRevision,
    reference: DiagnosticEvidenceRef,
}
struct Assembly<'a> {
    session: EvidenceSession<'a>,
    contract: &'a Contract,
    captures: BTreeMap<String, Arc<EvidenceCapture>>,
    metadata: BTreeMap<String, EvidenceCaptureMetadata>,
    revisions: BTreeMap<(u32, String, String), SelectedRevision>,
    incidents: BTreeMap<(String, String), DerivedIncident>,
    input_captures: BTreeMap<String, (DiagnosticEvidenceRef, DerivedInputReference)>,
    analyses: BTreeMap<String, IncidentAnalysisContext>,
    gaps: Vec<IncidentReadGap>,
    candidate_rows: u32,
}
impl<'a> Assembly<'a> {
    fn new(session: EvidenceSession<'a>, contract: &'a Contract) -> Self {
        Self {
            session,
            contract,
            captures: BTreeMap::new(),
            metadata: BTreeMap::new(),
            revisions: BTreeMap::new(),
            incidents: BTreeMap::new(),
            input_captures: BTreeMap::new(),
            analyses: BTreeMap::new(),
            gaps: vec![],
            candidate_rows: 0,
        }
    }
    async fn read(mut self, request: &IncidentRequest) -> Result<IncidentReadReport> {
        let window = request.discovery(None, None);
        let around = self.discover(window.clone()).await?;
        let mut episodes = BTreeSet::new();
        for hint in &around.candidates {
            if let Some(incident) = self.candidate(hint, &window).await? {
                episodes.insert((incident.entry.episode_id.clone(), incident.entry.controller_version));
            }
        }
        if around.next.is_some() {
            self.gaps.push(IncidentReadGap::CandidateLimit);
        }
        if episodes.len() > MAX_INCIDENT_EPISODES {
            self.gaps.push(IncidentReadGap::EpisodeLimit);
            episodes = episodes.into_iter().take(MAX_INCIDENT_EPISODES).collect();
        }
        for (episode, controller) in episodes {
            let mut after = None;
            loop {
                if self.candidate_rows as usize + MAX_INCIDENT_CANDIDATE_PAGE_ROWS as usize + 1
                    > MAX_INCIDENT_CANDIDATES
                {
                    self.gaps.push(IncidentReadGap::CandidateLimit);
                    break;
                }
                let selection = request.discovery(Some((&episode, controller)), after);
                let page = self.discover(selection.clone()).await?;
                for hint in &page.candidates {
                    self.candidate(hint, &selection).await?;
                }
                let Some(next) = page.next else {
                    break;
                };
                after = Some(next);
            }
        }
        let mut ancestry = Ancestry::default();
        for revision in self.revisions.values() {
            ancestry.observe(&revision.row, &revision.reference)?;
        }
        for (revision, input) in self.input_captures.values() {
            ancestry.input_capture(revision, input)?;
        }
        let relationships = self.authenticate(ancestry).await?;
        self.predecessor_gaps();
        let selected = self
            .revisions
            .values()
            .map(|row| HistoryRevision { revision: &row.row, original: Some(&row.reference) })
            .collect::<Vec<_>>();
        let history = explain(&selected, IncidentHistoryProvenance::AuthenticatedCapture, self.gaps.is_empty())?;
        Ok(IncidentReadReport {
            database: request.database.clone(),
            from: request.from.clone(),
            until: request.until.clone(),
            recorded_before: request.recorded_before.clone(),
            discovery_complete: self.gaps.is_empty(),
            incidents: self.incidents.into_values().collect(),
            analyses: self.analyses.into_values().collect(),
            history,
            relationships,
            gaps: self.gaps,
            archive_work: crate::collected_context::read_metrics(self.session.completed_metrics()),
            candidate_rows: self.candidate_rows,
        })
    }
    async fn discover(&mut self, request: IncidentDiscoveryRequest) -> Result<IncidentDiscoveryPage> {
        let page = self.session.incident_candidates(&request).await?;
        self.candidate_rows = self.candidate_rows.checked_add(page.charged_candidate_rows).ok_or_else(capacity)?;
        if self.candidate_rows as usize > MAX_INCIDENT_CANDIDATES {
            return Err(capacity());
        }
        Ok(page)
    }
    async fn capture(&mut self, range: &str, families: &[EvidenceFamily]) -> Result<Option<Arc<EvidenceCapture>>> {
        let mut families = families.to_vec();
        if let Some(cached) = self.captures.get(range) {
            if families.iter().all(|family| cached.families().contains(family)) {
                return Ok(Some(cached.clone()));
            }
            for family in cached.families() {
                if !families.contains(family) {
                    families.push(*family);
                }
            }
        } else if self.captures.len() + self.metadata.len() >= MAX_INCIDENT_CAPTURES {
            self.gaps.push(IncidentReadGap::CaptureLimit);
            return Ok(None);
        }
        let capture = self.session.read(range, &families).await?;
        if let Some(cached) = self.captures.get(range)
            && (cached.capture() != capture.capture()
                || cached.capture_manifest_sha256() != capture.capture_manifest_sha256()
                || cached.source() != capture.source())
        {
            return Err(invalid());
        }
        let capture = Arc::new(capture);
        self.captures.insert(range.into(), capture.clone());
        Ok(Some(capture))
    }
    async fn candidate(
        &mut self,
        hint: &IncidentCandidateHint,
        selection: &IncidentDiscoveryRequest,
    ) -> Result<Option<DerivedIncident>> {
        let Some(capture) = self.capture(&hint.position.range_id, &[EvidenceFamily::DerivedIncidents]).await? else {
            return Ok(None);
        };
        if capture
            .captured_rows(TelemetryStream::DerivedIncidents)
            .is_none_or(|count| hint.position.row_ordinal > count)
            || hint.position.batch_id != batch_id(&capture.capture().observation_id, TelemetryStream::DerivedIncidents)?
        {
            return Err(invalid());
        }
        let Some(record) = capture
            .records()
            .iter()
            .find(|row| row.batch_id() == hint.position.batch_id && row.row_ordinal() == hint.position.row_ordinal)
        else {
            self.gaps.push(IncidentReadGap::CandidateUnavailable {
                range_id: hint.position.range_id.clone(),
                batch_id: hint.position.batch_id.clone(),
                row_ordinal: hint.position.row_ordinal,
                gaps: capture.gaps().to_vec(),
            });
            return Ok(None);
        };
        let TelemetryRecord::DerivedIncident(incident) = &record.row().record else {
            return Err(invalid());
        };
        let SourceObservation::DerivedDiagnosis(description) = &capture.source().observation else {
            return Err(invalid());
        };
        if incident.database.as_ref() != Some(&hint.database)
            || incident.revision_reference != hint.original
            || incident.entry.revision_id != hint.revision_id
            || incident.entry.episode_id != hint.position.episode_id
            || incident.entry.controller_version != hint.position.controller_version
            || incident.entry.revision != hint.position.revision
            || !description.contains_capture_for(&incident.revision_reference)
        {
            return Err(invalid());
        }
        if !selection.accepts_original_clocks(
            &description.observed_from,
            &description.observed_until,
            &description.recorded_at,
            incident.original_interval.as_ref(),
        )? {
            self.gaps.push(IncidentReadGap::CandidateClockBoundary {
                range_id: hint.position.range_id.clone(),
                batch_id: hint.position.batch_id.clone(),
                row_ordinal: hint.position.row_ordinal,
            });
            return Ok(None);
        }
        let original = incident.revision_reference.clone();
        let analysis_capture = DiagnosticCaptureRef {
            capture: capture.capture().clone(),
            manifest_sha256: capture.capture_manifest_sha256().into(),
            source_scope_id: capture.source().captured_range()?.scope_id,
        };
        let analysis_key = analysis_capture.key()?;
        let context = IncidentAnalysisContext { capture: analysis_capture, description: (**description).clone() };
        if let Some(previous) = self.analyses.get(&analysis_key) {
            if previous.description != context.description {
                return Err(DeliveryProblem::Conflict.error());
            }
        } else {
            self.analyses.insert(analysis_key, context);
        }
        let range = range_id_for(self.contract, &original.capture)?;
        let Some(capture) = self.capture(&range, &[EvidenceFamily::OperationalSamples]).await? else {
            return Ok(None);
        };
        let Some(row) = references::selected_record(&capture, &original)? else {
            self.gaps.push(IncidentReadGap::OriginalUnavailable {
                reference: Box::new(original),
                gaps: capture.gaps().to_vec(),
            });
            return Ok(None);
        };
        let TelemetryRecord::Operational(observation) = &row.row().record else {
            return Err(invalid());
        };
        let OperationalFact::DiagnosticRevision(revision) = &observation.fact else {
            return Err(invalid());
        };
        let canonical = crate::collected_resources::incident_history(&capture)?;
        let entry = canonical
            .report
            .entries
            .iter()
            .find(|entry| entry.original.as_ref() == Some(&original))
            .ok_or_else(invalid)?;
        if !same_revision(entry, &incident.entry) {
            return Err(invalid());
        }
        for input in &description.inputs {
            let key = digest(&serde_json::to_vec(&(&original, input)).map_err(|_| invalid())?);
            self.input_captures.entry(key).or_insert_with(|| (original.clone(), input.clone()));
            if self.input_captures.len() > MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                return Err(capacity());
            }
        }
        let key = (revision.controller_version, revision.episode.id.clone(), revision.episode.revision.clone());
        if let Some(prior) = self.revisions.get(&key) {
            if prior.row != **revision || prior.reference != original {
                return Err(DeliveryProblem::Conflict.error());
            }
        } else {
            self.revisions.insert(key.clone(), SelectedRevision { row: (**revision).clone(), reference: original });
        }
        let analysis_key = (incident.analysis_id.clone(), incident.entry.revision_id.clone());
        if let Some(prior) = self.incidents.get(&analysis_key) {
            if prior != incident.as_ref() {
                return Err(DeliveryProblem::Conflict.error());
            }
        } else {
            self.incidents.insert(analysis_key, (**incident).clone());
        }
        Ok(Some((**incident).clone()))
    }
    fn predecessor_gaps(&mut self) {
        for revision in self.revisions.values() {
            if let Some(previous) = &revision.row.previous_revision {
                let key = (revision.row.controller_version, revision.row.episode.id.clone(), previous.clone());
                if !self.revisions.contains_key(&key) {
                    self.gaps.push(IncidentReadGap::PredecessorNotSelected {
                        episode_id: revision.row.episode.id.clone(),
                        controller_version: revision.row.controller_version,
                        revision: previous.clone(),
                    });
                }
            }
        }
    }
}
fn same_revision(left: &IncidentHistoryEntry, right: &IncidentHistoryEntry) -> bool {
    left.controller_version == right.controller_version
        && left.revision_id == right.revision_id
        && left.episode_id == right.episode_id
        && left.target_id == right.target_id
        && left.target_scope == right.target_scope
        && left.scope_id == right.scope_id
        && left.revision == right.revision
        && left.previous_revision == right.previous_revision
        && left.lifecycle == right.lifecycle
        && left.analysis == right.analysis
        && left.issue == right.issue
        && left.first_seen_at == right.first_seen_at
        && left.last_seen_at == right.last_seen_at
        && left.recorded_at == right.recorded_at
        && left.original == right.original
}
fn invalid() -> Error {
    DeliveryProblem::Integrity.error()
}
fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}

impl crate::WarehouseIncidentReader for EvidenceService {
    fn resources<'a>(
        &'a self,
        request: &'a PublishedResourceRequest,
        cancellation: &'a CollectionCancellation,
    ) -> crate::WarehouseResourcesTask<'a> {
        Box::pin(self.resources(request, EvidenceReadBudget::default(), cancellation))
    }
    fn incident<'a>(
        &'a self,
        resource: &'a PublishedResourceSelector,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> crate::WarehouseIncidentTask<'a> {
        Box::pin(self.resource_incident(resource, around, IncidentRequest::default_budget(), cancellation))
    }
}
