//! List authenticated published observations without a source database, native factory or current identity.
use super::{
    EvidenceService, EvidenceSession,
    session::{MetadataAccounting, ReadFence, SessionUsage, add_metrics, deduct},
};
use culprit_core::{telemetry::*, *};
use culprit_evidence::{
    analytical::evidence::{
        EvidenceCaptureMetadata, EvidenceReadBudget, EvidenceReadMetrics, PublishedResourceClock,
        PublishedResourceContent, PublishedResourceHint, PublishedResourceQuery, resource_namespace_id,
    },
    delivery::{
        object_store::ObjectRecoveryReader,
        recovery::{RecoveryRead, reopen},
    },
};
use culprit_read::cancellation::CollectionCancellation;

impl EvidenceService {
    pub async fn resources(
        &self,
        request: &PublishedResourceRequest,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<PublishedResources> {
        self.session(budget, cancellation)?.resources(request).await
    }
}
impl EvidenceSession<'_> {
    pub(super) async fn validate_resource_selector(&mut self, selector: &PublishedResourceSelector) -> Result<()> {
        self.perform(
            |_| selector.validate(),
            |service, budget, fence| async move {
                let accounting = MetadataAccounting::new(
                    service.recovery.clone(),
                    budget.max_queries,
                    budget.max_proof_bytes,
                    fence.clone(),
                );
                let recovery = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
                if resource_namespace_id(&service.contract, recovery.id())? != selector.namespace_id {
                    return Err(DeliveryProblem::DestinationChanged.error());
                }
                let metrics = EvidenceReadMetrics { proof_bytes: accounting.work()?.bytes, ..Default::default() };
                Ok(((), SessionUsage { metrics, candidate_rows: 0 }))
            },
        )
        .await
    }
    /// Pagination can share this same cumulative action with incident or selected-capture reads.
    pub async fn resources(&mut self, request: &PublishedResourceRequest) -> Result<PublishedResources> {
        Ok(self
            .selected_resources(request, None, PublishedResourceClock::Acquisition, PublishedResourceContent::All)
            .await?
            .report)
    }

    /// Window selection uses the same publication proof and metadata accounting as ordinary resource discovery.
    /// The caller can retain these exact headers for subsequent body/ancestry selection in this same session.
    pub(super) async fn selected_resources(
        &mut self,
        request: &PublishedResourceRequest,
        database: Option<IncidentDatabaseSelector>,
        clock: PublishedResourceClock,
        content: PublishedResourceContent,
    ) -> Result<SelectedPublications> {
        self.perform(
            |budget| {
                request.validate()?;
                budget.validate()
            },
            |service, budget, fence| async move {
                discover(service, request, database, clock, content, budget, fence).await
            },
        )
        .await
    }
}
pub(super) struct SelectedPublications {
    pub(super) report: PublishedResources,
    pub(super) metadata: Vec<EvidenceCaptureMetadata>,
}
async fn discover(
    service: &EvidenceService,
    selection: &PublishedResourceRequest,
    database: Option<IncidentDatabaseSelector>,
    clock: PublishedResourceClock,
    content: PublishedResourceContent,
    budget: EvidenceReadBudget,
    fence: ReadFence,
) -> Result<(SelectedPublications, SessionUsage)> {
    let accounting =
        MetadataAccounting::new(service.recovery.clone(), budget.max_queries, budget.max_proof_bytes, fence.clone());
    // The actual namespace is learned from its bounded authoritative root, never a config label or candidate row.
    let recovery = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
    let request = PublishedResourceQuery {
        selection: selection.clone(),
        recovery_store_id: recovery.id().into(),
        database,
        clock,
        content,
    };
    fence.check()?;
    let page = service.reader.resource_candidates(&service.contract, &request, budget).await?;
    let mut remaining = deduct(budget, &page.metrics)?;
    let mut metrics = page.metrics;
    let mut resources = Vec::new();
    let mut selected_metadata = Vec::new();
    let mut gaps = Vec::new();
    for hint in page.candidates {
        fence.check()?;
        let range = match reopen(&recovery, &hint.position.range_id).await {
            Ok(range) => range,
            Err(error) if error.detail == Some(ErrorDetail::Delivery(DeliveryProblem::NotReady)) => {
                gaps.push(PublishedResourceGap::OriginalUnavailable);
                continue;
            }
            Err(error) => return Err(error),
        };
        if !hint.matches_original(&range)? || !content.accepts_source(&range.manifest().source) {
            return Err(DeliveryProblem::Integrity.error());
        }
        let mut acquisition = request.selection.clone();
        if clock == PublishedResourceClock::NativeWindowCandidates {
            acquisition.window = None;
        }
        if !acquisition.accepts_acquisition(&range.manifest().collected_from, &range.manifest().collected_until) {
            gaps.push(PublishedResourceGap::AcquisitionClockBoundary);
            continue;
        }
        let mut metadata_budget = remaining;
        metadata_budget.max_proof_bytes = metadata_budget
            .max_proof_bytes
            .checked_sub(accounting.work()?.bytes)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(capacity)?;
        fence.check()?;
        let metadata = service.reader.read_metadata(&range, metadata_budget).await?;
        remaining = deduct(remaining, metadata.metrics())?;
        add_metrics(&mut metrics, metadata.metrics())?;
        if !metadata.is_read_complete() {
            gaps.push(PublishedResourceGap::NotPublished);
            continue;
        }
        resources.push(project(&hint, &metadata, &resource_namespace_id(&service.contract, recovery.id())?)?);
        selected_metadata.push(metadata);
    }
    if page.next.is_some() {
        gaps.push(PublishedResourceGap::MoreCandidates);
    }
    let metadata_work = accounting.work()?;
    metrics.proof_bytes = metrics.proof_bytes.checked_add(metadata_work.bytes).ok_or_else(capacity)?;
    let report = PublishedResources {
        as_of: selection.as_of.clone(),
        window: selection.window.clone(),
        resources,
        next: page.next,
        discovery_complete: gaps.is_empty(),
        gaps,
        candidate_rows: page.charged_candidate_rows,
        archive_work: crate::collected_context::read_metrics(&metrics),
        recovery_metadata_reads: metadata_work.reads,
    };
    Ok((
        SelectedPublications { report, metadata: selected_metadata },
        SessionUsage { metrics, candidate_rows: page.charged_candidate_rows },
    ))
}
fn project(
    hint: &PublishedResourceHint,
    metadata: &culprit_evidence::analytical::evidence::EvidenceCaptureMetadata,
    namespace_id: &str,
) -> Result<PublishedDatabaseResource> {
    let source = metadata.source();
    let database = source.target.database().ok_or_else(|| DeliveryProblem::Integrity.error())?;
    Ok(PublishedDatabaseResource {
        position: hint.position.clone(),
        logical_source_id: hint.logical_source_id.clone(),
        selector: PublishedResourceSelector {
            namespace_id: namespace_id.into(),
            database: IncidentDatabaseSelector::from_route(database)?,
        },
        engine: database.engine,
        database_name: database.database.clone(),
        connection_alias: None,
        source_name: source.source.clone(),
        collection: source.observation.collection(),
        continuity: source.continuity.clone(),
        collected_from: metadata.collected_from().into(),
        collected_until: metadata.collected_until().into(),
        native_time: native_time(source),
        coverage: if let Some(snapshot) = source.observation.snapshot() {
            PublishedResourceCoverage::Snapshot { sources: snapshot.sources.clone() }
        } else if let Some(activity) = source.observation.activity() {
            PublishedResourceCoverage::Activity { coverage: activity.coverage.clone() }
        } else {
            PublishedResourceCoverage::Unknown
        },
    })
}
fn native_time(source: &CollectionDescription) -> PublishedResourceTime {
    let bounds = if let Some(snapshot) = source.observation.snapshot() {
        Some((&snapshot.observed_from, &snapshot.observed_until))
    } else {
        source.observation.activity().map(|activity| (&activity.observed_from, &activity.observed_until))
    };
    let range = source.captured_range().ok();
    let bounds = bounds.or_else(|| match &range.as_ref()?.range {
        TelemetryRange::NativeInterval { interval } => Some((&interval.from, &interval.until)),
        TelemetryRange::TimeWindow { from, until } => Some((from, until)),
        _ => None,
    });
    match bounds {
        Some((from, until)) if utc_bounds(from, until) => {
            PublishedResourceTime::Utc { from: from.clone(), until: until.clone() }
        }
        _ => PublishedResourceTime::Unknown,
    }
}
fn utc_bounds(from: &str, until: &str) -> bool {
    ObservationBounds::new(from, until).is_some()
        && [from, until]
            .into_iter()
            .all(|value| chrono::DateTime::parse_from_rfc3339(value).is_ok_and(|at| at.offset().local_minus_utc() == 0))
}
fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}

#[cfg(test)]
mod tests;
