//! One namespace-bound database/window action shares publication, original, ancestry and CPU ownership.
use super::*;
use crate::evidence::resources::SelectedPublications;
use culprit_core::serialization::JsonSizeError;
use culprit_evidence::analytical::evidence::PublishedResourceClock;
mod published;
mod selection;

impl EvidenceService {
    pub async fn window(
        &self,
        request: &WarehouseWindowQuery,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<WarehouseWindowReport> {
        self.read_window(request, budget, cancellation, None).await
    }
    pub async fn window_with_permit(
        &self,
        request: &WarehouseWindowQuery,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
        permit: crate::evidence::EvidenceReadPermit,
    ) -> Result<WarehouseWindowReport> {
        self.read_window(request, budget, cancellation, Some(permit)).await
    }
    async fn read_window(
        &self,
        request: &WarehouseWindowQuery,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
        permit: Option<crate::evidence::EvidenceReadPermit>,
    ) -> Result<WarehouseWindowReport> {
        request.validate()?;
        let session = self.session(budget, cancellation)?.retaining(permit);
        Box::pin(read(self, request, budget, session)).await
    }
}

/// A friendly CLI name is resolved by the SAME authenticated session as the selected window.
/// This borrowed request has no serialized authority or source connection data.
pub struct NamedWindowQuery<'a> {
    pub resource_name: &'a str,
    pub window: &'a PublishedResourceWindow,
    pub as_of: &'a str,
    pub view: WarehouseWindowView,
    pub page_rows: u32,
    pub after: Option<&'a PublishedResourceCursor>,
}
impl EvidenceService {
    pub async fn window_named(
        &self,
        request: &NamedWindowQuery<'_>,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<WarehouseWindowReport> {
        let mut session = self.session(budget, cancellation)?;
        let query = resolve_named(&mut session, request).await?;
        Box::pin(read(self, &query, budget, session)).await
    }
}

pub(in crate::evidence) async fn resolve_named(
    session: &mut EvidenceSession<'_>,
    request: &NamedWindowQuery<'_>,
) -> Result<WarehouseWindowQuery> {
    let mut selection = PublishedResourceRequest {
        as_of: request.as_of.into(),
        window: None,
        after: None,
        page_rows: MAX_PUBLISHED_RESOURCE_PAGE_ROWS,
    };
    selection.validate()?;
    if !request.window.is_valid() {
        return Err(DeliveryProblem::InvalidContract.error());
    }
    let resource = named::resolve(session, request.resource_name, &mut selection).await?;
    let query = WarehouseWindowQuery {
        resource,
        window: request.window.clone(),
        as_of: request.as_of.into(),
        view: request.view,
        page_rows: request.page_rows,
        after: request.after.cloned(),
    };
    query.validate()?;
    Ok(query)
}
async fn read<'a>(
    service: &'a EvidenceService,
    request: &WarehouseWindowQuery,
    budget: EvidenceReadBudget,
    session: EvidenceSession<'a>,
) -> Result<WarehouseWindowReport> {
    read_retaining_session(service, request, budget, session).await.map(|(report, _)| report)
}
pub(in crate::evidence) async fn read_retaining_session<'a>(
    service: &'a EvidenceService,
    request: &WarehouseWindowQuery,
    budget: EvidenceReadBudget,
    session: EvidenceSession<'a>,
) -> Result<(WarehouseWindowReport, EvidenceSession<'a>)> {
    let mut assembly = Assembly::new(session, &service.contract);
    // This is a local selected-input ceiling. Every actual read still intersects the original action budget.
    assembly.capture_limit = MAX_COLLECTED_WINDOW_INPUTS;
    assembly.session.validate_resource_selector(&request.resource).await?;
    let mut gaps = Vec::new();
    let (discovery_complete, next) = selection::discover(&mut assembly, request, &mut gaps).await?;
    let originals = selection::read_originals(&mut assembly, request, &mut gaps).await?;
    let published = published::read(&mut assembly, request, &mut gaps).await?;
    let incident_request = IncidentRequest {
        database: request.resource.database.clone(),
        from: request.window.from.clone(),
        until: request.window.until.clone(),
        recorded_before: request.as_of.clone(),
        budget,
    };
    let episodes = assembly
        .revisions
        .values()
        .map(|revision| (revision.row.episode.id.clone(), revision.row.controller_version))
        .collect();
    assembly.complete_episodes(&incident_request, episodes).await?;
    let incidents = assembly.finish(&incident_request).await?;
    let workload = match request.view {
        WarehouseWindowView::PublishedAnswers => WindowWorkload::NotRequested,
        WarehouseWindowView::CompatibleWorkload => {
            let captures = assembly.captures.values().cloned().collect::<Vec<_>>();
            let window = request.window.clone();
            let analysis = assembly
                .session
                .analyze(move |fence| {
                    let captures = captures.iter().map(Arc::as_ref).collect::<Vec<_>>();
                    crate::collected_evidence::analyze_window_with(
                        &captures,
                        &window,
                        &culprit_checks::Pipeline::builtin(),
                        &|| fence.check(),
                    )
                })
                .await?;
            WindowWorkload::Evaluated { analysis: Box::new(analysis) }
        }
    };
    let mut report = WarehouseWindowReport {
        request: request.clone(),
        discovery_complete,
        next,
        originals,
        workload,
        incidents,
        incident_limitations: vec![],
        published,
        gaps,
        archive_work: crate::collected_context::read_metrics(assembly.session.completed_metrics()),
        qualifications: vec![],
    };
    report.prepare_for_reader();
    culprit_core::serialization::json_size(&report, MAX_DERIVED_OUTPUT_BYTES).map_err(|error| match error {
        JsonSizeError::LimitExceeded => capacity(),
        JsonSizeError::Serialization => invalid(),
    })?;
    assembly.session.check()?;
    Ok((report, assembly.session))
}

fn reference(capture: &EvidenceCapture) -> Result<DiagnosticCaptureRef> {
    references::validate_capture(capture)?;
    Ok(DiagnosticCaptureRef {
        capture: capture.capture().clone(),
        manifest_sha256: capture.capture_manifest_sha256().into(),
        source_scope_id: capture.source().captured_range()?.scope_id,
    })
}
fn metadata_reference(metadata: &EvidenceCaptureMetadata) -> Result<DiagnosticCaptureRef> {
    Ok(DiagnosticCaptureRef {
        capture: metadata.capture().clone(),
        manifest_sha256: metadata.capture_manifest_sha256().into(),
        source_scope_id: metadata.source().captured_range()?.scope_id,
    })
}

fn families(source: &CollectionDescription) -> Vec<EvidenceFamily> {
    source
        .observation
        .collection()
        .streams()
        .iter()
        .filter_map(|stream| EvidenceFamily::ALL.iter().find(|family| family.stream() == *stream).copied())
        .collect()
}
