//! Explicit artifact reads keep the existing namespace, selected-row, EOF, session and owned CPU boundaries.
use super::{EvidenceMetadataLookup, EvidenceReadPermit, EvidenceService, EvidenceSession, session::ReadFence};
use culprit_core::{telemetry::*, *};
use culprit_evidence::analytical::evidence::{EvidenceReadBudget, RecoveryPayloadBudget, SelectedEvidenceRecords};
use culprit_read::cancellation::CollectionCancellation;
use std::{collections::BTreeMap, sync::Arc};
mod text;
mod window;
pub use window::{WindowArtifactChoice, window_artifact_choices};

type RowResult = std::result::Result<WarehouseArtifactRecord, WarehouseArtifactGap>;

impl EvidenceService {
    /// Whole immutable recovery bodies are opt-in under the existing independent payload allowance.
    pub fn artifact_budget(query: &WarehouseArtifactQuery) -> EvidenceReadBudget {
        EvidenceReadBudget {
            recovery_payloads: query.full_payloads.then(RecoveryPayloadBudget::default),
            ..Default::default()
        }
    }
    pub async fn artifacts(
        &self,
        query: &WarehouseArtifactQuery,
        budget: EvidenceReadBudget,
        token: &CollectionCancellation,
    ) -> Result<WarehouseArtifactReport> {
        self.read_artifacts(query, budget, token, None).await
    }
    pub async fn artifacts_with_permit(
        &self,
        query: &WarehouseArtifactQuery,
        budget: EvidenceReadBudget,
        token: &CollectionCancellation,
        permit: EvidenceReadPermit,
    ) -> Result<WarehouseArtifactReport> {
        self.read_artifacts(query, budget, token, Some(permit)).await
    }
    async fn read_artifacts(
        &self,
        query: &WarehouseArtifactQuery,
        mut budget: EvidenceReadBudget,
        token: &CollectionCancellation,
        permit: Option<EvidenceReadPermit>,
    ) -> Result<WarehouseArtifactReport> {
        query.validate()?;
        if query.full_payloads && budget.recovery_payloads.is_none() {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        if !query.full_payloads {
            budget.recovery_payloads = None;
        }
        let mut session = self.session(budget, token)?.retaining(permit);
        session.validate_resource_selector(&query.resource).await?;
        Box::pin(read(&mut session, query)).await
    }
}

async fn read(session: &mut EvidenceSession<'_>, query: &WarehouseArtifactQuery) -> Result<WarehouseArtifactReport> {
    let mut groups = BTreeMap::<String, Vec<DiagnosticEvidenceRef>>::new();
    for reference in &query.references {
        let key = reference.capture_ref().key()?;
        if matches!(&query.text,WarehouseArtifactTextRead::Chunk {cursor} if cursor.capture_key!=key) {
            continue;
        }
        groups.entry(key).or_default().push(reference.clone());
    }
    let mut records = BTreeMap::new();
    let mut gaps = vec![];
    let mut allowance = ArtifactAllowance::new(query)?;
    for references in groups.into_values() {
        session.check()?;
        let Some(group) = select_group(session, query, &references, &mut gaps).await? else { continue };
        if records.len() + group.admitted.len() > MAX_WAREHOUSE_ARTIFACT_RECORDS {
            return Err(capacity());
        }
        let expected = query.expected_sql_sha256.clone();
        let text_read = text::ArtifactTextRead::new(query)?;
        let before = allowance.payloads;
        let (rows, next) = session
            .analyze(move |fence| ArtifactProjection { group, expected, allowance, text_read }.run(fence))
            .await?;
        allowance = next;
        for row in rows {
            match row {
                Ok(row) => {
                    if matches!(query.text, WarehouseArtifactTextRead::Chunk { .. }) && row.artifacts.is_empty() {
                        continue;
                    }
                    if records.insert(row.reference.key()?, row).is_some() {
                        return Err(invalid());
                    }
                }
                Err(gap) => gaps.push(gap),
            }
        }
        if allowance.payloads == before {
            gaps.push(WarehouseArtifactGap::NoDefinitionPayload { reference: Box::new(references[0].clone()) });
        }
    }
    if matches!(query.text, WarehouseArtifactTextRead::Chunk { .. })
        && allowance.payloads != 1
        && !gaps.iter().any(|gap| {
            matches!(
                gap,
                WarehouseArtifactGap::OriginalNotCommitted { .. }
                    | WarehouseArtifactGap::OriginalUnavailable { .. }
                    | WarehouseArtifactGap::DefinitionUnavailable { .. }
            )
        })
    {
        return Err(DeliveryProblem::InvalidContract.error());
    }
    let mut report = WarehouseArtifactReport {
        query: query.clone(),
        records: records.into_values().collect(),
        gaps,
        qualifications: vec![t!("artifact.reader.original_scope")],
        archive_work: crate::collected_context::read_metrics(session.completed_metrics()),
    };
    if !query.full_payloads {
        report.qualifications.push(t!("artifact.reader.projection_only"));
    }
    if !report.gaps.is_empty() {
        report.qualifications.push(t!("artifact.reader.partial"));
    }
    culprit_core::serialization::json_size(&report, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| capacity())?;
    session.check()?;
    Ok(report)
}

struct ArtifactGroup {
    selected: Arc<SelectedEvidenceRecords>,
    linked: Option<Arc<SelectedEvidenceRecords>>,
    admitted: Vec<(DiagnosticEvidenceRef, Vec<DiagnosticEvidenceRef>)>,
    scope: WarehouseArtifactScope,
}
impl ArtifactGroup {
    fn owner(&self, reference: &DiagnosticEvidenceRef) -> Result<&SelectedEvidenceRecords> {
        if self.selected.contains_reference(reference)? {
            Ok(self.selected.as_ref())
        } else {
            Ok(self.linked.as_ref().ok_or_else(invalid)?.as_ref())
        }
    }
}
async fn select_group(
    session: &mut EvidenceSession<'_>,
    query: &WarehouseArtifactQuery,
    references: &[DiagnosticEvidenceRef],
    gaps: &mut Vec<WarehouseArtifactGap>,
) -> Result<Option<ArtifactGroup>> {
    let original = references[0].capture_ref();
    let metadata = match session.lookup_metadata_reference(&original).await? {
        EvidenceMetadataLookup::NotCommitted => {
            gaps.push(WarehouseArtifactGap::OriginalNotCommitted { capture: original });
            return Ok(None);
        }
        EvidenceMetadataLookup::Available(metadata) => metadata,
    };
    let scope = original_scope(metadata.source(), &query.resource)?;
    let selected = Arc::new(session.read_records(references).await?);
    let mut wanted = BTreeMap::new();
    for link in selected.definition_links() {
        match link.reference() {
            Some(reference) if !selected.contains_reference(reference)? => {
                wanted.insert(reference.key()?, reference.clone());
            }
            None => gaps.push(WarehouseArtifactGap::DefinitionUnavailable {
                from: Box::new(link.from().clone()),
                content_kind: link.kind(),
            }),
            _ => (),
        }
    }
    if references.len() + wanted.len() > MAX_WAREHOUSE_ARTIFACT_RECORDS {
        return Err(capacity());
    }
    let linked = if wanted.is_empty() {
        None
    } else {
        Some(Arc::new(session.read_records(&wanted.into_values().collect::<Vec<_>>()).await?))
    };
    let mut admitted = BTreeMap::new();
    for reference in references {
        admitted.entry(reference.key()?).or_insert((reference.clone(), vec![]));
    }
    for link in selected.definition_links() {
        let Some(reference) = link.reference() else { continue };
        let owner = if selected.contains_reference(reference)? {
            selected.as_ref()
        } else {
            linked.as_ref().ok_or_else(invalid)?.as_ref()
        };
        if link.validate_selected(owner)? {
            admitted.entry(reference.key()?).or_insert((reference.clone(), vec![])).1.push(link.from().clone());
        } else {
            gaps.push(WarehouseArtifactGap::DefinitionUnavailable {
                from: Box::new(link.from().clone()),
                content_kind: link.kind(),
            });
        }
    }
    Ok(Some(ArtifactGroup { selected, linked, admitted: admitted.into_values().collect(), scope }))
}
fn original_scope(
    source: &CollectionDescription,
    resource: &PublishedResourceSelector,
) -> Result<WarehouseArtifactScope> {
    match &source.target {
        CollectionTarget::ConfiguredConnection(_) => Err(DeliveryProblem::UnsupportedSchema.error()),
        CollectionTarget::Database(database) => {
            let selector = IncidentDatabaseSelector::from_route(database)?;
            if selector != resource.database {
                return Err(DeliveryProblem::DestinationChanged.error());
            }
            Ok(WarehouseArtifactScope::Database { database: selector, label: database.database.clone() })
        }
        CollectionTarget::Shared(shared) => {
            Ok(WarehouseArtifactScope::Shared { member_id: shared.telemetry_id()?, member_kind: shared.kind })
        }
    }
}

/// This same local output allowance moves through each owned CPU worker. It never refills read/native work.
struct ArtifactAllowance {
    bytes: usize,
    payloads: usize,
}
impl ArtifactAllowance {
    fn new(query: &WarehouseArtifactQuery) -> Result<Self> {
        let size = culprit_core::serialization::json_size(query, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| capacity())?;
        Ok(Self { bytes: MAX_DERIVED_OUTPUT_BYTES.checked_sub(size).ok_or_else(capacity)?, payloads: 0 })
    }
    fn admit(&mut self, artifact: &WarehouseDefinitionArtifact) -> Result<()> {
        let bytes = culprit_core::serialization::json_size(artifact, self.bytes).map_err(|_| capacity())?;
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(capacity)?;
        self.payloads = self
            .payloads
            .checked_add(1)
            .filter(|count| *count <= MAX_WAREHOUSE_ARTIFACT_PAYLOADS)
            .ok_or_else(capacity)?;
        Ok(())
    }
}
struct ArtifactProjection {
    group: ArtifactGroup,
    expected: Option<String>,
    allowance: ArtifactAllowance,
    text_read: text::ArtifactTextRead,
}
impl ArtifactProjection {
    fn run(mut self, fence: &ReadFence) -> Result<(Vec<RowResult>, ArtifactAllowance)> {
        let selections = std::mem::take(&mut self.group.admitted);
        let mut output = Vec::with_capacity(selections.len());
        for (reference, linked_from) in selections {
            fence.check()?;
            output.push(self.record(reference, linked_from, fence)?);
        }
        Ok((output, self.allowance))
    }
    fn record(
        &mut self,
        reference: DiagnosticEvidenceRef,
        linked_from: Vec<DiagnosticEvidenceRef>,
        fence: &ReadFence,
    ) -> Result<RowResult> {
        let owner = self.group.owner(&reference)?;
        let Some(row) = owner.selected(&reference)? else {
            return Ok(Err(WarehouseArtifactGap::OriginalUnavailable {
                reference: Box::new(reference),
                gaps: owner.gaps().to_vec(),
            }));
        };
        let metadata = owner.metadata();
        let mut artifacts = vec![];
        for field in row.row().record.definition_payloads() {
            fence.check()?;
            if !matches!(field.kind, DefinitionContentKind::Sql | DefinitionContentKind::Plan) {
                continue;
            }
            let Some(payload) =
                self.text_read.payload(&reference, field.pointer.as_ref(), field.payload, self.allowance.bytes)?
            else {
                continue;
            };
            if self.allowance.payloads >= MAX_WAREHOUSE_ARTIFACT_PAYLOADS {
                return Err(capacity());
            }
            let (plan, limitations) = if matches!(payload, WarehouseArtifactText::Complete { .. }) {
                parsed_plan(field.kind, field.payload, metadata.source().target.engine())
            } else {
                (None, vec![])
            };
            let matches_requested_sql = field.kind == DefinitionContentKind::Sql
                && match field.payload {
                    DefinitionPayload::Available { sha256, .. } => self.expected.as_ref() == Some(sha256),
                    _ => false,
                };
            let artifact = WarehouseDefinitionArtifact {
                location: field.pointer.into_owned(),
                kind: field.kind,
                label: field.kind.label(),
                payload,
                matches_requested_sql,
                plan,
                limitations,
            };
            self.allowance.admit(&artifact)?;
            artifacts.push(artifact);
        }
        Ok(Ok(WarehouseArtifactRecord {
            reference,
            engine: metadata.source().target.engine(),
            scope: self.group.scope.clone(),
            source: metadata.source().source.clone(),
            native_clock: WindowNativeClock::from_original(metadata.source()),
            collected_from: metadata.collected_from().into(),
            collected_until: metadata.collected_until().into(),
            artifacts,
            linked_from,
        }))
    }
}
fn parsed_plan(
    kind: DefinitionContentKind,
    payload: &DefinitionPayload,
    engine: Engine,
) -> (Option<Box<Plan>>, Vec<String>) {
    if kind != DefinitionContentKind::Plan {
        return (None, vec![]);
    }
    let DefinitionPayload::Available { text, .. } = payload else {
        return (None, vec![]);
    };
    match culprit_plans::parse_native_plan(text, engine) {
        Ok(mut plan) => {
            plan.raw = None;
            plan.facts_mut().constants_redacted = true;
            (Some(Box::new(plan)), vec![])
        }
        Err(_) => (None, vec![t!("artifact.reader.plan_unavailable")]),
    }
}
fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}
fn invalid() -> Error {
    DeliveryProblem::Integrity.error()
}
