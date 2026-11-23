//! One validated manifest snapshot yields locators only for exact source-owned native definition references.
use super::*;
use crate::delivery::{evidence::BatchManifest, recovery::CommittedRange};
use culprit_core::{Result, telemetry::*};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct EvidenceDefinitionLink {
    from: DiagnosticEvidenceRef,
    kind: DefinitionContentKind,
    reference: Option<DiagnosticEvidenceRef>,
    expected: StatementReferences,
}
impl EvidenceDefinitionLink {
    pub fn from(&self) -> &DiagnosticEvidenceRef {
        &self.from
    }
    pub fn kind(&self) -> DefinitionContentKind {
        self.kind
    }
    pub fn reference(&self) -> Option<&DiagnosticEvidenceRef> {
        self.reference.as_ref()
    }
    /// The caller first pays/authenticates the selected linked body through the SAME ordinary row reader.
    /// No equal ID, warehouse row, pointer or cache entry substitutes for the original native namespace.
    pub fn validate_selected(&self, selected: &SelectedEvidenceRecords) -> Result<bool> {
        let reference = self.reference.as_ref().ok_or_else(invalid)?;
        let Some(row) = selected.selected(reference)? else { return Ok(false) };
        let source = selected.metadata().source();
        match (&row.row().record, self.kind) {
            (TelemetryRecord::QueryDefinition(definition), DefinitionContentKind::Sql) => self
                .expected
                .bind_query_definition(source, Some(definition))
                .map(|value| value.is_some())
                .map_err(|_| invalid()),
            (TelemetryRecord::PlanDefinition(definition), DefinitionContentKind::Plan) => self
                .expected
                .bind_plan_definition(source, Some(definition))
                .map(|value| value.is_some())
                .map_err(|_| invalid()),
            _ => Err(invalid()),
        }
    }
}
pub(super) fn from_selected(
    range: &CommittedRange,
    selected: &SelectedEvidenceRecords,
) -> Result<Vec<EvidenceDefinitionLink>> {
    let mut wanted = Vec::new();
    for from in selected.references() {
        let Some(row) = selected.selected(from)? else { continue };
        let Some(key) = row.row().record.measurement_key() else { continue };
        key.references.bind_query_definition(&range.manifest().source, None).map_err(|_| invalid())?;
        key.references.bind_plan_definition(&range.manifest().source, None).map_err(|_| invalid())?;
        for (kind, native) in
            [(DefinitionContentKind::Sql, &key.references.query), (DefinitionContentKind::Plan, &key.references.plan)]
        {
            if let Some(native) = native {
                wanted.push((from.clone(), kind, native.definition_id.clone(), key.references.clone()));
            }
        }
    }
    if wanted.is_empty() {
        return Ok(vec![]);
    }
    // At most two links per requested native measurement; never scan the full manifest once per selected row.
    let mut positions = BTreeMap::new();
    for batch in &range.manifest().batches {
        if !batch.delivery_required
            || !matches!(batch.stream, TelemetryStream::QueryDefinitions | TelemetryStream::PlanDefinitions)
        {
            continue;
        }
        for (ordinal, row) in batch.rows.iter().enumerate() {
            let key = match &row.native_key {
                NativeRecordIdentity::QueryDefinition(id) if batch.stream == TelemetryStream::QueryDefinitions => {
                    (false, id.clone())
                }
                NativeRecordIdentity::PlanDefinition(id) if batch.stream == TelemetryStream::PlanDefinitions => {
                    (true, id.clone())
                }
                _ => return Err(invalid()),
            };
            if positions.insert(key, (batch, ordinal)).is_some() {
                return Err(invalid());
            }
        }
    }
    wanted
        .into_iter()
        .map(|(from, kind, id, expected)| {
            let key = (kind == DefinitionContentKind::Plan, id.unwrap_or_default());
            let reference = positions.get(&key).map(|&(batch, ordinal)| locator(range, batch, ordinal)).transpose()?;
            Ok(EvidenceDefinitionLink { from, kind, reference, expected })
        })
        .collect()
}
fn locator(range: &CommittedRange, batch: &BatchManifest, ordinal: usize) -> Result<DiagnosticEvidenceRef> {
    let manifest = range.manifest();
    let row = batch.rows.get(ordinal).ok_or_else(invalid)?;
    let reference = DiagnosticEvidenceRef {
        capture: manifest.capture.clone(),
        manifest_sha256: manifest.capture_manifest_sha256.clone(),
        source_scope_id: manifest.source.captured_range()?.scope_id,
        stream: batch.stream,
        native_key: row.native_key.clone(),
        batch_id: batch.batch_id.clone(),
        record_id: row.record_id.clone().ok_or_else(invalid)?,
        payload_sha256: row.payload_digest.clone(),
        representation_version: row.key.representation_version,
        row_ordinal: u32::try_from(ordinal).map_err(|_| invalid())?,
    };
    if !reference.is_valid() {
        return Err(invalid());
    }
    Ok(reference)
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
