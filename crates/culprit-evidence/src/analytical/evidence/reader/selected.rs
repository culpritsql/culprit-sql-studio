//! Exact native receipt positions use the SAME publication/membership/index/page/decoder/EOF owners.
//! The result deliberately cannot impersonate a complete EvidenceCapture or OriginalProjection.
use super::*;
use std::collections::BTreeMap;

impl EvidenceReader {
    pub async fn read_records(
        &self,
        range: &CommittedRange,
        references: &[DiagnosticEvidenceRef],
        budget: EvidenceReadBudget,
        recovery: Option<&dyn RecoveryRead>,
    ) -> Result<SelectedEvidenceRecords> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(u64::from(budget.timeout_ms));
        budget.validate()?;
        if budget.recovery_payloads.is_some() {
            let recovery = recovery.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
            if recovery.id() != range.store_id() {
                return Err(DeliveryProblem::DestinationChanged.error());
            }
        }
        let catalog = self.admit(range)?;
        let requested = requested(range, references, budget)?;
        let result = tokio::time::timeout_at(deadline, self.selected(range, requested, budget, catalog, recovery))
            .await
            .map_err(|_| CollectionStop::TimedOut.error())??;
        if tokio::time::Instant::now() >= deadline {
            return Err(CollectionStop::TimedOut.error());
        }
        Ok(result)
    }

    async fn selected(
        &self,
        range: &CommittedRange,
        requested: BTreeMap<(String, u32), DiagnosticEvidenceRef>,
        budget: EvidenceReadBudget,
        catalog: Arc<dyn ProjectionCatalog>,
        recovery: Option<&dyn RecoveryRead>,
    ) -> Result<SelectedEvidenceRecords> {
        let metadata = self.metadata(range, budget, catalog.clone()).await?;
        let metrics = EvidenceReadMetrics {
            selected_rows: requested.len().try_into().map_err(|_| capacity())?,
            ..metadata.metrics().clone()
        };
        let mut selected = SelectedEvidenceRecords {
            gaps: metadata.gaps().to_vec(),
            metadata,
            requested,
            records: vec![],
            metrics,
            definition_links: vec![],
        };
        if !selected.metadata.is_read_complete() {
            return Ok(selected);
        }
        let mut state = ReadState { budget, metrics: &mut selected.metrics };
        state.query()?;
        let audit_bytes = self.store.audit_membership(range, state.byte_limit()?).await?;
        state.wire_response(audit_bytes)?;
        let mut batches = BTreeMap::<&str, BTreeSet<usize>>::new();
        for reference in selected.requested.values() {
            batches.entry(&reference.batch_id).or_default().insert(reference.row_ordinal as usize + 1);
        }
        for (id, wanted) in batches {
            let batch = range.manifest().batches.iter().find(|batch| batch.batch_id == id).ok_or_else(invalid)?;
            self.batch(
                range,
                batch,
                catalog.as_ref(),
                &mut state,
                BatchSelection::Ordinals(&wanted),
                ReadRows { records: &mut selected.records, gaps: &mut selected.gaps },
            )
            .await?;
        }
        if let Some(budget) = budget.recovery_payloads {
            let recovery = recovery.ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
            super::super::hydration::restore(
                range,
                recovery,
                budget,
                &mut selected.records,
                &mut selected.gaps,
                &mut selected.metrics,
            )
            .await?;
        }
        selected.records.sort_by(|a, b| (a.batch_id(), a.row_ordinal()).cmp(&(b.batch_id(), b.row_ordinal())));
        for reference in selected.requested.values() {
            selected.selected(reference)?;
        }
        selected.definition_links = super::super::definition_links::from_selected(range, &selected)?;
        Ok(selected)
    }
}

fn requested(
    range: &CommittedRange,
    references: &[DiagnosticEvidenceRef],
    budget: EvidenceReadBudget,
) -> Result<BTreeMap<(String, u32), DiagnosticEvidenceRef>> {
    if references.is_empty() || references.len() > budget.max_rows as usize {
        return Err(capacity());
    }
    // Local request admission only; this does not spend/refill native-response, proof or recovery-body allowances.
    culprit_core::serialization::json_size(&references, culprit_core::MAX_DERIVED_OUTPUT_BYTES).map_err(|error| {
        match error {
            culprit_core::serialization::JsonSizeError::LimitExceeded => capacity(),
            culprit_core::serialization::JsonSizeError::Serialization => invalid(),
        }
    })?;
    let manifest = range.manifest();
    let scope = manifest.source.captured_range()?.scope_id;
    let mut wanted = BTreeMap::new();
    for reference in references {
        if !reference.is_valid()
            || reference.capture != manifest.capture
            || reference.manifest_sha256 != manifest.capture_manifest_sha256
            || reference.source_scope_id != scope
        {
            return Err(invalid());
        }
        let batch = manifest
            .batches
            .iter()
            .find(|batch| batch.batch_id == reference.batch_id && batch.stream == reference.stream)
            .ok_or_else(invalid)?;
        if !batch.delivery_required || !EvidenceFamily::ALL.iter().any(|family| family.stream() == batch.stream) {
            return Err(DeliveryProblem::UnsupportedSchema.error());
        }
        let expected = batch.rows.get(reference.row_ordinal as usize).ok_or_else(invalid)?;
        if expected.record_id.as_deref() != Some(reference.record_id.as_str())
            || expected.native_key != reference.native_key
            || expected.payload_digest != reference.payload_sha256
            || expected.key.representation_version != reference.representation_version
        {
            return Err(invalid());
        }
        let key = (reference.batch_id.clone(), reference.row_ordinal);
        if wanted.get(&key).is_some_and(|previous| previous != reference) {
            return Err(invalid());
        }
        wanted.insert(key, reference.clone());
    }
    Ok(wanted)
}
