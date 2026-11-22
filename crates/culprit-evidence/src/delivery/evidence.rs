//! The delivery manifest carries exact row membership, independently from physical submission attempts.
use super::contract::*;
use culprit_core::{Result, telemetry::*};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One canonical identity for a committed capture inside its exact delivery contract.
pub fn range_id_for(contract: &Contract, capture: &CaptureIdentity) -> Result<String> {
    serde_json::to_vec(&(contract, capture)).map(|bytes| digest(&bytes)).map_err(|_| DeliveryProblem::Integrity.error())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct RecordKey {
    pub batch_id: String,
    /// One-based position in the original sealed stream, independent of semantic resolution.
    pub row_ordinal: u32,
    pub observation_id: String,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub representation_version: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RowReceipt {
    pub key: RecordKey,
    pub record_id: Option<String>,
    pub payload_digest: String,
    pub native_key: NativeRecordIdentity,
    pub critical_fields_valid: bool,
}
impl RowReceipt {
    pub fn from_export(row: &ExportedRecord, row_ordinal: u32) -> Result<Self> {
        let revision = row.capture.revision.as_ref().ok_or_else(|| DeliveryProblem::Integrity.error())?;
        if row.format_version != EXPORT_FORMAT_VERSION
            || row.representation_version != REPRESENTATION_VERSION
            || !row.record.is_valid()
            || row.batch_id != batch_id(&row.capture.observation_id, row.record.stream())?
            || row_ordinal == 0
            || row.record_id.as_deref().is_some_and(|id| !valid_digest(id))
            || row.record.measurement_key().is_some_and(|key| key.references.is_resolved() != row.record_id.is_some())
            || row.record_id.is_none() && row.record.measurement_key().is_none_or(|key| key.references.is_resolved())
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        let positive = |value: &str| {
            value.parse::<u64>().ok().filter(|value| *value > 0).ok_or_else(|| DeliveryProblem::Integrity.error())
        };
        Ok(Self {
            key: RecordKey {
                batch_id: row.batch_id.clone(),
                row_ordinal,
                observation_id: row.capture.observation_id.clone(),
                owner_epoch: positive(&revision.owner_epoch)?,
                sequence: positive(&revision.sequence)?,
                representation_version: row.representation_version,
            },
            record_id: row.record_id.clone(),
            native_key: row.record.native_identity(),
            payload_digest: row.payload_sha256().map_err(|_| DeliveryProblem::Integrity.error())?,
            critical_fields_valid: true,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BatchManifest {
    pub batch_id: String,
    pub stream: TelemetryStream,
    pub object: String,
    pub sha256: String,
    pub bytes: u64,
    pub rows: Vec<RowReceipt>,
    pub delivery_required: bool,
}
impl BatchManifest {
    /// Verify a one-based row position against both its native source and its exact committed receipt.
    pub fn validate_row(
        &self,
        source: &CollectionDescription,
        capture: &CaptureIdentity,
        ordinal: usize,
        row: &ExportedRecord,
    ) -> Result<RowReceipt> {
        let invalid = || DeliveryProblem::Integrity.error();
        let expected = ordinal.checked_sub(1).and_then(|index| self.rows.get(index)).ok_or_else(invalid)?;
        let receipt = RowReceipt::from_export(row, ordinal.try_into().map_err(|_| invalid())?)?;
        if row.capture != *capture
            || row.batch_id != self.batch_id
            || row.record.stream() != self.stream
            || ExportedRecord::new(source, capture, row.record.clone())? != *row
            || *expected != receipt
        {
            return Err(invalid());
        }
        Ok(receipt)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StreamAdvance {
    pub stream_key: String,
    pub previous: Option<String>,
    pub next: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RangeManifest {
    pub contract: Contract,
    pub range_id: String,
    pub capture: CaptureIdentity,
    /// Digest of the original sealed CollectionManifest bytes, attested at preparation.
    pub capture_manifest_sha256: String,
    /// Admitted investigation ownership survives native failure and is independent of the native source result.
    pub investigation: Option<DiagnosticInvestigationLineage>,
    pub source: CollectionDescription,
    /// Original acquisition integrity and semantic definition coverage; neither is inferred from row IDs.
    pub reference_coverage: ReferenceCoverage,
    /// Collector clock readings, separate from native measurement and observation times.
    pub collected_from: String,
    pub collected_until: String,
    pub source_queries: u32,
    pub read_metrics: Option<ReadWorkMetrics>,
    pub batches: Vec<BatchManifest>,
    pub advances: Vec<StreamAdvance>,
}
impl RangeManifest {
    pub fn receipts_digest(&self) -> Result<String> {
        let rows = self
            .expected_receipts()
            .map(|receipt| (&receipt.key, receipt))
            .collect::<std::collections::BTreeMap<_, _>>();
        Ok(digest(
            &serde_json::to_vec(&rows.into_values().collect::<Vec<_>>())
                .map_err(|_| DeliveryProblem::Integrity.error())?,
        ))
    }
    pub fn validate(&self) -> Result<()> {
        self.contract.validate()?;
        self.validate_investigation()?;
        if chrono::DateTime::parse_from_rfc3339(&self.collected_from).is_err()
            || chrono::DateTime::parse_from_rfc3339(&self.collected_until).is_err()
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        if self.read_metrics.as_ref().is_some_and(|metrics| !metrics.is_valid() || metrics.has_unconfirmed_work()) {
            return Err(DeliveryProblem::Integrity.error());
        }
        if !valid_digest(&self.range_id)
            || !valid_digest(&self.capture_manifest_sha256)
            || self.capture.revision.is_none()
            || self.batches.len() > MAX_BATCHES
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        let identity = range_id_for(&self.contract, &self.capture)?;
        if identity != self.range_id {
            return Err(DeliveryProblem::Integrity.error());
        }
        let revision = self.capture.revision.as_ref().ok_or_else(|| DeliveryProblem::Integrity.error())?;
        let epoch = revision
            .owner_epoch
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| DeliveryProblem::Integrity.error())?;
        let sequence = revision
            .sequence
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| DeliveryProblem::Integrity.error())?;
        if !bounded_id(&self.capture.observation_id)
            || !bounded_id(&revision.outbox_id)
            || self.batches.len() != self.source.observation.collection().streams().len()
            || !self.source.observation.covers_request()
            || !self.source.partition_is_consistent()
            || !self.reference_coverage.is_valid()
            || !self.reference_coverage.integrity_complete
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        self.source.captured_range()?;
        let mut batches = BTreeSet::new();
        let mut rows = BTreeSet::new();
        let mut native_rows = std::collections::HashSet::new();
        let mut semantic_rows = BTreeSet::new();
        let mut bytes = 0u64;
        for batch in &self.batches {
            bytes = bytes.checked_add(batch.bytes).ok_or_else(|| DeliveryProblem::Capacity.error())?;
            if batch.batch_id != batch_id(&self.capture.observation_id, batch.stream)?
                || !self.source.observation.collection().streams().contains(&batch.stream)
                || !valid_digest(&batch.batch_id)
                || !valid_digest(&batch.sha256)
                || !bounded_id(&batch.object)
                || !batches.insert(&batch.batch_id)
                || batch.delivery_required != (batch.stream.delivery_required())
            {
                return Err(DeliveryProblem::Integrity.error());
            }
            for (index, row) in batch.rows.iter().enumerate() {
                if row.key.batch_id != batch.batch_id
                    || row.key.row_ordinal as usize != index + 1
                    || row.key.observation_id != self.capture.observation_id
                    || row.key.owner_epoch != epoch
                    || row.key.sequence != sequence
                    || row.record_id.as_deref().is_some_and(|id| !valid_digest(id))
                    || row.record_id.is_none()
                        && (self.reference_coverage.complete
                            || !matches!(
                                &row.native_key,
                                NativeRecordIdentity::Runtime(_) | NativeRecordIdentity::Waits { .. }
                            ))
                    || row.record_id.as_ref().is_some_and(|id| !semantic_rows.insert(id))
                    || !valid_digest(&row.payload_digest)
                    || !row.critical_fields_valid
                    || row.key.representation_version == 0
                    || row.key.representation_version != REPRESENTATION_VERSION
                    || !row.native_key.is_valid()
                    || row.native_key.stream() != batch.stream
                    || !native_rows.insert(&row.native_key)
                    || !rows.insert(&row.key)
                {
                    return Err(DeliveryProblem::Integrity.error());
                }
            }
        }
        if bytes > MAX_RANGE_BYTES || rows.len() > MAX_RANGE_RECORDS {
            return Err(DeliveryProblem::Capacity.error());
        }
        let mut checkpoints = BTreeSet::new();
        for advance in &self.advances {
            if !bounded_id(&advance.stream_key)
                || !checkpoints.insert(&advance.stream_key)
                || advance.previous.as_ref().is_some_and(|cursor| cursor.len() > MAX_CURSOR_BYTES)
                || advance.next.is_empty()
                || advance.next.len() > MAX_CURSOR_BYTES
            {
                return Err(DeliveryProblem::Integrity.error());
            }
        }
        if serde_json::to_vec(self).map_err(|_| DeliveryProblem::Integrity.error())?.len() > MAX_MANIFEST_BYTES {
            return Err(DeliveryProblem::Capacity.error());
        }
        Ok(())
    }
    fn validate_investigation(&self) -> Result<()> {
        let Some(owner) = &self.investigation else { return Ok(()) };
        let intent = owner.intent_for_source(&self.source).map_err(|_| DeliveryProblem::Integrity.error())?;
        let request = CollectionRequest {
            subject: match &self.source.target {
                CollectionTarget::Database(_) => CollectionSubject::Database,
                CollectionTarget::Shared(resource) => CollectionSubject::Shared { resource: resource.clone() },
            },
            selection: intent.selection()?,
            budget: intent.budget.clone(),
        };
        if !owner.matches_request(&request)
            || !self.source.matches_request(&request)
            || !intent.matches_result_source(&self.source)
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        Ok(digest(&serde_json::to_vec(self).map_err(|_| DeliveryProblem::Integrity.error())?))
    }
    pub fn expected_receipts(&self) -> impl Iterator<Item = &RowReceipt> {
        self.batches.iter().filter(|batch| batch.delivery_required).flat_map(|batch| batch.rows.iter())
    }
}
