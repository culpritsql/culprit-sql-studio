//! Local sealed-file preparation; the shared crate owns immutable manifest and receipt semantics.
use culprit_core::{Result, telemetry::*};
use culprit_evidence::delivery::{contract::*, evidence::*};
use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
/// Created only from a ready outbox capture which has passed the journal's artifact audit.
pub struct PreparedRange {
    pub(crate) manifest: Arc<RangeManifest>,
    pub(crate) directory: PathBuf,
}
impl PreparedRange {
    pub fn manifest(&self) -> &RangeManifest {
        &self.manifest
    }
    pub(crate) fn from_validated(
        directory: &Path,
        manifest: &CollectionManifest,
        contract: Contract,
        advances: Vec<StreamAdvance>,
    ) -> Result<Self> {
        if manifest.status != CollectionStatus::Complete
            || !manifest.references.is_valid()
            || !manifest.references.integrity_complete
        {
            return Err(DeliveryProblem::NotReady.error());
        }
        manifest.request.validate().map_err(|_| DeliveryProblem::Integrity.error())?;
        manifest.investigation_intent().map_err(|_| DeliveryProblem::Integrity.error())?;
        let mut native_manifest_bytes = Vec::new();
        crate::durable_fs::regular_file(&directory.join("manifest.json"))
            .map_err(|_| DeliveryProblem::Storage.error())?
            .take(128 * 1024 + 1)
            .read_to_end(&mut native_manifest_bytes)
            .map_err(|_| DeliveryProblem::Storage.error())?;
        if native_manifest_bytes.len() > 128 * 1024
            || serde_json::from_slice::<serde_json::Value>(&native_manifest_bytes)
                .map_err(|_| DeliveryProblem::Integrity.error())?
                != serde_json::to_value(manifest).map_err(|_| DeliveryProblem::Integrity.error())?
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        let mut batches = Vec::new();
        let mut total_rows = 0usize;
        for file in &manifest.files {
            let mut input = BufReader::new(
                crate::durable_fs::regular_file(&directory.join(&file.file))
                    .map_err(|_| DeliveryProblem::Storage.error())?
                    .take(u64::from(file.bytes) + 1),
            );
            let mut rows = Vec::new();
            let mut line = Vec::new();
            loop {
                line.clear();
                let read = (&mut input)
                    .take(MAX_RECORD_BYTES as u64 + 1)
                    .read_until(b'\n', &mut line)
                    .map_err(|_| DeliveryProblem::Storage.error())?;
                if read == 0 {
                    break;
                }
                if line.len() > MAX_RECORD_BYTES || line.last() != Some(&b'\n') {
                    return Err(DeliveryProblem::Integrity.error());
                }
                let row: ExportedRecord =
                    serde_json::from_slice(&line).map_err(|_| DeliveryProblem::Integrity.error())?;
                if row.batch_id != file.batch_id || row.capture != manifest.capture || !row.record.is_valid() {
                    return Err(DeliveryProblem::Integrity.error());
                }
                let ordinal = u32::try_from(rows.len() + 1).map_err(|_| DeliveryProblem::Capacity.error())?;
                rows.push(RowReceipt::from_export(&row, ordinal)?);
                total_rows += 1;
                if total_rows > MAX_RANGE_RECORDS {
                    return Err(DeliveryProblem::Capacity.error());
                }
            }
            if rows.len() != file.rows as usize {
                return Err(DeliveryProblem::Integrity.error());
            }
            let sha = file.sha256.clone().ok_or_else(|| DeliveryProblem::Integrity.error())?;
            batches.push(BatchManifest {
                batch_id: file.batch_id.clone(),
                stream: file.stream,
                object: format!("payloads/{sha}.ndjson"),
                sha256: sha,
                bytes: u64::from(file.bytes),
                rows,
                delivery_required: file.stream.delivery_required(),
            });
        }
        let range_id = range_id_for(&contract, &manifest.capture)?;
        let value = RangeManifest {
            contract,
            range_id,
            capture: manifest.capture.clone(),
            capture_manifest_sha256: digest(&native_manifest_bytes),
            investigation: manifest.investigation.clone(),
            source: manifest.description.clone().ok_or_else(|| DeliveryProblem::Integrity.error())?,
            reference_coverage: manifest.references.clone(),
            collected_from: manifest.started_at.clone(),
            collected_until: manifest.finished_at.clone().ok_or_else(|| DeliveryProblem::NotReady.error())?,
            source_queries: manifest.source_queries,
            read_metrics: manifest.read_metrics.clone(),
            batches,
            advances,
        };
        value.validate()?;
        Ok(Self { manifest: Arc::new(value), directory: directory.into() })
    }
    pub(crate) fn batch_path(&self, batch: &BatchManifest) -> PathBuf {
        self.directory.join(batch.stream.file_name())
    }
}
