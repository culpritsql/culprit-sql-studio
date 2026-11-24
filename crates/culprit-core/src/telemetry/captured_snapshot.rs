//! Compact positions bind unchanged canonical snapshot rows; the caller authenticates their stored originals.
use super::*;
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CapturedSnapshotPosition {
    pub row_ordinal: u32,
    pub payload_sha256: String,
}
impl CapturedSnapshotPosition {
    pub fn from_export(row: &ExportedRecord, row_ordinal: u32) -> Result<Self> {
        Ok(Self { row_ordinal, payload_sha256: row.payload_sha256()? })
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CapturedSnapshotOrigin {
    pub capture: DiagnosticCaptureRef,
    pub source_sha256: String,
    pub positions: Vec<CapturedSnapshotPosition>,
}
impl CapturedSnapshotOrigin {
    /// Compute a structural descriptor binding. This does not authenticate the manifest or original positions.
    pub fn captured(
        source: &CollectionDescription,
        capture: DiagnosticCaptureRef,
        positions: Vec<CapturedSnapshotPosition>,
    ) -> Result<Self> {
        Ok(Self { capture, source_sha256: source_digest(source)?, positions })
    }

    pub fn validate(
        &self,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = TelemetryRecord>,
        read_complete: bool,
    ) -> Result<()> {
        visit_rows(source, &self.capture, &self.source_sha256, &self.positions, rows, read_complete, |_| {})
    }

    pub fn references(
        &self,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = TelemetryRecord>,
        read_complete: bool,
    ) -> Result<Vec<DiagnosticEvidenceRef>> {
        references(source, &self.capture, &self.source_sha256, &self.positions, rows, read_complete)
    }
}

pub(super) fn source_digest(source: &CollectionDescription) -> Result<String> {
    serde_json::to_vec(source).map(|bytes| digest(&bytes)).map_err(|_| invalid())
}

pub(super) fn visit_rows(
    source: &CollectionDescription,
    capture: &DiagnosticCaptureRef,
    source_sha256: &str,
    positions: &[CapturedSnapshotPosition],
    rows: impl IntoIterator<Item = TelemetryRecord>,
    read_complete: bool,
    mut visit: impl FnMut(DiagnosticEvidenceRef),
) -> Result<()> {
    let stream = valid_header(source, capture, source_sha256, positions, read_complete)?;
    let mut count = 0;
    for row in rows {
        let position = positions.get(count).ok_or_else(invalid)?;
        if row.stream() != stream {
            return Err(invalid());
        }
        visit(reference(source, capture, &row, position)?);
        count += 1;
    }
    if count != positions.len() {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn references(
    source: &CollectionDescription,
    capture: &DiagnosticCaptureRef,
    source_sha256: &str,
    positions: &[CapturedSnapshotPosition],
    rows: impl IntoIterator<Item = TelemetryRecord>,
    read_complete: bool,
) -> Result<Vec<DiagnosticEvidenceRef>> {
    let mut result = Vec::new();
    visit_rows(source, capture, source_sha256, positions, rows, read_complete, |reference| result.push(reference))?;
    Ok(result)
}

fn valid_header(
    source: &CollectionDescription,
    capture: &DiagnosticCaptureRef,
    source_sha256: &str,
    positions: &[CapturedSnapshotPosition],
    read_complete: bool,
) -> Result<TelemetryStream> {
    let (stream, records) = match &source.observation {
        SourceObservation::MssqlManagedInstanceResources(history) if history.matches_source(source) => {
            (TelemetryStream::ResourceSamples, history.coverage.records)
        }
        _ => {
            let snapshot = source.observation.snapshot().ok_or_else(invalid)?;
            let [stream] = snapshot.dataset.streams() else { return Err(invalid()) };
            (*stream, snapshot.records)
        }
    };
    let mut ordinals = BTreeSet::new();
    if !capture.is_valid()
        || !valid_digest(source_sha256)
        || source_digest(source)? != source_sha256
        || source.captured_range()?.scope_id != capture.source_scope_id
        || positions.len() > CollectionBudget::upper_limits().max_snapshot_rows as usize
        || positions.iter().any(|position| {
            !valid_digest(&position.payload_sha256)
                || position.row_ordinal >= records
                || !ordinals.insert(position.row_ordinal)
        })
        || read_complete && positions.len() != records as usize
    {
        return Err(invalid());
    }
    Ok(stream)
}

fn reference(
    source: &CollectionDescription,
    capture: &DiagnosticCaptureRef,
    record: &TelemetryRecord,
    position: &CapturedSnapshotPosition,
) -> Result<DiagnosticEvidenceRef> {
    if !record.is_valid() {
        return Err(invalid());
    }
    let exported = ExportedRecord::new(source, &capture.capture, record.clone())?;
    if record != &exported.record {
        return Err(invalid());
    }
    let exported_bytes = serde_json::to_vec(&exported).map_err(|_| invalid())?;
    let record_bytes = serde_json::to_vec(record).map_err(|_| invalid())?;
    if !exported_bytes_end_with_record(&exported_bytes, &record_bytes) {
        return Err(invalid());
    }
    let payload_sha256 = digest(&exported_bytes);
    if payload_sha256 != position.payload_sha256 {
        return Err(invalid());
    }
    DiagnosticEvidenceRef::from_verified_export(
        source,
        &capture.source_scope_id,
        &capture.manifest_sha256,
        &exported,
        payload_sha256,
        position.row_ordinal,
    )
}
pub(super) fn exported_bytes_end_with_record(exported_bytes: &[u8], record_bytes: &[u8]) -> bool {
    const FIELD_PREFIX: &[u8] = b",\"record\":";
    let suffix_len = FIELD_PREFIX.len() + record_bytes.len() + 1;
    exported_bytes.len() > suffix_len
        && exported_bytes.last() == Some(&b'}')
        && &exported_bytes[exported_bytes.len() - suffix_len..exported_bytes.len() - record_bytes.len() - 1]
            == FIELD_PREFIX
        && &exported_bytes[exported_bytes.len() - record_bytes.len() - 1..exported_bytes.len() - 1] == record_bytes
}
fn invalid() -> Error {
    CollectionStop::InvalidPage.error()
}
