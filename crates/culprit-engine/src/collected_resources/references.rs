//! Original archive receipts share one validation path across native point and Activity explanations.
use super::*;
use culprit_evidence::analytical::evidence::VerifiedEvidenceRecord;

pub(crate) fn validate_capture(capture: &EvidenceCapture) -> Result<()> {
    if !valid_digest(capture.range_id())
        || !valid_digest(capture.recovery_manifest_sha256())
        || !valid_digest(capture.capture_manifest_sha256())
        || ObservationBounds::new(capture.collected_from(), capture.collected_until()).is_none()
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn from_record(capture: &EvidenceCapture, record: &VerifiedEvidenceRecord) -> Result<DiagnosticEvidenceRef> {
    let ordinal = original_ordinal(capture, record)?;
    DiagnosticEvidenceRef::from_export(capture.source(), capture.capture_manifest_sha256(), record.row(), ordinal)
}

/// An exact historical pointer must match the independently authenticated original, never a latest same-name source.
pub(crate) fn validate_capture_reference(capture: &EvidenceCapture, reference: &DiagnosticCaptureRef) -> Result<()> {
    validate_capture(capture)?;
    if !reference.is_valid()
        || capture.capture() != &reference.capture
        || capture.capture_manifest_sha256() != reference.manifest_sha256
        || capture.source().captured_range()?.scope_id != reference.source_scope_id
    {
        return Err(invalid());
    }
    Ok(())
}

/// A read gap may withhold this row; an actually returned different row at its original position is an integrity error.
pub(crate) fn selected_record<'a>(
    capture: &'a EvidenceCapture,
    reference: &DiagnosticEvidenceRef,
) -> Result<Option<&'a VerifiedEvidenceRecord>> {
    if !reference.is_valid() {
        return Err(invalid());
    }
    validate_capture_reference(capture, &reference.capture_ref())?;
    if capture.captured_rows(reference.stream).is_none_or(|count| reference.row_ordinal >= count) {
        return Err(invalid());
    }
    for record in capture.records() {
        if record.batch_id() == reference.batch_id && record.row_ordinal().checked_sub(1) == Some(reference.row_ordinal)
        {
            if from_record(capture, record)? != *reference {
                return Err(invalid());
            }
            return Ok(Some(record));
        }
    }
    Ok(None)
}

pub(super) fn captured_read(capture: &EvidenceCapture) -> Result<CapturedReadRef> {
    validate_capture(capture)?;
    let streams = capture
        .families()
        .iter()
        .map(|family| {
            let stream = family.stream();
            let rows = capture.captured_rows(stream).ok_or_else(invalid)?;
            Ok(CapturedStreamRows { stream, rows })
        })
        .collect::<Result<Vec<_>>>()?;
    CapturedReadRef::new(capture.source(), capture.capture().clone(), capture.capture_manifest_sha256().into(), streams)
}

pub(crate) fn analysis_reference(
    original: &CapturedReadBinding<'_>,
    record: &culprit_core::collected_analysis::CollectedAnalysisRecord,
) -> Result<CapturedRecordReference> {
    let ordinal = record.row_ordinal.checked_sub(1).ok_or_else(invalid)?;
    if record.batch_id != record.row.batch_id || record.native_payload_sha256 != record.row.payload_sha256()? {
        return Err(invalid());
    }
    original.reference(&record.row, ordinal)
}

fn original_ordinal(capture: &EvidenceCapture, record: &VerifiedEvidenceRecord) -> Result<u32> {
    let row = record.row();
    let ordinal = record.row_ordinal().checked_sub(1).ok_or_else(invalid)?;
    if row.capture != *capture.capture()
        || row.batch_id != record.batch_id()
        || row.payload_sha256()? != record.native_payload_sha256()
    {
        return Err(invalid());
    }
    Ok(ordinal)
}

pub(super) fn operational_read(capture: &EvidenceCapture, dataset: SnapshotDataset) -> Result<CapturedOperationalRead> {
    validate_capture(capture)?;
    CapturedState::sources(capture, dataset)?;
    let mut selected = capture.records().iter().collect::<Vec<_>>();
    selected.sort_by_key(|record| record.row_ordinal());
    let mut positions = Vec::new();
    let mut observations = Vec::new();
    for record in selected {
        let reference = from_record(capture, record)?;
        reference.validate_canonical_record(capture.source(), &record.row().record)?;
        let TelemetryRecord::Operational(row) = &record.row().record else { return Err(invalid()) };
        positions.push(CapturedSnapshotPosition::from_export(record.row(), reference.row_ordinal)?);
        observations.push(row.clone());
    }
    CapturedOperationalRead::captured(
        capture.source().clone(),
        DiagnosticCaptureRef {
            capture: capture.capture().clone(),
            manifest_sha256: capture.capture_manifest_sha256().into(),
            source_scope_id: capture.source().captured_range()?.scope_id,
        },
        positions,
        observations,
        capture.is_read_complete(),
    )
}
