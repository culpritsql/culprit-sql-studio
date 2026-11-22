//! Jobs use the original native descriptor and retained rows; no current definition or worker is consulted.
use super::*;
use culprit_core::collected_resource_analysis::CollectedJobsAnalysis;
use culprit_evidence::original_projection::OriginalProjection;
pub fn jobs(capture: &EvidenceCapture) -> Result<CollectedJobsAnalysis> {
    // Preserve the warehouse entry's selected-family contract before its shared borrowed analysis boundary.
    CapturedState::sources(capture, SnapshotDataset::Jobs)?;
    let original = capture.verified_original()?;
    let snapshot = from_original(&original)?;
    let report = culprit_checks::jobs::explain(&snapshot).map_err(|_| invalid())?;
    Ok(CollectedJobsAnalysis { context: crate::collected_context::from_capture(capture), report })
}
pub(crate) fn from_original(original: &dyn OriginalProjection) -> Result<JobsSnapshot> {
    if original.source().observation.snapshot().is_none_or(|snapshot| snapshot.dataset != SnapshotDataset::Jobs) {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    let rows = original
        .records()
        .iter()
        .map(|row| {
            let TelemetryRecord::Operational(native) = &row.row().record else {
                return Err(invalid());
            };
            Ok(native.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    JobsSnapshot::from_source(original.source(), rows, original.is_read_complete()).map_err(|_| invalid())
}
