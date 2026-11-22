//! Pure borrowed analysis adapter over the existing canonical Outbox proof carrier.
use crate::outbox::SynthesisCapture;
use culprit_core::{Result, collected_analysis::EvidenceGap, telemetry::*};
use culprit_evidence::original_projection::{OriginalProjection, OriginalProjectionRecord};
pub struct LocalOriginalProjection<'a> {
    capture: &'a SynthesisCapture,
    rows: Vec<OriginalProjectionRecord<'a>>,
}
impl SynthesisCapture {
    pub fn verified_original(&self) -> Result<LocalOriginalProjection<'_>> {
        if self.records().len() != self.row_ordinals().len() {
            return Err(DeliveryProblem::Integrity.error());
        }
        let rows = self
            .records()
            .iter()
            .zip(self.row_ordinals())
            .map(|(row, ordinal)| Ok(OriginalProjectionRecord::new(row, *ordinal, row.payload_sha256()?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(LocalOriginalProjection { capture: self, rows })
    }
}
impl OriginalProjection for LocalOriginalProjection<'_> {
    fn source(&self) -> &CollectionDescription {
        self.capture.source()
    }
    fn original(&self) -> &DiagnosticCaptureRef {
        self.capture.original()
    }
    fn records(&self) -> &[OriginalProjectionRecord<'_>] {
        &self.rows
    }
    fn reference_coverage(&self) -> &ReferenceCoverage {
        self.capture.reference_coverage()
    }
    fn captured_rows(&self, stream: TelemetryStream) -> Option<u32> {
        self.capture.captured_rows(stream)
    }
    fn selected_streams(&self) -> Box<dyn Iterator<Item = TelemetryStream> + '_> {
        Box::new(
            self.source()
                .observation
                .collection()
                .streams()
                .iter()
                .copied()
                .filter(|stream| self.captured_rows(*stream).is_some()),
        )
    }
    fn collected_from(&self) -> &str {
        self.capture.collected_from()
    }
    fn collected_until(&self) -> &str {
        self.capture.collected_until()
    }
    fn gaps(&self) -> &[EvidenceGap] {
        &[]
    }
}
