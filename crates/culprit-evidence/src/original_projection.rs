//! Borrowed analysis input and the privately constructed warehouse adapter.
//! The public projection interface is not a proof/capability and invents no warehouse range.
use crate::analytical::evidence::EvidenceCapture;
use culprit_core::{Result, collected_analysis::EvidenceGap, telemetry::*};

pub struct OriginalProjectionRecord<'a> {
    row: &'a ExportedRecord,
    original_ordinal: u32,
    payload_sha256: String,
}
impl<'a> OriginalProjectionRecord<'a> {
    /// A pure borrowed analysis row. This constructor confers no source or read-proof authority.
    pub fn new(row: &'a ExportedRecord, original_ordinal: u32, payload_sha256: String) -> Self {
        Self { row, original_ordinal, payload_sha256 }
    }
    pub fn row(&self) -> &'a ExportedRecord {
        self.row
    }
    pub fn row_ordinal(&self) -> u32 {
        self.original_ordinal
    }
    pub fn batch_id(&self) -> &str {
        &self.row.batch_id
    }
    pub fn native_payload_sha256(&self) -> &str {
        &self.payload_sha256
    }
}
/// Structural row qualification remains below the transport-specific proof wrappers. Every ordinal here is
/// zero-based in its original stream; destination RowReceipt ordinals are converted once by the warehouse adapter.
pub struct VerifiedOriginal<'a> {
    source: &'a CollectionDescription,
    original: DiagnosticCaptureRef,
    rows: Vec<OriginalProjectionRecord<'a>>,
    coverage: &'a ReferenceCoverage,
    captured_rows: Vec<(TelemetryStream, u32)>,
    collected_from: &'a str,
    collected_until: &'a str,
    gaps: &'a [EvidenceGap],
}
/// Borrowed pure-analysis input, not an authenticated read or source capability.
/// Engine's generic projector is private; public entry points require the existing verified carriers.
pub trait OriginalProjection {
    fn source(&self) -> &CollectionDescription;
    fn original(&self) -> &DiagnosticCaptureRef;
    fn records(&self) -> &[OriginalProjectionRecord<'_>];
    fn reference_coverage(&self) -> &ReferenceCoverage;
    fn captured_rows(&self, stream: TelemetryStream) -> Option<u32>;
    fn selected_streams(&self) -> Box<dyn Iterator<Item = TelemetryStream> + '_>;
    fn collected_from(&self) -> &str;
    fn collected_until(&self) -> &str;
    fn gaps(&self) -> &[EvidenceGap];
    fn capture(&self) -> &CaptureIdentity {
        &self.original().capture
    }
    fn capture_manifest_sha256(&self) -> &str {
        &self.original().manifest_sha256
    }
    fn is_read_complete(&self) -> bool {
        self.gaps().is_empty()
    }
    fn reference(&self, position: usize) -> Result<DiagnosticEvidenceRef> {
        let row = self.records().get(position).ok_or_else(invalid)?;
        DiagnosticEvidenceRef::from_export(self.source(), self.capture_manifest_sha256(), row.row, row.original_ordinal)
    }
    fn operational_read(&self) -> Result<CapturedOperationalRead> {
        let mut positions = Vec::new();
        let mut observations = Vec::new();
        let mut selected = self
            .records()
            .iter()
            .filter(|row| matches!(row.row.record, TelemetryRecord::Operational(_)))
            .collect::<Vec<_>>();
        selected.sort_by_key(|row| row.original_ordinal);
        for row in selected {
            let TelemetryRecord::Operational(observation) = &row.row.record else { return Err(invalid()) };
            positions.push(CapturedSnapshotPosition::from_export(row.row, row.original_ordinal)?);
            observations.push(observation.clone());
        }
        CapturedOperationalRead::captured(
            self.source().clone(),
            self.original().clone(),
            positions,
            observations,
            self.is_read_complete(),
        )
    }
}
impl OriginalProjection for VerifiedOriginal<'_> {
    fn source(&self) -> &CollectionDescription {
        self.source
    }
    fn original(&self) -> &DiagnosticCaptureRef {
        &self.original
    }
    fn records(&self) -> &[OriginalProjectionRecord<'_>] {
        &self.rows
    }
    fn reference_coverage(&self) -> &ReferenceCoverage {
        self.coverage
    }
    fn captured_rows(&self, stream: TelemetryStream) -> Option<u32> {
        self.captured_rows.iter().find_map(|(known, rows)| (*known == stream).then_some(*rows))
    }
    fn selected_streams(&self) -> Box<dyn Iterator<Item = TelemetryStream> + '_> {
        Box::new(self.captured_rows.iter().map(|(stream, _)| *stream))
    }
    fn collected_from(&self) -> &str {
        self.collected_from
    }
    fn collected_until(&self) -> &str {
        self.collected_until
    }
    fn gaps(&self) -> &[EvidenceGap] {
        self.gaps
    }
}
impl EvidenceCapture {
    /// Existing warehouse intake remains responsible for publication, row proof, EOF and recovery authentication.
    pub fn verified_original(&self) -> Result<VerifiedOriginal<'_>> {
        let source = self.source();
        let original = DiagnosticCaptureRef {
            capture: self.capture().clone(),
            manifest_sha256: self.capture_manifest_sha256().into(),
            source_scope_id: source.captured_range()?.scope_id,
        };
        if !original.is_valid() {
            return Err(invalid());
        }
        let rows = self
            .records()
            .iter()
            .map(|row| {
                Ok(OriginalProjectionRecord {
                    row: row.row(),
                    original_ordinal: row.row_ordinal().checked_sub(1).ok_or_else(invalid)?,
                    payload_sha256: row.native_payload_sha256().into(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let captured_rows = self
            .families()
            .iter()
            .map(|family| {
                let stream = family.stream();
                Ok((stream, self.captured_rows(stream).ok_or_else(invalid)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(VerifiedOriginal {
            source,
            original,
            rows,
            coverage: self.reference_coverage(),
            captured_rows,
            collected_from: self.collected_from(),
            collected_until: self.collected_until(),
            gaps: self.gaps(),
        })
    }
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
