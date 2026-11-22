use super::*;
use crate::{DiagnosticAncestryRole, DiagnosticAncestryTarget};
#[test]
fn one_ancestry_walk_preserves_original_positions_and_independent_first_latest_peak_and_report_roles() {
    let mut row = revision();
    let later = reference("later-native-frame");
    row.episode.latest = later.clone();
    row.episode.peak = Some(later.clone());
    row.inputs.push(later.clone());
    let mut result = reference("actual-page-detail");
    result.stream = TelemetryStream::OperationalSamples;
    result.batch_id = batch_id(&result.capture.observation_id, result.stream).unwrap();
    result.native_key =
        NativeRecordIdentity::Operational { source: "page wait targets".into(), key: digest(b"original page point") };
    result.row_ordinal = 17;
    row.episode.report = Some(DiagnosticReport::PageWait { observation: result.clone(), blocking_contexts: vec![] });
    row.episode.analysis = DiagnosticAnalysisStatus::Analyzed;
    row.inputs.push(result.clone());
    row.id = row.expected_id().unwrap();
    assert!(row.is_valid());
    let links = row.referenced_evidence().unwrap();
    let selected = |role, reference: &DiagnosticEvidenceRef| {
        links.iter().any(|link| {
            link.role == role && matches!(link.target,DiagnosticAncestryTarget::Record(actual) if actual == reference)
        })
    };
    assert!(selected(DiagnosticAncestryRole::FirstObservation, &row.episode.first));
    assert!(selected(DiagnosticAncestryRole::LatestObservation, &later));
    assert!(selected(DiagnosticAncestryRole::PeakObservation, &later));
    assert!(selected(DiagnosticAncestryRole::ReportedEvidence, &result));
    assert!(selected(DiagnosticAncestryRole::Input, &result));
    assert_eq!(result.row_ordinal, 17, "the walk does not reindex original stream positions");
}
#[test]
fn qualified_recovery_names_its_empty_capture_without_manufacturing_a_representative_row() {
    let mut row = revision();
    let empty = reference("qualified-empty-original").capture_ref();
    row.episode.lifecycle = DiagnosticLifecycle::Resolved;
    row.episode.analysis = DiagnosticAnalysisStatus::Unresolved;
    row.episode.issue = Some(DiagnosticAnalysisIssue::EvidenceUnavailable);
    row.episode.recovery = Some(DiagnosticRecoveryProof::CompleteScope { capture: empty.clone() });
    row.id = row.expected_id().unwrap();
    assert!(row.is_valid());
    let links = row.referenced_evidence().unwrap();
    assert!(links.iter().any(|link| link.role == DiagnosticAncestryRole::Recovery
        && matches!(link.target,DiagnosticAncestryTarget::Capture(capture) if capture == &empty)));
    assert!(!links.iter().any(
        |link| matches!(link.target,DiagnosticAncestryTarget::Record(reference) if reference.capture_ref() == empty)
    ));
    row.id = digest(b"tampered revision body");
    assert!(row.referenced_evidence().is_err());
}
