use super::*;
use culprit_core::collected_resource_analysis::CollectedSessionCensusAnalysis;

/// The existing Activity owner authenticates the original header, positions and every retained row once.
pub fn session_census(capture: &EvidenceCapture) -> Result<CollectedSessionCensusAnalysis> {
    let original = activity::CapturedActivity::new(capture)?;
    let frame = original.frame()?;
    Ok(CollectedSessionCensusAnalysis {
        context: crate::collected_context::from_capture(capture),
        report: Box::new(culprit_checks::session_census::explain_frame(&frame)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collected_evidence::RetainedEvidenceFixture;
    use culprit_evidence::analytical::{evidence::RecoveryPayloadBudget, test_support};

    #[tokio::test]
    async fn canonical_instance_census_keeps_native_server_scope_partial_or_empty_counts_and_source_free_receipts() {
        for (index, (source, rows)) in test_support::instance_activity_evidence_cases().into_iter().enumerate() {
            let expected = source.observation.activity().unwrap().clone();
            let capture = RetainedEvidenceFixture::with_sequence(source.clone(), rows, 960_000 + index as u64)
                .await
                .read_families_with_recovery(&[EvidenceFamily::ActivityObservations], RecoveryPayloadBudget::default())
                .await;
            assert!(capture.is_read_complete(), "{:?}", capture.gaps());
            let analysis = session_census(&capture).unwrap();
            assert_eq!(analysis.report.scope, culprit_core::MeasurementScope::Server);
            assert_eq!(analysis.report.context.target, source.target);
            assert_eq!(analysis.report.context.partition, source.partition);
            assert_eq!(analysis.report.context.database_writable, None);
            assert_eq!(analysis.report.native_rows, expected.native_rows);
            assert_eq!(analysis.report.observed_rows, expected.observations);
            assert_eq!(analysis.report.native_source, expected.coverage);
            assert!(
                analysis
                    .report
                    .entries
                    .iter()
                    .all(|entry| entry.scope == ActivityScopeBasis::InstanceMember && entry.reference.is_some())
            );
            assert!(
                analysis.report.context.target.database().is_none(),
                "reported DB labels never become route identities"
            );
        }
    }
}
