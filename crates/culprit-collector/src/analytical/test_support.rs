//! Local encoding lifecycle controls. Shared native corpora belong to culprit-evidence.
use culprit_evidence::{analytical::ProjectionCatalog, delivery::recovery::RecoveryRead};
use std::path::Path;
/// Wait for native completion to release the scratch lock, including destructors on another worker thread.
pub async fn released_spool(
    catalog: std::sync::Arc<dyn ProjectionCatalog>,
    recovery: std::sync::Arc<dyn RecoveryRead>,
    scratch: &Path,
) -> super::encoding::BatchEncoder {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let Ok(encoder) = super::encoding::BatchEncoder::new(catalog.clone(), recovery.clone(), scratch) {
                break encoder;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native completion did not release its spool owner")
}

use culprit_core::telemetry::*;
use culprit_evidence::analytical::test_support::*;
mod local_capture;
pub use local_capture::{LocalCaptureFixture, LocalSnapshotCase, SourceAcquisitionCase, local_snapshot_evidence_cases};
mod local_recovery;
pub use local_recovery::collected_resource_recovery;

mod qualification;
pub use qualification::{QualificationFixture, QualificationFixtureCase};

/// Actual source captures retained by one fixture journal and the production canonical verifier.
pub mod native_capture;
