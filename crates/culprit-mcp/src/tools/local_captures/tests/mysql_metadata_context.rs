use super::*;
use culprit_collector::{analytical::test_support::LocalCaptureFixture, outbox::CompletedCaptureBudget};
use culprit_engine::OutboxCaptureReader;
use culprit_evidence::analytical::test_support::{MetadataLockContextCase, metadata_lock_context_evidence};
#[tokio::test]
async fn authenticated_ticket_context_uses_the_existing_local_tool_and_preserves_candidate_only_roles() {
    for case in [MetadataLockContextCase::Mysql, MetadataLockContextCase::Mariadb, MetadataLockContextCase::Prefix] {
        let (source, rows) = metadata_lock_context_evidence(case);
        let fixture = LocalCaptureFixture::activity(source, rows).await;
        let reader = OutboxCaptureReader::delivery(
            fixture.outbox.as_ref().unwrap().delivery(),
            CompletedCaptureBudget::default(),
        )
        .unwrap();
        let mut host = Host::new();
        host.server = host.server.clone().with_local_captures(Arc::new(reader));
        let args = json!({"connection":"local-evidence","observation_id":fixture.manifest.capture.observation_id,"analysis":"blocking"});
        let result = host.server.handle("fixture", "analyze_capture", args.as_object().unwrap().clone()).await;
        assert_ne!(result.is_error, Some(true), "{case:?}");
        let value = body(&result);
        let report = &value["blocking"]["metadata_queue"];
        assert_eq!(report["evidence"]["status"], "unavailable");
        assert_eq!(report["evidence"]["cascades"], json!([]));
        assert!(
            report["evidence"]["issues"].as_array().unwrap().iter().any(|issue| issue == "pending-blocker-not-exposed")
        );
        assert_eq!(
            report["evidence"]["candidates"].as_array().unwrap().len(),
            if case == MetadataLockContextCase::Prefix { 0 } else { 1 }
        );
        assert_eq!(value["blocking"]["original_authenticated"], true);
        assert_eq!(value["blocking"]["relationships"], json!([]));
        assert!(!value.to_string().contains("mdl-private-canary"));
    }
}
