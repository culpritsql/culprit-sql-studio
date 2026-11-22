//! Atomic publication/analysis phases retain the actual existing writer policy.
use culprit_evidence::delivery::{heads::capture_reference, pending::PendingRanges, recovery::RecoveryOrder};

#[test]
fn publication_and_initial_analysis_have_distinct_views_of_one_atomic_index() {
    let root = tempfile::tempdir().unwrap();
    let native = crate::delivery::batch_tests::quiet_activity(root.path());
    let derived = crate::delivery::batch_tests::empty_diagnostics(root.path(), native.manifest(), 2);
    let store = culprit_core::telemetry::digest(b"store");
    let mut pending = PendingRanges::default();
    pending
        .observe(
            1,
            native.manifest(),
            crate::outbox::diagnostic_manifest_eligible(
                &native.manifest().source,
                native.manifest().investigation.as_ref(),
                &native.manifest().reference_coverage,
            )
            .unwrap(),
        )
        .unwrap();
    pending.retire(&native.manifest().range_id);
    assert!(pending.page(None, 1, &store, 1, RecoveryOrder::OldestFirst).unwrap().0.is_empty());
    assert_eq!(pending.diagnostic_page(None, 1, &store, 1).unwrap().0, vec![native.manifest().range_id.clone()]);
    pending
        .observe(
            2,
            derived.manifest(),
            crate::outbox::diagnostic_manifest_eligible(
                &derived.manifest().source,
                derived.manifest().investigation.as_ref(),
                &derived.manifest().reference_coverage,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(pending.diagnostic_page(None, 1, &store, 2).unwrap().0.is_empty());
    assert_eq!(
        pending.page(None, 1, &store, 2, RecoveryOrder::OldestFirst).unwrap().0,
        vec![derived.manifest().range_id.clone()]
    );
}

#[test]
fn another_controller_version_cannot_acknowledge_the_current_native_obligation() {
    use culprit_core::telemetry::{DIAGNOSTIC_CONTROLLER_VERSION, digest};
    let root = tempfile::tempdir().unwrap();
    let native = crate::delivery::batch_tests::quiet_activity(root.path());
    let other_version = crate::delivery::batch_tests::empty_diagnostics_for_version(
        root.path(),
        native.manifest(),
        2,
        DIAGNOSTIC_CONTROLLER_VERSION + 1,
    );
    let current = crate::delivery::batch_tests::empty_diagnostics(root.path(), native.manifest(), 3);
    let reference = capture_reference(native.manifest()).unwrap();
    let store = digest(b"store");
    let mut pending = PendingRanges::default();
    pending
        .observe(
            1,
            native.manifest(),
            crate::outbox::diagnostic_manifest_eligible(
                &native.manifest().source,
                native.manifest().investigation.as_ref(),
                &native.manifest().reference_coverage,
            )
            .unwrap(),
        )
        .unwrap();
    pending
        .observe(
            2,
            other_version.manifest(),
            crate::outbox::diagnostic_manifest_eligible(
                &other_version.manifest().source,
                other_version.manifest().investigation.as_ref(),
                &other_version.manifest().reference_coverage,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(pending.diagnostic_input_pending(&reference).unwrap());
    pending.retire(&native.manifest().range_id);
    assert_eq!(pending.diagnostic_page(None, 1, &store, 2).unwrap().0, [native.manifest().range_id.clone()]);
    pending
        .observe(
            3,
            current.manifest(),
            crate::outbox::diagnostic_manifest_eligible(
                &current.manifest().source,
                current.manifest().investigation.as_ref(),
                &current.manifest().reference_coverage,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(!pending.diagnostic_input_pending(&reference).unwrap());
    assert!(pending.diagnostic_page(None, 1, &store, 3).unwrap().0.is_empty());
}
