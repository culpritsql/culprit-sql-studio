//! The retained middle basis must survive old archive pins and later native Activity revisions.
use super::*;
use crate::delivery::file_store::FileRecoveryStore;
use crate::fleet::investigations::tests::source_reads::recovery::archive_ready;
use culprit_evidence::delivery::contract::Contract;

async fn positive_baseline(runtime: &mut Runtime, at: &str) {
    let SourceObservation::Activity(header) = &mut runtime.fixture.backend.activity.source.observation else {
        panic!()
    };
    header.at = at.into();
    header.observed_from = at.into();
    header.observed_until = at.into();
    runtime.started -= runtime.scheduler.wait_allowed(runtime.started.elapsed(), |_| false);
    runtime.baseline().await;
}
#[tokio::test]
async fn middle_basis_prerequisite_and_answer_refs_remain_known_after_archive_eviction_and_unchanged_continuation() {
    let archive = tempfile::tempdir().unwrap();
    let contract = Contract::new(
        "retained-middle-basis".into(),
        "one".into(),
        "file:retained-middle-basis".into(),
        digest(b"retained-middle-basis-schema"),
    )
    .unwrap();
    let store = FileRecoveryStore::create(&archive.path().join("recovery"), contract.clone()).unwrap();
    let owner = store.acquire().await.unwrap();
    let mut runtime = prepared_with_fence(true, Some(owner.fence())).await;
    let before = episode(&runtime).await;
    let basis =
        before.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().focus.basis.as_ref().clone();
    assert_ne!(basis.capture, before.first.capture, "actual Parent4 selection is a distinct middle Activity capture B");
    let (result, calls) = capture_retained_answer(&mut runtime).await;
    runtime.reconcile().await;
    assert!(runtime.fixture.journal.focused_intents().await.unwrap().is_empty());
    assert!(runtime.fixture.outbox.investigation_parent_keys().await.unwrap().is_empty());
    assert_eq!(runtime.native_calls(), calls);
    let answered = episode(&runtime).await;
    let stage = answered.source_reads.get(SourceReadQuestion::RelationalInputContext).unwrap().clone();
    let SourceReadStageState::Answered { answer } = &stage.state else { panic!() };
    let all = stage.focus.references().chain(answer.references()).cloned().collect::<Vec<_>>();
    assert!(all.iter().any(|reference| reference.capture_ref() == result));
    // A genuine later positive source point advances latest to C, leaving A/B/C independently original.
    positive_baseline(&mut runtime, "2020-01-01T12:00:05Z").await;
    let continued = episode(&runtime).await;
    assert_eq!(continued.first, before.first);
    assert_ne!(continued.latest.capture, basis.capture);
    assert_ne!(continued.latest.capture, before.first.capture);
    assert_eq!(continued.source_reads.get(SourceReadQuestion::RelationalInputContext), Some(&stage));
    // Inspect only this verified nonce-owned local journal. Exact remote acknowledgement deliberately
    // releases head pins, so head protection must be asserted before archiving that current head.
    let pins = rusqlite::Connection::open_with_flags(
        runtime.fixture._directory.path().join("outbox/journal.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    for reference in &all {
        let pinned: bool = pins
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM diagnostic_pins p JOIN diagnostic_heads h
             ON p.owner='head:'||h.controller_key WHERE p.observation_id=?1)",
                [&reference.capture.observation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pinned, "current head must retain exact stage reference {reference:?}");
    }
    drop(pins);
    let archived = archive_ready(&mut runtime, &store, owner.fence(), &contract).await;
    let mut evicted = std::collections::BTreeSet::new();
    for reference in &all {
        if evicted.insert(reference.capture.observation_id.clone()) {
            let original = &archived[&reference.capture.observation_id];
            assert!(
                runtime.fixture.outbox.delivery().evict(original).await.unwrap(),
                "exact remote archive permits bounded raw-cache eviction for {reference:?}"
            );
        }
        require_evicted(&runtime, reference).await;
        let recovered =
            crate::delivery::diagnostic_recovery::load_diagnostic_evidence(&store, reference).await.unwrap();
        assert!(reference.validate_canonical_record(recovered.source(), &recovered.record().record).is_ok());
        // This standalone EOF proof is intentionally not installed in the local Outbox.
        require_evicted(&runtime, reference).await;
    }
    let calls = runtime.native_calls();
    positive_baseline(&mut runtime, "2020-01-01T12:00:06Z").await;
    assert_eq!(runtime.native_calls(), calls.map(|value| value + 1), "only the one scheduled baseline opens a source");
    let after = episode(&runtime).await;
    assert_eq!(after.first, before.first);
    assert_eq!(after.source_reads.get(SourceReadQuestion::RelationalInputContext), Some(&stage));
    assert!(!runtime.enroll().await, "terminal retained answer never reopens after archive/continuation");
    for reference in &all {
        require_evicted(&runtime, reference).await;
    }
}

async fn require_evicted(runtime: &Runtime, reference: &DiagnosticEvidenceRef) {
    assert_eq!(
        runtime.fixture.outbox.diagnostic_evidence(reference).await.unwrap_err().detail,
        Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::MissingCapture)),
        "the archived body must remain absent from the local cache: {reference:?}"
    );
}
