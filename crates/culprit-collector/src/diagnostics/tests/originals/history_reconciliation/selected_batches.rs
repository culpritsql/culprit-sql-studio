use super::*;
use crate::delivery::diagnostic_recovery::authenticate_original_capture;
use crate::delivery::evidence::PreparedRange;
use crate::delivery::object_store::ObjectRecoveryStore;
use crate::delivery::object_store::tests::MemoryBlobs;
use crate::delivery::recovery::stage_and_commit;
use culprit_evidence::delivery::originals::OriginalHistoryLookup;
use culprit_evidence::delivery::recovery::PayloadObject;
use culprit_evidence::delivery::recovery::RecoveryRead;
use culprit_read::cancellation::CollectionCancellation;
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::AsyncReadExt;

fn group_keys(revisions: &[DiagnosticRevision]) -> Vec<String> {
    revisions.iter().map(|revision| revision.episode.original_group_key().unwrap().unwrap()).collect()
}

#[tokio::test]
async fn selected_file_lookup_reads_one_index_and_authenticates_one_body_for_all_sibling_groups() {
    let mut fixture = History::new().await;
    let (_, original, revisions) = fixture.process(frame(0, 8)).await;
    let mut keys = group_keys(&revisions);
    let absent = digest(b"no prior group in this exact complete index");
    keys.push(absent.clone());
    let before = fixture.store.original_history_read_counts().await.unwrap();
    let lookup = fixture.store.diagnostic_original_groups(&keys).await.unwrap();
    let selected = fixture.store.original_history_read_counts().await.unwrap();
    assert_eq!(selected.0, before.0 + 1, "one actual serialized index read for the entire selected roster");
    assert_eq!(lookup.entries().len(), keys.len());
    assert!(lookup.entries()[&absent].is_none());
    let groups = lookup.captures().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), revisions.len());
    let input = authenticate_original_capture(&fixture.store, &groups[0]).await.unwrap();
    let after = fixture.store.original_history_read_counts().await.unwrap();
    for batch in &original.manifest().batches {
        let initial = selected.1.get(&batch.object).copied().unwrap_or(0);
        assert_eq!(
            after.1.get(&batch.object).copied().unwrap_or(0),
            initial + 1,
            "each actual immutable stream reaches EOF once, regardless of selected sibling count"
        );
    }
    for pointer in &groups[0] {
        let row = input.record(&pointer.reference).unwrap();
        let revision = original_revision(&row.record).unwrap();
        let expected = revisions.iter().find(|original| original.id == revision.id).unwrap();
        assert_eq!(revision, expected);
        assert_eq!(pointer.reference.row_ordinal as usize, revisions.iter().position(|item| item == expected).unwrap());
    }
    assert_eq!(fixture.harness.calls.counts(), [1, 1, 1, 1]);
}

#[tokio::test]
async fn local_selected_groups_authenticate_the_same_sealed_capture_once_without_losing_any_original() {
    let mut fixture = History::new().await;
    let native = fixture.harness.capture_with_budget(frame(0, 8), budget()).await;
    fixture.archive(&native).await;
    assert_eq!(
        analyze_pending(
            &mut fixture.harness.outbox,
            &fixture.harness.policies,
            &CollectionCancellation::default(),
            1,
            Some(&fixture.store)
        )
        .await
        .unwrap(),
        1
    );
    let revisions = fixture.harness.revisions(&native).await;
    let keys = group_keys(&revisions);
    let (references, reads) = fixture.harness.outbox.local_original_groups_with_read_count(&keys).await.unwrap();
    assert_eq!(reads, 1, "one actual complete body verifier for sibling local native-group hints");
    assert_eq!(references.len(), revisions.len());
    for (key, revision) in keys.iter().zip(&revisions) {
        let reference = references[key].as_ref().unwrap();
        let (_, row) = fixture.harness.outbox.diagnostic_evidence(reference).await.unwrap();
        assert_eq!(original_revision(&row.record).unwrap(), revision);
    }
    assert!(fixture.harness.checkpoint().await.episodes.is_empty());
    assert_eq!(fixture.harness.calls.counts(), [1, 1, 1, 1]);
}

#[tokio::test]
async fn selected_lookup_rejects_omitted_duplicate_foreign_and_cross_group_membership_before_reuse() {
    let mut fixture = History::new().await;
    let (_, _, revisions) = fixture.process(frame(0, 2)).await;
    let keys = group_keys(&revisions);
    let before = fixture.store.original_history_read_counts().await.unwrap().0;
    assert!(fixture.store.diagnostic_original_groups(&[keys[0].clone(), keys[0].clone()]).await.is_err());
    assert!(
        fixture.store.diagnostic_original_groups(&vec![keys[0].clone(); MAX_DIAGNOSTIC_EPISODES + 1]).await.is_err()
    );
    assert!(fixture.store.diagnostic_original_groups(&["not-a-native-group-key".into()]).await.is_err());
    assert_eq!(fixture.store.original_history_read_counts().await.unwrap().0, before);
    let lookup = fixture.store.diagnostic_original_groups(&keys).await.unwrap();
    let first = lookup.entries()[&keys[0]].clone().unwrap();
    let other = lookup.entries()[&keys[1]].clone().unwrap();
    assert!(OriginalHistoryLookup::new(&keys, BTreeMap::from([(keys[0].clone(), Some(first.clone()))])).is_err());
    assert!(
        OriginalHistoryLookup::new(&[keys[0].clone()], BTreeMap::from([(keys[1].clone(), Some(other.clone()))]))
            .is_err()
    );
    assert!(
        OriginalHistoryLookup::new(&[keys[0].clone()], BTreeMap::from([(keys[0].clone(), Some(other.clone()))]))
            .is_err()
    );
    let mut wrong_position = first.clone();
    wrong_position.reference.row_ordinal = other.reference.row_ordinal;
    assert!(authenticate_original_capture(&fixture.store, &[&wrong_position]).await.is_err());
    let mut conflicting = other;
    conflicting.manifest_digest = digest(b"different immutable manifest claimed for the same range");
    let lookup = OriginalHistoryLookup::new(
        &keys,
        BTreeMap::from([(keys[0].clone(), Some(first)), (keys[1].clone(), Some(conflicting))]),
    )
    .unwrap();
    assert!(lookup.captures().is_err());
    assert_eq!(fixture.harness.calls.counts(), [1, 1, 1, 1]);
}

#[tokio::test]
async fn a_missing_or_tampered_selected_pointer_and_late_body_failure_never_become_proven_absence() {
    for fault in ["missing", "pointer", "body"] {
        let mut fixture = History::new().await;
        let (_, original, revisions) = fixture.process(frame(0, 2)).await;
        let keys = group_keys(&revisions);
        let pointer = fixture.store.diagnostic_original_group(&keys[0]).await.unwrap().unwrap();
        let path = fixture
            .archive
            .path()
            .join("recovery/diagnostic-originals")
            .join(format!("{}.json", digest(&pointer.bytes().unwrap())));
        match fault {
            "missing" => std::fs::remove_file(path).unwrap(),
            "pointer" => std::fs::write(path, b"{}").unwrap(),
            "body" => {
                let lookup = fixture.store.diagnostic_original_groups(&keys).await.unwrap();
                let batch = original
                    .manifest()
                    .batches
                    .iter()
                    .find(|batch| batch.stream == TelemetryStream::OperationalSamples)
                    .unwrap();
                let path = fixture.archive.path().join("recovery").join(&batch.object);
                let mut bytes = std::fs::read(&path).unwrap();
                bytes.push(b' ');
                std::fs::write(path, bytes).unwrap();
                assert!(authenticate_original_capture(&fixture.store, &lookup.captures().unwrap()[0]).await.is_err());
                continue;
            }
            _ => unreachable!(),
        }
        assert!(fixture.store.diagnostic_original_groups(&keys).await.is_err(), "{fault}");
        assert!(fixture.harness.checkpoint().await.episodes.is_empty());
        assert_eq!(fixture.harness.calls.counts(), [1, 1, 1, 1]);
    }
}

// Copy only the already authenticated fixture-owned immutable archive. The same stage/commit owner verifies the
// original manifests and bodies again in Object storage; there is no synthetic group pointer or native attestation.
async fn mirror_archive(
    fixture: &History,
    store: &ObjectRecoveryStore,
    fence: &culprit_evidence::delivery::mutations::Fence,
    range: &CommittedRange,
    previous: Option<&str>,
) {
    let root = tempfile::tempdir().unwrap();
    for batch in &range.manifest().batches {
        let mut input = fixture.store.open_object(&PayloadObject::from(batch)).await.unwrap();
        let mut bytes = Vec::new();
        input.as_mut().take(batch.bytes + 1).read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes.len() as u64, batch.bytes);
        std::fs::write(root.path().join(batch.stream.file_name()), bytes).unwrap();
    }
    let prepared = PreparedRange { manifest: Arc::new(range.manifest().clone()), directory: root.path().into() };
    let mirrored = stage_and_commit(store, fence, previous, &prepared).await.unwrap();
    assert_eq!(mirrored.manifest(), range.manifest());
}

#[tokio::test]
async fn object_selected_lookup_uses_one_authoritative_root_and_keeps_genuine_pointer_integrity() {
    let mut fixture = History::new().await;
    let (source, original, revisions) = fixture.process(frame(0, 4)).await;
    let blobs = Arc::new(MemoryBlobs::default());
    let store = ObjectRecoveryStore::create(blobs.clone(), fixture.contract.clone()).await.unwrap();
    let fence = store.acquire().await.unwrap();
    mirror_archive(&fixture, &store, &fence, &source, None).await;
    mirror_archive(&fixture, &store, &fence, &original, Some(&source.manifest().range_id)).await;
    let mut keys = group_keys(&revisions);
    let absent = digest(b"no original group in this exact object root");
    keys.push(absent.clone());
    let before = blobs.root_reads();
    let lookup = store.diagnostic_original_groups(&keys).await.unwrap();
    assert_eq!(blobs.root_reads(), before + 1);
    assert_eq!(lookup.entries().len(), keys.len());
    assert!(lookup.entries()[&absent].is_none());
    let pointers = lookup.captures().unwrap();
    let input = authenticate_original_capture(&store, &pointers[0]).await.unwrap();
    for pointer in &pointers[0] {
        assert_eq!(
            original_revision(&input.record(&pointer.reference).unwrap().record)
                .unwrap()
                .episode
                .original_group_key()
                .unwrap()
                .as_deref(),
            Some(pointer.group_key.as_str())
        );
    }
    let pointer = lookup.entries()[&keys[0]].as_ref().unwrap();
    blobs.corrupt_original_pointer(&digest(&pointer.bytes().unwrap()));
    assert!(store.diagnostic_original_groups(&keys).await.is_err());
}

#[tokio::test]
async fn cancellation_at_real_sibling_body_eof_keeps_the_input_pending_without_caching_or_native_work() {
    use crate::diagnostics::tests::recovery::{reach_stalled_eof, stalling_recovery_probe};
    let mut fixture = History::new().await;
    let (_, original, _) = fixture.process(frame(0, 4)).await;
    let mut repeated = frame(0, 4);
    source_at(&mut repeated.source, 6);
    let capture = fixture.harness.capture_with_budget(repeated, budget()).await;
    let before = fixture.harness.calls.counts();
    let object = original
        .manifest()
        .batches
        .iter()
        .find(|batch| batch.stream == TelemetryStream::OperationalSamples)
        .unwrap()
        .object
        .clone();
    let History { archive: _archive, store, owner: _owner, contract: _, mut harness } = fixture;
    let (probe, reads) = stalling_recovery_probe(Arc::new(store), object);
    let cancellation = CollectionCancellation::default();
    let stop = cancellation.clone();
    let processing = tokio::spawn(async move {
        let result = analyze_pending(&mut harness.outbox, &harness.policies, &stop, 1, Some(&probe)).await;
        (harness, result)
    });
    reach_stalled_eof(&reads).await;
    cancellation.cancel();
    let (harness, result) = tokio::time::timeout(std::time::Duration::from_secs(1), processing).await.unwrap().unwrap();
    assert_eq!(result.unwrap(), 0);
    assert_eq!(reads.active_readers(), 0);
    assert_eq!(harness.calls.counts(), before);
    let pending = harness.outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    assert_eq!(pending.parent.capture, capture);
    assert!(pending.original_history.is_none());
    assert!(!harness.outbox.status(20, None).await.unwrap().entries.iter().any(|entry|
        matches!(&entry.request.selection, CollectionSelection::Diagnostics { parent, .. } if parent.capture == capture)));
}
