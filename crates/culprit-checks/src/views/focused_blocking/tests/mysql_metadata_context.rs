use super::*;
use culprit_core::blocking::*;
use culprit_evidence::analytical::test_support::{MetadataLockContextCase as Case, metadata_lock_context_evidence};
fn inspect(case: Case, complete: bool) -> BlockingReport {
    let (source, records) = metadata_lock_context_evidence(case);
    let rows = records
        .iter()
        .map(|row| match row {
            TelemetryRecord::Activity(actor) => ActivityFrameRow { observation: actor, reference: None },
            _ => panic!(),
        })
        .collect::<Vec<_>>();
    explain(&source, &rows, complete).unwrap()
}
#[test]
fn same_object_native_tickets_remain_candidates_and_never_manufacture_a_pending_blocker_edge() {
    for &case in Case::ALL {
        let report = inspect(case, true);
        assert_eq!(report.metadata_queue.evidence.status, MetadataQueueStatus::Unavailable, "{case:?}");
        assert!(report.metadata_queue.evidence.cascades.is_empty(), "{case:?}");
        assert_eq!(!report.metadata_queue.evidence.candidates.is_empty(), case.has_candidates(), "{case:?}");
        assert!(report.metadata_queue.evidence.issues.contains(&MetadataQueueIssue::PendingBlockerNotExposed));
        assert!(report.relationships.is_empty(), "ticket presence must not populate native wait owners");
        if case.has_candidates() {
            let candidate = &report.metadata_queue.evidence.candidates[0];
            assert!(candidate.is_valid());
            assert_eq!(candidate.pending_exclusive.len(), 1);
            assert_eq!(candidate.granted_holders.len(), 1);
            assert!(!report.metadata_queue.explanations[0].investigation.is_empty());
        }
    }
}
#[test]
fn partial_replay_changed_ticket_unknown_lifetime_and_conversions_keep_independent_native_gaps() {
    let partial = inspect(Case::Mysql, false);
    assert!(partial.metadata_queue.evidence.candidates.is_empty());
    assert!(partial.metadata_queue.evidence.issues.contains(&MetadataQueueIssue::IncompleteRead));
    assert!(
        inspect(Case::UnknownTransaction, true)
            .metadata_queue
            .evidence
            .issues
            .contains(&MetadataQueueIssue::NativeLifetimeNotRecorded)
    );
    assert!(
        inspect(Case::MixedTickets, true)
            .metadata_queue
            .evidence
            .issues
            .contains(&MetadataQueueIssue::MixedGrantedAndPending)
    );
    assert!(
        inspect(Case::Prefix, true).metadata_queue.evidence.issues.contains(&MetadataQueueIssue::LockEvidenceLimited)
    );
    assert!(
        inspect(Case::Convert, true).metadata_queue.evidence.issues.contains(&MetadataQueueIssue::UnknownModeOrEdge)
    );
}
#[test]
fn a_candidate_cohort_cannot_be_laundered_into_the_existing_reachable_blocking_frame_or_foreign_scope() {
    let (mut source, records) = metadata_lock_context_evidence(Case::Mysql);
    let mut actors = records
        .into_iter()
        .map(|record| match record {
            TelemetryRecord::Activity(actor) => *actor,
            _ => panic!(),
        })
        .collect::<Vec<_>>();
    let SourceObservation::MetadataLockContext { snapshot, result, .. } = source.observation.clone() else { panic!() };
    for actor in &mut actors {
        if let Some(wait) = &mut actor.wait {
            wait.resources = None;
        }
    }
    source.observation = SourceObservation::FocusedBlocking { snapshot, result: Box::new(result.request) };
    let rows = actors.iter().map(|actor| ActivityFrameRow { observation: actor, reference: None }).collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&source, &rows, true).unwrap();
    let SourceObservation::FocusedBlocking { result, .. } = &source.observation else { panic!() };
    assert!(!result.accepts_frame(&frame), "candidate presence cannot repair missing native reachability");
    let (mut source, records) = metadata_lock_context_evidence(Case::Mysql);
    let SourcePartition::InstanceLocal { epoch, .. } = &mut source.partition else { panic!() };
    *epoch = Some("recreated-process".into());
    assert!(!source.partition_is_consistent());
    let capture = CaptureIdentity {
        observation_id: "foreign-metadata-original".into(),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"metadata foreign outbox"),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    assert!(ExportedRecord::new(&source, &capture, records[0].clone()).is_err());
}

#[test]
fn saved_typed_candidates_render_in_the_requested_language_without_reclassifying_original_evidence() {
    let english = culprit_core::i18n::scope("en", || inspect(Case::Mysql, true));
    let french = culprit_core::i18n::scope("fr", || inspect(Case::Mysql, true));
    assert_eq!(english.metadata_queue.evidence, french.metadata_queue.evidence);
    assert_ne!(english.metadata_queue.summary, french.metadata_queue.summary);
    assert!(french.metadata_queue.evidence.cascades.is_empty());
    assert!(french.relationships.is_empty());
    assert!(!serde_json::to_string(&french).unwrap().contains("mdl-private-canary"));
}
