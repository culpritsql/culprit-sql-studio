//! A closer original replaces a source-capture endpoint before any native ownership/reset matching.
use super::*;
use culprit_core::MAX_COMMIT_PIPELINE_FACTS;

enum Change {
    Epoch,
    Member,
    UnknownOwner { empty: bool },
    OtherResource,
}
pub(super) fn fixture(case: CommitPipelineIntervalCase) -> Option<CommitPipelineFixture> {
    use CommitPipelineIntervalCase as Case;
    if case == Case::ManyUnrelatedSharedFrames {
        let mut fixture = commit_pipeline_interval_fixture(Case::UnrelatedSharedResource);
        let foreign = duplicate(&fixture.originals[3]);
        // The existing unrelated case has two foreign frames. Retain65, one beyond the report's64-frame cap.
        for _ in 2..=MAX_COMMIT_PIPELINE_FACTS {
            fixture.originals.push(duplicate(&foreign));
        }
        return Some(fixture);
    }
    let (before, after, change) = match case {
        Case::CloserRestartBefore => (true, false, Change::Epoch),
        Case::CloserRestartAfter => (false, true, Change::Epoch),
        Case::CloserRestartPair => (true, true, Change::Epoch),
        Case::CloserMemberBefore => (true, false, Change::Member),
        Case::CloserMemberAfter => (false, true, Change::Member),
        Case::CloserUnknownOwner => (true, false, Change::UnknownOwner { empty: false }),
        Case::CloserEmptyUnknownOwner => (false, true, Change::UnknownOwner { empty: true }),
        Case::UnrelatedSharedResource => (true, true, Change::OtherResource),
        _ => return None,
    };
    let mut fixture = commit_pipeline_interval_fixture(Case::Postgres17);
    if before {
        let mut nearer = duplicate(&fixture.originals[1]);
        bounds(&mut nearer, "2020-01-01T12:00:00.5Z", "2020-01-01T12:00:00.6Z");
        apply(&mut nearer, &change);
        fixture.originals.push(nearer);
    }
    if after {
        let mut nearer = duplicate(&fixture.originals[2]);
        bounds(&mut nearer, "2020-01-01T12:00:01.3Z", "2020-01-01T12:00:01.4Z");
        apply(&mut nearer, &change);
        fixture.originals.push(nearer);
    }
    Some(fixture)
}

fn apply(original: &mut CommitPipelineOriginalFixture, change: &Change) {
    let SourcePartition::InstanceLocal { member, epoch } = &mut original.source.partition else { unreachable!() };
    match change {
        Change::Epoch => *epoch = Some("changed-native-process".into()),
        Change::Member | Change::OtherResource => {
            let owner = member.as_mut().unwrap();
            owner.native.id = "other-native-member".into();
            if matches!(change, Change::OtherResource) {
                original.source.target = CollectionTarget::Shared(owner.clone());
            }
        }
        Change::UnknownOwner { empty } => {
            *member = None;
            *epoch = None;
            let SourceObservation::Snapshot(header) = &mut original.source.observation else { unreachable!() };
            header.sources[0].state = culprit_core::SourceState::Partial;
            header.sources[0].complete_scope = false;
            if *empty {
                original.records.clear();
                header.records = 0;
                header.sources[0].records = 0;
            }
        }
    }
    assert!(original.source.partition_is_consistent(), "each controlled replacement is a valid original source");
}
