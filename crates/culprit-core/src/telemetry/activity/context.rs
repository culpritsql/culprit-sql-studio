//! One activity-index traversal owns native generation matching for ordinary and bounded selections.
use super::*;
use std::collections::VecDeque;

#[derive(Clone, Copy)]
enum ContextPolicy {
    AllObserved,
    FocusedBlocking { max_participants: u32 },
}
impl ContextPolicy {
    fn participant_refusal(self, depth: u32, participants: usize) -> Option<BlockingClosureStatus> {
        let Self::FocusedBlocking { max_participants } = self else { return None };
        if depth > MAX_BLOCKING_DEPTH {
            Some(BlockingClosureStatus::DepthLimit)
        } else if participants >= max_participants as usize {
            Some(BlockingClosureStatus::ParticipantLimit)
        } else {
            None
        }
    }
    fn lookup_refusal(self, lookups: usize) -> Option<BlockingClosureStatus> {
        (matches!(self, Self::FocusedBlocking { .. }) && lookups >= MAX_BLOCKING_LOOKUPS as usize)
            .then_some(BlockingClosureStatus::LookupLimit)
    }
}

pub(super) fn all_observed(roots: &[ActivityKey], index: &ActivityIndex<'_>) -> BTreeSet<ActivityKey> {
    select(roots, index, ContextPolicy::AllObserved).0
}
pub(super) fn bounded(
    roots: &[ActivityKey],
    index: &ActivityIndex<'_>,
    max_participants: u32,
) -> BlockingContextSelection {
    let (keys, status) = select(roots, index, ContextPolicy::FocusedBlocking { max_participants });
    BlockingContextSelection {
        keys,
        closure: BlockingClosureDescription {
            status,
            max_lookups: MAX_BLOCKING_LOOKUPS,
            max_depth: MAX_BLOCKING_DEPTH,
            max_participants,
        },
    }
}
fn select(
    roots: &[ActivityKey],
    index: &ActivityIndex<'_>,
    policy: ContextPolicy,
) -> (BTreeSet<ActivityKey>, BlockingClosureStatus) {
    let mut pending: VecDeque<_> =
        roots.iter().filter(|key| index.by_key.contains_key(*key)).cloned().map(|key| (key, 0u32)).collect();
    let mut selected = BTreeSet::new();
    let mut looked_up = BTreeSet::new();
    let mut status = BlockingClosureStatus::Observed;
    while let Some((key, depth)) = pending.pop_front() {
        if selected.contains(&key) {
            continue;
        }
        // A new participant can arrive from an already-paid lookup. Lookup admission itself is below.
        if let Some(refusal) = policy.participant_refusal(depth, selected.len()) {
            if status == BlockingClosureStatus::Observed {
                status = refusal;
            }
            continue;
        }
        selected.insert(key.clone());
        let row = index.by_key[&key];
        if !index.has_session_generation_authority(&row.key.session) {
            continue;
        }
        for session in row.wait.iter().flat_map(|wait| &wait.owners).filter_map(ActivityWaitOwner::session) {
            if looked_up.contains(&session) {
                continue;
            }
            if let Some(refusal) = policy.lookup_refusal(looked_up.len()) {
                if status == BlockingClosureStatus::Observed {
                    status = refusal;
                }
                continue;
            }
            looked_up.insert(session.clone());
            for owner in index.by_session.get(&session).into_iter().flatten() {
                pending.push_back((owner.key.clone(), depth.saturating_add(1)));
            }
        }
    }
    (selected, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(length: usize) -> Vec<ActivityObservation> {
        (0..length)
            .map(|index| {
                let mut row = super::super::tests::request(&(index + 1).to_string());
                if index + 1 < length {
                    row.wait = Some(ActivityWait {
                        class: None,
                        native_type: Some("LCK_M_X".into()),
                        elapsed_ms: Some("1".into()),
                        owners: vec![ActivityWaitOwner::Session {
                            id: (index + 2).to_string(),
                            generation: Some(row.key.session.generation.clone()),
                        }],
                        resources: None,
                        identified_resources: vec![],
                    });
                }
                row
            })
            .collect()
    }
    #[test]
    fn ordinary_and_bounded_context_share_generation_matching_and_preserve_complete_short_closures() {
        let rows = chain(3);
        let roots = BTreeSet::from([rows[0].key.clone()]);
        let ordinary = ActivityEvidence::context_for(&rows, &roots).unwrap();
        let bounded = ActivityEvidence::bounded_context_for(&rows, &roots, MAX_BLOCKING_PARTICIPANTS).unwrap();
        assert_eq!(bounded.keys, ordinary);
        assert_eq!(bounded.closure.status, BlockingClosureStatus::Observed);
        let mut recycled = rows.clone();
        recycled[1].key.session.generation = "different-login".into();
        let ordinary = ActivityEvidence::context_for(&recycled, &roots).unwrap();
        let bounded = ActivityEvidence::bounded_context_for(&recycled, &roots, MAX_BLOCKING_PARTICIPANTS).unwrap();
        assert_eq!(bounded.keys, ordinary);
        assert_eq!(bounded.keys, roots, "a reused session number cannot supply an old owner");
    }
    #[test]
    fn depth_and_participant_limits_remain_explicit_without_changing_ordinary_context() {
        let rows = chain(MAX_BLOCKING_DEPTH as usize + 2);
        let roots = BTreeSet::from([rows[0].key.clone()]);
        assert_eq!(ActivityEvidence::context_for(&rows, &roots).unwrap().len(), rows.len());
        let bounded = ActivityEvidence::bounded_context_for(&rows, &roots, MAX_BLOCKING_PARTICIPANTS).unwrap();
        assert_eq!(bounded.keys.len(), MAX_BLOCKING_DEPTH as usize + 1);
        assert_eq!(bounded.closure.status, BlockingClosureStatus::DepthLimit);
        let bounded = ActivityEvidence::bounded_context_for(&rows, &roots, 1).unwrap();
        assert_eq!(bounded.keys, roots);
        assert_eq!(bounded.closure.status, BlockingClosureStatus::ParticipantLimit);
    }
    #[test]
    fn distinct_owner_lookups_are_bounded_and_cycles_do_not_duplicate_the_original() {
        let mut rows = chain(MAX_BLOCKING_LOOKUPS as usize + 2);
        let root = rows[0].key.clone();
        let owners = rows
            .iter()
            .skip(1)
            .take(MAX_BLOCKING_LOOKUPS as usize)
            .map(|row| ActivityWaitOwner::Session {
                id: row.key.session.id.clone(),
                generation: Some(row.key.session.generation.clone()),
            })
            .collect();
        rows[0].wait.as_mut().unwrap().owners = owners;
        for row in rows.iter_mut().skip(1) {
            row.wait = None;
        }
        let mut next_wait = rows[0].wait.as_ref().unwrap().clone();
        let extra_owner = &rows.last().unwrap().key.session;
        next_wait.owners = vec![ActivityWaitOwner::Session {
            id: extra_owner.id.clone(),
            generation: Some(extra_owner.generation.clone()),
        }];
        rows[1].wait = Some(next_wait);
        let roots = BTreeSet::from([root]);
        let selected = ActivityEvidence::bounded_context_for(&rows, &roots, MAX_BLOCKING_PARTICIPANTS).unwrap();
        assert_eq!(selected.keys.len(), MAX_BLOCKING_PARTICIPANTS as usize);
        assert_eq!(selected.closure.status, BlockingClosureStatus::LookupLimit);
        assert_eq!(ActivityEvidence::context_for(&rows, &roots).unwrap().len(), rows.len());
        let mut cycle = chain(2);
        cycle[1].wait = cycle[0].wait.clone();
        cycle[1].wait.as_mut().unwrap().owners = vec![ActivityWaitOwner::Session {
            id: cycle[0].key.session.id.clone(),
            generation: Some(cycle[0].key.session.generation.clone()),
        }];
        let roots = BTreeSet::from([cycle[0].key.clone()]);
        let selected = ActivityEvidence::bounded_context_for(&cycle, &roots, MAX_BLOCKING_PARTICIPANTS).unwrap();
        assert_eq!(selected.keys.len(), 2);
        assert_eq!(selected.closure.status, BlockingClosureStatus::Observed);
    }
    #[test]
    fn ambiguous_native_generations_and_invalid_bounds_are_refused_by_the_same_index_owner() {
        let mut rows = chain(2);
        let roots = BTreeSet::from([rows[0].key.clone()]);
        let mut recycled = rows[1].clone();
        recycled.key.session.generation = "different-login".into();
        rows.push(recycled);
        assert!(ActivityEvidence::context_for(&rows, &roots).is_err());
        assert!(ActivityEvidence::bounded_context_for(&rows, &roots, MAX_BLOCKING_PARTICIPANTS).is_err());
        for cap in [0, MAX_BLOCKING_PARTICIPANTS + 1] {
            assert!(ActivityEvidence::bounded_context_for(&[], &BTreeSet::new(), cap).is_err());
        }
    }
}
