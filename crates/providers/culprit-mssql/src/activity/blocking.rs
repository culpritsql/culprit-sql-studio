//! A focused closure reuses the held Activity reader; selected native details keep its original lifecycle.
use super::{
    ActivityFrame, ActivityReader, ActivityRecord, NativeRead, RequestLocator,
    capture::{CompletedActivityFrame, PendingActivityFrame},
    participants::{self, RequestState},
};
use culprit_core::{Engine, Result, telemetry::*};
use std::collections::BTreeSet;

mod metadata_locks;
pub(in crate::activity) mod output_context;

pub(crate) const SOURCE: &str = "mssql-blocking";

impl<R: NativeRead> ActivityReader<R> {
    pub(crate) async fn capture_focused_blocking(
        &mut self,
        focus: &ActivityRequestFocus,
    ) -> Result<culprit_read::FrozenObservation> {
        validate_focus(focus)?;
        let mut pending = PendingActivityFrame::begin_blocking(self, focus).await?;
        // A truncated/hidden census cannot prove that the original native request is absent.
        pending.require_complete()?;
        let authority = pending.native_authority()?;
        participants::matches_owner(focus, authority)?;
        let connection_generation = authority.connection_generation();
        self.require_blocking_connection(connection_generation)?;
        let selected = BlockingSelection::from_frame(&pending.baseline.frame, focus, &self.options.budget)?;
        selected.restrict(&mut pending);
        let lock_snapshot = if selected.observed()
            && selected.context.closure.status == BlockingClosureStatus::Observed
            && metadata_locks::selected(&pending.baseline.frame, focus)
            && let Some(limit) = metadata_locks::snapshot_limit(&pending.baseline.frame, &self.options.budget)
        {
            let observed = metadata_locks::read(self, &pending.baseline.frame, limit).await?;
            self.require_blocking_connection(connection_generation)?;
            Some(observed)
        } else {
            None
        };
        if selected.observed() && selected.context.closure.status != BlockingClosureStatus::NotRead {
            let limit = self.options.budget.max_sql_bytes;
            // Current SQL only. The same enrichment guards generations/locators; idle previous batches stay uncollected.
            self.enrich(&mut pending.baseline.frame, &selected.context.keys, false, limit).await?;
            self.require_blocking_connection(connection_generation)?;
        }
        if let Some(first) = lock_snapshot {
            let last = metadata_locks::read(self, &pending.baseline.frame, first.limit).await?;
            self.require_blocking_connection(connection_generation)?;
            metadata_locks::attach(&mut pending.baseline.frame, &first, &last)?;
        }
        let check = if selected.observed() {
            self.check_blocking_request(&pending.baseline.frame, focus).await?
        } else {
            FinalRequestCheck { state: BlockingRequestState::NotRead, at: None }
        };
        self.require_blocking_connection(connection_generation)?;
        let completed = pending.finish(self).await?;
        finish_observation(completed, focus, selected, check)
    }
    fn require_blocking_connection(&self, original: Option<u64>) -> Result<()> {
        if self.connection_generation() != original {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
    async fn check_blocking_request(
        &mut self,
        frame: &ActivityFrame,
        focus: &ActivityRequestFocus,
    ) -> Result<FinalRequestCheck> {
        let ordinal = frame.records.iter().position(|row| row.facts.key == focus.original.key).ok_or_else(invalid)?;
        let checks = self.request_participants(frame, &[ordinal]).await?;
        let check = checks.get(&ordinal).ok_or_else(invalid)?;
        let state = match check.observation_state(&focus.original) {
            RequestState::SameStatement => BlockingRequestState::SameStatement,
            RequestState::Missing => BlockingRequestState::Missing,
            RequestState::Changed => BlockingRequestState::Changed,
            RequestState::Unverified => return Err(CollectionStop::SourceUnavailable.error()),
        };
        Ok(FinalRequestCheck { state, at: Some(check.at.clone()) })
    }
}
struct FinalRequestCheck {
    state: BlockingRequestState,
    at: Option<String>,
}
struct BlockingSelection {
    binding: FocusedRequestBinding,
    context: BlockingContextSelection,
}
impl BlockingSelection {
    fn from_frame(frame: &ActivityFrame, focus: &ActivityRequestFocus, budget: &CollectionBudget) -> Result<Self> {
        let binding = binding(focus, &frame.records)?;
        let max_participants = budget.max_snapshot_rows.min(MAX_BLOCKING_PARTICIPANTS);
        let context = if binding == FocusedRequestBinding::Matched {
            let current = frame.records.iter().find(|row| row.facts.key == focus.original.key).ok_or_else(invalid)?;
            let roots = BTreeSet::from([focus.original.key.clone()]);
            if current.facts.waiting_on_lock == Some(true) {
                ActivityEvidence::bounded_context_for(&frame.observations(), &roots, max_participants)?
            } else {
                not_read(roots, max_participants)
            }
        } else {
            not_read(BTreeSet::new(), max_participants)
        };
        Ok(Self { binding, context })
    }
    fn observed(&self) -> bool {
        self.binding == FocusedRequestBinding::Matched
    }
    fn restrict(&self, pending: &mut PendingActivityFrame) {
        pending.baseline.frame.records.retain(|row| self.context.keys.contains(&row.facts.key));
        let count = pending.baseline.frame.records.len() as u32;
        pending.baseline.coverage = StateSourceCoverage::observed(SOURCE.into(), count, false);
        if self.context.closure.is_limited() {
            pending.baseline.coverage.stopped = Some(CollectionStop::RowLimit);
        }
    }
}
fn finish_observation(
    mut completed: CompletedActivityFrame,
    focus: &ActivityRequestFocus,
    selected: BlockingSelection,
    check: FinalRequestCheck,
) -> Result<culprit_read::FrozenObservation> {
    let context = completed.action_context.as_ref().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    if !focused_request_original_scope_matches(focus, context)
        || check.at.as_deref().is_some_and(|at| {
            ObservationBounds::new(&completed.frame.context.at, at).is_none()
                || ObservationBounds::new(at, &context.observed_until).is_none()
        })
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    let SourceObservation::Activity(snapshot) = completed.description.observation else { return Err(invalid()) };
    let result = ActivityBlockingFocusResult {
        focus: focus.clone(),
        binding: selected.binding,
        membership: if selected.observed() {
            FocusedRequestMembership::Observed
        } else {
            FocusedRequestMembership::NotObserved
        },
        request_end: check.state,
        closure: selected.context.closure,
    };
    completed.description.source = SOURCE.into();
    completed.description.observation = SourceObservation::FocusedBlocking { snapshot, result: Box::new(result) };
    let observations = crate::telemetry::activity::export_observations(completed.frame.records);
    let rows =
        observations.iter().map(|row| ActivityFrameRow { observation: row, reference: None }).collect::<Vec<_>>();
    let evidence = ActivityFrameEvidence::new(&completed.description, &rows, true)?;
    let SourceObservation::FocusedBlocking { result, .. } = &completed.description.observation else {
        return Err(invalid());
    };
    if !result.accepts_frame(&evidence) {
        return Err(invalid());
    }
    Ok(culprit_read::FrozenObservation {
        description: completed.description,
        records: observations.into_iter().map(TelemetryRecord::from).collect(),
    })
}
fn validate_focus(focus: &ActivityRequestFocus) -> Result<()> {
    if !focus.is_blocking_focus()
        || focus.source != culprit_core::source_id::SESSIONS
        || focus.target.engine() != Engine::Mssql
        || !focus.original.has_session_generation_authority()
        || focus.database_writable != Some(true)
        || !focus.partition.is_qualified()
        || RequestLocator::from_identity(&focus.statement_identity).is_none()
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn validate_native_profile(authority: &crate::local_source::Context) -> Result<()> {
    // The new manual route declares only the owned boxed SQL16 profile. Ordinary Activity keeps its own support.
    if authority.activity.edition != 3
        || authority.activity.major != 16
        || authority.activity.writable != Some(true)
        || authority.activity.replica_id.is_some()
    {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    if !authority.is_qualified() || !authority.database_wide {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    Ok(())
}
fn binding(focus: &ActivityRequestFocus, rows: &[ActivityRecord]) -> Result<FocusedRequestBinding> {
    let original_request = focus.original.key.request.as_ref().ok_or_else(invalid)?;
    let mut candidates = rows.iter().filter(|row| {
        row.facts.key.session.id == focus.original.key.session.id
            && row.facts.key.request.as_ref().is_some_and(|request| request.id == original_request.id)
    });
    let current = candidates.next();
    if current.is_some_and(|row| !row.facts.has_session_generation_authority()) {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    if candidates.next().is_some() {
        return Err(invalid());
    }
    Ok(match current {
        None => FocusedRequestBinding::ParticipantMissing,
        Some(row)
            if row.facts.key == focus.original.key
                && row.facts.statement_identity.as_ref() == Some(&focus.statement_identity) =>
        {
            FocusedRequestBinding::Matched
        }
        Some(_) => FocusedRequestBinding::ParticipantChanged,
    })
}
fn not_read(keys: BTreeSet<ActivityKey>, max_participants: u32) -> BlockingContextSelection {
    BlockingContextSelection {
        keys,
        closure: BlockingClosureDescription {
            status: BlockingClosureStatus::NotRead,
            max_lookups: MAX_BLOCKING_LOOKUPS,
            max_depth: MAX_BLOCKING_DEPTH,
            max_participants,
        },
    }
}
fn invalid() -> culprit_core::Error {
    CollectionStop::InvalidPage.error()
}

#[cfg(test)]
pub(crate) mod tests;
