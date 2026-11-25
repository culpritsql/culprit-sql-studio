//! A selected local AG secondary retains redo/blocker endpoints without user-workload execution or a new pool.
use super::participants::{self, RequestState};
use super::*;
use crate::local_source::Context;
mod redo;

pub(crate) fn validate_focus(focus: &ActivityRequestFocus) -> Result<()> {
    if !focus.is_readable_secondary_recovery_focus()
        || focus.target.engine() != Engine::Mssql
        || focus.source != culprit_core::source_id::SESSIONS
        || RequestLocator::from_identity(&focus.statement_identity).is_none()
    {
        return Err(invalid_data());
    }
    Ok(())
}
fn validate_native(owner: &Context) -> Result<()> {
    if owner.activity.major != 16 || owner.activity.edition != 3 || owner.activity.writable != Some(false) {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    if !owner.is_qualified() || !owner.database_wide {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    Ok(())
}
impl<R: NativeRead> ActivityReader<R> {
    #[cfg(test)]
    pub(crate) async fn capture_readable_secondary_recovery(
        &mut self,
        focus: &SourceReadFocus,
        execution: &SourceReadExecutionScope,
    ) -> Result<ReadableSecondaryRecoveryPoint> {
        self.capture_readable_secondary_recovery_with_profile(focus, execution, None).await
    }
    pub(crate) async fn capture_readable_secondary_recovery_with_profile(
        &mut self,
        focus: &SourceReadFocus,
        execution: &SourceReadExecutionScope,
        profile: Option<&culprit_read::SourceProfileGuard>,
    ) -> Result<ReadableSecondaryRecoveryPoint> {
        if self.observed {
            return Err(invalid_data());
        }
        self.observed = true;
        if let Some(profile) = profile {
            profile.ensure_active()?;
        }
        let original = focus.readable_secondary_request().ok_or_else(invalid_data)?;
        validate_focus(original)?;
        let before = Box::new(Box::pin(Context::read(&mut self.io, &self.host, self.port)).await?);
        validate_native(&before)?;
        if let Some(policy) = profile.and_then(culprit_read::SourceProfileGuard::builtin_policy) {
            crate::builtin_safety::verify_readable_secondary_recovery(policy, &before)?;
        }
        let initial = before.completed(&before)?;
        if !execution.matches_context(&initial) || !focused_request_original_scope_matches(original, &initial) {
            return Err(CollectionStop::SourceChanged.error());
        }
        if let Some(profile) = profile {
            profile.bind_native(&initial, self.io.connection_generation())?;
        }
        let first = Box::pin(participants::selected_check(&mut self.io, original, before.activity.writable)).await?;
        let state = first.selector_state(&original.original.key, &original.statement_identity);
        let (left, right, end) = if state == RequestState::SameStatement {
            let left =
                Box::pin(redo::read(&mut self.io, &before, original, self.options.budget.max_snapshot_rows as usize))
                    .await?;
            let right =
                Box::pin(redo::read(&mut self.io, &before, original, self.options.budget.max_snapshot_rows as usize))
                    .await?;
            let end = Box::pin(participants::selected_check(&mut self.io, original, before.activity.writable)).await?;
            (Some(left), Some(right), Some(end))
        } else {
            (None, None, None)
        };
        let after = Box::new(Box::pin(before.recheck(&mut self.io, &self.host, self.port)).await?);
        validate_native(&after)?;
        let context = before.completed(&after)?;
        if !before.same_source(&after)
            || !execution.matches_context(&context)
            || self.io.connection_generation() != before.connection_generation()
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        if let Some(profile) = profile {
            profile.verify_final(&context, self.io.connection_generation())?;
        }
        let (binding, membership) = match state {
            RequestState::SameStatement => (FocusedRequestBinding::Matched, FocusedRequestMembership::Observed),
            RequestState::Missing => (FocusedRequestBinding::ParticipantMissing, FocusedRequestMembership::NotObserved),
            RequestState::Changed => (FocusedRequestBinding::ParticipantChanged, FocusedRequestMembership::NotObserved),
            RequestState::Unverified => return Err(CollectionStop::SourceUnavailable.error()),
        };
        let request_end =
            match end.as_ref().map(|end| end.selector_state(&original.original.key, &original.statement_identity)) {
                Some(RequestState::SameStatement) => BlockingRequestState::SameStatement,
                Some(RequestState::Missing) => BlockingRequestState::Missing,
                Some(RequestState::Changed) => BlockingRequestState::Changed,
                Some(RequestState::Unverified) => return Err(CollectionStop::SourceUnavailable.error()),
                None => BlockingRequestState::NotRead,
            };
        let point = ReadableSecondaryRecoveryPoint {
            context,
            focus: Box::new(original.clone()),
            observed_at: first.at.clone(),
            checked_until: end.as_ref().map_or(first.at.clone(), |end| end.at.clone()),
            binding,
            membership,
            request_end,
            native: ReadableSecondaryRecoveryNative::Mssql { before: left, after: right },
        };
        if !point.is_valid() {
            return Err(invalid_data());
        }
        Ok(point)
    }
}
#[cfg(test)]
pub(crate) mod tests;
