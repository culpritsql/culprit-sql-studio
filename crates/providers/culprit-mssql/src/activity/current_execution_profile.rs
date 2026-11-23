//! One bounded request profile on the existing held Activity reader. Errors stop; no profiling is enabled.
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-exec-query-profiles-transact-sql
use super::{
    ActivityReader, IdentityDetail, current_request_work as request_work,
    participants::{Check, RequestState},
};
use crate::{
    actual::{Availability, Layout, progress},
    read::{NativeRead, invalid_data},
};
use culprit_core::{Result, telemetry::*};

struct Detail {
    work: Box<MssqlCurrentRequestWork>,
    statement: DefinitionPayload,
    workers_before: Option<MssqlSelectedWorkerPoint>,
    workers_after: Option<MssqlSelectedWorkerPoint>,
    profiles: MssqlExecutionProfiles,
    original_inputs: Option<Box<request_work::OriginalCacheInputs>>,
    request_query_hash: Option<String>,
}
struct SelectedDetail {
    detail: Option<Box<Detail>>,
    checked_until: String,
    binding: FocusedRequestBinding,
    membership: FocusedRequestMembership,
    unavailable: MssqlExecutionProfilesUnavailable,
}
impl<R: NativeRead> ActivityReader<R> {
    pub(crate) async fn capture_current_execution_profile(
        &mut self,
        focus: &ActivityRequestFocus,
    ) -> Result<Box<MssqlCurrentExecutionProfilePoint>> {
        if self.observed {
            return Err(invalid_data());
        }
        self.observed = true;
        request_work::validate_focus(focus)?;
        let before = Box::new(super::database_context::initial(self).await?);
        request_work::admit(&before, focus)?;
        self.options.identity = IdentityDetail::for_native_edition(before.activity.edition);
        let first = Box::new(self.current_work_point(focus, before.activity.writable).await?);
        let observed_at = first.check.at.clone();
        let selected = self.execution_profile_detail(focus, &before, first).await?;
        let after = Box::new(super::database_context::recheck(self, &before).await?);
        let context = before.completed(&after)?;
        if !before.same_source(&after) || self.io.connection_generation() != before.connection_generation() {
            return Err(CollectionStop::SourceChanged.error());
        }
        let (work, statement, workers_before, workers_after, profiles, original_inputs, request_query_hash) =
            match selected.detail {
                Some(detail) => (
                    Some(detail.work),
                    detail.statement,
                    detail.workers_before,
                    detail.workers_after,
                    detail.profiles,
                    detail.original_inputs,
                    detail.request_query_hash,
                ),
                None => (
                    None,
                    DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
                    None,
                    None,
                    MssqlExecutionProfiles::Unavailable { reason: selected.unavailable },
                    None,
                    None,
                ),
            };
        let mut point = Box::new(MssqlCurrentExecutionProfilePoint {
            context,
            focus: Box::new(focus.clone()),
            observed_at,
            checked_until: selected.checked_until,
            binding: selected.binding,
            membership: selected.membership,
            statement,
            work,
            workers_before,
            workers_after,
            profiles,
            relational_inputs: None,
        });
        let (operators, inputs) =
            progress::operators::from_point(&point, request_query_hash.as_deref(), original_inputs);
        point.relational_inputs = inputs;
        if let MssqlExecutionProfiles::Observed { operators: selected, .. } = &mut point.profiles {
            *selected = operators;
        }
        if !point.is_valid() {
            return Err(invalid_data());
        }
        Ok(point)
    }
    async fn execution_profile_detail(
        &mut self,
        focus: &ActivityRequestFocus,
        owner: &crate::local_source::Context,
        mut first: Box<request_work::Point>,
    ) -> Result<SelectedDetail> {
        let state = first.check.selector_state(&focus.original.key, &focus.statement_identity);
        if state != RequestState::SameStatement {
            return Ok(unavailable(&first.check, state));
        }
        let (_, plan) = first.check.identities(&first.check).ok_or_else(invalid_data)?;
        let Some(plan) = plan else {
            let after = self.current_work_point(focus, owner.activity.writable).await?;
            if after.check.selector_state(&focus.original.key, &focus.statement_identity) != RequestState::SameStatement
            {
                return Ok(unavailable(
                    &after.check,
                    after.check.selector_state(&focus.original.key, &focus.statement_identity),
                ));
            }
            let mut work = first.work.take().ok_or_else(invalid_data)?;
            work.checked_until = after.check.at.clone();
            return Ok(SelectedDetail {
                checked_until: after.check.at,
                binding: FocusedRequestBinding::Matched,
                membership: FocusedRequestMembership::Observed,
                unavailable: MssqlExecutionProfilesUnavailable::PlanUnavailable,
                detail: Some(Box::new(Detail {
                    work: Box::new(work),
                    statement: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
                    workers_before: None,
                    workers_after: None,
                    profiles: MssqlExecutionProfiles::Unavailable {
                        reason: MssqlExecutionProfilesUnavailable::PlanUnavailable,
                    },
                    original_inputs: None,
                    request_query_hash: first.check.query_hash().map(str::to_owned),
                })),
            });
        };
        let workers_before = self.selected_worker_point(owner, focus).await?;
        let layout = Layout::read_selected_profiles(&mut self.io).await?;
        if layout.request_page_server_read_capability != owner.activity.request_page_server_read_capability {
            return Err(CollectionStop::SourceChanged.error());
        }
        let cache_before = self.current_execution_cache(focus, &plan, owner.activity.writable).await?;
        let limit = (self.options.budget.max_snapshot_rows as usize / 2).min(MAX_EXECUTION_PROFILE_ROWS);
        if limit == 0 {
            return Err(CollectionStop::RowLimit.error());
        }
        let profiles = match layout.profiles {
            Availability::Available => {
                progress::selected::read(&mut self.io, owner, &first.check, &plan, limit).await?
            }
            Availability::Absent => {
                progress::selected::SelectedProfiles::Unavailable(MssqlExecutionProfilesUnavailable::LayoutUnavailable)
            }
            Availability::Hidden => {
                progress::selected::SelectedProfiles::Unavailable(MssqlExecutionProfilesUnavailable::MetadataHidden)
            }
        };
        let workers_after = self.selected_worker_point(owner, focus).await?;
        let cache_after = if let request_work::CacheReading::Unavailable(reason) = &cache_before {
            request_work::CacheReading::Unavailable(*reason)
        } else {
            self.current_execution_cache(focus, &plan, owner.activity.writable).await?
        };
        let after = self.current_work_point(focus, owner.activity.writable).await?;
        let state = after.check.selector_state(&focus.original.key, &focus.statement_identity);
        if state != RequestState::SameStatement {
            return Ok(unavailable(&after.check, state));
        }
        if ObservationBounds::new(&first.check.at, &after.check.at).is_none() {
            return Err(CollectionStop::SourceChanged.error());
        }
        let mut work = first.work.take().ok_or_else(invalid_data)?;
        work.checked_until = after.check.at.clone();
        work.request_plan = first.check.identities(&after.check).and_then(|(_, plan)| plan);
        let mut statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
        let (current_plan, original_inputs) =
            request_work::qualify_cache_inputs(cache_before, cache_after, work.request_plan.as_ref(), &mut statement)?;
        work.current_plan = current_plan;
        let original_inputs = original_inputs
            .filter(|input| progress::operators::original_context_matches(&input.context, first.check.query_hash()));
        if let Some(original) = &original_inputs {
            let mut bindings = crate::relational_inputs::Bindings::default();
            if !bindings.admit(&original.context)? {
                return Err(invalid_data());
            }
            // Independent plan→original SQL-token proof follows both selected payload endpoints.
            // An errored/unconfirmed read returns immediately; final native verification is never skipped.
            bindings.require_verified(&mut self.io).await?;
        }
        let profiles = match profiles {
            _ if work.request_plan.is_none() => {
                MssqlExecutionProfiles::Unavailable { reason: MssqlExecutionProfilesUnavailable::PlanChanged }
            }
            progress::selected::SelectedProfiles::Unavailable(reason) => MssqlExecutionProfiles::Unavailable { reason },
            progress::selected::SelectedProfiles::Observed(read) => MssqlExecutionProfiles::Observed {
                observed_from: read.observed_from,
                observed_until: read.observed_until,
                rows: read.rows,
                limited: read.limited,
                operators: MssqlExecutionOperators::Unavailable {
                    reason: MssqlExecutionOperatorsUnavailable::SelectedPlanUnavailable,
                },
            },
        };
        Ok(SelectedDetail {
            checked_until: after.check.at,
            binding: FocusedRequestBinding::Matched,
            membership: FocusedRequestMembership::Observed,
            unavailable: MssqlExecutionProfilesUnavailable::NotObserved,
            detail: Some(Box::new(Detail {
                work: Box::new(work),
                statement,
                workers_before: Some(workers_before),
                workers_after: Some(workers_after),
                profiles,
                original_inputs,
                request_query_hash: first.check.query_hash().map(str::to_owned),
            })),
        })
    }
}
fn unavailable(check: &Check, state: RequestState) -> SelectedDetail {
    let (binding, membership, reason) = match state {
        RequestState::Missing => (
            FocusedRequestBinding::Matched,
            FocusedRequestMembership::NotObserved,
            MssqlExecutionProfilesUnavailable::NotObserved,
        ),
        RequestState::Changed => (
            FocusedRequestBinding::ParticipantChanged,
            FocusedRequestMembership::NotObserved,
            MssqlExecutionProfilesUnavailable::ParticipantChanged,
        ),
        _ => (
            FocusedRequestBinding::Matched,
            FocusedRequestMembership::Unavailable,
            MssqlExecutionProfilesUnavailable::UnverifiedParticipant,
        ),
    };
    SelectedDetail { detail: None, checked_until: check.at.clone(), binding, membership, unavailable: reason }
}
#[cfg(test)]
pub(crate) mod tests;
