//! Finite worker-resource questions use the held Activity reader and its source/participant/SQL owners.
//! All native errors stop this source; a missing request never means ended/recovered under filtered visibility.
use super::{
    ActivityContext, ActivityFrame, ActivityReader, ActivityRecord, NativeStatement,
    participants::{Check, RequestState},
    rows,
};
use crate::{
    read::{NativeRead, Parameter, ReadError, boolean, integer, invalid_data},
    value::Rec,
};
use culprit_core::{Result, telemetry::*};
use std::collections::{BTreeMap, BTreeSet};
mod decode;
mod sql;

pub(crate) struct Capture {
    pub owner: crate::local_source::Context,
    pub point: MssqlWorkerStarvationPoint,
}
struct Population {
    at: String,
    tasks: Vec<MssqlWorkerTask>,
    waits: Vec<MssqlWorkerWait>,
    frame: ActivityFrame,
    tasks_limited: bool,
    waits_limited: bool,
}
impl<R: NativeRead> ActivityReader<R> {
    pub(crate) async fn capture_worker_starvation(&mut self, expected: &SourceReadExecutionScope) -> Result<Capture> {
        if self.observed || !expected.accepts_question(SourceReadQuestion::WorkerStarvation) {
            return Err(invalid_data());
        }
        self.observed = true;
        let owner = super::database_context::initial(self).await?;
        if !owner.is_qualified() || owner.connection_generation().is_none_or(|generation| generation == 0) {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        if !matches!(owner.activity.edition, 2 | 3 | 4 | 5 | 8) || owner.activity.major < 13 {
            return Err(culprit_core::Error::new(
                culprit_core::ErrorCode::Unsupported,
                culprit_core::t!("collection.unsupported"),
            ));
        }
        let initial = execution_context(&owner, &owner, expected)?;
        if !expected.matches_context(&initial) {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.options.identity = super::IdentityDetail::for_native_edition(owner.activity.edition);
        let mut population = self.worker_population(&owner).await?;
        let indices = (0..population.frame.records.len()).collect::<Vec<_>>();
        let before = self.request_participants(&population.frame, &indices).await?;
        let selected = before
            .iter()
            .filter(|(index, check)| {
                check.request_state(&population.frame.records[**index]) == RequestState::SameStatement
            })
            .map(|(index, _)| population.frame.records[*index].facts.key.clone())
            .collect::<BTreeSet<_>>();
        let detail = self
            .enrich_using_policy(
                &mut population.frame,
                &selected,
                false,
                self.options.budget.max_sql_bytes,
                super::sql::QueryPolicy::SerialMetadata,
            )
            .await?;
        // The shared enrichment owner can preserve optional SQL failure for interactive callers. This source
        // must not dispatch checks after a native error or silently reacquire a replacement session/SET.
        if let ActivityDetailStatus::Unavailable { code } = detail {
            let mut error = CollectionStop::SourceUnavailable.error();
            error.code = code;
            return Err(error);
        }
        let after = self.request_participants(&population.frame, &indices).await?;
        let requests = checked_requests(population.frame, &before, &after)?;
        let final_owner = super::database_context::recheck(self, &owner).await?;
        let context = execution_context(&owner, &final_owner, expected)?;
        let worker_scope = owner.source_instance().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        let point = MssqlWorkerStarvationPoint {
            context,
            worker_scope,
            observed_at: population.at,
            tasks: population.tasks,
            threadpool_waits: population.waits,
            requests,
            tasks_limited: population.tasks_limited,
            waits_limited: population.waits_limited,
        };
        if !point.is_valid() {
            return Err(invalid_data());
        }
        Ok(Capture { owner, point })
    }
    async fn worker_population(&mut self, owner: &crate::local_source::Context) -> Result<Population> {
        let cap = (self.options.budget.max_snapshot_rows as usize / 2).min(MAX_WORKER_TASKS).min(MAX_WORKER_WAITERS);
        if cap == 0 {
            return Err(CollectionStop::RowLimit.error());
        }
        let maximum = 2 * (cap + 1) + 1;
        let native = self
            .io
            .query(
                &sql::observation(self.options.identity, owner.activity.request_page_server_read_capability),
                vec![
                    Parameter::Integer(maximum as i64),
                    Parameter::Integer((cap + 1) as i64),
                    Parameter::Integer((cap + 1) as i64),
                ],
                maximum,
            )
            .await
            .map_err(ReadError::into_error)?;
        let first = native.first().ok_or_else(invalid_data)?;
        let context = ActivityContext::decode(first, &self.host, self.port, self.options.identity)?;
        if !owner.matches_frame(&context) || owner.activity.at > context.at {
            return Err(CollectionStop::SourceChanged.error());
        }
        if !boolean(first, "worker_visibility")? {
            return Err(CollectionStop::SourceUnavailable.error());
        }
        let mut tasks = Vec::new();
        let mut waits = Vec::new();
        let mut requests = BTreeMap::<ActivityKey, ActivityRecord>::new();
        let mut task_rows = 0;
        let mut wait_rows = 0;
        for row in &native {
            if context != ActivityContext::decode(row, &self.host, self.port, self.options.identity)?
                || !boolean(row, "worker_visibility")?
            {
                return Err(CollectionStop::SourceChanged.error());
            }
            if matches!(rows::value(row, "worker_row_kind")?, crate::value::Val::Null) {
                continue;
            }
            let kind = integer(row, "worker_row_kind")?;
            let retain = match kind {
                1 => {
                    task_rows += 1;
                    task_rows <= cap
                }
                2 => {
                    wait_rows += 1;
                    wait_rows <= cap
                }
                _ => return Err(invalid_data()),
            };
            if !retain {
                continue;
            }
            let row = decode::row(row, kind)?;
            if let Some(request) = row.request {
                if let Some(previous) = requests.get(&request.facts.key) {
                    if previous.facts.statement_identity != request.facts.statement_identity {
                        return Err(CollectionStop::SourceChanged.error());
                    }
                } else {
                    requests.insert(request.facts.key.clone(), request);
                }
            }
            tasks.extend(row.task);
            waits.extend(row.wait);
        }
        let frame = ActivityFrame { context: context.clone(), records: requests.into_values().collect() };
        ActivityEvidence::validate(&frame.observations())?;
        Ok(Population {
            at: context.at,
            tasks,
            waits,
            frame,
            tasks_limited: task_rows > cap,
            waits_limited: wait_rows > cap,
        })
    }
    pub(crate) async fn verify_worker_starvation(
        &mut self,
        owner: &crate::local_source::Context,
        until: &str,
    ) -> Result<bool> {
        let after = super::database_context::recheck(self, owner).await?;
        Ok(owner.same_source(&after) && ObservationBounds::new(until, &after.activity.at).is_some())
    }
}
fn execution_context(
    before: &crate::local_source::Context,
    after: &crate::local_source::Context,
    expected: &SourceReadExecutionScope,
) -> Result<NativeReadContext> {
    let mut context = before.completed(after)?;
    if matches!(expected, SourceReadExecutionScope::Shared(_)) {
        let owner = before.source_instance().ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
        let partition = if let Some(instance) = &before.instance {
            if after.instance.as_ref() != Some(instance) {
                return Err(CollectionStop::SourceChanged.error());
            }
            SourcePartition::InstanceLocal {
                member: Some(instance.identity.clone()),
                epoch: Some(instance.capture_epoch()?),
            }
        } else {
            before.source_partition()?
        };
        context.target = CollectionTarget::Shared(owner);
        context.partition = partition;
        context.database_writable = None;
    }
    if !expected.matches_context(&context) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(context)
}
fn checked_requests(
    frame: ActivityFrame,
    before: &BTreeMap<usize, Check>,
    after: &BTreeMap<usize, Check>,
) -> Result<Vec<MssqlWorkerRequest>> {
    frame
        .records
        .into_iter()
        .enumerate()
        .map(|(index, mut record)| {
            let check = |values: &BTreeMap<usize, Check>| {
                values.get(&index).map(|check| TaskRequestCheck {
                    observed_at: check.at.clone(),
                    state: match check.request_state(&record) {
                        RequestState::SameStatement => TaskRequestState::SameStatement,
                        RequestState::Changed => TaskRequestState::Changed,
                        RequestState::Missing => TaskRequestState::Missing,
                        RequestState::Unverified => TaskRequestState::Unverified,
                    },
                })
            };
            let before_check = check(before);
            let after_check = check(after);
            if TaskRequestContext::from_checks(true, &before_check, &after_check) != TaskRequestContext::CheckedAtBounds
            {
                record.current_sql = NativeStatement::Unavailable(PayloadUnavailable::Missing);
            }
            Ok(MssqlWorkerRequest { observation: record.into_redacted_observation(), before_check, after_check })
        })
        .collect()
}
#[cfg(test)]
pub(crate) mod tests;
