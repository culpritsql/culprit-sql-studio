//! One original request, selected native plan and its cache-generation endpoints use the held Activity reader.
//! Positive exact rows remain useful under documented visibility; an empty selection proves no actor outcome.
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-exec-requests-transact-sql
//! Row-mode parallel reads/writes/logical_reads/row_count are coordinator fields, not whole-query totals.
use super::participants::{self, Check, RequestState, payloads};
use super::*;
use crate::{
    local_source::Context,
    query_store::payload::Payload,
    read::{optional_counter, optional_signed_integer},
};

pub(crate) const SOURCE: &str = culprit_core::source_id::MSSQL_CURRENT_REQUEST_WORK;
pub(super) struct Point {
    pub check: Check,
    pub work: Option<MssqlCurrentRequestWork>,
}
pub(super) struct CachePoint {
    at: String,
    context: Option<CachedStatementContext>,
    unavailable: MssqlCurrentPlanUnavailable,
    bodies: payloads::Bodies,
    module: ModuleRead,
}
enum ModuleRead {
    NotRequested,
    Unavailable,
    Observed(Option<MssqlModuleParameters>),
}
impl ModuleRead {
    fn from_row(row: &Rec, context: &CachedStatementContext) -> Result<Self> {
        if crate::read::optional_boolean(row, "encrypted")? != Some(false) {
            return Ok(Self::Unavailable);
        }
        if let Some(parameters) = crate::relational_inputs::parameters(row, context)? {
            return Ok(Self::Observed(Some(parameters)));
        }
        let native_module = ["module_database_id", "module_object_id", "module_number"]
            .into_iter()
            .map(|name| crate::read::optional_signed_integer(row, name))
            .collect::<Result<Vec<_>>>()?;
        // Native ad-hoc text has no module header. A hidden/truncated positive module never becomes that case.
        Ok(if native_module.iter().all(Option::is_none) { Self::Observed(None) } else { Self::Unavailable })
    }
}
/// Original bounded inputs remain private until the completed native owner can bind their classification.
pub(crate) struct OriginalCacheInputs {
    pub context: CachedStatementContext,
    pub module_parameters: Option<MssqlModuleParameters>,
    pub statement: String,
    pub plan: String,
}

impl<R: NativeRead> ActivityReader<R> {
    pub(crate) async fn capture_current_request_work(
        &mut self,
        focus: &ActivityRequestFocus,
    ) -> Result<CurrentRequestWorkSnapshot> {
        if self.observed {
            return Err(invalid_data());
        }
        self.observed = true;
        validate_focus(focus)?;
        let before = Box::new(Context::read(&mut self.io, &self.host, self.port).await?);
        admit(&before, focus)?;
        let first = self.current_work_point(focus, before.activity.writable).await?;
        let first_state = first.check.selector_state(&focus.original.key, &focus.statement_identity);
        let mut binding = focused_binding(first_state);
        let mut unavailable = first_state == RequestState::Unverified;
        let mut observations = Vec::new();
        if first_state == RequestState::SameStatement {
            let (_, plan) = first.check.identities(&first.check).ok_or_else(invalid_data)?;
            let mut work = first.work.ok_or_else(invalid_data)?;
            let mut statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
            let caches = if let Some(plan) = &plan {
                Some(self.current_work_cache_pair(focus, plan, before.activity.writable).await?)
            } else {
                None
            };
            let after = self.current_work_point(focus, before.activity.writable).await?;
            let state = after.check.selector_state(&focus.original.key, &focus.statement_identity);
            binding = focused_binding(state);
            unavailable = state == RequestState::Unverified;
            if state == RequestState::SameStatement {
                if ObservationBounds::new(&first.check.at, &after.check.at).is_none() {
                    return Err(CollectionStop::SourceChanged.error());
                }
                work.checked_until = after.check.at.clone();
                work.request_plan = first.check.identities(&after.check).and_then(|(_, plan)| plan);
                if let Some((left, right)) = caches {
                    work.current_plan = qualify_cache(left, right, work.request_plan.as_ref(), &mut statement)?;
                }
                observations.push(CurrentRequestWorkObservation {
                    observed_at: first.check.at,
                    actor: focus.original.key.clone(),
                    database: before.activity.database.database.clone(),
                    scope: ActivityScopeBasis::RequestDatabase,
                    statement_identity: focus.statement_identity.clone(),
                    statement,
                    native: CurrentRequestWorkNative::Mssql { work: Box::new(work) },
                });
            }
        }
        let after = Box::new(before.recheck(&mut self.io, &self.host, self.port).await?);
        let context = before.completed(&after)?;
        if !before.same_source(&after)
            || self.io.connection_generation() != before.connection_generation()
            || observations.iter().any(|row| {
                !ObservationBounds::new(&context.observed_from, &context.observed_until)
                    .is_some_and(|bounds| bounds.contains(&row.observed_at))
            })
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        let membership = if unavailable {
            FocusedRequestMembership::Unavailable
        } else if observations.is_empty() {
            FocusedRequestMembership::NotObserved
        } else {
            FocusedRequestMembership::Observed
        };
        let coverage = if unavailable {
            StateSourceCoverage::unavailable(SOURCE.into(), ErrorCode::Unsupported, None)
        } else {
            StateSourceCoverage::observed(SOURCE.into(), observations.len() as u32, !observations.is_empty())
        };
        let snapshot = CurrentRequestWorkSnapshot {
            profile: CurrentRequestWorkProfile::MssqlRequest,
            source: SOURCE.into(),
            context,
            coverage,
            result: FocusedRequestResult {
                focus: Some(Box::new(focus.clone())),
                database_writable: before.activity.writable,
                binding,
                membership,
            },
            observations,
        };
        if !snapshot.is_valid() {
            return Err(invalid_data());
        }
        Ok(snapshot)
    }

    pub(super) async fn current_work_point(
        &mut self,
        focus: &ActivityRequestFocus,
        role: Option<bool>,
    ) -> Result<Point> {
        let rows = self
            .io
            .query(&point_sql(), vec![Parameter::Text(participants::selector_keys(&[(0, &focus.original.key)])?)], 2)
            .await
            .map_err(ReadError::into_error)?;
        let [row] = rows.as_slice() else {
            return Err(invalid_data());
        };
        crate::cached_statements::acquisition_frame::require_role(row, role)?;
        let check = Check::decode(row)?;
        if check.ordinal != 0 {
            return Err(invalid_data());
        }
        let signed = |name| {
            optional_signed_integer(row, name)?.map(|n| i32::try_from(n).map_err(|_| invalid_data())).transpose()
        };
        let native_database_id = crate::read::optional_integer(row, "work_database_id")?;
        let work = native_database_id
            .map(|database| -> Result<_> {
                Ok(MssqlCurrentRequestWork {
                    native_database_id: u32::try_from(database).map_err(|_| invalid_data())?,
                    checked_until: check.at.clone(),
                    elapsed_ms: signed("work_elapsed_ms")?,
                    cpu_ms: signed("work_cpu_ms")?,
                    reads: optional_counter(row, "work_reads")?,
                    writes: optional_counter(row, "work_writes")?,
                    logical_reads: optional_counter(row, "work_logical_reads")?,
                    rows_returned: optional_counter(row, "work_rows_returned")?,
                    request_plan: None,
                    current_plan: MssqlCurrentRequestPlan::Unavailable {
                        reason: MssqlCurrentPlanUnavailable::NotCollected,
                    },
                })
            })
            .transpose()?;
        if let Some(work) = &work
            && check.selector().is_some_and(|(_, statement)| {
                statement.qualifiers.get("database_id") != Some(&work.native_database_id.to_string())
            })
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(Point { check, work })
    }

    async fn current_work_cache_pair(
        &mut self,
        focus: &ActivityRequestFocus,
        plan: &NativeIdentity,
        role: Option<bool>,
    ) -> Result<(CacheReading, CacheReading)> {
        let left = self.current_work_cache(focus, plan, role).await?;
        // Failure, cancellation or unconfirmed work returns immediately; it never dispatches the second read.
        if let CacheReading::Unavailable(reason) = left {
            return Ok((CacheReading::Unavailable(reason), CacheReading::Unavailable(reason)));
        }
        let right = self.current_work_cache(focus, plan, role).await?;
        Ok((left, right))
    }
    pub(super) async fn current_work_cache(
        &mut self,
        focus: &ActivityRequestFocus,
        plan: &NativeIdentity,
        role: Option<bool>,
    ) -> Result<CacheReading> {
        self.selected_cache(focus, plan, role, payloads::Kind::CurrentRequest).await
    }
    pub(super) async fn current_execution_cache(
        &mut self,
        focus: &ActivityRequestFocus,
        plan: &NativeIdentity,
        role: Option<bool>,
    ) -> Result<CacheReading> {
        self.selected_cache(focus, plan, role, payloads::Kind::CurrentExecutionInputs).await
    }
    async fn selected_cache(
        &mut self,
        focus: &ActivityRequestFocus,
        plan: &NativeIdentity,
        role: Option<bool>,
        projection: payloads::Kind,
    ) -> Result<CacheReading> {
        let allowance = payload_allowance(&self.options.budget)?;
        // An errored native read retires ReadSession's held client. Continuing would need a new
        // acquisition/setup before the final generation check; it cannot preserve this source owner.
        let rows = self
            .io
            .query(
                &payloads::query(projection),
                vec![
                    payloads::parameter(&focus.original.key, &focus.statement_identity, plan)?,
                    Parameter::Integer(allowance.0 as i64),
                    Parameter::Integer(allowance.1 as i64),
                ],
                3,
            )
            .await
            .map_err(ReadError::into_error)?;
        if rows.len() > 2 {
            return Err(CollectionStop::RowLimit.error());
        }
        for row in &rows {
            crate::cached_statements::acquisition_frame::require_role(row, role)?;
            if integer(row, "ordinal")? != 0 {
                return Err(invalid_data());
            }
        }
        if rows.len() > 1 {
            return Ok(CacheReading::Unavailable(MssqlCurrentPlanUnavailable::Ambiguous));
        }
        let Some(row) = rows.first() else {
            return Ok(CacheReading::Unavailable(MssqlCurrentPlanUnavailable::NotObserved));
        };
        let mut unavailable = MssqlCurrentPlanUnavailable::NotObserved;
        let context = if crate::read::optional_integer(row, "plan_generation_num")?.is_some()
            && crate::read::optional_integer(row, "database_id")?.is_some()
        {
            let context = crate::cached_statements::context(row)?;
            if focus.statement_identity.qualifiers.get("database_id") != Some(&context.database_id.to_string()) {
                unavailable = MssqlCurrentPlanUnavailable::Unsupported;
                None
            } else {
                let candidate = MssqlCurrentRequestPlan::Observed {
                    observed_from: text(row, "observed_at")?,
                    observed_until: text(row, "observed_at")?,
                    context: Box::new(context.clone()),
                    plan: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
                };
                if !candidate.matches_statement(&focus.statement_identity)
                    || !context
                        .identity
                        .plan_handle
                        .strip_prefix("0x")
                        .is_some_and(|handle| handle.eq_ignore_ascii_case(&plan.id))
                {
                    return Err(CollectionStop::SourceChanged.error());
                }
                Some(context)
            }
        } else {
            None
        };
        let module = match (projection, context.as_ref()) {
            (payloads::Kind::CurrentExecutionInputs, Some(context)) => ModuleRead::from_row(row, context)?,
            _ => ModuleRead::NotRequested,
        };
        Ok(CacheReading::Observed(Box::new(CachePoint {
            at: canonical_time(&text(row, "observed_at")?)?,
            context,
            unavailable,
            bodies: payloads::decode(row, allowance.0, allowance.1)?,
            module,
        })))
    }
}

pub(super) enum CacheReading {
    Observed(Box<CachePoint>),
    Unavailable(MssqlCurrentPlanUnavailable),
}
pub(super) fn qualify_cache(
    left: CacheReading,
    right: CacheReading,
    request_plan: Option<&NativeIdentity>,
    statement: &mut DefinitionPayload,
) -> Result<MssqlCurrentRequestPlan> {
    qualify_cache_inputs(left, right, request_plan, statement).map(|(plan, _)| plan)
}
pub(super) fn qualify_cache_inputs(
    left: CacheReading,
    right: CacheReading,
    request_plan: Option<&NativeIdentity>,
    statement: &mut DefinitionPayload,
) -> Result<(MssqlCurrentRequestPlan, Option<Box<OriginalCacheInputs>>)> {
    let (left, right) = match (left, right) {
        (CacheReading::Observed(left), CacheReading::Observed(right)) => (left, right),
        (CacheReading::Unavailable(reason), _) | (_, CacheReading::Unavailable(reason)) => {
            return Ok((MssqlCurrentRequestPlan::Unavailable { reason }, None));
        }
    };
    if ObservationBounds::new(&left.at, &right.at).is_none() {
        return Err(CollectionStop::SourceChanged.error());
    }
    let original_module = match (&left.module, &right.module) {
        (ModuleRead::Observed(first), ModuleRead::Observed(last)) if first == last => Some(first.clone()),
        (ModuleRead::Observed(_), ModuleRead::Observed(_)) => return Err(CollectionStop::SourceChanged.error()),
        _ => None,
    };
    let original_bodies = original_module.and_then(|module_parameters| {
        let statement = left.bodies.statement.text()?;
        let plan = left.bodies.plan.text()?;
        (Some(statement) == right.bodies.statement.text() && Some(plan) == right.bodies.plan.text())
            .then(|| (module_parameters, statement.to_owned(), plan.to_owned()))
    });
    *statement = if left.bodies.statement.text() == right.bodies.statement.text() {
        match left.bodies.statement {
            Payload::Text(raw) => DefinitionPayload::sql(&raw, Engine::Mssql),
            Payload::Unavailable(reason) => DefinitionPayload::Unavailable { reason },
        }
    } else {
        DefinitionPayload::Unavailable { reason: PayloadUnavailable::SourceUnavailable }
    };
    let missing_reason = if left.context.is_none() { left.unavailable } else { right.unavailable };
    let (Some(left_context), Some(right_context)) = (left.context, right.context) else {
        return Ok((MssqlCurrentRequestPlan::Unavailable { reason: missing_reason }, None));
    };
    if request_plan.is_none()
        || left_context.identity.key()? != right_context.identity.key()?
        || left_context.database_id != right_context.database_id
        || left_context.query_hash != right_context.query_hash
    {
        return Ok((
            MssqlCurrentRequestPlan::Unavailable { reason: MssqlCurrentPlanUnavailable::ChangedDuringRead },
            None,
        ));
    }
    let plan = if left.bodies.plan.text() == right.bodies.plan.text() {
        match left.bodies.plan {
            Payload::Text(raw) => DefinitionPayload::plan(&raw, Engine::Mssql),
            Payload::Unavailable(reason) => DefinitionPayload::Unavailable { reason },
        }
    } else {
        DefinitionPayload::Unavailable { reason: PayloadUnavailable::SourceUnavailable }
    };
    let inputs = original_bodies.map(|(module_parameters, statement, plan)| {
        Box::new(OriginalCacheInputs { context: left_context.clone(), module_parameters, statement, plan })
    });
    Ok((
        MssqlCurrentRequestPlan::Observed {
            observed_from: left.at,
            observed_until: right.at,
            context: Box::new(left_context),
            plan,
        },
        inputs,
    ))
}
fn focused_binding(state: RequestState) -> FocusedRequestBinding {
    match state {
        RequestState::SameStatement => FocusedRequestBinding::Matched,
        RequestState::Missing => FocusedRequestBinding::Matched,
        RequestState::Changed => FocusedRequestBinding::ParticipantChanged,
        RequestState::Unverified => FocusedRequestBinding::Matched,
    }
}
pub(crate) fn validate_focus(focus: &ActivityRequestFocus) -> Result<()> {
    if !CurrentRequestWorkProfile::MssqlRequest.accepts_focus(focus)
        || !focus.original.has_session_generation_authority()
        || !super::established_login(&focus.original.key.session.generation)
        || focus.source != culprit_core::source_id::SESSIONS
    {
        return Err(invalid_data());
    }
    if focus.database_writable.is_none()
        || !focus.partition.is_qualified()
        || focus.partition.native_epoch() != Some(focus.epoch.as_str())
    {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    Ok(())
}
pub(super) fn admit(context: &Context, focus: &ActivityRequestFocus) -> Result<()> {
    let supported = matches!(context.activity.edition, 2 | 3 | 4 | 8) && context.activity.major >= 16
        || context.activity.edition == 5 && context.activity.major >= 12;
    if !supported {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    if !context.is_qualified() || context.connection_generation().is_none_or(|n| n == 0) {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    let current = context.completed(context)?;
    if !focused_request_original_scope_matches(focus, &current)
        || focus.statement_identity.qualifiers.get("database_id") != Some(&context.database_id.to_string())
        || ObservationBounds::new(&focus.original_observed_at, &current.observed_from).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
fn point_sql() -> String {
    participants::participant_query_with_declarations(
        ",@culprit_request_updateability AS updateability,r.database_id AS work_database_id,r.total_elapsed_time AS work_elapsed_ms,r.cpu_time AS work_cpu_ms,r.reads AS work_reads,r.writes AS work_writes,r.logical_reads AS work_logical_reads,r.row_count AS work_rows_returned",
        "",
        "\nDECLARE @culprit_request_updateability nvarchar(60)=CONVERT(nvarchar(60),DATABASEPROPERTYEX(DB_NAME(),N'Updateability'));",
    )
}
fn payload_allowance(budget: &CollectionBudget) -> Result<(usize, usize)> {
    // Two possible cache candidates plus JSON/UTF-8 expansion and native metadata fit the same caller envelope.
    const CANDIDATES: usize = 2;
    const ENCODING: usize = 3;
    const METADATA: usize = 8 * 1024;
    let available =
        (budget.page_bytes as usize).min(budget.total_bytes as usize).saturating_sub(METADATA) / CANDIDATES / ENCODING;
    let each = available / 2;
    if each == 0 {
        return Err(CollectionStop::ByteLimit.error());
    }
    Ok((each.min(budget.max_sql_bytes as usize), each.min(budget.max_plan_bytes as usize)))
}

#[cfg(test)]
pub(crate) mod tests;
