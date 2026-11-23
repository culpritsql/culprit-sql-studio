//! One batched original-session cohort, in the existing metadata dispatch. Unknown scope/count never selects a request.
use super::*;
use crate::{
    read::{integer, invalid_data, optional_counter, optional_text},
    sql_time::native_time,
    value::Rec,
};
use culprit_core::blocking::{MssqlBlockingSessionPoint, NativeBlockingOwnerContext, NativeLockEvidence};
const MAX_COHORT_SESSIONS: usize = MAX_BLOCKING_LOOKUPS as usize + 1;

pub(in crate::activity) fn query(
    focus: &ActivityRequestFocus,
    identity: super::super::IdentityDetail,
    availability: super::super::page_server_reads::Availability,
) -> Result<(String, String)> {
    let cohort = cohort(focus)?;
    let columns = "1 AS output_context_collected,@output_population_visible AS output_request_population_visible,output_roster.current_requests AS output_owner_current_requests,output_roster.request_id AS output_owner_request_id,CONVERT(varchar(40),CAST(output_roster.request_generation AS datetime2),126) AS output_owner_request_generation,output_roster.wait_type AS output_owner_wait_type,output_roster.wait_ms AS output_owner_wait_ms,output_roster.sql_handle AS output_owner_sql_handle,output_roster.statement_start AS output_owner_statement_start,output_roster.statement_end AS output_owner_statement_end,output_roster.request_database_id AS output_owner_request_database_id";
    // The cohort is applied before aggregation; no selected-database predicate can hide a MARS sibling.
    let relation = r#" LEFT JOIN (
 SELECT os.session_id,os.login_time,COUNT_BIG(*) AS current_requests,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.request_id) END AS request_id,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.start_time) END AS request_generation,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.wait_type) END AS wait_type,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(CASE WHEN orr.wait_type IS NOT NULL THEN orr.wait_time END) END AS wait_ms,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(CONVERT(varchar(128),orr.sql_handle,2)) END AS sql_handle,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.statement_start_offset) END AS statement_start,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.statement_end_offset) END AS statement_end,
  CASE WHEN COUNT_BIG(*)=1 THEN MAX(orr.database_id) END AS request_database_id
 FROM @output_cohort.nodes('/cohort/session') oc(x)
 JOIN sys.dm_exec_sessions os ON os.session_id=oc.x.value('@id[1]','int')
  AND os.login_time=CAST(CONVERT(datetime2,oc.x.value('@login[1]','varchar(40)'),126) AS datetime)
 JOIN sys.dm_exec_requests orr ON orr.session_id=os.session_id AND @output_population_visible=1
 GROUP BY os.session_id,os.login_time) output_roster
 ON output_roster.session_id=b.session_id AND output_roster.login_time=b.login_time"#;
    let rows = super::super::sql::metadata_rows(availability, &format!(",{columns}"), relation);
    let sql = format!(
        "{}DECLARE /* culprit */ @output_cohort xml=CONVERT(xml,@P3), @output_population_visible bit=CASE WHEN HAS_PERMS_BY_NAME(NULL,NULL,'VIEW SERVER PERFORMANCE STATE')=1 THEN 1 ELSE 0 END;\n{}",
        super::super::sql::FRAME_CLOCK,
        super::super::sql::request_frame_select_with_context(
            &rows,
            identity,
            super::super::sql::QueryPolicy::Activity,
            ""
        )
    );
    Ok((sql, cohort))
}
fn cohort(focus: &ActivityRequestFocus) -> Result<String> {
    let mut sessions = BTreeSet::from([focus.original.key.session.clone()]);
    for owner in focus.original.wait.iter().flat_map(|wait| &wait.owners) {
        if let Some(session) = owner.session() {
            sessions.insert(session);
        }
    }
    if sessions.len() > MAX_COHORT_SESSIONS {
        return Err(CollectionStop::RowLimit.error());
    }
    let mut xml = String::from("<cohort>");
    for session in sessions {
        let id = session.id.parse::<u16>().map_err(|_| invalid_data())?;
        if id == 0 || id > i16::MAX as u16 || id.to_string() != session.id {
            return Err(invalid_data());
        }
        // Existing native datetime normalization rejects quotes, offsets and control text before XML construction.
        let login = native_time(&session.generation)?;
        xml.push_str(&format!("<session id=\"{id}\" login=\"{login}\"/>"));
    }
    xml.push_str("</cohort>");
    Ok(xml)
}
pub(in crate::activity) fn attach(row: &Rec, actor: &mut ActivityObservation) -> Result<()> {
    if super::super::rows::value(row, "output_context_collected").is_err() || actor.waiting_on_lock != Some(true) {
        return Ok(());
    }
    if integer(row, "output_context_collected")? != 1 {
        return Err(invalid_data());
    }
    let owners = actor.wait.as_ref().ok_or_else(invalid_data)?.owners.clone();
    let contexts = owners.into_iter().map(|owner| owner_context(row, owner)).collect::<Result<Vec<_>>>()?;
    let point = MssqlBlockingSessionPoint {
        observed_at: culprit_core::telemetry::canonical_time(&crate::read::text(row, "observed_at")?)?,
        waiter: actor.key.clone(),
        owners: contexts,
    };
    if !point.is_valid_for(&actor.key) {
        return Err(invalid_data());
    }
    let lock = culprit_core::LockWait {
        session: actor.key.session.id.clone(),
        mode: None,
        resource: None,
        object: None,
        index: None,
        native: Some(Box::new(NativeLockEvidence::MssqlBlockingSession(Box::new(point)))),
    };
    actor.wait.as_mut().ok_or_else(invalid_data)?.resources.get_or_insert_with(Vec::new).push(lock);
    Ok(())
}
fn owner_context(row: &Rec, owner: ActivityWaitOwner) -> Result<NativeBlockingOwnerContext> {
    let visible = match super::super::rows::value(row, "output_request_population_visible")? {
        crate::value::Val::Int(1) | crate::value::Val::Bool(true) => true,
        crate::value::Val::Int(0) | crate::value::Val::Bool(false) | crate::value::Val::Null => false,
        _ => return Err(invalid_data()),
    };
    let count = optional_counter(row, "output_owner_current_requests")?;
    let mut point = NativeBlockingOwnerContext {
        owner,
        actor: None,
        statement: None,
        native_wait: None,
        wait_ms: None,
        current_requests: count.clone(),
        complete_request_population: visible && count.is_some(),
    };
    if visible
        && count.as_deref() == Some("1")
        && let Some(session) = point.owner.session()
    {
        let id = optional_counter(row, "output_owner_request_id")?;
        let generation = optional_text(row, "output_owner_request_generation")?;
        if let (Some(id), Some(generation)) = (id, generation) {
            point.actor = Some(ActivityKey {
                session,
                request: Some(NativeActivityIdentity { id, generation: native_time(&generation)? }),
                transaction: None,
            });
            point.statement = super::super::rows::request_locator_with_prefix(row, "output_owner_")?
                .as_ref()
                .map(RequestLocator::identity);
            point.native_wait = optional_text(row, "output_owner_wait_type")?;
            point.wait_ms = optional_counter(row, "output_owner_wait_ms")?;
        }
    }
    if !point.is_valid() {
        return Err(invalid_data());
    }
    Ok(point)
}

#[cfg(test)]
mod tests;
