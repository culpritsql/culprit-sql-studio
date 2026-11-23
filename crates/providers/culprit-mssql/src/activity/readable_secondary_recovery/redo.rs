//! The documented DB STARTUP/parallel-redo request row links only a native blocker session, not acquiring SQL.
use super::*;
use crate::read::{optional_counter, optional_signed_integer, optional_text};
pub(super) async fn read<R: NativeRead>(
    reader: &mut R,
    owner: &Context,
    original: &ActivityRequestFocus,
    limit: usize,
) -> Result<Box<MssqlRecoveryEndpoint>> {
    let (replica, replica_at) = Box::pin(crate::replication::local_secondary(reader, owner)).await?;
    let limit = limit.min(MAX_BLOCKING_PARTICIPANTS as usize);
    if limit == 0 {
        return Err(CollectionStop::RowLimit.error());
    }
    let session = original
        .original
        .key
        .session
        .id
        .parse::<i32>()
        .ok()
        .filter(|id| *id > 0 && *id <= i16::MAX as i32)
        .ok_or_else(invalid_data)?;
    let login = crate::sql_time::native_time(&original.original.key.session.generation)?;
    let rows = reader
        .query(
            SQL,
            vec![
                Parameter::Integer(i64::from(owner.database_id)),
                Parameter::Integer(i64::from(session)),
                Parameter::Text(login),
                Parameter::Integer((limit + 1) as i64),
            ],
            limit + 1,
        )
        .await
        .map_err(ReadError::into_error)?;
    if rows.is_empty() || rows.len() > limit + 1 {
        return Err(invalid_data());
    }
    let mut observed_at = None;
    let mut requests = Vec::new();
    for row in rows {
        let at = canonical_time(&text(&row, "observed_at")?)?;
        if observed_at.as_ref().is_some_and(|previous| previous != &at)
            || integer(&row, "selected_database_id")? != i64::from(owner.database_id)
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        crate::cached_statements::acquisition_frame::require_role(&row, owner.activity.writable)?;
        observed_at = Some(at);
        let Some(session_id) = optional_counter(&row, "redo_session_id")? else {
            if requests.is_empty() { continue } else { return Err(invalid_data()) }
        };
        let number = |name| -> Result<u32> { integer(&row, name)?.try_into().map_err(|_| invalid_data()) };
        let reader_session =
            match (optional_counter(&row, "reader_session_id")?, optional_text(&row, "reader_generation")?) {
                (Some(id), Some(generation)) => {
                    Some(NativeActivityIdentity { id, generation: crate::sql_time::native_time(&generation)? })
                }
                (None, None) => None,
                _ => return Err(invalid_data()),
            };
        requests.push(MssqlRedoRequest {
            session_id: session_id.parse().map_err(|_| invalid_data())?,
            request_id: number("redo_request_id")?,
            session_generation_without_offset: crate::sql_time::native_time(&text(&row, "redo_generation")?)?,
            started_without_offset: optional_text(&row, "redo_started")?
                .map(|value| crate::sql_time::native_time(&value))
                .transpose()?,
            command: text(&row, "redo_command")?,
            wait_type: optional_text(&row, "redo_wait_type")?,
            wait_ms: optional_counter(&row, "redo_wait_ms")?,
            wait_resource: optional_text(&row, "redo_wait_resource")?,
            blocking_session_id: optional_signed_integer(&row, "redo_blocker")?
                .map(|id| id.try_into().map_err(|_| invalid_data()))
                .transpose()?,
            reader_session,
        });
    }
    let limited = requests.len() > limit;
    requests.truncate(limit);
    let point = Box::new(MssqlRecoveryEndpoint {
        observed_at: observed_at.ok_or_else(invalid_data)?,
        replica_observed_at: replica_at,
        database_id: owner.database_id,
        replica: Box::new(replica),
        requests,
        requests_limited: limited,
    });
    if !point.is_valid() {
        return Err(invalid_data());
    }
    Ok(point)
}
const SQL: &str = r#"DECLARE /* culprit */ @at datetime2=SYSUTCDATETIME();
SELECT /* culprit */ DB_ID() AS selected_database_id,CONVERT(nvarchar(60),DATABASEPROPERTYEX(DB_NAME(),N'Updateability')) AS updateability,
 CONVERT(varchar(40),@at,126)+'Z' AS observed_at,r.session_id AS redo_session_id,r.request_id AS redo_request_id,
 CONVERT(varchar(40),CAST(r.start_time AS datetime2),126) AS redo_started,r.redo_generation,r.command AS redo_command,
 r.wait_type AS redo_wait_type,r.wait_time AS redo_wait_ms,r.wait_resource AS redo_wait_resource,r.blocking_session_id AS redo_blocker,
 reader.session_id AS reader_session_id,CONVERT(varchar(40),CAST(reader.login_time AS datetime2),126) AS reader_generation
FROM (SELECT @P1 AS database_id) selected
OUTER APPLY(SELECT TOP(@P4) native.*,CONVERT(varchar(40),CAST(s.login_time AS datetime2),126) AS redo_generation FROM sys.dm_exec_requests native JOIN sys.dm_exec_sessions s ON s.session_id=native.session_id
 WHERE native.database_id=selected.database_id AND native.database_id=DB_ID() AND s.is_user_process=0
 AND native.command IN(N'DB STARTUP',N'PARALLEL REDO TASK',N'PARALLEL REDO HELP TASK') ORDER BY native.session_id,native.request_id) r
LEFT JOIN sys.dm_exec_sessions reader ON reader.session_id=r.blocking_session_id AND reader.session_id=@P2
 AND CAST(reader.login_time AS datetime2)=CAST(@P3 AS datetime2)
OPTION(MAXDOP 1,LOOP JOIN)"#;
