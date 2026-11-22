//! One selected request/statement/plan payload owner, shared by writer and current-work investigations.
use crate::{
    activity::RequestLocator,
    query_store::payload::Payload,
    read::{Parameter, invalid_data},
    value::Rec,
};
use culprit_core::{Result, telemetry::*};

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Writers,
    CurrentRequest,
}

pub(crate) fn attributes(
    ordinal: usize,
    key: &ActivityKey,
    statement: &NativeIdentity,
    plan: &NativeIdentity,
) -> Result<String> {
    let locator = RequestLocator::from_identity(statement).ok_or_else(invalid_data)?;
    let request = key.request.as_ref().ok_or_else(invalid_data)?;
    if !CurrentRequestWorkProfile::MssqlRequest.matches_identity(key, ActivityScopeBasis::RequestDatabase, statement)
        || plan.qualifiers.get("native_object").map(String::as_str) != Some("sys.dm_exec_requests.plan_handle")
        || plan.id.is_empty()
        || plan.id.len() > 128
        || !plan.id.len().is_multiple_of(2)
        || !plan.id.bytes().all(|v| v.is_ascii_hexdigit())
        || plan.id.bytes().all(|v| v == b'0')
    {
        return Err(invalid_data());
    }
    Ok(format!(
        "ordinal=\"{ordinal}\" session=\"{}\" login=\"{}\" request=\"{}\" started=\"{}\" handle=\"{}\" begin=\"{}\" end=\"{}\" database=\"{}\" plan=\"{}\"",
        key.session.id,
        crate::sql_time::native_time(&key.session.generation)?,
        request.id,
        crate::sql_time::native_time(&request.generation)?,
        locator.handle,
        locator.start,
        locator.end,
        locator.database,
        plan.id
    ))
}
pub(crate) fn parameter(key: &ActivityKey, statement: &NativeIdentity, plan: &NativeIdentity) -> Result<Parameter> {
    Ok(Parameter::Text(format!("<requests><row {}/></requests>", attributes(0, key, statement, plan)?)))
}
pub(crate) struct Bodies {
    pub statement: Payload,
    pub plan: Payload,
}
pub(crate) fn decode(row: &Rec, statement: usize, plan: usize) -> Result<Bodies> {
    Ok(Bodies {
        statement: crate::cached_payload::read(row, "statement_bytes", "statement", statement)?.0,
        plan: crate::cached_payload::read(row, "plan_bytes", "plan", plan)?.0,
    })
}
pub(crate) fn query(kind: Kind) -> String {
    let statement = crate::cached_statements::statement_expression("r", "st.text");
    let statement_columns = crate::cached_payload::columns("fragment.statement", "statement_bytes", "statement", "@P2");
    let plan_columns = crate::cached_payload::columns("qp.query_plan", "plan_bytes", "plan", "@P3");
    let (clock,columns,cache) = match kind {
        Kind::Writers => (String::new(),String::new(),String::new()),
        Kind::CurrentRequest => (
            ",@at datetime2=SYSUTCDATETIME()".into(),
            format!(",CONVERT(varchar(40),@at,126)+'Z' AS observed_at,@culprit_cache_updateability AS updateability,{}",crate::cached_statements::context_columns_from("qs","st","qp.dbid")),
            "\nOUTER APPLY(SELECT TOP(2) stats.plan_handle,stats.sql_handle,stats.statement_start_offset,stats.statement_end_offset,stats.creation_time,stats.plan_generation_num,stats.query_hash FROM sys.dm_exec_query_stats stats WHERE stats.plan_handle=r.plan_handle AND stats.sql_handle=r.sql_handle AND stats.statement_start_offset=r.statement_start_offset AND stats.statement_end_offset=r.statement_end_offset)qs".into(),
        ),
    };
    let path = match kind {
        Kind::Writers => "writers",
        Kind::CurrentRequest => "requests",
    };
    let sql = format!(
        r#"
DECLARE /* culprit */ @selected xml=CAST(@P1 AS xml){clock};
SELECT /* culprit */ a.ordinal,st.encrypted,{statement_columns},{plan_columns}{columns}
FROM @selected.nodes('/{path}/row') selected(n)
CROSS APPLY(SELECT n.value('@ordinal[1]','int') AS ordinal,n.value('@session[1]','int') AS session_id,
 n.value('@login[1]','varchar(40)') AS login_time,n.value('@request[1]','int') AS request_id,
 n.value('@started[1]','varchar(40)') AS start_time,n.value('@handle[1]','varchar(128)') AS sql_handle,
 n.value('@begin[1]','int') AS statement_start_offset,n.value('@end[1]','int') AS statement_end_offset,
 n.value('@database[1]','int') AS database_id,n.value('@plan[1]','varchar(128)') AS plan_handle) a
LEFT JOIN sys.dm_exec_sessions s ON s.session_id=a.session_id
 AND s.login_time=CAST(CONVERT(datetime2,a.login_time,126) AS datetime)
LEFT JOIN sys.dm_exec_requests r ON r.session_id=s.session_id AND r.request_id=a.request_id
 AND r.start_time=CAST(CONVERT(datetime2,a.start_time,126) AS datetime)
 AND r.database_id=DB_ID() AND r.database_id=a.database_id
 AND r.sql_handle=CONVERT(varbinary(64),a.sql_handle,2)
 AND r.plan_handle=CONVERT(varbinary(64),a.plan_handle,2)
 AND r.statement_start_offset=a.statement_start_offset AND r.statement_end_offset=a.statement_end_offset
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY(SELECT {statement} AS statement) fragment
{plan_relation}{cache}
OPTION(MAXDOP 1,LOOP JOIN)"#,
        plan_relation = crate::cached_statements::statement_plan_relation("r", "qp"),
    );
    match kind {
        Kind::Writers => sql,
        Kind::CurrentRequest => crate::cached_statements::acquisition_frame::Projection::AcquisitionFrame.batch(sql),
    }
}
