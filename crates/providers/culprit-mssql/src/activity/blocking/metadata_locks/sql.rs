//! One native endpoint statement, called twice by the existing held selected reader. No baseline SQL changes.
use crate::activity::participants::CHECK_COLUMNS;

pub(super) fn query() -> String {
    TEMPLATE.replace("{CHECK_COLUMNS}", CHECK_COLUMNS)
}
const TEMPLATE: &str = r#"
DECLARE /* culprit */ @targets xml=CONVERT(xml,@P1),@limit int=@P2,@at datetime2(7)=SYSUTCDATETIME();
DECLARE @visible bit=CASE WHEN HAS_PERMS_BY_NAME(NULL,NULL,'VIEW SERVER PERFORMANCE STATE')=1 THEN 1 ELSE 0 END;
DECLARE @selected table(ordinal int PRIMARY KEY,session_id smallint,login_time datetime2(7),request_id int);
INSERT @selected SELECT n.value('@ordinal[1]','int'),n.value('@session[1]','smallint'),
 CONVERT(datetime2(7),n.value('@login[1]','varchar(40)'),126),n.value('@request[1]','int')
 FROM @targets.nodes('/activity/row') t(n);
DECLARE @actors table(ordinal int,observed_at varchar(40),checked_session_id smallint,
 checked_session_generation varchar(40),checked_request_id int,checked_request_generation varchar(40),
 checked_sql_handle varchar(128),checked_begin int,checked_end int,checked_database_id smallint,
 checked_wait_type nvarchar(60),checked_wait_resource nvarchar(256),checked_plan_handle varchar(128),
 checked_query_hash varchar(16),checked_plan_hash varchar(16),native_session_id smallint,native_login varchar(40),
 current_requests bigint,request_transaction_id bigint);
DECLARE @locks table(session_id int,resource_type nvarchar(60),subtype nvarchar(60),database_id int,
 description nvarchar(256),associated_entity_id varchar(20),lock_partition int,created_without_offset varchar(40),
 object_label nvarchar(517),mode nvarchar(60),status nvarchar(60),owner_type nvarchar(60),owner_id bigint,
 owner_guid uniqueidentifier,owner_address varchar(16),request_id int,execution_context_id int);
DECLARE @entries table(kind varchar(4),payload nvarchar(max),sort_session int,sort_address varchar(16));
IF @visible=1
BEGIN
 -- Restrict to the original selected/reached cohort before retaining ALL its current requests across databases.
 DECLARE @requests table(session_id smallint,current_requests bigint,request_id int,start_time datetime,sql_handle varbinary(64),
 statement_start_offset int,statement_end_offset int,database_id smallint,wait_type nvarchar(60),
 wait_resource nvarchar(256),plan_handle varbinary(64),query_hash binary(8),query_plan_hash binary(8),transaction_id bigint);
 -- Only a positively single current request can populate these native locator fields. No MAX selects a MARS sibling.
 INSERT @requests SELECT s.session_id,COUNT_BIG(*) AS current_requests,
 CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.request_id) END,CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.start_time) END,
 CASE WHEN COUNT_BIG(*)=1 THEN CONVERT(varbinary(64),MAX(CONVERT(varchar(128),r.sql_handle,2)),2) END,
 CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.statement_start_offset) END,CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.statement_end_offset) END,
 CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.database_id) END,CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.wait_type) END,
 CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.wait_resource) END,
 CASE WHEN COUNT_BIG(*)=1 THEN CONVERT(varbinary(64),MAX(CONVERT(varchar(128),r.plan_handle,2)),2) END,
 CASE WHEN COUNT_BIG(*)=1 THEN CONVERT(binary(8),MAX(CONVERT(varchar(16),r.query_hash,2)),2) END,
 CASE WHEN COUNT_BIG(*)=1 THEN CONVERT(binary(8),MAX(CONVERT(varchar(16),r.query_plan_hash,2)),2) END,
 CASE WHEN COUNT_BIG(*)=1 THEN MAX(r.transaction_id) END
 FROM @selected a JOIN sys.dm_exec_sessions s ON s.session_id=a.session_id
 AND CONVERT(datetime2(7),s.login_time)=a.login_time
 JOIN sys.dm_exec_requests r ON r.session_id=s.session_id GROUP BY s.session_id;
 INSERT @actors SELECT {CHECK_COLUMNS},s.session_id,
 CONVERT(varchar(40),CAST(s.login_time AS datetime2),126),ISNULL(population.current_requests,0),r.transaction_id
 FROM @selected a LEFT JOIN sys.dm_exec_sessions s ON s.session_id=a.session_id
 LEFT JOIN @requests r ON r.session_id=a.session_id AND r.request_id=a.request_id
 LEFT JOIN @requests population ON population.session_id=s.session_id;
 -- TOP bounds retained rows. It does not bound lock-manager work or qualify the source's native cost.
 INSERT @locks SELECT TOP(@limit+1) l.request_session_id,l.resource_type,l.resource_subtype,l.resource_database_id,
 l.resource_description,CONVERT(varchar(20),l.resource_associated_entity_id),l.resource_lock_partition,
 CONVERT(varchar(40),CAST(o.create_date AS datetime2),126),
 CASE WHEN o.object_id IS NOT NULL THEN QUOTENAME(SCHEMA_NAME(o.schema_id))+N'.'+QUOTENAME(o.name) END,
 l.request_mode,l.request_status,l.request_owner_type,l.request_owner_id,l.request_owner_guid,
 CONVERT(varchar(16),l.lock_owner_address,2),l.request_request_id,l.request_exec_context_id
 FROM sys.dm_tran_locks l JOIN @selected a ON l.request_session_id=a.session_id
 JOIN sys.dm_exec_sessions s ON s.session_id=a.session_id AND CONVERT(datetime2(7),s.login_time)=a.login_time
 LEFT JOIN sys.objects o ON o.object_id=l.resource_associated_entity_id
 WHERE l.resource_type=N'OBJECT' AND l.resource_database_id=DB_ID()
 ORDER BY l.request_session_id,l.resource_associated_entity_id,l.resource_lock_partition,l.lock_owner_address;
 DECLARE @transactions table(session_id int,transaction_id bigint,begun_without_offset varchar(40),is_local bit);
 INSERT @transactions SELECT st.session_id,st.transaction_id,
 MAX(CONVERT(varchar(40),CAST(at.transaction_begin_time AS datetime2),126)),
 CONVERT(bit,MIN(CASE WHEN st.is_local=1 AND st.is_user_transaction=1 AND st.is_bound=0 AND st.is_enlisted=0 AND at.transaction_type IN(1,2)
 AND at.transaction_state=2 THEN 1 ELSE 0 END))
 FROM(SELECT DISTINCT session_id,owner_id FROM @locks WHERE owner_type=N'TRANSACTION') ids
 JOIN sys.dm_tran_session_transactions st ON st.session_id=ids.session_id AND st.transaction_id=ids.owner_id
 JOIN sys.dm_tran_active_transactions at ON at.transaction_id=st.transaction_id GROUP BY st.session_id,st.transaction_id;
 INSERT @entries SELECT 'lock',(
 SELECT l.session_id,
 JSON_QUERY((SELECT l.resource_type,l.subtype,l.database_id,l.description,l.associated_entity_id,l.lock_partition,
 l.created_without_offset FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER)) AS resource,
 l.object_label,l.mode,l.status,l.owner_type,CONVERT(varchar(20),l.owner_id) AS owner_id,
 CONVERT(varchar(36),l.owner_guid) AS owner_guid,l.owner_address,l.request_id,l.execution_context_id,
 JSON_QUERY(CASE WHEN t.transaction_id IS NOT NULL THEN(SELECT CONVERT(varchar(20),t.transaction_id) AS id,
 t.begun_without_offset,t.is_local AS active_local_user_transaction FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER) END) AS [transaction]
 FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER),l.session_id,l.owner_address
 FROM @locks l LEFT JOIN @transactions t ON t.session_id=l.session_id AND t.transaction_id=l.owner_id
 AND l.owner_type=N'TRANSACTION';
 INSERT @entries SELECT TOP(@limit+1) 'task',(
 SELECT CONVERT(varchar(16),w.waiting_task_address,2) AS task_address,w.session_id,w.exec_context_id AS execution_context_id,
 CONVERT(varchar(16),w.resource_address,2) AS owner_address,w.blocking_session_id,
 w.blocking_exec_context_id AS blocker_execution_context_id
 FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER),w.session_id,CONVERT(varchar(16),w.resource_address,2)
 FROM sys.dm_os_waiting_tasks w JOIN @locks l ON l.session_id=w.session_id
 AND l.execution_context_id=w.exec_context_id AND l.owner_address=CONVERT(varchar(16),w.resource_address,2)
 WHERE l.status IN(N'WAIT',N'CONVERT')
 ORDER BY w.session_id,w.exec_context_id,w.waiting_task_address;
END;
INSERT @actors(ordinal,observed_at) VALUES(-1,CONVERT(varchar(40),@at,126)+'Z');
SELECT /* culprit */ a.*,@visible AS population_visible,
 CASE WHEN a.ordinal=-1 THEN(SELECT TOP(@limit+1) kind,JSON_QUERY(payload) AS data FROM @entries
 ORDER BY kind,sort_session,sort_address FOR JSON PATH,INCLUDE_NULL_VALUES) END AS lock_payload
 FROM @actors a ORDER BY a.ordinal OPTION(MAXDOP 1);
"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn one_bounded_native_endpoint_materializes_the_cohort_before_aggregation_and_reuses_shared_checks() {
        let sql = query();
        assert!(sql.contains(CHECK_COLUMNS));
        assert_eq!(sql.matches("FROM sys.dm_tran_locks").count(), 1);
        assert_eq!(sql.matches("FROM sys.dm_os_waiting_tasks").count(), 1);
        assert_eq!(sql.matches("JOIN sys.dm_exec_requests").count(), 1);
        let cohort = sql.find("FROM @selected a JOIN sys.dm_exec_sessions").unwrap();
        let requests = sql.find("JOIN sys.dm_exec_requests").unwrap();
        assert!(cohort < requests);
        let roster = sql.split("INSERT @requests").nth(1).unwrap().split("INSERT @actors").next().unwrap();
        assert!(roster.contains("COUNT_BIG(*) AS current_requests") && roster.contains("GROUP BY s.session_id"));
        assert!(!roster.contains("DB_ID()") && !roster.contains("OVER("));
        for field in [
            "resource_type",
            "resource_subtype",
            "resource_database_id",
            "resource_description",
            "resource_associated_entity_id",
            "resource_lock_partition",
            "transaction_begin_time",
            "st.is_local=1",
            "st.is_user_transaction=1",
            "l.owner_address=CONVERT(varchar(16),w.resource_address,2)",
            "o.create_date",
        ] {
            assert!(sql.contains(field), "{field}");
        }
        assert!(sql.contains("VIEW SERVER PERFORMANCE STATE"));
        assert!(sql.contains("IF @visible=1"));
        assert_eq!(sql.matches("SELECT /* culprit */ a.*").count(), 1);
        assert!(!sql.contains("sql_text(") && !sql.contains("DBCC") && !sql.contains("sp_executesql"));
    }
}
