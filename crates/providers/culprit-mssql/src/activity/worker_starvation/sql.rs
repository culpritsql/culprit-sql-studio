//! One bounded native census. TOP bounds returned rows, not native enumeration/cost. No per-request DMV scan.
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-os-workers-transact-sql
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-os-tasks-transact-sql
//! https://learn.microsoft.com/sql/relational-databases/system-dynamic-management-objects/sys-dm-os-waiting-tasks-transact-sql
use super::super::{IdentityDetail, page_server_reads::Availability, sql};

const TASK_COLUMNS: &str = r#"CONVERT(varchar(16),task.task_address,2) AS worker_task_address,
 task.task_state AS worker_task_state,task.scheduler_id AS worker_scheduler_id,node.parent_node_id AS worker_node_id,node.work_queue_count AS worker_scheduler_queue_count,
 task.session_id AS worker_session_id,task.request_id AS worker_request_id,task.exec_context_id AS worker_execution_context_id,
 CONVERT(varchar(16),task.worker_address,2) AS assigned_worker_address,
 CONVERT(varchar(16),worker.task_address,2) AS worker_current_task_address,
 worker.worker_created_ms_ticks,worker.task_bound_ms_ticks,worker.state AS worker_state,worker.is_preemptive"#;
const EMPTY_TASK_COLUMNS: &str = r#"CAST(NULL AS varchar(16)) AS worker_task_address,CAST(NULL AS nvarchar(60)) AS worker_task_state,
 CAST(NULL AS int) AS worker_scheduler_id,CAST(NULL AS int) AS worker_node_id,CAST(NULL AS bigint) AS worker_scheduler_queue_count,
 CAST(NULL AS smallint) AS worker_session_id,CAST(NULL AS int) AS worker_request_id,CAST(NULL AS int) AS worker_execution_context_id,
 CAST(NULL AS varchar(16)) AS assigned_worker_address,CAST(NULL AS varchar(16)) AS worker_current_task_address,
 CAST(NULL AS bigint) AS worker_created_ms_ticks,CAST(NULL AS bigint) AS task_bound_ms_ticks,
 CAST(NULL AS nvarchar(60)) AS worker_state,CAST(NULL AS bit) AS is_preemptive"#;
const WAIT_COLUMNS: &str = r#"CONVERT(varchar(16),waiter.waiting_task_address,2) AS worker_waiting_task_address,
 waiter.session_id AS worker_wait_session_id,waiter.exec_context_id AS worker_wait_execution_context_id,
 task.request_id AS worker_wait_request_id,task.scheduler_id AS worker_wait_scheduler_id,node.parent_node_id AS worker_wait_node_id,waiter.wait_type AS worker_wait_type,waiter.wait_duration_ms AS worker_wait_ms,
 CONVERT(varchar(16),waiter.blocking_task_address,2) AS worker_blocking_task_address,
 waiter.blocking_session_id AS worker_blocking_session_id,waiter.blocking_exec_context_id AS worker_blocking_execution_context_id"#;
const REQUEST_JOIN: &str = r#"LEFT JOIN sys.dm_exec_requests r ON r.session_id=task.session_id AND r.request_id=task.request_id
 LEFT JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id AND s.is_user_process=1
 LEFT JOIN sys.dm_exec_sessions b ON b.session_id=r.blocking_session_id"#;

pub(super) fn observation(identity: IdentityDetail, availability: Availability) -> String {
    let requests = format!(
        r#"{},CASE WHEN r.request_id IS NULL THEN 1 ELSE 0 END AS no_current_request,
 CASE WHEN r.database_id=DB_ID() THEN 1 ELSE 5 END AS scope_basis,
 CAST(NULL AS bigint) AS transaction_id,CAST(NULL AS varchar(40)) AS transaction_start"#,
        sql::request_columns(availability)
    );
    let rows = format!(
        r#"SELECT 1 AS worker_row_kind,{TASK_COLUMNS},{WAIT_COLUMNS},{requests}
 FROM (SELECT TOP(@P2) task_address,task_state,scheduler_id,session_id,request_id,exec_context_id,worker_address
       FROM sys.dm_os_tasks WHERE @worker_visibility=1 AND (session_id IS NULL OR session_id<>@@SPID)) task
 LEFT JOIN sys.dm_os_workers worker ON worker.worker_address=task.worker_address
 LEFT JOIN sys.dm_os_schedulers node ON node.scheduler_id=task.scheduler_id
 LEFT JOIN sys.dm_os_waiting_tasks waiter ON waiter.waiting_task_address=task.task_address
   AND (waiter.session_id=task.session_id OR waiter.session_id IS NULL AND task.session_id IS NULL)
   AND waiter.exec_context_id=task.exec_context_id
 {REQUEST_JOIN}
 UNION ALL
 SELECT 2 AS worker_row_kind,{EMPTY_TASK_COLUMNS},{WAIT_COLUMNS},{requests}
 FROM (SELECT TOP(@P3) waiting_task_address,session_id,exec_context_id,wait_type,wait_duration_ms,
       blocking_task_address,blocking_session_id,blocking_exec_context_id
       FROM sys.dm_os_waiting_tasks WHERE @worker_visibility=1 AND wait_type=N'THREADPOOL'
          AND (session_id IS NULL OR session_id<>@@SPID)) waiter
 LEFT JOIN sys.dm_os_tasks task ON task.task_address=waiter.waiting_task_address
   AND (task.session_id=waiter.session_id OR task.session_id IS NULL AND waiter.session_id IS NULL)
   AND task.exec_context_id=waiter.exec_context_id
 LEFT JOIN sys.dm_os_schedulers node ON node.scheduler_id=task.scheduler_id
 {REQUEST_JOIN}"#
    );
    // The existing positive role/permission witness is deliberately conservative. A missing or denied view
    // remains unavailable; a successful filtered request join never proves actor absence or full visibility.
    let permission = super::super::visibility::server_state();
    let select = sql::request_frame_select_with_context(
        &rows,
        identity,
        sql::QueryPolicy::SerialMetadata,
        "@worker_visibility AS worker_visibility,",
    );
    format!("{}DECLARE /* culprit */ @worker_visibility bit={permission};\n{select}", sql::FRAME_CLOCK)
}
