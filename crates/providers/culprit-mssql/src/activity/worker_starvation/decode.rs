//! Decode only documented native columns; unknown joins stay absent rather than gaining an actor by ID.
use super::*;
use crate::read::{boolean, optional_counter, optional_signed_integer, optional_text, text};

pub(super) struct Decoded {
    pub task: Option<MssqlWorkerTask>,
    pub wait: Option<MssqlWorkerWait>,
    pub request: Option<ActivityRecord>,
}
pub(super) fn row(row: &Rec, kind: i64) -> Result<Decoded> {
    let request = if optional_counter(row, "session_id")?.is_some()
        && optional_counter(row, "request_id")?.is_some()
        && optional_text(row, "session_generation")?.is_some()
        && optional_text(row, "request_generation")?.is_some()
    {
        rows::record(row)?
    } else {
        None
    };
    let request_key = request.as_ref().map(|row| row.facts.key.clone());
    let wait = wait(row, request_key.clone())?;
    let task = if kind == 1 {
        let worker = optional_text(row, "assigned_worker_address")?
            .map(|address| {
                Ok::<_, culprit_core::Error>(MssqlWorkerBinding {
                    address,
                    task_address: optional_text(row, "worker_current_task_address")?,
                    created_ms_ticks: optional_signed_integer(row, "worker_created_ms_ticks")?
                        .map(|value| value.to_string()),
                    task_bound_ms_ticks: optional_signed_integer(row, "task_bound_ms_ticks")?
                        .map(|value| value.to_string()),
                    state: optional_text(row, "worker_state")?,
                    is_preemptive: optional_bool(row, "is_preemptive")?,
                })
            })
            .transpose()?;
        Some(MssqlWorkerTask {
            address: text(row, "worker_task_address")?,
            state: text(row, "worker_task_state")?,
            scheduler_id: unsigned(row, "worker_scheduler_id")?,
            parent_node_id: unsigned(row, "worker_node_id")?,
            scheduler_work_queue_count: optional_counter(row, "worker_scheduler_queue_count")?,
            session_id: small(row, "worker_session_id")?,
            request_id: unsigned(row, "worker_request_id")?,
            execution_context_id: unsigned(row, "worker_execution_context_id")?,
            worker,
            wait: wait.clone(),
            request: request_key,
        })
    } else {
        None
    };
    if task.as_ref().is_some_and(|task| !task.is_valid()) || wait.as_ref().is_some_and(|wait| !wait.is_valid()) {
        return Err(invalid_data());
    }
    if kind == 2 && wait.is_none() {
        return Err(invalid_data());
    }
    Ok(Decoded { task, wait: if kind == 2 { wait } else { None }, request })
}
fn wait(row: &Rec, request: Option<ActivityKey>) -> Result<Option<MssqlWorkerWait>> {
    let Some(task_address) = optional_text(row, "worker_waiting_task_address")? else { return Ok(None) };
    Ok(Some(MssqlWorkerWait {
        task_address,
        session_id: small(row, "worker_wait_session_id")?,
        request_id: unsigned(row, "worker_wait_request_id")?,
        scheduler_id: unsigned(row, "worker_wait_scheduler_id")?,
        parent_node_id: unsigned(row, "worker_wait_node_id")?,
        request,
        execution_context_id: unsigned(row, "worker_wait_execution_context_id")?,
        native_type: text(row, "worker_wait_type")?,
        elapsed_ms: optional_counter(row, "worker_wait_ms")?,
        blocking_task_address: optional_text(row, "worker_blocking_task_address")?,
        blocking_session_id: small(row, "worker_blocking_session_id")?,
        blocking_execution_context_id: optional_signed_integer(row, "worker_blocking_execution_context_id")?
            .map(|value| i32::try_from(value).map_err(|_| invalid_data()))
            .transpose()?,
    }))
}
fn small(row: &Rec, name: &str) -> Result<Option<i16>> {
    optional_signed_integer(row, name)?.map(|value| i16::try_from(value).map_err(|_| invalid_data())).transpose()
}
fn unsigned(row: &Rec, name: &str) -> Result<Option<u32>> {
    optional_counter(row, name)?.map(|value| value.parse().map_err(|_| invalid_data())).transpose()
}
fn optional_bool(row: &Rec, name: &str) -> Result<Option<bool>> {
    if matches!(rows::value(row, name)?, crate::value::Val::Null) { Ok(None) } else { boolean(row, name).map(Some) }
}
