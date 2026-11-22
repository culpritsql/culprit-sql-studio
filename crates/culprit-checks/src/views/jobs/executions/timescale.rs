use super::*;
pub(super) fn entry(
    row: &OperationalObservation,
    acquisition: &JobAcquisitionReference,
) -> Result<Option<JobExecution>> {
    let OperationalFact::TimescaleJobRun(point) = &row.fact else {
        return Ok(None);
    };
    let native = match &point.native_run_id {
        Some(run_id) => JobExecutionIdentity::TimescaleHistory {
            job_id: point.job_id.clone(),
            run_id: run_id.clone(),
            started_at: canonical_time(&point.started_at)?,
        },
        None => JobExecutionIdentity::TimescaleError {
            job_id: point.job_id.clone(),
            position: point.position,
            started_at: canonical_time(&point.started_at)?,
        },
    };
    Ok(Some(JobExecution {
        reference: reference(row, acquisition, native),
        status: timescale_status(point),
        label: point.procedure.as_ref().map(|procedure| JobExecutionLabel {
            text: format!("{}.{}", procedure.label.schema, procedure.label.name),
            origin: match procedure.origin {
                TimescaleJobProcedureOrigin::Stored => JobExecutionLabelOrigin::Stored,
                TimescaleJobProcedureOrigin::StoredOrCurrent => JobExecutionLabelOrigin::StoredOrCurrent,
                TimescaleJobProcedureOrigin::Unknown => JobExecutionLabelOrigin::Unknown,
            },
        }),
        time: JobExecutionTime::Utc { started_at: point.started_at.clone(), reported_until: point.finished_at.clone() },
        diagnostics: JobExecutionDiagnostics::Postgres { sql_state: point.sql_state.clone() },
        placement: if point.native_run_id.is_some() {
            JobExecutionPlacement::Utc
        } else {
            JobExecutionPlacement::NoNativeRunIdentity
        },
    }))
}
