use super::*;
use chrono::{NaiveDate, NaiveTime};
pub(super) fn entry(
    row: &OperationalObservation,
    acquisition: &JobAcquisitionReference,
) -> Result<Option<JobExecution>> {
    let OperationalFact::MssqlAgentHistory(point) = &row.fact else {
        return Ok(None);
    };
    Ok(Some(JobExecution {
        reference: reference(
            row,
            acquisition,
            JobExecutionIdentity::MssqlAgentHistory {
                job_id: point.job_id.clone(),
                history_id: point.instance_id.clone(),
                step_id: point.step_id.clone(),
                run_date: point.run_date.clone(),
                run_time: point.run_time.clone(),
            },
        ),
        status: agent_status(point.run_status),
        label: point
            .stored_step_name
            .as_ref()
            .map(|name| JobExecutionLabel { text: name.clone(), origin: JobExecutionLabelOrigin::Stored }),
        time: JobExecutionTime::MssqlLocal {
            run_date: point.run_date.clone(),
            run_time: point.run_time.clone(),
            run_duration: point.run_duration.clone(),
            started_without_offset: local_start(point),
            duration_seconds: duration(&point.run_duration),
            history_written_time: point.run_status == 4,
        },
        diagnostics: JobExecutionDiagnostics::Mssql {
            sql_message_id: point.sql_message_id.clone(),
            sql_severity: point.sql_severity.clone(),
            retries_attempted: point.retries_attempted.clone(),
            native_status: point.run_status,
        },
        placement: JobExecutionPlacement::NativeLocal,
    }))
}
fn local_start(point: &MssqlAgentHistory) -> Option<String> {
    let date = point.run_date.parse::<u32>().ok()?;
    let time = point.run_time.parse::<u32>().ok()?;
    let day = NaiveDate::from_ymd_opt((date / 10000).try_into().ok()?, date / 100 % 100, date % 100)?;
    let time = NaiveTime::from_hms_opt(time / 10000, time / 100 % 100, time % 100)?;
    Some(day.and_time(time).format("%Y-%m-%dT%H:%M:%S").to_string())
}
fn duration(raw: &str) -> Option<String> {
    let raw = raw.parse::<u64>().ok()?;
    raw.checked_div(10000)?
        .checked_mul(3600)?
        .checked_add((raw / 100 % 100).checked_mul(60)?)?
        .checked_add(raw % 100)
        .map(|seconds| seconds.to_string())
}
