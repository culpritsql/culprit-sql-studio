use super::*;
use chrono::{NaiveDate, NaiveTime};

pub(super) struct QualifiedAgentWindow {
    pub agent_session_id: String,
    pub observed_from: String,
    pub observed_until: String,
}

/// Active SQL Server Agent job rows remain in `sysjobactivity` with `stop_execution_date IS NULL` even if Agent stops
/// or restarts. Only a complete, unchanged read bracketed by the same running Agent session and running Agent service
/// rows qualifies the UTC observation window `[observed_from, observed_until]`.
pub(super) fn qualified_active_bounds(
    snapshot: &MssqlAgentJobsSnapshot,
    read_complete: bool,
) -> Option<QualifiedAgentWindow> {
    if !read_complete
        || snapshot.metadata.availability != MssqlAgentAvailability::Supported
        || snapshot.metadata.recheck != MssqlAgentMetadataRecheck::Unchanged
        || ![MssqlAgentObject::Jobs, MssqlAgentObject::Activity, MssqlAgentObject::Sessions, MssqlAgentObject::Services]
            .into_iter()
            .all(|object| snapshot.metadata.available(object))
    {
        return None;
    }
    let before = snapshot.observations.iter().find_map(|row| match &row.fact {
        OperationalFact::MssqlAgentContext(point) if point.bound == MssqlAgentContextBound::Before => {
            Some(point.as_ref())
        }
        _ => None,
    })?;
    let after = snapshot.observations.iter().find_map(|row| match &row.fact {
        OperationalFact::MssqlAgentContext(point) if point.bound == MssqlAgentContextBound::After => {
            Some(point.as_ref())
        }
        _ => None,
    })?;
    let session = before.session.as_ref().filter(|session| after.session.as_ref() == Some(*session))?;
    if before.services_limited
        || after.services_limited
        || before.services.is_empty()
        || before.services != after.services
        || !before.services.iter().all(|service| service.status == Some(4))
    {
        return None;
    }
    let observed_from = canonical_time(&before.observed_at).ok()?;
    let observed_until = canonical_time(&after.observed_at).ok()?;
    ObservationBounds::new(&observed_from, &observed_until)?;
    Some(QualifiedAgentWindow { agent_session_id: session.session_id.clone(), observed_from, observed_until })
}

pub(super) fn entry(
    row: &OperationalObservation,
    acquisition: &JobAcquisitionReference,
    qualified_window: Option<&QualifiedAgentWindow>,
) -> Result<Option<JobExecution>> {
    match &row.fact {
        OperationalFact::MssqlAgentHistory(point) => Ok(Some(JobExecution {
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
        })),
        OperationalFact::MssqlAgentJob(point) => {
            let Some(activity) = point.activity.as_ref() else {
                return Ok(None);
            };
            let Some(started_without_offset) =
                activity.started_without_offset.as_ref().filter(|_| activity.stopped_without_offset.is_none())
            else {
                return Ok(None);
            };
            let observed_at = canonical_time(&point.observed_at).map_err(|_| invalid())?;
            let qualified = qualified_window.filter(|window| {
                window.agent_session_id == activity.agent_session_id
                    && ObservationBounds::new(&window.observed_from, &observed_at).is_some()
                    && ObservationBounds::new(&observed_at, &window.observed_until).is_some()
            });
            Ok(Some(JobExecution {
                reference: reference(
                    row,
                    acquisition,
                    JobExecutionIdentity::MssqlAgentActive {
                        job_id: point.job_id.clone(),
                        agent_session_id: activity.agent_session_id.clone(),
                        started_without_offset: started_without_offset.clone(),
                    },
                ),
                status: JobExecutionStatus::InProgress,
                label: Some(JobExecutionLabel {
                    text: point.name.clone(),
                    origin: JobExecutionLabelOrigin::CurrentDefinition,
                }),
                time: JobExecutionTime::MssqlAgentActive {
                    observed_from: qualified.map(|window| window.observed_from.clone()),
                    observed_at,
                    observed_until: qualified.map(|window| window.observed_until.clone()),
                    started_without_offset: started_without_offset.clone(),
                    last_executed_step_id: activity.last_executed_step_id.clone(),
                    last_executed_step_without_offset: activity.last_executed_step_without_offset.clone(),
                },
                diagnostics: JobExecutionDiagnostics::Unavailable,
                placement: if qualified.is_some() {
                    JobExecutionPlacement::SourceQualifiedWindow
                } else {
                    JobExecutionPlacement::NativeLocal
                },
            }))
        }
        _ => Ok(None),
    }
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
