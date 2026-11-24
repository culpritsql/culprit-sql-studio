//! Pure native record classification shared by the jobs report, saved timeline and Gold adapter.
use super::*;
use culprit_core::{
    ChangeEvent, EventKind, EventSource, JobAcquisitionReference, JobExecution, JobExecutionAssessment,
    JobExecutionDiagnostics, JobExecutionIdentity, JobExecutionKind, JobExecutionLabel, JobExecutionLabelOrigin,
    JobExecutionPlacement, JobExecutionProjection, JobExecutionReference, JobExecutionSourceCoverage,
    JobExecutionStatus, JobExecutionTime, MAX_JOB_EXECUTION_ENTRIES,
};
mod agent;
mod mysql;
mod timescale;

fn invalid() -> Error {
    Error::invalid(t!("view.jobs.invalid"))
}

pub(super) fn project(read: &JobsSnapshot, maximum: usize) -> Result<JobExecutionProjection> {
    let acquisition = acquisition(read)?;
    let qualified_agent_window = match read {
        JobsSnapshot::MssqlAgent { snapshot, read_complete } => {
            agent::qualified_active_bounds(snapshot, *read_complete)
        }
        _ => None,
    };
    let mut entries = Vec::new();
    for row in read.observations() {
        let entry = match read {
            JobsSnapshot::MssqlAgent { .. } => agent::entry(row, &acquisition, qualified_agent_window.as_ref()),
            JobsSnapshot::Timescale { .. } => timescale::entry(row, &acquisition),
            JobsSnapshot::MysqlEvents { snapshot, .. } => {
                mysql::entry(row, &acquisition, snapshot.metadata.session_time_zone.as_deref())
            }
        }?;
        if let Some(entry) = entry {
            entries.push(entry);
        }
    }
    let mut identities = BTreeMap::new();
    for entry in &entries {
        *identities.entry(entry.reference.key()?).or_insert(0usize) += 1;
    }
    for entry in &mut entries {
        if identities.get(&entry.reference.key()?).copied().unwrap_or(0) > 1 {
            entry.placement = JobExecutionPlacement::AmbiguousNativeIdentity;
        }
    }
    let events = entries.iter().filter_map(event).collect::<Result<Vec<_>>>()?;
    let sources = read
        .sources()
        .iter()
        .map(|source| JobExecutionSourceCoverage {
            source: source.source.clone(),
            state: source.state,
            complete_scope: source.complete_scope,
            native_rows: source.records,
            observed_rows: read.observations().iter().filter(|row| row.source == source.source).count() as u32,
            stopped: source.stopped,
            error_code: source.error_code,
        })
        .collect();
    let mut assessment = JobExecutionAssessment {
        read_complete: read.read_complete(),
        sources,
        run_records: 0,
        step_records: 0,
        error_records: 0,
        last_start_records: 0,
        active_window_records: 0,
        utc_placed_records: events.len() as u32,
        unplaced_records: (entries.len() - events.len()) as u32,
        omitted_entries: entries.len().saturating_sub(maximum.min(MAX_JOB_EXECUTION_ENTRIES)) as u32,
    };
    for entry in &entries {
        match entry.reference.native.kind() {
            JobExecutionKind::Run | JobExecutionKind::Summary => assessment.run_records += 1,
            JobExecutionKind::Step => assessment.step_records += 1,
            JobExecutionKind::ErrorRecord => assessment.error_records += 1,
            JobExecutionKind::LastStart => assessment.last_start_records += 1,
            JobExecutionKind::ActiveWindow => assessment.active_window_records += 1,
        }
    }
    entries.truncate(maximum.min(MAX_JOB_EXECUTION_ENTRIES));
    if !assessment.is_valid() {
        return Err(invalid());
    }
    Ok(JobExecutionProjection { assessment, entries, events })
}
fn acquisition(read: &JobsSnapshot) -> Result<JobAcquisitionReference> {
    let context = read.context();
    let (member_id, process_generation) = match &context.partition {
        SourcePartition::Target => (None, None),
        SourcePartition::InstanceLocal { member, epoch } => (
            member.as_ref().map(SharedResourceIdentity::telemetry_id).transpose()?,
            epoch.as_ref().map(|value| digest(value.as_bytes())),
        ),
    };
    let namespace = match read {
        JobsSnapshot::MssqlAgent { snapshot, .. } => {
            serde_json::to_vec(&(read.primary_source(), snapshot.metadata.capability(MssqlAgentObject::History)))
        }
        JobsSnapshot::Timescale { snapshot, .. } => serde_json::to_vec(&(
            read.primary_source(),
            &snapshot.metadata.extension,
            snapshot.metadata.run_view,
            snapshot.metadata.run_view.and_then(|view| snapshot.metadata.view(view)),
        )),
        JobsSnapshot::MysqlEvents { snapshot, .. } => {
            serde_json::to_vec(&(read.primary_source(), snapshot.metadata.layout))
        }
    }
    .map_err(|_| invalid())?;
    Ok(JobAcquisitionReference {
        target_id: context.target.telemetry_id()?,
        member_id,
        process_generation,
        namespace: digest(&namespace),
        observed_from: context.observed_from.clone(),
        observed_until: context.observed_until.clone(),
    })
}
fn reference(
    row: &OperationalObservation,
    acquisition: &JobAcquisitionReference,
    native: JobExecutionIdentity,
) -> JobExecutionReference {
    JobExecutionReference {
        acquisition: acquisition.clone(),
        source: row.source.clone(),
        observation_key: row.key.clone(),
        native,
    }
}
fn event(entry: &JobExecution) -> Option<Result<ChangeEvent>> {
    match entry.placement {
        JobExecutionPlacement::Utc => {
            let JobExecutionTime::Utc { started_at, reported_until } = &entry.time else {
                return Some(Err(invalid()));
            };
            Some((|| {
                let subject = entry.reference.key()?;
                let job = entry.reference.native.job_id();
                let status = match entry.status {
                    JobExecutionStatus::Failed => "failed",
                    JobExecutionStatus::Succeeded => "succeeded",
                    _ => "unknown",
                };
                let mut event = ChangeEvent::new(
                    EventKind::JobExecution,
                    EventSource::Database,
                    &subject,
                    &canonical_time(started_at)?,
                    culprit_core::message!("view.jobs.execution.event.title", job = &job, status = status),
                )
                .detail(culprit_core::message!("view.jobs.execution.event.detail"));
                event.until = reported_until.as_deref().map(canonical_time).transpose()?;
                event.job_execution =
                    Some(culprit_core::JobExecutionEvent { reference: entry.reference.clone(), status: entry.status });
                Ok(event)
            })())
        }
        JobExecutionPlacement::SourceQualifiedWindow => {
            let JobExecutionTime::MssqlAgentActive {
                observed_from: Some(observed_from),
                observed_until: Some(observed_until),
                ..
            } = &entry.time
            else {
                return Some(Err(invalid()));
            };
            Some((|| {
                let subject = entry.reference.key()?;
                let job = entry.reference.native.job_id();
                let mut event = ChangeEvent::new(
                    EventKind::JobExecution,
                    EventSource::Database,
                    &subject,
                    &canonical_time(observed_from)?,
                    culprit_core::message!("view.jobs.execution.event.active_title", job = &job),
                )
                .detail(culprit_core::message!("view.jobs.execution.event.active_detail"))
                .until(&canonical_time(observed_until)?);
                event.job_execution =
                    Some(culprit_core::JobExecutionEvent { reference: entry.reference.clone(), status: entry.status });
                Ok(event)
            })())
        }
        _ => None,
    }
}
pub(super) fn agent_status(native: i32) -> JobExecutionStatus {
    match native {
        0 => JobExecutionStatus::Failed,
        1 => JobExecutionStatus::Succeeded,
        2 => JobExecutionStatus::Retry,
        3 => JobExecutionStatus::Cancelled,
        4 => JobExecutionStatus::InProgress,
        _ => JobExecutionStatus::Unknown,
    }
}
pub(super) fn timescale_status(point: &TimescaleJobRun) -> JobExecutionStatus {
    match (point.succeeded, point.finished_at.as_ref()) {
        (Some(false), Some(_)) => JobExecutionStatus::Failed,
        (Some(true), Some(_)) => JobExecutionStatus::Succeeded,
        _ => JobExecutionStatus::Unknown,
    }
}
