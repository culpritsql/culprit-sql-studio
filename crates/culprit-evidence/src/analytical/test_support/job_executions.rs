//! Controlled mutations of the existing canonical job corpora, never native execution/clock qualification.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobExecutionCase {
    AgentSummaryAndRetries,
    AgentEditedDefinition,
    AgentDst,
    AgentZeroDate,
    AgentUnknownStatus,
    AgentRestart,
    TimescaleFailed,
    TimescaleSucceeded,
    TimescaleUnfinished,
    TimescaleErrorsOnly,
    TimescaleDuplicateRun,
    TimescalePartial,
    TimescaleDenied,
    MysqlLastStart,
}
impl JobExecutionCase {
    pub const ALL: &'static [Self] = &[
        Self::AgentSummaryAndRetries,
        Self::AgentEditedDefinition,
        Self::AgentDst,
        Self::AgentZeroDate,
        Self::AgentUnknownStatus,
        Self::AgentRestart,
        Self::TimescaleFailed,
        Self::TimescaleSucceeded,
        Self::TimescaleUnfinished,
        Self::TimescaleErrorsOnly,
        Self::TimescaleDuplicateRun,
        Self::TimescalePartial,
        Self::TimescaleDenied,
        Self::MysqlLastStart,
    ];
}
pub fn job_execution_evidence(case: JobExecutionCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, mut records) = match case {
        JobExecutionCase::AgentRestart => mssql_agent_jobs_evidence(MssqlAgentJobsCase::AgentRestart),
        JobExecutionCase::AgentSummaryAndRetries
        | JobExecutionCase::AgentEditedDefinition
        | JobExecutionCase::AgentDst
        | JobExecutionCase::AgentZeroDate
        | JobExecutionCase::AgentUnknownStatus => mssql_agent_jobs_evidence(MssqlAgentJobsCase::Failed),
        JobExecutionCase::TimescaleErrorsOnly => jobs_evidence(JobsCase::ErrorsOnly),
        JobExecutionCase::TimescaleUnfinished => jobs_evidence(JobsCase::Unfinished),
        JobExecutionCase::TimescalePartial => jobs_evidence(JobsCase::Limited),
        JobExecutionCase::TimescaleDenied => jobs_evidence(JobsCase::Unavailable),
        JobExecutionCase::MysqlLastStart => mysql_events_evidence(MysqlEventsCase::Enabled),
        _ => jobs_evidence(JobsCase::Failed),
    };
    match case {
        JobExecutionCase::AgentSummaryAndRetries => add_summary_and_retry(&mut records),
        JobExecutionCase::TimescaleDuplicateRun => duplicate_run(&mut records),
        _ => mutate_records(case, &mut records),
    }
    for record in &mut records {
        refresh_key(record);
    }
    let snapshot = match &mut source.observation {
        SourceObservation::MssqlAgentJobs { snapshot, .. }
        | SourceObservation::Jobs { snapshot, .. }
        | SourceObservation::MysqlEvents { snapshot, .. } => snapshot,
        _ => unreachable!(),
    };
    snapshot.records = records.len() as u32;
    for coverage in &mut snapshot.sources {
        coverage.records = records
            .iter()
            .filter(|record| matches!(record,TelemetryRecord::Operational(row) if row.source==coverage.source))
            .count() as u32;
    }
    let rows = records
        .iter()
        .map(|record| {
            let TelemetryRecord::Operational(row) = record else { unreachable!() };
            row.clone()
        })
        .collect();
    assert!(JobsSnapshot::from_source(&source, rows, true).unwrap().is_valid());
    (source, records)
}
fn add_summary_and_retry(records: &mut Vec<TelemetryRecord>) {
    let original = records
        .iter()
        .find(|row| {
            matches!(
                row,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlAgentHistory(_),
                    ..
                })
            )
        })
        .unwrap()
        .clone();
    let mut summary = original.clone();
    let TelemetryRecord::Operational(row) = &mut summary else { unreachable!() };
    let OperationalFact::MssqlAgentHistory(point) = &mut row.fact else { unreachable!() };
    point.instance_id = "2147483646".into();
    point.step_id = "0".into();
    point.stored_step_name = None;
    let mut retry = original;
    let TelemetryRecord::Operational(row) = &mut retry else { unreachable!() };
    let OperationalFact::MssqlAgentHistory(point) = &mut row.fact else { unreachable!() };
    point.instance_id = "2147483645".into();
    point.run_status = 2;
    point.retries_attempted = "2".into();
    records.extend([summary, retry]);
}
fn duplicate_run(records: &mut Vec<TelemetryRecord>) {
    let mut duplicate = records
        .iter()
        .find(|row| {
            matches!(
                row,
                TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TimescaleJobRun(_), .. })
            )
        })
        .unwrap()
        .clone();
    let TelemetryRecord::Operational(row) = &mut duplicate else { unreachable!() };
    let OperationalFact::TimescaleJobRun(point) = &mut row.fact else { unreachable!() };
    point.position = 2;
    records.push(duplicate);
}
fn mutate_records(case: JobExecutionCase, records: &mut [TelemetryRecord]) {
    for record in records {
        let TelemetryRecord::Operational(row) = record else { unreachable!() };
        match (&mut row.fact, case) {
            (OperationalFact::MssqlAgentJob(point), JobExecutionCase::AgentEditedDefinition) => {
                point.name = "Changed current job".into();
                point.version_number = "77".into();
            }
            (OperationalFact::MssqlAgentStep(point), JobExecutionCase::AgentEditedDefinition) => {
                point.name = "Changed current step".into()
            }
            (OperationalFact::MssqlAgentHistory(point), JobExecutionCase::AgentDst) => {
                point.run_date = "20261025".into();
                point.run_time = "33000".into();
            }
            (OperationalFact::MssqlAgentHistory(point), JobExecutionCase::AgentZeroDate) => {
                point.run_date = "0".into();
                point.run_time = "0".into();
                point.run_duration = "0".into();
            }
            (OperationalFact::MssqlAgentHistory(point), JobExecutionCase::AgentUnknownStatus) => point.run_status = 42,
            (OperationalFact::TimescaleJobRun(point), JobExecutionCase::TimescaleSucceeded) => {
                point.succeeded = Some(true)
            }
            _ => {}
        }
    }
}
fn refresh_key(record: &mut TelemetryRecord) {
    let TelemetryRecord::Operational(row) = record else { unreachable!() };
    row.key = match &row.fact {
        OperationalFact::MssqlAgentJob(point) => point.key(),
        OperationalFact::MssqlAgentStep(point) => point.key(),
        OperationalFact::MssqlAgentHistory(point) => point.key(),
        OperationalFact::MssqlAgentContext(point) => point.key(),
        OperationalFact::TimescaleJobRun(point) => point.key(),
        OperationalFact::TimescaleJobState(point) => point.key(),
        OperationalFact::TimescaleContinuousAggregate(point) => point.key(),
        OperationalFact::MysqlScheduledEvent(point) => point.key(),
        _ => unreachable!(),
    }
    .unwrap();
}

pub fn job_execution_evidence_cases() -> Vec<(CollectionDescription, Vec<TelemetryRecord>)> {
    JobExecutionCase::ALL.iter().copied().map(job_execution_evidence).collect()
}
