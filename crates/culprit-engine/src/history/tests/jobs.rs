use super::*;
use crate::trends;
use culprit_core::telemetry::{JobsSnapshot, TelemetryRecord};
use culprit_evidence::analytical::test_support::{JobExecutionCase, job_execution_evidence};

#[test]
fn a_late_native_job_record_keeps_its_original_time_and_completion_in_retained_scan_history() {
    let (source, records) = job_execution_evidence(JobExecutionCase::TimescaleFailed);
    let rows = records
        .into_iter()
        .map(|record| {
            let TelemetryRecord::Operational(row) = record else { unreachable!() };
            row
        })
        .collect();
    let read = JobsSnapshot::from_source(&source, rows, true).unwrap();
    let expected = culprit_checks::jobs::execution_projection(&read).unwrap().events.remove(0);
    let ended = chrono::DateTime::parse_from_rfc3339(expected.until.as_deref().unwrap()).unwrap();
    let mut current = demo();
    current.identity = source.target.database().cloned();
    current.server.engine = source.target.engine();
    current.server.database = source.target.database().unwrap().database.clone();
    current.operations.jobs = Some(Box::new(read));
    assert_eq!(culprit_checks::jobs::execution_events_snapshot(&current).unwrap(), vec![expected.clone()]);
    current.taken_at = (ended + chrono::Duration::hours(2)).to_rfc3339();
    let mut previous = current.clone();
    previous.operations.jobs = None;
    previous.taken_at = (ended + chrono::Duration::hours(1)).to_rfc3339();

    let old_mark = ChangeEvent::new(EventKind::Mark, EventSource::Mark, "older-mark", &expected.at, "Earlier mark");
    let events = diff(Some(&previous), &current, vec![old_mark.clone()]);
    let jobs = events.iter().filter(|event| event.kind == EventKind::JobExecution).collect::<Vec<_>>();
    assert_eq!(jobs, vec![&expected]);
    assert!(!events.iter().any(|event| event.id == old_mark.id), "other scan event boundaries stay unchanged");

    current.taken_at = (ended + chrono::Duration::days(trends::HISTORY_DAYS as i64 + 1)).to_rfc3339();
    assert!(diff(Some(&previous), &current, vec![]).iter().all(|event| event.kind != EventKind::JobExecution));
}
