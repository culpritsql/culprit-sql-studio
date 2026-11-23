//! Existing PostgreSQL structured decoder, isolated from the source-free XML intake.
use super::*;
use culprit_postgres::structured_logs::{self, LogFormat, ParsedLogBatch};

pub(super) fn decode(input: &[u8], format: NativeLogFormat, limits: NativeLogLimits, complete: bool) -> DecodedLog {
    // A byte-budget prefix may end inside one UTF-8 scalar. The artifact still hashes every observed byte.
    let parse_input = match std::str::from_utf8(input) {
        Err(error) if !complete && error.error_len().is_none() => &input[..error.valid_up_to()],
        _ => input,
    };
    let format = match format {
        NativeLogFormat::PostgresJson => LogFormat::Json,
        NativeLogFormat::PostgresCsv13 => LogFormat::Csv13,
        NativeLogFormat::PostgresCsv => LogFormat::Csv,
        NativeLogFormat::MssqlExtendedEventsXml => unreachable!("closed XML dispatch"),
    };
    match structured_logs::parse(parse_input, format, limits) {
        Ok(batch) => safe_rows(batch),
        Err(problem) => DecodedLog::stopped(problem),
    }
}
fn safe_rows(batch: ParsedLogBatch) -> DecodedLog {
    let mut issues = batch.issues;
    let mut observations = vec![];
    for row in batch.records {
        let position = row.position;
        match row.into_observation() {
            Ok(row) => observations.push(NativeFailureRecord::PostgresLog(Box::new(row))),
            Err(_) => issues.push(NativeLogIssue { position, problem: NativeLogProblem::NativeShape }),
        }
    }
    issues.sort_by_key(|issue| issue.position);
    DecodedLog {
        observations,
        issues,
        stopped: batch.stopped,
        records_examined: batch.records_examined,
        non_failure_records: batch.non_failure_records,
        native_events: None,
    }
}
