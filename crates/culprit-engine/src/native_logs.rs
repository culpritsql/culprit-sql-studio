//! One bounded local intake over the provider's acquisition-independent parser. No workspace, credentials or source
//! database are touched, and a native database name never establishes resource ownership or feed publication.
use chrono::{SecondsFormat, Utc};
use culprit_core::limits::{OperationGuard, OperationHandle};
use culprit_core::telemetry::CollectionStop;
use culprit_core::{
    Error, ErrorCode, NativeLogAnalysis, NativeLogArtifact, NativeLogFormat, NativeLogIssue, NativeLogLimits,
    NativeLogProblem, Result, SourceOutcome, SourceState, message, t,
    telemetry::{NativeFailureRecord, NativeFailureSnapshot, digest},
};

mod mssql;
#[cfg(feature = "postgres")]
mod postgres;
use std::{fs::File, io::Read, path::Path};

pub const LOG_IMPORT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The existing blocking-work owner retains the file/permit while a dropped caller cancels the bounded intake.
pub async fn open_selected_file(
    path: std::path::PathBuf,
    format: NativeLogFormat,
    limits: NativeLogLimits,
) -> Result<NativeLogAnalysis> {
    validate_limits(limits)?;
    let guard = OperationGuard::new(LOG_IMPORT_TIMEOUT)?;
    let operation = guard.handle();
    let remaining = operation.remaining();
    let language = culprit_core::i18n::current();
    tokio::time::timeout(
        remaining,
        crate::run_blocking(move || {
            culprit_core::i18n::scope(&language.code, || analyze_file_with(&path, format, limits, &operation))
        }),
    )
    .await
    .map_err(|_| CollectionStop::TimedOut.error())?
}

pub fn analyze_bytes(input: &[u8], format: NativeLogFormat, limits: NativeLogLimits) -> Result<NativeLogAnalysis> {
    let guard = OperationGuard::new(LOG_IMPORT_TIMEOUT)?;
    analyze_bytes_with(input, format, limits, &guard.handle())
}

/// Supplied text uses the caller's existing deadline and cancellation owner; it never opens a path or connection.
pub fn analyze_bytes_with(
    input: &[u8],
    format: NativeLogFormat,
    limits: NativeLogLimits,
    operation: &OperationHandle,
) -> Result<NativeLogAnalysis> {
    validate_limits(limits)?;
    check(operation)?;
    let started = utc_now();
    let prefix = &input[..input.len().min(limits.input_bytes)];
    let result = analyze(prefix, format, limits, started, input.len() <= limits.input_bytes, None)?;
    check(operation)?;
    Ok(result)
}

/// Inspect an explicitly selected regular local file. The size-plus-one read is bounded even if the file grows.
pub fn analyze_file(path: &Path, format: NativeLogFormat, limits: NativeLogLimits) -> Result<NativeLogAnalysis> {
    let guard = OperationGuard::new(LOG_IMPORT_TIMEOUT)?;
    analyze_file_with(path, format, limits, &guard.handle())
}

/// The same bounded owner accepts an explicit operation handle; cancellation never publishes a partial success.
pub fn analyze_file_with(
    path: &Path,
    format: NativeLogFormat,
    limits: NativeLogLimits,
    operation: &OperationHandle,
) -> Result<NativeLogAnalysis> {
    validate_limits(limits)?;
    check(operation)?;
    let started = utc_now();
    let initial = std::fs::metadata(path).map_err(read_failed)?;
    if !initial.is_file() {
        return Err(not_regular_file());
    }
    check(operation)?;
    let file = File::open(path).map_err(read_failed)?;
    let opened = file.metadata().map_err(read_failed)?;
    if !opened.is_file() {
        return Err(not_regular_file());
    }
    let mut file = file;
    let mut bytes = read_bounded(&mut file, limits.input_bytes, operation)?;
    let after = file.metadata().map_err(read_failed)?;
    let changed = opened.len() != after.len() || opened.modified().ok() != after.modified().ok();
    let complete = !changed && bytes.len() <= limits.input_bytes && bytes.len() as u64 == opened.len();
    bytes.truncate(limits.input_bytes);
    check(operation)?;
    let result = analyze(&bytes, format, limits, started, complete, changed.then_some(NativeLogProblem::InputChanged))?;
    check(operation)?;
    Ok(result)
}

fn validate_limits(limits: NativeLogLimits) -> Result<()> {
    if limits.is_valid() { Ok(()) } else { Err(Error::invalid(t!("native_logs.invalid_limits"))) }
}

fn analyze(
    input: &[u8],
    format: NativeLogFormat,
    limits: NativeLogLimits,
    started: String,
    complete_input: bool,
    input_problem: Option<NativeLogProblem>,
) -> Result<NativeLogAnalysis> {
    let artifact = NativeLogArtifact {
        format,
        sha256: digest(input),
        bytes: input.len() as u64,
        complete: complete_input,
        limitation: if complete_input { None } else { Some(input_problem.unwrap_or(NativeLogProblem::InputLimit)) },
    };
    let decoded = decode(input, format, limits, complete_input, &artifact.sha256)?;
    let source_name = match format {
        NativeLogFormat::MssqlExtendedEventsXml => "supplied-mssql-event-xml",
        _ => "supplied-postgres-log",
    };
    let partial = !complete_input || decoded.stopped.is_some() || !decoded.issues.is_empty();
    let reason = partial.then(|| message!("native_logs.partial"));
    let source = SourceOutcome {
        source: source_name.into(),
        state: if partial {
            SourceState::Partial
        } else if decoded.observations.is_empty() {
            SourceState::Empty
        } else {
            SourceState::Collected
        },
        reason: reason.map(culprit_core::LocalizedText::new),
    };
    let evidence = NativeFailureSnapshot {
        source: source_name.into(),
        observed_from: started,
        observed_until: utc_now(),
        resource: None,
        observations: decoded.observations,
    };
    if !evidence.is_valid() {
        return Err(Error::invalid(t!("native_logs.invalid_capture")));
    }
    let report = culprit_checks::native_failures::explain(&evidence, Some(&source))?;
    Ok(NativeLogAnalysis {
        input: artifact,
        evidence,
        source,
        records_examined: decoded.records_examined,
        non_failure_records: decoded.non_failure_records,
        issues: decoded.issues,
        stopped: decoded.stopped,
        report,
        native_events: decoded.native_events,
    })
}

struct DecodedLog {
    observations: Vec<NativeFailureRecord>,
    issues: Vec<NativeLogIssue>,
    stopped: Option<NativeLogProblem>,
    records_examined: u32,
    non_failure_records: u32,
    native_events: Option<culprit_core::ImportedNativeEvents>,
}
impl DecodedLog {
    fn stopped(problem: NativeLogProblem) -> Self {
        Self {
            observations: vec![],
            issues: vec![],
            stopped: Some(problem),
            records_examined: 0,
            non_failure_records: 0,
            native_events: None,
        }
    }
}
fn decode(
    input: &[u8],
    format: NativeLogFormat,
    limits: NativeLogLimits,
    complete: bool,
    artifact: &str,
) -> Result<DecodedLog> {
    match format {
        NativeLogFormat::MssqlExtendedEventsXml => Ok(mssql::decode(input, limits, artifact)),
        _ => decode_postgres(input, format, limits, complete),
    }
}
#[cfg(feature = "postgres")]
fn decode_postgres(
    input: &[u8],
    format: NativeLogFormat,
    limits: NativeLogLimits,
    complete: bool,
) -> Result<DecodedLog> {
    Ok(postgres::decode(input, format, limits, complete))
}
#[cfg(not(feature = "postgres"))]
fn decode_postgres(_: &[u8], _: NativeLogFormat, _: NativeLogLimits, _: bool) -> Result<DecodedLog> {
    Err(Error::new(ErrorCode::Unsupported, t!("native_logs.unsupported_format")))
}
fn read_bounded(reader: &mut impl Read, limit: usize, operation: &OperationHandle) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    while bytes.len() <= limit {
        check(operation)?;
        let allowed = chunk.len().min(limit + 1 - bytes.len());
        let read = reader.read(&mut chunk[..allowed]).map_err(read_failed)?;
        check(operation)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes)
}
fn check(operation: &OperationHandle) -> Result<()> {
    if operation.is_cancelled() {
        return Err(CollectionStop::Cancelled.error());
    }
    if operation.remaining().is_zero() {
        return Err(CollectionStop::TimedOut.error());
    }
    Ok(())
}
fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}
fn not_regular_file() -> Error {
    Error::invalid(t!("native_logs.not_regular_file"))
}
fn read_failed(error: std::io::Error) -> Error {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => ErrorCode::NotFound,
        std::io::ErrorKind::PermissionDenied => ErrorCode::PermissionDenied,
        _ => ErrorCode::InvalidInput,
    };
    Error::new(code, t!("native_logs.read_failed"))
}

#[cfg(test)]
mod event_tests;
#[cfg(all(test, feature = "postgres"))]
mod tests;
