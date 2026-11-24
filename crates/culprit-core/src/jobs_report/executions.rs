//! Selected native records, not a complete execution count or a binding to today's job definition/SQL worker.
use crate::{ChangeEvent, SourceState, telemetry::*};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const MAX_JOB_EXECUTION_ENTRIES: usize = 256;
pub const MAX_JOB_EXECUTION_RECORDS: usize = MAX_MSSQL_AGENT_JOB_OBSERVATIONS;
pub const MAX_JOB_EXECUTION_SOURCES: usize = 8;
const MAX_JOB_EXECUTION_LABEL_BYTES: usize = 2048;
fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_JOB_EXECUTION_LABEL_BYTES && !value.contains('\0')
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobAcquisitionReference {
    pub target_id: String,
    pub member_id: Option<String>,
    /// Opaque original reader generation. This is not the historical job worker's epoch.
    pub process_generation: Option<String>,
    pub namespace: String,
    pub observed_from: String,
    pub observed_until: String,
}
impl JobAcquisitionReference {
    pub fn is_valid(&self) -> bool {
        valid_digest(&self.target_id)
            && self.member_id.as_deref().is_none_or(valid_digest)
            && self.process_generation.as_deref().is_none_or(valid_digest)
            && valid_digest(&self.namespace)
            && ObservationBounds::new(&self.observed_from, &self.observed_until).is_some()
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum JobExecutionIdentity {
    MssqlAgentHistory {
        job_id: String,
        history_id: String,
        step_id: String,
        run_date: String,
        run_time: String,
    },
    MssqlAgentActive {
        job_id: String,
        agent_session_id: String,
        started_without_offset: String,
    },
    TimescaleHistory {
        job_id: String,
        run_id: String,
        started_at: String,
    },
    TimescaleError {
        job_id: String,
        position: u32,
        started_at: String,
    },
    MysqlLastStart {
        schema: String,
        name: String,
        created_without_offset: Option<String>,
        altered_without_offset: Option<String>,
        started_without_offset: String,
    },
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum JobExecutionKind {
    Run,
    Summary,
    Step,
    ErrorRecord,
    LastStart,
    ActiveWindow,
}
impl JobExecutionIdentity {
    pub fn kind(&self) -> JobExecutionKind {
        match self {
            Self::MssqlAgentHistory { step_id, .. } if step_id == "0" => JobExecutionKind::Summary,
            Self::MssqlAgentHistory { .. } => JobExecutionKind::Step,
            Self::MssqlAgentActive { .. } => JobExecutionKind::ActiveWindow,
            Self::TimescaleHistory { .. } => JobExecutionKind::Run,
            Self::TimescaleError { .. } => JobExecutionKind::ErrorRecord,
            Self::MysqlLastStart { .. } => JobExecutionKind::LastStart,
        }
    }
    pub fn job_id(&self) -> String {
        match self {
            Self::MssqlAgentHistory { job_id, .. }
            | Self::MssqlAgentActive { job_id, .. }
            | Self::TimescaleHistory { job_id, .. }
            | Self::TimescaleError { job_id, .. } => job_id.clone(),
            Self::MysqlLastStart { schema, name, .. } => format!("{schema}.{name}"),
        }
    }
    fn is_valid(&self) -> bool {
        let text = label;
        match self {
            Self::MssqlAgentHistory { job_id, history_id, step_id, run_date, run_time } => {
                [job_id, history_id, step_id, run_date, run_time].into_iter().all(|value| text(value))
            }
            Self::MssqlAgentActive { job_id, agent_session_id, started_without_offset } => {
                [job_id, agent_session_id, started_without_offset].into_iter().all(|value| text(value))
                    && chrono::DateTime::parse_from_rfc3339(started_without_offset).is_err()
            }
            Self::TimescaleHistory { job_id, run_id, started_at } => {
                text(job_id) && text(run_id) && canonical_time(started_at).is_ok()
            }
            Self::TimescaleError { job_id, position, started_at } => {
                text(job_id) && *position > 0 && canonical_time(started_at).is_ok()
            }
            Self::MysqlLastStart {
                schema,
                name,
                created_without_offset,
                altered_without_offset,
                started_without_offset,
            } => {
                text(schema)
                    && text(name)
                    && [
                        created_without_offset.as_deref(),
                        altered_without_offset.as_deref(),
                        Some(started_without_offset.as_str()),
                    ]
                    .into_iter()
                    .flatten()
                    .all(text)
            }
        }
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecutionReference {
    pub acquisition: JobAcquisitionReference,
    pub source: String,
    pub observation_key: String,
    pub native: JobExecutionIdentity,
}
impl JobExecutionReference {
    /// Deterministic recorded reference, never a globally unique execution or current authority.
    /// Positional/error records without a native run identity are kept off the UTC timeline.
    pub fn key(&self) -> crate::Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        serde_json::to_vec(&(
            "job-execution-reference-v1",
            &self.acquisition.target_id,
            &self.acquisition.member_id,
            &self.acquisition.process_generation,
            &self.acquisition.namespace,
            &self.source,
            &self.native,
        ))
        .map(|bytes| digest(&bytes))
        .map_err(|_| CollectionStop::InvalidPage.error())
    }
    pub fn is_valid(&self) -> bool {
        self.acquisition.is_valid()
            && label(&self.source)
            && valid_digest(&self.observation_key)
            && self.native.is_valid()
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum JobExecutionStatus {
    Failed,
    Succeeded,
    Retry,
    Cancelled,
    InProgress,
    Unknown,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum JobExecutionLabelOrigin {
    Stored,
    StoredOrCurrent,
    CurrentDefinition,
    Unknown,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecutionLabel {
    pub text: String,
    pub origin: JobExecutionLabelOrigin,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum JobExecutionTime {
    Utc {
        started_at: String,
        reported_until: Option<String>,
    },
    MssqlAgentActive {
        observed_from: Option<String>,
        observed_at: String,
        observed_until: Option<String>,
        started_without_offset: String,
        last_executed_step_id: Option<String>,
        last_executed_step_without_offset: Option<String>,
    },
    MssqlLocal {
        run_date: String,
        run_time: String,
        run_duration: String,
        started_without_offset: Option<String>,
        duration_seconds: Option<String>,
        history_written_time: bool,
    },
    EventLocal {
        started_without_offset: String,
        event_time_zone: Option<String>,
        catalog_time_zone: Option<String>,
    },
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum JobExecutionDiagnostics {
    Mssql { sql_message_id: String, sql_severity: String, retries_attempted: String, native_status: i32 },
    Postgres { sql_state: Option<String> },
    Unavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum JobExecutionPlacement {
    Utc,
    SourceQualifiedWindow,
    NativeLocal,
    NoNativeRunIdentity,
    AmbiguousNativeIdentity,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecution {
    pub reference: JobExecutionReference,
    pub status: JobExecutionStatus,
    pub label: Option<JobExecutionLabel>,
    pub time: JobExecutionTime,
    pub diagnostics: JobExecutionDiagnostics,
    pub placement: JobExecutionPlacement,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecutionSourceCoverage {
    pub source: String,
    pub state: SourceState,
    pub complete_scope: bool,
    pub native_rows: u32,
    pub observed_rows: u32,
    pub stopped: Option<CollectionStop>,
    pub error_code: Option<crate::ErrorCode>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecutionAssessment {
    pub read_complete: bool,
    pub sources: Vec<JobExecutionSourceCoverage>,
    /// Native record categories, never unique lifetime executions or scheduler totals.
    pub run_records: u32,
    pub step_records: u32,
    pub error_records: u32,
    pub last_start_records: u32,
    pub active_window_records: u32,
    pub utc_placed_records: u32,
    pub unplaced_records: u32,
    pub omitted_entries: u32,
}
impl JobExecutionAssessment {
    pub fn record_count(&self) -> Option<u32> {
        self.run_records
            .checked_add(self.step_records)?
            .checked_add(self.error_records)?
            .checked_add(self.last_start_records)?
            .checked_add(self.active_window_records)
    }
    pub fn is_valid(&self) -> bool {
        let mut sources = std::collections::BTreeSet::new();
        !self.sources.is_empty()
            && self.sources.len() <= MAX_JOB_EXECUTION_SOURCES
            && self.sources.iter().all(|source| {
                StateSourceCoverage {
                    source: source.source.clone(),
                    state: source.state,
                    records: source.native_rows,
                    complete_scope: source.complete_scope,
                    stopped: source.stopped,
                    error_code: source.error_code,
                }
                .is_valid()
                    && sources.insert(&source.source)
                    && source.observed_rows <= source.native_rows
                    && (!self.read_complete || source.observed_rows == source.native_rows)
            })
            && self.record_count().is_some_and(|count| {
                count as usize <= MAX_JOB_EXECUTION_RECORDS
                    && self
                        .sources
                        .iter()
                        .try_fold(0u32, |sum, source| sum.checked_add(source.observed_rows))
                        .is_some_and(|observed| count <= observed && observed as usize <= MAX_JOB_EXECUTION_RECORDS)
                    && self.utc_placed_records.checked_add(self.unplaced_records) == Some(count)
                    && self.omitted_entries <= count
            })
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[ts(export)]
pub struct JobExecutionProjection {
    pub assessment: JobExecutionAssessment,
    pub entries: Vec<JobExecution>,
    pub events: Vec<ChangeEvent>,
}

/// Typed native verdict attached to one timeline record, independent of translated wording.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct JobExecutionEvent {
    pub reference: JobExecutionReference,
    pub status: JobExecutionStatus,
}

#[cfg(test)]
mod tests;
