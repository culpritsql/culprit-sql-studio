//! Derived diagnosis outputs use the same canonical range, projection and receipt contract as raw streams.
//! Details is the whole bounded, redacted typed DTO, not a destination-specific reconstruction or native fact.
use super::*;
use ScalarType::*;

const SHARED: &[Field] = &[
    Field::required("ChangeEvaluations", "/_culprit/derived/change_evaluations", JsonContainer),
    Field::required("FindingEvaluations", "/_culprit/derived/finding_evaluations", JsonContainer),
    Field::required("ContextIssues", "/_culprit/derived/context_issues", JsonContainer),
    Field::required("OutputCoverage", "/_culprit/derived/output_coverage", JsonContainer),
    Field::required("OutputLimitations", "/_projection/output_limitations", JsonContainer),
    Field::required("AnalysisId", "/record/data/analysis_id", String),
    Field::required("Analyzer", "/_culprit/derived/analyzer", JsonContainer),
    Field::required("RulesVersion", "/_culprit/derived/rules_version", String),
    Field::required("Language", "/_culprit/derived/language", String),
    Field::required("AnalysisOrigin", "/_culprit/derived/origin", String),
    Field::required("InputReadComplete", "/_culprit/derived/input_read_complete", Bool),
    Field::required("InputReferences", "/_culprit/derived/inputs", JsonContainer),
    Field::required("OriginalObservedFrom", "/_culprit/derived/observed_from", Timestamp),
    Field::required("OriginalObservedUntil", "/_culprit/derived/observed_until", Timestamp),
    Field::required("RecordedAt", "/_culprit/derived/recorded_at", Timestamp),
    Field::optional("DatabaseSelector", "/record/data/database", String),
    Field::required("ConnectionAlias", "/record/data/display/connection_alias", String),
    Field::optional("Environment", "/record/data/display/environment", String),
    Field::optional("DatabaseName", "/record/data/display/database_name", String),
    Field::required("DetailState", "/_projection/detail_state", String),
    Field::required("DetailSha256", "/_projection/detail_sha256", String),
    Field::required("Details", "/_projection/details", JsonContainer),
];
const INCIDENT: &[Field] = &[
    Field::required("EpisodeId", "/record/data/entry/episode_id", String),
    Field::required("ControllerVersion", "/record/data/entry/controller_version", Long),
    Field::required("RevisionId", "/record/data/entry/revision_id", String),
    Field::required("Revision", "/record/data/entry/revision", ExactCounter),
    Field::optional("PreviousRevision", "/record/data/entry/previous_revision", String),
    Field::required("TargetId", "/record/data/entry/target_id", String),
    Field::required("TargetScope", "/record/data/entry/target_scope", String),
    Field::required("OriginalScopeId", "/record/data/entry/scope_id", String),
    Field::required("OriginalScope", "/record/data/entry/scope", String),
    Field::required("OriginalSource", "/record/data/entry/source", String),
    Field::required("Title", "/record/data/entry/title", String),
    Field::required("Phase", "/record/data/entry/phase", String),
    Field::required("Lifecycle", "/record/data/entry/lifecycle", String),
    Field::required("EnrichmentStatus", "/record/data/entry/analysis", String),
    Field::required("OriginalRecordedAt", "/record/data/entry/recorded_at", Timestamp),
    Field::required("NativeFirstSeenAt", "/record/data/entry/first_seen_at", String),
    Field::required("NativeLastSeenAt", "/record/data/entry/last_seen_at", String),
    Field::optional("OriginalIntervalId", "/record/data/original_interval/native_id", String),
    Field::optional("OriginalIntervalFrom", "/record/data/original_interval/from", Timestamp),
    Field::optional("OriginalIntervalUntil", "/record/data/original_interval/until", Timestamp),
    Field::required("OriginalReference", "/record/data/revision_reference", JsonContainer),
    Field::required("VictimSummary", "/record/data/victim_summary", String),
    Field::required("AffectedQueries", "/record/data/affected_queries", JsonContainer),
    Field::required("CulpritQueries", "/record/data/culprit_queries", JsonContainer),
    Field::required("CandidateQueries", "/record/data/candidate_queries", JsonContainer),
    Field::required("AttributionStatus", "/record/data/attribution_status", String),
    Field::optional("Mechanism", "/record/data/mechanism", String),
    Field::required("DeveloperAction", "/record/data/developer_action", String),
    Field::required("Verification", "/record/data/verification", String),
    Field::optional("Caller", "/record/data/caller", JsonContainer),
];
const FINDING: &[Field] = &[
    Field::required("FindingId", "/record/data/finding/id", String),
    Field::required("CheckId", "/record/data/finding/check", String),
    Field::required("Category", "/record/data/finding/category", String),
    Field::required("Severity", "/record/data/finding/severity", String),
    Field::required("Actionability", "/record/data/finding/actionability", String),
    Field::required("Confidence", "/record/data/finding/confidence", String),
    Field::required("Title", "/record/data/finding/title", String),
    Field::required("Summary", "/record/data/finding/summary", String),
    Field::required("Subject", "/record/data/finding/subject", JsonContainer),
    Field::required("Impact", "/record/data/finding/impact", JsonContainer),
    Field::required("Metrics", "/record/data/finding/metrics", JsonContainer),
    Field::required("AffectedQueries", "/record/data/finding/queries", JsonContainer),
    Field::optional("OriginalQuery", "/record/data/query", JsonContainer),
    Field::optional("EstablishedCause", "/record/data/cause", JsonContainer),
    Field::optional("DeveloperAction", "/record/data/finding/fix", JsonContainer),
    Field::required("Verification", "/record/data/finding/verify", JsonContainer),
    Field::required("Caveats", "/record/data/finding/caveats", JsonContainer),
    Field::required("Inactive", "/record/data/finding/inactive", Bool),
    Field::required("Guidance", "/record/data/guidance", String),
    Field::required("OriginalReferences", "/record/data/inputs", JsonContainer),
];
const CHANGE: &[Field] = &[
    Field::required("ChangeId", "/record/data/change/id", String),
    Field::required("ChangeKind", "/record/data/change/kind", String),
    Field::required("OriginalSource", "/record/data/change/source", String),
    Field::optional("Action", "/record/data/change/action", String),
    Field::required("ChangedAt", "/record/data/change/at", String),
    Field::optional("ChangedAfter", "/record/data/change/after", String),
    Field::optional("ChangedUntil", "/record/data/change/until", String),
    Field::required("Title", "/record/data/change/title/text", String),
    Field::optional("Detail", "/record/data/change/detail/text", String),
    Field::optional("TableName", "/record/data/change/table", String),
    Field::optional("ObjectName", "/record/data/change/object", String),
    Field::required("OriginalReferences", "/record/data/inputs", JsonContainer),
];
pub fn incidents(common: &[Field]) -> Result<LogicalSchema> {
    LogicalSchema::new("CulpritIncidentsV1", common.iter().chain(SHARED).chain(INCIDENT).copied())
}
pub fn findings(common: &[Field]) -> Result<LogicalSchema> {
    LogicalSchema::new("CulpritFindingsV1", common.iter().chain(SHARED).chain(FINDING).copied())
}
pub fn changes(common: &[Field]) -> Result<LogicalSchema> {
    LogicalSchema::new("CulpritChangesV1", common.iter().chain(SHARED).chain(CHANGE).copied())
}

#[cfg(test)]
mod tests;
