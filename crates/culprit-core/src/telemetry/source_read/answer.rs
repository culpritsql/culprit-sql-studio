//! A closed answer preserves one source family's interpretation without a generic payload or second lifecycle.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(tag = "kind", content = "answer", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum SourceReadAnswer {
    TemporaryResources(Box<TemporaryResourceAnswer>),
    QueryCompilation(Box<QueryCompilationAnswer>),
    TransactionMetadata(Box<TransactionMetadataAnswer>),
    ReadableSecondaryRecovery(Box<ReadableSecondaryRecoveryAnswer>),
    WorkerStarvation(Box<WorkerStarvationAnswer>),
    RelationalInputs(Box<RelationalInputAnswer>),
    CurrentExecutionProfile(Box<CurrentExecutionProfileAnswer>),
    OutputGrantRetention(Box<OutputGrantRetentionAnswer>),
    MetadataLockContext(Box<MetadataLockContextAnswer>),
}
impl SourceReadAnswer {
    pub fn is_valid(&self) -> bool {
        match self {
            Self::TemporaryResources(answer) => answer.is_valid(),
            Self::QueryCompilation(answer) => answer.is_valid(),
            Self::TransactionMetadata(answer) => answer.is_valid(),
            Self::ReadableSecondaryRecovery(answer) => answer.is_valid(),
            Self::WorkerStarvation(answer) => answer.is_valid(),
            Self::RelationalInputs(answer) => answer.is_valid(),
            Self::CurrentExecutionProfile(answer) => answer.is_valid(),
            Self::OutputGrantRetention(answer) => answer.is_valid(),
            Self::MetadataLockContext(answer) => answer.is_valid(),
        }
    }
    pub fn matches_focus(&self, focus: &SourceReadFocus) -> bool {
        self.is_valid()
            && self.question() == focus.question
            && self.basis() == focus.basis.as_ref()
            && match self {
                Self::TransactionMetadata(answer) => focus.transaction_metadata_participant().is_some_and(|(key, _, mechanism)| {
                    answer.mechanism == mechanism && matches!(&answer.basis.native_key, NativeRecordIdentity::Activity(original) if original == key)
                }),
                Self::RelationalInputs(answer) => answer.matches_prerequisite(&focus.prerequisite),
                Self::CurrentExecutionProfile(answer) => answer.matches_prerequisite(&focus.prerequisite),
                Self::OutputGrantRetention(answer) => answer.matches_prerequisite(&focus.prerequisite),
                Self::MetadataLockContext(answer) => answer.matches_prerequisite(&focus.prerequisite),
                _ => true,
            }
    }
    pub fn question(&self) -> SourceReadQuestion {
        match self {
            Self::TemporaryResources(_) => SourceReadQuestion::TemporarySpaceContributors,
            Self::QueryCompilation(_) => SourceReadQuestion::CompilationHistory,
            Self::TransactionMetadata(_) => SourceReadQuestion::TransactionMetadata,
            Self::ReadableSecondaryRecovery(_) => SourceReadQuestion::ReadableSecondaryRecovery,
            Self::WorkerStarvation(_) => SourceReadQuestion::WorkerStarvation,
            Self::RelationalInputs(_) => SourceReadQuestion::RelationalInputContext,
            Self::CurrentExecutionProfile(_) => SourceReadQuestion::CurrentExecutionProfile,
            Self::OutputGrantRetention(_) => SourceReadQuestion::OutputGrantRetention,
            Self::MetadataLockContext(_) => SourceReadQuestion::MetadataLockContext,
        }
    }
    pub fn basis(&self) -> &DiagnosticEvidenceRef {
        match self {
            Self::TemporaryResources(answer) => &answer.basis,
            Self::QueryCompilation(answer) => &answer.basis,
            Self::TransactionMetadata(answer) => &answer.basis,
            Self::ReadableSecondaryRecovery(answer) => &answer.basis,
            Self::WorkerStarvation(answer) => &answer.basis,
            Self::RelationalInputs(answer) => &answer.basis,
            Self::CurrentExecutionProfile(answer) => &answer.basis,
            Self::OutputGrantRetention(answer) => &answer.basis,
            Self::MetadataLockContext(answer) => &answer.basis,
        }
    }
    pub fn result(&self) -> &DiagnosticCaptureRef {
        match self {
            Self::TemporaryResources(answer) => &answer.result,
            Self::QueryCompilation(answer) => &answer.result,
            Self::TransactionMetadata(answer) => &answer.result,
            Self::ReadableSecondaryRecovery(answer) => &answer.result,
            Self::WorkerStarvation(answer) => &answer.result,
            Self::RelationalInputs(answer) => &answer.result,
            Self::CurrentExecutionProfile(answer) => &answer.result,
            Self::OutputGrantRetention(answer) => &answer.result,
            Self::MetadataLockContext(answer) => &answer.result,
        }
    }
    pub fn references(&self) -> std::vec::IntoIter<&DiagnosticEvidenceRef> {
        let references: Vec<_> = match self {
            Self::TemporaryResources(answer) => answer.references().collect(),
            Self::QueryCompilation(answer) => answer.references().collect(),
            Self::TransactionMetadata(answer) => answer.references().collect(),
            Self::ReadableSecondaryRecovery(answer) => answer.references().collect(),
            Self::WorkerStarvation(answer) => answer.references().collect(),
            Self::RelationalInputs(answer) => answer.references().collect(),
            Self::CurrentExecutionProfile(answer) => answer.references().collect(),
            Self::OutputGrantRetention(answer) => answer.references().collect(),
            Self::MetadataLockContext(answer) => answer.references().collect(),
        };
        references.into_iter()
    }
}

impl DiagnosticEpisode {
    /// One closed answer dispatcher serves offline analysis and the canonical journal recheck.
    pub fn complete_source_read_from_originals(
        &self,
        intent: &DiagnosticInvestigationIntent,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { return Err(invalid()) };
        if !intent.matches_parent_record(original_source, original) {
            return Err(invalid());
        }
        match &focus.prerequisite {
            SourceReadPrerequisite::ObservedMetadataLockWait { .. } => {
                self.complete_metadata_lock_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedClientOutputWait { .. } => {
                self.complete_output_grant_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedReadableSecondaryReader { .. } => {
                self.complete_readable_secondary_recovery(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedTransactionMetadataWait { .. }
            | SourceReadPrerequisite::ObservedTransactionMetadataContinuation { .. } => {
                self.complete_transaction_metadata_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedWorkerQueue => {
                self.complete_worker_starvation_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedCleanupBoundary { .. } => {
                self.complete_temporary_resource_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedQueryHistoryFailure { .. } => {
                self.complete_query_compilation_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedCurrentExecutionRequest { .. } => {
                self.complete_current_execution_profile_read(intent, original_source, original, source, capture, rows)
            }
            SourceReadPrerequisite::ObservedCurrentRequestPlan { .. } => {
                self.complete_relational_input_read(intent, original_source, original, source, capture, rows)
            }
            _ if rows.is_empty() => self.complete_source_read(intent, source, capture),
            _ => Err(invalid()),
        }
    }
}
