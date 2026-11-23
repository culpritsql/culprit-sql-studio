//! Finite source questions keep logical data identity separate from the qualified execution/cost owner.
use super::*;

mod scope;
pub use scope::*;
mod answer;
pub use answer::*;
mod stage;
pub(in crate::telemetry) use stage::ObservationOrdinal;
pub use stage::{SourceReadStage, SourceReadStageState};
mod stages;
pub use stages::{MAX_SOURCE_READ_STAGES, MAX_TRANSACTION_METADATA_OBSERVATIONS, SourceReadStages};
mod followup;
mod lifecycle;
mod preference;
mod temporary;
pub use temporary::*;
mod readable_secondary_recovery;
pub use readable_secondary_recovery::*;
mod transaction_metadata;
pub use transaction_metadata::*;
mod transaction_metadata_continuation;
pub use transaction_metadata_continuation::TransactionMetadataContinuation;
mod output_grant;
pub use output_grant::*;
mod metadata_locks;
pub use metadata_locks::*;
mod workers;
pub use workers::*;
mod query_compilation;
pub use query_compilation::*;
mod execution_profile;
pub use execution_profile::*;
mod relational_input;
pub use relational_input::*;

/// Native updateability qualifies this exact execution expectation. It names neither a replica role nor a
/// query/execution population, and cannot supply a member or process identity that the source did not report.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct NativeSourceReadScope {
    pub target: crate::DatabaseIdentity,
    pub partition: SourcePartition,
    pub database_writable: bool,
}
impl NativeSourceReadScope {
    pub fn from_context(context: &NativeReadContext) -> Result<Self> {
        let CollectionTarget::Database(target) = &context.target else { return Err(invalid()) };
        let value = Self {
            target: target.clone(),
            partition: context.partition.clone(),
            database_writable: context.database_writable.ok_or_else(invalid)?,
        };
        if !context.is_valid() || !value.is_valid() {
            return Err(invalid());
        }
        Ok(value)
    }
    pub fn is_valid(&self) -> bool {
        let target = CollectionTarget::Database(self.target.clone());
        target.is_valid()
            && self.target.incarnation.as_ref().is_some_and(|identity| !identity.is_empty())
            && self.partition.is_valid_for(&target)
            && matches!(&self.partition, SourcePartition::InstanceLocal { member: Some(_), epoch: Some(_) })
            && self.partition.is_qualified()
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(invalid());
        }
        hash(&("native-source-read-scope-v1", self.target.telemetry_id()?, &self.partition, self.database_writable))
    }
    pub fn matches_context(&self, context: &NativeReadContext) -> bool {
        self.is_valid() && Self::from_context(context).is_ok_and(|actual| actual.key().ok() == self.key().ok())
    }
    pub fn matches_target(&self, target: &CollectionTarget) -> bool {
        self.is_valid() && target.database().is_some() && target.telemetry_id().ok() == self.target.telemetry_id().ok()
    }
}

/// Only implemented questions can acquire automatic source work. Native providers declare supported routes;
/// engine identity and a generic Snapshot selector confer no additional admission or counter semantics.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum SourceReadQuestion {
    CompilationHistory,
    HyperscaleLogComponents,
    TemporarySpaceContributors,
    TransactionMetadata,
    ReadableSecondaryRecovery,
    WorkerStarvation,
    RelationalInputContext,
    CurrentExecutionProfile,
    OutputGrantRetention,
    MetadataLockContext,
}
impl SourceReadQuestion {
    pub const ALL: &'static [Self] = &[
        Self::CompilationHistory,
        Self::HyperscaleLogComponents,
        Self::TemporarySpaceContributors,
        Self::TransactionMetadata,
        Self::ReadableSecondaryRecovery,
        Self::WorkerStarvation,
        Self::RelationalInputContext,
        Self::CurrentExecutionProfile,
        Self::OutputGrantRetention,
        Self::MetadataLockContext,
    ];

    pub const fn dataset(self) -> Option<SnapshotDataset> {
        Some(match self {
            Self::CompilationHistory => SnapshotDataset::QueryCompilation,
            Self::HyperscaleLogComponents => SnapshotDataset::HyperscaleLogComponents,
            Self::TemporarySpaceContributors => SnapshotDataset::TaskSpace,
            Self::TransactionMetadata => SnapshotDataset::TransactionMetadata,
            Self::ReadableSecondaryRecovery => SnapshotDataset::ReadableSecondaryRecovery,
            Self::WorkerStarvation => SnapshotDataset::WorkerStarvation,
            Self::RelationalInputContext => SnapshotDataset::RetainedPlans,
            Self::CurrentExecutionProfile => SnapshotDataset::CurrentExecutionProfile,
            Self::OutputGrantRetention => SnapshotDataset::CurrentRequestMemory,
            Self::MetadataLockContext => return None,
        })
    }
    pub const fn collection(self) -> TelemetryCollection {
        match self.dataset() {
            Some(dataset) => TelemetryCollection::Snapshot(dataset),
            None => TelemetryCollection::Activity,
        }
    }
    pub const fn selector(self) -> RecipeSelector {
        match self {
            Self::WorkerStarvation => RecipeSelector::WorkerStarvation,
            Self::MetadataLockContext => RecipeSelector::FocusedMetadataLocks,
            _ => RecipeSelector::Collection(self.collection()),
        }
    }
    pub fn accepts_dataset(self, dataset: SnapshotDataset) -> bool {
        self.dataset() == Some(dataset)
    }
    pub fn for_dataset(dataset: SnapshotDataset) -> Option<Self> {
        Self::ALL.iter().copied().find(|question| question.accepts_dataset(dataset))
    }
}

/// Only compilation history requires a prior gateway answer. A native log-governance wait is itself the
/// original incident evidence; it cannot borrow or manufacture a compilation prerequisite.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum SourceReadPrerequisite {
    CompilationAdmission {
        report: Box<CompilationAdmissionReport>,
    },
    ObservedReadableSecondaryReader {
        request: Box<ActivityRequestFocus>,
    },
    ObservedLogRateWait,
    ObservedTransactionMetadataWait {
        key: ActivityKey,
        statement_identity: NativeIdentity,
        mechanism: TransactionMetadataWait,
    },
    ObservedTransactionMetadataContinuation {
        continuation: Box<TransactionMetadataContinuation>,
    },
    ObservedCleanupBoundary {
        session: NativeActivityIdentity,
        transaction: NativeActivityIdentity,
    },
    ObservedQueryHistoryFailure {
        query: NativeQueryIdentity,
    },
    ObservedWorkerQueue,
    /// Existing adverse original +exact matched CurrentWork row; never a plan/type-only trigger.
    ObservedCurrentExecutionRequest {
        observation: Box<DiagnosticEvidenceRef>,
        request: Box<ActivityRequestFocus>,
        observed_until: String,
        #[serde(deserialize_with = "Option::deserialize")]
        exchange_wait: Option<MssqlParallelExchangeWait>,
    },
    ObservedClientOutputWait {
        request: Box<ActivityRequestFocus>,
    },
    ObservedMetadataLockWait {
        request: Box<ActivityRequestFocus>,
    },
    /// A later current-cache point, not an execution-plan or runtime-input attestation for the adverse parent.
    ObservedCurrentRequestPlan {
        observation: Box<DiagnosticEvidenceRef>,
        context: Box<CachedStatementContext>,
        observed_from: String,
        observed_until: String,
    },
}
impl SourceReadPrerequisite {
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        let references: Vec<_> = match self {
            Self::CompilationAdmission { report } => report.references().collect(),
            Self::ObservedReadableSecondaryReader { request } => vec![&request.parent],
            Self::ObservedTransactionMetadataContinuation { continuation } => {
                vec![&continuation.previous, &continuation.request.parent]
            }
            Self::ObservedCurrentRequestPlan { observation, .. }
            | Self::ObservedCurrentExecutionRequest { observation, .. } => vec![observation.as_ref()],
            _ => Vec::new(),
        };
        references.into_iter()
    }

    /// The same canonical parent owner authenticates either implemented detail prerequisite.
    pub fn observation(&self) -> Option<&DiagnosticEvidenceRef> {
        match self {
            Self::CompilationAdmission { report } => Some(&report.observation),
            Self::ObservedTransactionMetadataContinuation { continuation } => Some(&continuation.previous),
            Self::ObservedCurrentRequestPlan { observation, .. }
            | Self::ObservedCurrentExecutionRequest { observation, .. } => Some(observation),
            _ => None,
        }
    }

    /// A prior selected metadata point has its own completed canonical source proof, not an Activity focus.
    /// Other existing native prerequisites must still authenticate their actual focused participant.
    pub fn activity_participant<'a>(
        &self,
        source: &'a CollectionDescription,
    ) -> Result<Option<&'a ActivityRequestFocus>> {
        match self {
            Self::ObservedTransactionMetadataContinuation { .. } => {
                if !matches!(&source.observation, SourceObservation::SourceRead { snapshot, .. }
                    if snapshot.dataset == SnapshotDataset::TransactionMetadata)
                {
                    return Err(invalid());
                }
                Ok(None)
            }
            _ => source.observation.focus().map(Some).ok_or_else(invalid),
        }
    }

    pub fn compilation_admission(&self) -> Option<&CompilationAdmissionReport> {
        match self {
            Self::CompilationAdmission { report } => Some(report),
            Self::ObservedReadableSecondaryReader { .. }
            | Self::ObservedTransactionMetadataWait { .. }
            | Self::ObservedTransactionMetadataContinuation { .. }
            | Self::ObservedLogRateWait
            | Self::ObservedClientOutputWait { .. }
            | Self::ObservedMetadataLockWait { .. }
            | Self::ObservedWorkerQueue
            | Self::ObservedCleanupBoundary { .. }
            | Self::ObservedQueryHistoryFailure { .. }
            | Self::ObservedCurrentRequestPlan { .. }
            | Self::ObservedCurrentExecutionRequest { .. } => None,
        }
    }
    fn matches_cleanup_condition(&self, condition: &DiagnosticOriginalCondition) -> bool {
        match (self, condition) {
            (
                Self::ObservedCleanupBoundary { session, transaction },
                DiagnosticOriginalCondition::MssqlCleanupBoundary {
                    session: actual_session,
                    transaction: actual_transaction,
                },
            ) => session == actual_session && transaction == actual_transaction,
            _ => false,
        }
    }
    fn is_valid_for(&self, question: SourceReadQuestion) -> bool {
        match (self, question) {
            (Self::ObservedMetadataLockWait { request }, SourceReadQuestion::MetadataLockContext) => {
                request.is_metadata_lock_focus()
            }
            (Self::CompilationAdmission { report }, SourceReadQuestion::CompilationHistory) => report.is_valid(),
            (Self::ObservedQueryHistoryFailure { query }, SourceReadQuestion::CompilationHistory) => {
                query_store_query_identity_is_complete(query)
            }
            (Self::ObservedReadableSecondaryReader { request }, SourceReadQuestion::ReadableSecondaryRecovery) => {
                request.is_readable_secondary_recovery_focus()
            }
            (
                Self::ObservedTransactionMetadataContinuation { continuation },
                SourceReadQuestion::TransactionMetadata,
            ) => continuation.is_valid(),
            (Self::ObservedLogRateWait, SourceReadQuestion::HyperscaleLogComponents) => true,
            (Self::ObservedWorkerQueue, SourceReadQuestion::WorkerStarvation) => true,
            (Self::ObservedClientOutputWait { request }, SourceReadQuestion::OutputGrantRetention) => {
                request.is_output_memory_focus()
            }
            (
                Self::ObservedCurrentExecutionRequest { observation, request, observed_until, exchange_wait },
                SourceReadQuestion::CurrentExecutionProfile,
            ) => {
                observation.is_valid()
                    && observation.stream == TelemetryStream::OperationalSamples
                    && matches!(&observation.native_key,NativeRecordIdentity::Operational{source,..} if source==crate::source_id::MSSQL_CURRENT_REQUEST_WORK)
                    && request.is_valid()
                    && CurrentRequestWorkProfile::MssqlRequest.accepts_focus(request)
                    && request.database_writable.is_some()
                    && request.partition.native_epoch().is_some()
                    && request
                        .original
                        .wait
                        .as_ref()
                        .and_then(|wait| wait.native_type.as_deref())
                        .and_then(MssqlParallelExchangeWait::from_native)
                        == *exchange_wait
                    && ObservationBounds::new(&request.original_observed_at, observed_until).is_some()
            }
            (
                Self::ObservedCurrentRequestPlan { observation, context, observed_from, observed_until },
                SourceReadQuestion::RelationalInputContext,
            ) => {
                observation.is_valid()
                    && matches!(&observation.native_key, NativeRecordIdentity::Operational { source, .. } if source == crate::source_id::MSSQL_CURRENT_REQUEST_WORK)
                    && observation.stream == TelemetryStream::OperationalSamples
                    && context.is_valid()
                    && context.identity.has_plan_handle()
                    && context.identity.has_sql_handle()
                    && context.origin == StatementOrigin::Application
                    && ObservationBounds::new(observed_from, observed_until).is_some()
            }
            (
                Self::ObservedTransactionMetadataWait { key, statement_identity, .. },
                SourceReadQuestion::TransactionMetadata,
            ) => key.is_valid() && key.request.is_some() && statement_identity.is_valid(),
            (
                Self::ObservedCleanupBoundary { session, transaction },
                SourceReadQuestion::TemporarySpaceContributors,
            ) => {
                session.is_valid()
                    && transaction.is_valid()
                    && !session.generation.trim().is_empty()
                    && !transaction.generation.trim().is_empty()
            }
            _ => false,
        }
    }
}

/// An exact genuine incident receipt justifies one finite context question. No Activity/request/SQL
/// envelope is invented for source-global evidence. The caller authenticates the original independently.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SourceReadFocus {
    pub basis: Box<DiagnosticEvidenceRef>,
    /// Logical dataset namespace. An admitted native execution partition never replaces it.
    pub target: CollectionTarget,
    pub question: SourceReadQuestion,
    pub prerequisite: SourceReadPrerequisite,
}
impl SourceReadFocus {
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        std::iter::once(self.basis.as_ref()).chain(self.prerequisite.references())
    }

    pub fn is_valid(&self) -> bool {
        self.basis.is_valid()
            && self.prerequisite.is_valid_for(self.question)
            && match (&self.prerequisite, self.question) {
                (
                    SourceReadPrerequisite::ObservedCurrentExecutionRequest { request, observation, .. },
                    SourceReadQuestion::CurrentExecutionProfile,
                ) => {
                    request.parent == *self.basis
                        && request.target == self.target
                        && observation.capture != self.basis.capture
                }
                (
                    SourceReadPrerequisite::ObservedQueryHistoryFailure { .. },
                    SourceReadQuestion::CompilationHistory,
                ) => {
                    self.basis.stream == TelemetryStream::QueryRuntime
                        && matches!(self.basis.native_key, NativeRecordIdentity::Runtime(_))
                }
                (SourceReadPrerequisite::ObservedWorkerQueue, SourceReadQuestion::WorkerStarvation) => {
                    matches!(
                        (&self.basis.stream, &self.basis.native_key),
                        (TelemetryStream::OperationalSamples, NativeRecordIdentity::Operational { .. })
                            | (TelemetryStream::ActivityObservations, NativeRecordIdentity::Activity(_))
                    )
                }
                (_, SourceReadQuestion::TemporarySpaceContributors) => {
                    self.basis.stream == TelemetryStream::OperationalSamples
                        && matches!(self.basis.native_key, NativeRecordIdentity::Operational { .. })
                }
                _ => {
                    self.basis.stream == TelemetryStream::ActivityObservations
                        && matches!(self.basis.native_key, NativeRecordIdentity::Activity(_))
                }
            }
            && match &self.prerequisite {
                SourceReadPrerequisite::ObservedReadableSecondaryReader { request }
                | SourceReadPrerequisite::ObservedMetadataLockWait { request } => {
                    request.parent == *self.basis && request.target == self.target
                }
                SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } => {
                    continuation.request.parent == *self.basis && continuation.request.target == self.target
                }
                _ => true,
            }
            && self.target.is_valid()
            && (self.target.database().is_some()
                || self.question == SourceReadQuestion::WorkerStarvation
                    && matches!(&self.target, CollectionTarget::Shared(resource)
                        if matches!(resource.kind, SharedResourceKind::Instance|SharedResourceKind::Node)))
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(invalid());
        }
        hash(&("source-read-focus-v1", &self.basis, self.target.telemetry_id()?, self.question, &self.prerequisite))
    }
    /// Consistency over separately caller-authenticated original bodies. The existing native request key
    /// compares full source/request/statement ownership, without using display SQL, IDs alone or poll clocks.
    pub fn matches_prerequisite_input<'a>(
        &self,
        original_source: &CollectionDescription,
        original_row: &ExportedRecord,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = (&'a DiagnosticEvidenceRef, &'a OperationalObservation)>,
    ) -> bool {
        if let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &self.prerequisite {
            return continuation.matches_input(self, original_source, original_row, source, rows);
        }
        if matches!(self.prerequisite, SourceReadPrerequisite::ObservedCurrentExecutionRequest { .. }) {
            return self.matches_execution_profile_input(original_source, original_row, source, rows);
        }
        if matches!(self.prerequisite, SourceReadPrerequisite::ObservedCurrentRequestPlan { .. }) {
            return self.matches_current_plan_input(original_source, original_row, source, rows);
        }
        let Some(prerequisite) = self.prerequisite.compilation_admission() else { return false };
        let (
            Ok(original),
            SourceObservation::FocusedCompilationAdmission { result, .. },
            TelemetryRecord::Activity(actor),
        ) = (NativeReadContext::from_activity(original_source), &source.observation, &original_row.record)
        else {
            return false;
        };
        let Ok(execution) = SourceReadExecutionScope::from_context(self.question, &original) else { return false };
        let Some(statement) = &actor.statement_identity else { return false };
        let participant = &result.focus;
        let context = NativeReadContext {
            target: participant.target.clone(),
            partition: participant.partition.clone(),
            observed_from: participant.original_observed_at.clone(),
            observed_until: participant.original_observed_at.clone(),
            database_writable: participant.database_writable,
        };
        self.matches_basis(original_source, original_row, &execution)
            && participant.source == original_source.source
            && original.partition.native_epoch() == Some(participant.epoch.as_str())
            && original
                .request_key(&actor.key, statement)
                .zip(context.request_key(&participant.original.key, &participant.statement_identity))
                .is_some_and(|(left, right)| left == right)
            && prerequisite.matches_input(source, rows)
    }
    /// Only closed, bounded answer families materialize their original rows for offline diagnosis.
    pub fn retains_result_rows(&self) -> bool {
        matches!(
            self.prerequisite,
            SourceReadPrerequisite::ObservedReadableSecondaryReader { .. }
                | SourceReadPrerequisite::ObservedCleanupBoundary { .. }
                | SourceReadPrerequisite::ObservedQueryHistoryFailure { .. }
                | SourceReadPrerequisite::ObservedTransactionMetadataWait { .. }
                | SourceReadPrerequisite::ObservedTransactionMetadataContinuation { .. }
                | SourceReadPrerequisite::ObservedClientOutputWait { .. }
                | SourceReadPrerequisite::ObservedMetadataLockWait { .. }
                | SourceReadPrerequisite::ObservedWorkerQueue
                | SourceReadPrerequisite::ObservedCurrentRequestPlan { .. }
                | SourceReadPrerequisite::ObservedCurrentExecutionRequest { .. }
        )
    }
    /// The existing canonical reader verifies every original row and byte before retaining this selection.
    /// Query-number candidates include changed incarnations, which the answer must explicitly refuse.
    pub fn retains_result_record(&self, record: &TelemetryRecord) -> bool {
        match &self.prerequisite {
            SourceReadPrerequisite::ObservedQueryHistoryFailure { query } => match record {
                TelemetryRecord::Operational(OperationalObservation {
                    source,
                    fact: OperationalFact::MssqlQueryCompilation(point),
                    ..
                }) if source == crate::source_id::QUERY_COMPILATION => point.identity.query_id == query.query_id,
                // Omit only known unrelated queries. A foreign family/source survives selection for refusal.
                _ => true,
            },
            _ => self.retains_result_rows(),
        }
    }
    pub fn selects_result_rows(&self) -> bool {
        matches!(self.prerequisite, SourceReadPrerequisite::ObservedQueryHistoryFailure { .. })
    }
    pub fn original_context(&self, source: &CollectionDescription) -> Result<NativeReadContext> {
        if matches!(
            self.prerequisite,
            SourceReadPrerequisite::ObservedQueryHistoryFailure { .. } | SourceReadPrerequisite::ObservedWorkerQueue
        ) {
            return SourceReadExecutionScope::context_from_source(source);
        }
        match self.question {
            SourceReadQuestion::TemporarySpaceContributors => NativeReadContext::from_snapshot(source),
            _ => NativeReadContext::from_activity(source),
        }
    }
    fn original_condition_matches(&self, source: &CollectionDescription, row: &TelemetryRecord) -> bool {
        if let SourceReadPrerequisite::ObservedQueryHistoryFailure { query } = &self.prerequisite {
            let Some(DiagnosticOriginalCondition::MssqlQueryHistory { query: original, .. }) =
                DiagnosticOriginalCondition::from_record(source, row)
            else {
                return false;
            };
            return query_store_query_incarnation_key(query).ok()
                == query_store_query_incarnation_key(&NativeQueryIdentity {
                    query_id: original.id,
                    qualifiers: original.qualifiers,
                })
                .ok();
        }
        if self.question == SourceReadQuestion::WorkerStarvation {
            return worker_starvation::original_worker_queue_matches(source, row);
        }
        match self.question {
            SourceReadQuestion::CurrentExecutionProfile => {
                let SourceReadPrerequisite::ObservedCurrentExecutionRequest { request, .. } = &self.prerequisite else {
                    return false;
                };
                matches!(row,TelemetryRecord::Activity(original)
                    if original.as_ref()==&request.original && request.parent==*self.basis)
            }
            SourceReadQuestion::MetadataLockContext => self.metadata_lock_request().is_some_and(|request|matches!(row,TelemetryRecord::Activity(original) if request.matches_parent(source,original,&self.basis))),
            SourceReadQuestion::OutputGrantRetention => self.output_request().is_some_and(|request|matches!(row,TelemetryRecord::Activity(original) if request.matches_parent(source,original,&self.basis))),
            SourceReadQuestion::RelationalInputContext => matches!(row, TelemetryRecord::Activity(original)
                if original.has_session_generation_authority()
                    && original.scope == ActivityScopeBasis::RequestDatabase
                    && original.key.request.is_some()
                    && original.statement_identity.as_ref().is_some_and(mssql_request_statement_identity_is_valid)),
            SourceReadQuestion::ReadableSecondaryRecovery => {
                let SourceReadPrerequisite::ObservedReadableSecondaryReader { request } = &self.prerequisite else {return false};
                matches!(row, TelemetryRecord::Activity(original) if original.as_ref() == &request.original && request.matches_parent(source, original, &self.basis))
            }
            SourceReadQuestion::TransactionMetadata => {
                if let SourceReadPrerequisite::ObservedTransactionMetadataContinuation { continuation } = &self.prerequisite {
                    return matches!(row, TelemetryRecord::Activity(original)
                        if continuation.request.matches_parent(source, original, &self.basis));
                }
                let Some((key, statement_identity, mechanism)) = self.transaction_metadata_participant() else {
                    return false;
                };
                matches!(row, TelemetryRecord::Activity(original)
                    if original.has_session_generation_authority()
                        && matches!(original.scope, ActivityScopeBasis::RequestDatabase | ActivityScopeBasis::SessionDatabase)
                        && original.key == *key && original.statement_identity.as_ref() == Some(statement_identity)
                        && original.wait.as_ref().is_some_and(|wait| wait.class == Some(mechanism.class())))
            }
            SourceReadQuestion::TemporarySpaceContributors => {
                let Some(condition) = DiagnosticOriginalCondition::from_record(source, row) else {
                    return false;
                };
                self.prerequisite.matches_cleanup_condition(&condition) && cleanup_attention(row)
            }
            question => matches!(row, TelemetryRecord::Activity(original)
            if original.has_session_generation_authority()
                && original.scope == ActivityScopeBasis::RequestDatabase
                && original.wait.as_ref().is_some_and(|wait| wait.class == Some(match question {
                    SourceReadQuestion::CompilationHistory => ActivityWaitClass::CompilationAdmission,
                    SourceReadQuestion::HyperscaleLogComponents => ActivityWaitClass::LogRateGovernance,
                    SourceReadQuestion::TemporarySpaceContributors | SourceReadQuestion::WorkerStarvation | SourceReadQuestion::RelationalInputContext | SourceReadQuestion::CurrentExecutionProfile | SourceReadQuestion::TransactionMetadata | SourceReadQuestion::OutputGrantRetention | SourceReadQuestion::ReadableSecondaryRecovery | SourceReadQuestion::MetadataLockContext => return false,
                }))),
        }
    }
    /// Authenticate the exact retained prerequisite independently of the new read's native admission.
    pub fn matches_original(&self, source: &CollectionDescription, row: &ExportedRecord) -> bool {
        self.is_valid()
            && source.target.telemetry_id().ok() == self.target.telemetry_id().ok()
            && self.original_condition_matches(source, &row.record)
            && self.basis.validate_canonical_record(source, &row.record).is_ok()
            && self.basis.capture == row.capture
            && row.batch_id == self.basis.batch_id
            && row.payload_sha256().is_ok_and(|digest| digest == self.basis.payload_sha256)
    }
    pub fn matches_basis(
        &self,
        source: &CollectionDescription,
        row: &ExportedRecord,
        execution: &SourceReadExecutionScope,
    ) -> bool {
        self.matches_original(source, row)
            && execution.matches_target(&self.target)
            && execution.accepts_question(self.question)
            && self.original_context(source).is_ok_and(|context| execution.matches_original_context(&context))
    }
}

/// The completed waiter/gateway answer stays immutable. This separately owed question transitions only through
/// revision/event history, and retains one successful capture rather than a per-row compiler roster.
impl SourceObservation {
    pub fn is_source_read_result(&self) -> bool {
        matches!(self, Self::SourceRead { .. } | Self::MetadataLockContext { .. })
    }
    pub fn execution_context(&self) -> Option<&NativeReadContext> {
        match self {
            Self::SourceRead { execution, .. }
            | Self::PagedSourceRead { execution, .. }
            | Self::MetadataLockContext { execution, .. } => Some(execution),
            _ => None,
        }
    }
}
fn cleanup_attention(row: &TelemetryRecord) -> bool {
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::MssqlCleanupHorizon(horizon),
        ..
    }) = row
    else {
        return false;
    };
    horizon.oldest_confirmed.as_deref().is_some_and(MssqlCleanupTransaction::needs_cleanup_investigation)
}
pub(super) fn same_time(left: &str, right: &str) -> bool {
    canonical_time(left).ok().zip(canonical_time(right).ok()).is_some_and(|(left, right)| left == right)
}
fn hash(value: &impl Serialize) -> Result<String> {
    serde_json::to_vec(value).map(|bytes| digest(&bytes)).map_err(|_| invalid())
}
fn invalid() -> Error {
    CollectionStop::InvalidPage.error()
}

#[cfg(test)]
mod tests;
