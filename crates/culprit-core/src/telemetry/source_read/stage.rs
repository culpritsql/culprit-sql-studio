//! One immutable question focus owns its finite state transitions.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SourceReadStage {
    pub focus: SourceReadFocus,
    pub state: SourceReadStageState,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum SourceReadStageState {
    Pending,
    Deferred { issue: DiagnosticAnalysisIssue },
    Collecting,
    Captured { capture: DiagnosticCaptureRef },
    Answered { answer: Box<SourceReadAnswer> },
    Recovered,
    Stopped { issue: DiagnosticAnalysisIssue },
}
impl SourceReadStage {
    pub fn is_valid(&self) -> bool {
        self.focus.is_valid()
            && match &self.state {
                SourceReadStageState::Answered { answer } => answer.matches_focus(&self.focus),
                SourceReadStageState::Captured { capture } => {
                    !self.focus.retains_result_rows()
                        && capture.is_valid()
                        && capture.capture != self.focus.basis.capture
                        && capture.source_scope_id != self.focus.basis.source_scope_id
                }
                SourceReadStageState::Recovered => self.focus.question.requires_live_episode(),
                _ => true,
            }
    }
    /// Stable finite-stage identity can be checked before recovering original bodies or cost artifacts.
    pub fn key(&self, episode_id: &str) -> Result<String> {
        if !self.is_valid() {
            return Err(invalid());
        }
        crate::telemetry::investigations::question_stage_key(
            episode_id,
            crate::telemetry::investigations::QuestionStage::SourceRead(self.focus.question),
            &self.focus.target,
        )
    }
    pub fn needs_work(&self) -> bool {
        matches!(
            self.state,
            SourceReadStageState::Pending | SourceReadStageState::Deferred { .. } | SourceReadStageState::Collecting
        )
    }
    /// An admitted owner remains owed without granting a replacement admission after a cold restore.
    pub fn needs_admission(&self) -> bool {
        matches!(self.state, SourceReadStageState::Pending | SourceReadStageState::Deferred { .. })
    }
    pub fn result(&self) -> Option<&DiagnosticCaptureRef> {
        match &self.state {
            SourceReadStageState::Captured { capture } => Some(capture),
            SourceReadStageState::Answered { answer } => Some(answer.result()),
            _ => None,
        }
    }
    pub fn matches_episode(&self, episode: &DiagnosticEpisode) -> bool {
        self.is_valid()
            && self.basis_matches_episode(episode)
            && self.focus.target.telemetry_id().ok() == episode.scope.target.telemetry_id().ok()
            && (!matches!(self.state, SourceReadStageState::Recovered)
                || self.focus.question.requires_live_episode() && episode.lifecycle == DiagnosticLifecycle::Resolved)
            && match (&episode.signal, &self.focus.prerequisite, &episode.report) {
                (
                    DiagnosticSignal::CompilationAdmission { .. },
                    SourceReadPrerequisite::CompilationAdmission { report },
                    Some(DiagnosticReport::CompilationAdmission { report: original }),
                ) => report == original,
                (
                    DiagnosticSignal::OriginalObservation {
                        condition: DiagnosticOriginalCondition::MssqlQueryHistory { query: original, .. },
                        ..
                    },
                    SourceReadPrerequisite::ObservedQueryHistoryFailure { query },
                    Some(DiagnosticReport::OriginalObservation { .. }),
                ) => query.native() == *original,
                (DiagnosticSignal::LogRateGovernance { .. }, SourceReadPrerequisite::ObservedLogRateWait, None) => true,
                (DiagnosticSignal::WorkerStarvation { .. }, SourceReadPrerequisite::ObservedWorkerQueue, None) => true,
                (
                    DiagnosticSignal::OriginalObservation {
                        condition: DiagnosticOriginalCondition::MssqlWorkerQueue,
                        ..
                    },
                    SourceReadPrerequisite::ObservedWorkerQueue,
                    Some(DiagnosticReport::OriginalObservation { .. }),
                ) => true,
                (
                    DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. },
                    SourceReadPrerequisite::ObservedCurrentRequestPlan { observation, .. },
                    Some(DiagnosticReport::CurrentRequestWork { observation: actual }),
                ) => observation.as_ref() == actual,
                (
                    DiagnosticSignal::TransactionMetadataWait { key, statement_identity: Some(statement), mechanism },
                    SourceReadPrerequisite::ObservedTransactionMetadataWait {
                        key: expected,
                        statement_identity,
                        mechanism: expected_kind,
                    },
                    None,
                ) => key == expected && statement == statement_identity && mechanism == expected_kind,
                (
                    DiagnosticSignal::OriginalObservation {
                        condition: DiagnosticOriginalCondition::MssqlCleanupBoundary { session, transaction },
                        ..
                    },
                    SourceReadPrerequisite::ObservedCleanupBoundary {
                        session: expected_session,
                        transaction: expected_transaction,
                    },
                    Some(DiagnosticReport::OriginalObservation { .. }),
                ) => session == expected_session && transaction == expected_transaction,
                _ => false,
            }
    }
    fn basis_matches_episode(&self, episode: &DiagnosticEpisode) -> bool {
        // These questions may retain a later authenticated observation of the same native original.
        match self.focus.question {
            SourceReadQuestion::TemporarySpaceContributors | SourceReadQuestion::RelationalInputContext => {
                self.focus.basis.source_scope_id == episode.first.source_scope_id
                    && self.focus.basis.native_key == episode.first.native_key
            }
            _ => self.focus.basis.as_ref() == &episode.first,
        }
    }
    pub fn transition(&self, state: SourceReadStageState) -> Result<Self> {
        if !self.is_valid() {
            return Err(invalid());
        }
        if self.state == state {
            return Ok(self.clone());
        }
        let permitted = match self.state {
            SourceReadStageState::Pending => matches!(
                state,
                SourceReadStageState::Deferred { .. }
                    | SourceReadStageState::Collecting
                    | SourceReadStageState::Recovered
                    | SourceReadStageState::Stopped { .. }
            ),
            SourceReadStageState::Deferred { .. } => matches!(
                state,
                SourceReadStageState::Deferred { .. }
                    | SourceReadStageState::Collecting
                    | SourceReadStageState::Recovered
                    | SourceReadStageState::Stopped { .. }
            ),
            SourceReadStageState::Collecting => {
                matches!(
                    state,
                    SourceReadStageState::Captured { .. }
                        | SourceReadStageState::Answered { .. }
                        | SourceReadStageState::Stopped { .. }
                )
            }
            SourceReadStageState::Captured { .. }
            | SourceReadStageState::Answered { .. }
            | SourceReadStageState::Recovered
            | SourceReadStageState::Stopped { .. } => false,
        };
        let next = Self { focus: self.focus.clone(), state };
        if !permitted || !next.is_valid() {
            return Err(invalid());
        }
        Ok(next)
    }
}
