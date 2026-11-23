//! Each question retains its own answer while the episode keeps one original identity.
use super::*;

impl DiagnosticEpisode {
    /// Return this question's immutable enrolled focus, or the one original prerequisite that can nominate it.
    /// Authentication and native safety admission remain separate mandatory owners.
    pub fn source_read_focus(&self, question: SourceReadQuestion) -> Result<Option<SourceReadFocus>> {
        if !self.is_valid() {
            return Err(invalid());
        }
        if let Some(stage) = self.source_reads.latest(question) {
            return Ok(Some(stage.focus.clone()));
        }
        self.nominate_source_read_focus(question)
    }
    /// Order is canonical model representation, never scheduling priority or proof of native eligibility.
    pub fn source_read_foci(&self) -> Result<Vec<SourceReadFocus>> {
        SourceReadQuestion::ALL
            .iter()
            .copied()
            .filter_map(|question| self.source_read_focus(question).transpose())
            .collect()
    }
    fn nominate_source_read_focus(&self, selected_question: SourceReadQuestion) -> Result<Option<SourceReadFocus>> {
        let (question, prerequisite) = match (&self.signal, &self.report) {
            (
                DiagnosticSignal::CompilationAdmission { .. },
                Some(DiagnosticReport::CompilationAdmission { report }),
            ) => (
                SourceReadQuestion::CompilationHistory,
                SourceReadPrerequisite::CompilationAdmission { report: report.clone() },
            ),
            (
                DiagnosticSignal::OriginalObservation {
                    condition: DiagnosticOriginalCondition::MssqlQueryHistory { query, .. },
                    ..
                },
                Some(DiagnosticReport::OriginalObservation { .. }),
            ) => (
                SourceReadQuestion::CompilationHistory,
                SourceReadPrerequisite::ObservedQueryHistoryFailure {
                    query: NativeQueryIdentity { query_id: query.id.clone(), qualifiers: query.qualifiers.clone() },
                },
            ),
            (DiagnosticSignal::WorkerStarvation { .. }, None)
            | (
                DiagnosticSignal::OriginalObservation {
                    condition: DiagnosticOriginalCondition::MssqlWorkerQueue, ..
                },
                Some(DiagnosticReport::OriginalObservation { .. }),
            ) => (SourceReadQuestion::WorkerStarvation, SourceReadPrerequisite::ObservedWorkerQueue),
            (DiagnosticSignal::LogRateGovernance { .. }, None) => {
                (SourceReadQuestion::HyperscaleLogComponents, SourceReadPrerequisite::ObservedLogRateWait)
            }
            (
                DiagnosticSignal::TransactionMetadataWait { key, statement_identity: Some(statement), mechanism },
                None,
            ) => (
                SourceReadQuestion::TransactionMetadata,
                SourceReadPrerequisite::ObservedTransactionMetadataWait {
                    key: key.clone(),
                    statement_identity: statement.clone(),
                    mechanism: *mechanism,
                },
            ),
            (
                DiagnosticSignal::OriginalObservation {
                    condition: DiagnosticOriginalCondition::MssqlCleanupBoundary { session, transaction },
                    ..
                },
                Some(DiagnosticReport::OriginalObservation { .. }),
            ) => (
                SourceReadQuestion::TemporarySpaceContributors,
                SourceReadPrerequisite::ObservedCleanupBoundary {
                    session: session.clone(),
                    transaction: transaction.clone(),
                },
            ),
            _ => return Ok(None),
        };
        if question != selected_question {
            return Ok(None);
        }
        Ok(Some(SourceReadFocus {
            basis: Box::new(if question == SourceReadQuestion::TemporarySpaceContributors {
                self.latest.clone()
            } else {
                self.first.clone()
            }),
            target: self.scope.target.clone(),
            question,
            prerequisite,
        }))
    }
    /// Primary interpretation and each child question own independent debt.
    pub(in crate::telemetry) fn owes_primary_analysis(&self) -> bool {
        self.signal.has_primary_question()
            && self.analysis.needs_work()
            && !(self.lifecycle == DiagnosticLifecycle::Resolved
                && self.analysis == DiagnosticAnalysisStatus::Partial
                && self.report.is_some())
    }
    pub fn has_owed_question(&self) -> bool {
        if self.owes_primary_analysis() {
            return true;
        }
        if self.source_reads.is_empty() && !self.signal.has_primary_question() {
            let question = match self.signal {
                DiagnosticSignal::WorkerStarvation { .. } => Some(SourceReadQuestion::WorkerStarvation),
                DiagnosticSignal::ClientResultOutput { .. } => Some(SourceReadQuestion::OutputGrantRetention),
                _ => None,
            };
            return self.analysis.needs_work()
                && !(self.lifecycle == DiagnosticLifecycle::Resolved
                    && question.is_some_and(SourceReadQuestion::requires_live_episode));
        }
        self.source_reads.iter().any(|stage| self.owes_source_read(stage))
    }
    pub(super) fn owes_source_read(&self, stage: &SourceReadStage) -> bool {
        stage.needs_work()
            && self.source_read_context_reuse(&stage.focus).is_none()
            && !(self.lifecycle == DiagnosticLifecycle::Resolved
                && stage.needs_admission()
                && stage.focus.question.requires_live_episode())
    }
    /// One terminal sibling never changes another question's owed work.
    pub fn needs_question(&self, focus: &DiagnosticFocus) -> bool {
        self.matches_focus(focus)
            && match focus {
                DiagnosticFocus::SourceRead { focus } => {
                    self.source_reads.get_focus(focus).is_some_and(|stage| self.owes_source_read(stage))
                }
                _ => self.owes_primary_analysis(),
            }
    }
    /// Reuse the exact caller-authenticated original receipt for this selected question.
    pub fn plan_source_read_focus(
        &self,
        focus: &SourceReadFocus,
        source: &CollectionDescription,
        row: &ExportedRecord,
    ) -> Result<DiagnosticFocusPlan> {
        if !self.is_valid() {
            return Err(invalid());
        }
        if self.source_reads.get_focus(focus).is_some_and(|stage| !stage.needs_admission())
            || self.source_read_context_reuse(focus).is_some()
        {
            return Ok(DiagnosticFocusPlan::NotRequired);
        }
        let context = focus.original_context(source)?;
        let Ok(execution) = SourceReadExecutionScope::from_context(focus.question, &context) else {
            return Ok(DiagnosticFocusPlan::Unavailable(DiagnosticAnalysisIssue::MissingNativeIdentity));
        };
        self.plan_source_read_for_execution(focus, source, row, &execution)
    }
    /// Current native execution is independent of the original focus and the state of every sibling.
    pub fn plan_source_read_for_execution(
        &self,
        focus: &SourceReadFocus,
        source: &CollectionDescription,
        row: &ExportedRecord,
        execution: &SourceReadExecutionScope,
    ) -> Result<DiagnosticFocusPlan> {
        if !self.is_valid() || !focus.is_valid() {
            return Err(invalid());
        }
        if self.source_reads.get_focus(focus).is_some_and(|stage| !stage.needs_admission())
            || self.source_read_context_reuse(focus).is_some()
            || self.lifecycle == DiagnosticLifecycle::Resolved && focus.question.requires_live_episode()
        {
            return Ok(DiagnosticFocusPlan::NotRequired);
        }
        if self.source_read_focus(focus.question)?.as_ref() != Some(focus) {
            return Ok(DiagnosticFocusPlan::NotRequired);
        }
        focus.basis.validate_canonical_record(source, &row.record)?;
        if !focus.matches_basis(source, row, execution) {
            return Ok(DiagnosticFocusPlan::Unavailable(DiagnosticAnalysisIssue::MissingNativeIdentity));
        }
        Ok(DiagnosticFocusPlan::Ready(Box::new(DiagnosticFocus::SourceRead { focus: Box::new(focus.clone()) })))
    }
    pub fn enroll_source_read(&self, focus: SourceReadFocus) -> Result<Self> {
        if !self.is_valid() {
            return Err(invalid());
        }
        let stage = SourceReadStage { focus, state: SourceReadStageState::Pending };
        if !stage.matches_episode(self)
            || self.source_reads.get_focus(&stage.focus).is_none()
                && (self.lifecycle == DiagnosticLifecycle::Resolved && stage.focus.question.requires_live_episode()
                    || stage.focus.question == SourceReadQuestion::TemporarySpaceContributors
                        && stage.focus.basis.as_ref() != &self.latest)
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.source_reads.enroll(stage)?;
        if !next.is_valid() {
            return Err(invalid());
        }
        Ok(next)
    }
    pub fn transition_source_read(&self, focus: &SourceReadFocus, state: SourceReadStageState) -> Result<Self> {
        if !self.is_valid()
            || matches!(state, SourceReadStageState::Collecting) && self.source_read_context_reuse(focus).is_some()
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.source_reads.transition(focus, state)?;
        if !next.is_valid() {
            return Err(invalid());
        }
        Ok(next)
    }
    /// Authentication of the completed, settled capture stays with the canonical result owner.
    pub fn complete_source_read(
        &self,
        intent: &DiagnosticInvestigationIntent,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
    ) -> Result<Self> {
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { return Err(invalid()) };
        if !self.is_valid()
            || !self.needs_question(&intent.focus)
            || !intent.matches_episode(self)
            || !intent.matches_result_source(source)
            || !matches!(source.observation, SourceObservation::SourceRead { .. })
            || !source.observation.covers_request()
            || !capture.is_valid()
            || source.captured_range()?.scope_id != capture.source_scope_id
            || focus.retains_result_rows()
        {
            return Err(invalid());
        }
        self.transition_source_read(focus, SourceReadStageState::Captured { capture: capture.clone() })
    }
    /// Only an already qualified resolved episode can retire unadmitted questions requiring a live participant.
    pub(in crate::telemetry) fn recover_source_read_stages(&self) -> Result<Self> {
        if !self.is_valid() || self.lifecycle != DiagnosticLifecycle::Resolved || self.recovery.is_none() {
            return Err(invalid());
        }
        let mut next = self.clone();
        for stage in self
            .source_reads
            .iter()
            .filter(|stage| stage.needs_admission() && stage.focus.question.requires_live_episode())
        {
            next = next.transition_source_read(&stage.focus, SourceReadStageState::Recovered)?;
        }
        Ok(next)
    }
    pub fn source_read_capture(&self, question: SourceReadQuestion) -> Option<&DiagnosticCaptureRef> {
        self.source_reads.get(question).and_then(SourceReadStage::result)
    }
    pub fn source_read_captures(&self) -> impl Iterator<Item = &DiagnosticCaptureRef> {
        self.source_reads.iter().filter_map(SourceReadStage::result)
    }
    pub fn accepts_source_read_transition(&self, next: &Self) -> bool {
        if !self.source_reads.accepts_transition(&next.source_reads) {
            return false;
        }
        // A primary-only update cannot consume, replace or advance a child stage.
        if self.signal.has_primary_question() && self.source_reads == next.source_reads {
            return true;
        }
        if self.signal.has_primary_question()
            && matches!(self.analysis, DiagnosticAnalysisStatus::Pending | DiagnosticAnalysisStatus::Deferred)
            && next.analysis == DiagnosticAnalysisStatus::Deferred
            && self.report == next.report
            && self.source_reads.accepts_admission_refresh(&next.source_reads)
        {
            return true;
        }
        if self.source_reads.is_empty() {
            return next.source_reads.is_empty()
                || self.report.as_ref().is_none_or(|report| {
                    next.report.as_ref() == Some(report) && self.analysis == next.analysis && self.issue == next.issue
                });
        }
        self.analysis == next.analysis
            && match (&self.signal, &self.report, &next.report) {
                (
                    DiagnosticSignal::OriginalObservation { .. },
                    Some(DiagnosticReport::OriginalObservation { .. }),
                    Some(DiagnosticReport::OriginalObservation { .. }),
                ) => self.issue == next.issue,
                _ => self.report == next.report,
            }
    }
}
impl SourceReadQuestion {
    pub fn requires_live_episode(self) -> bool {
        matches!(
            self,
            Self::TemporarySpaceContributors
                | Self::WorkerStarvation
                | Self::CurrentExecutionProfile
                | Self::OutputGrantRetention
                | Self::MetadataLockContext
                | Self::ReadableSecondaryRecovery
                | Self::TransactionMetadata
        )
    }
}

impl DiagnosticSignal {
    /// Native follow-ups for these original signals interpret the episode's primary report.
    /// Other signals own only their explicit child questions, even while their aggregate status is pending.
    fn has_primary_question(&self) -> bool {
        matches!(
            self,
            Self::LockWait { .. }
                | Self::PageWait { .. }
                | Self::PageTaskWait { .. }
                | Self::RequestMemory { .. }
                | Self::CompilationAdmission { .. }
                | Self::Activity { condition: ActivityCondition::LongRunning, .. }
        )
    }
}
