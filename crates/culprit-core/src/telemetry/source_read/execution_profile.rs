//! One shared current-execution acquisition can answer parallel-worker questions and later TVP interpretation.
//! It never turns a newly observed cache into the historical victim's plan or claims complete profiling.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CurrentExecutionProfileAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub current_work: DiagnosticEvidenceRef,
    pub prerequisite_observed_until: String,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub observation: DiagnosticEvidenceRef,
    pub observed_at: String,
    pub checked_until: String,
    pub binding: FocusedRequestBinding,
    pub membership: FocusedRequestMembership,
    pub source_state: crate::SourceState,
    pub evidence: CurrentExecutionProfileEvidence,
    pub profiles: ExecutionProfileAvailability,
}
impl CurrentExecutionProfileAnswer {
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let SourceReadPrerequisite::ObservedCurrentExecutionRequest {
            observation: work, request, observed_until, ..
        } = &focus.prerequisite
        else {
            return Err(invalid());
        };
        let SourceObservation::SourceRead { snapshot, focus_key, execution } = &source.observation else {
            return Err(invalid());
        };
        let authority = SourceReadExecutionScope::from_context(focus.question, execution)?;
        let [(reference, row)] = rows else { return Err(invalid()) };
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::MssqlCurrentExecutionProfile(point),
            ..
        }) = &row.record
        else {
            return Err(invalid());
        };
        if focus.question != SourceReadQuestion::CurrentExecutionProfile
            || !focus.matches_basis(original_source, original, &authority)
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || focus.key()? != *focus_key
            || !point.fits_source(source)
            || point.focus.as_ref() != request.as_ref()
            || snapshot.records != 1
            || !capture.is_valid()
            || capture.capture == focus.basis.capture
            || capture.capture == work.capture
            || source.captured_range()?.scope_id != capture.source_scope_id
            || reference.row_ordinal != 0
            || row.capture != capture.capture
            || reference.capture_ref() != *capture
            || reference.validate_canonical_record(source, &row.record).is_err()
            || ObservationBounds::new(observed_until, &execution.observed_from).is_none()
        {
            return Err(invalid());
        }
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context: focus.original_context(original_source)?,
            current_work: work.as_ref().clone(),
            prerequisite_observed_until: observed_until.clone(),
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            observation: (*reference).clone(),
            observed_at: point.observed_at.clone(),
            checked_until: point.checked_until.clone(),
            binding: point.binding,
            membership: point.membership,
            source_state: snapshot.coverage_state()?,
            evidence: CurrentExecutionProfileEvidence::from_point(point)?,
            profiles: point.profiles.availability(),
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        self.basis.is_valid()
            && self.basis.stream == TelemetryStream::ActivityObservations
            && self.current_work.is_valid()
            && self.current_work.capture != self.basis.capture
            && matches!(&self.current_work.native_key,NativeRecordIdentity::Operational{source,..} if source==crate::source_id::MSSQL_CURRENT_REQUEST_WORK)
            && self.result.is_valid()
            && self.result.capture != self.basis.capture
            && self.result.capture != self.current_work.capture
            && self.observation.is_valid()
            && self.observation.capture_ref() == self.result
            && matches!(&self.observation.native_key,NativeRecordIdentity::Operational{source,..} if source==crate::source_id::CURRENT_EXECUTION_PROFILE)
            && SourceReadExecutionScope::from_context(SourceReadQuestion::CurrentExecutionProfile, &self.result_context)
                .is_ok_and(|scope| scope.matches_original_context(&self.basis_context))
            && self.result_context.target.engine() == crate::Engine::Mssql
            && ObservationBounds::new(&self.basis_context.observed_until, &self.prerequisite_observed_until).is_some()
            && ObservationBounds::new(&self.prerequisite_observed_until, &self.result_context.observed_from).is_some()
            && ObservationBounds::new(&self.observed_at, &self.checked_until).is_some()
            && ObservationBounds::new(&self.result_context.observed_from, &self.result_context.observed_until)
                .is_some_and(|bounds| bounds.contains(&self.observed_at) && bounds.contains(&self.checked_until))
            && matches!(self.source_state, crate::SourceState::Collected | crate::SourceState::Partial)
            && self.observation.row_ordinal == 0
            && self.evidence.is_valid()
            && self.evidence.profile_rows as usize <= MAX_EXECUTION_PROFILE_ROWS
            && self.evidence.nodes.len() <= MAX_PRESENTED_EXECUTION_NODES
            && self.evidence.profile_nodes <= self.evidence.profile_rows
            && match self.profiles {
                ExecutionProfileAvailability::Observed => self.evidence.profile_rows > 0,
                _ => self.evidence.profile_rows == 0,
            }
            && self.evidence.nodes.len() <= self.evidence.profile_nodes as usize
            && (self.evidence.profile_rows == 0
                || self.membership == FocusedRequestMembership::Observed
                    && self.binding == FocusedRequestBinding::Matched)
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        [&self.basis, &self.current_work, &self.observation].into_iter()
    }
    pub(super) fn matches_prerequisite(&self, prerequisite: &SourceReadPrerequisite) -> bool {
        matches!(prerequisite,SourceReadPrerequisite::ObservedCurrentExecutionRequest{observation,request,observed_until,..}
            if observation.as_ref()==&self.current_work && request.parent==self.basis && observed_until==&self.prerequisite_observed_until)
    }
}
impl SourceReadFocus {
    pub fn execution_profile_observed_until(&self) -> Option<&str> {
        match (&self.prerequisite, self.question) {
            (
                SourceReadPrerequisite::ObservedCurrentExecutionRequest { observed_until, .. },
                SourceReadQuestion::CurrentExecutionProfile,
            ) => Some(observed_until),
            _ => None,
        }
    }
    pub fn execution_profile_request(&self) -> Option<&ActivityRequestFocus> {
        match (&self.prerequisite, self.question) {
            (
                SourceReadPrerequisite::ObservedCurrentExecutionRequest { request, .. },
                SourceReadQuestion::CurrentExecutionProfile,
            ) => Some(request),
            _ => None,
        }
    }
    pub(super) fn from_current_execution_work(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.lifecycle == DiagnosticLifecycle::Resolved
            || episode.source_reads.get(SourceReadQuestion::CurrentExecutionProfile).is_some()
            || !matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
            || !matches!(&episode.report,Some(DiagnosticReport::CurrentRequestWork{observation}) if observation==reference)
        {
            return Ok(None);
        }
        let Some(point) = super::relational_input::current_plan_point(source, &row.record) else { return Ok(None) };
        // Independent LongRunning adversity +the exact complete CurrentWork binding justify one shared
        // serial-or-parallel execution question. A native exchange wait is context, not the admission gate.
        let exchange_wait = point
            .focus
            .original
            .wait
            .as_ref()
            .and_then(|wait| wait.native_type.as_deref())
            .and_then(MssqlParallelExchangeWait::from_native);
        if !episode.matches_current_request_work_focus(point.focus)
            || reference.validate_canonical_record(source, &row.record).is_err()
            || reference.capture != row.capture
            || reference.batch_id != row.batch_id
            || row.payload_sha256()? != reference.payload_sha256
            || !current_request_origin_is_eligible(point.context.origin)
        {
            return Err(invalid());
        }
        let observed_until = source.observation.snapshot().ok_or_else(invalid)?.observed_until.clone();
        let focus = Self {
            basis: Box::new(point.focus.parent.clone()),
            target: episode.scope.target.clone(),
            question: SourceReadQuestion::CurrentExecutionProfile,
            prerequisite: SourceReadPrerequisite::ObservedCurrentExecutionRequest {
                observation: Box::new(reference.clone()),
                request: Box::new(point.focus.clone()),
                observed_until,
                exchange_wait,
            },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(Some(focus))
    }
    pub(super) fn matches_execution_profile_input<'a>(
        &self,
        original_source: &CollectionDescription,
        original_row: &ExportedRecord,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = (&'a DiagnosticEvidenceRef, &'a OperationalObservation)>,
    ) -> bool {
        let SourceReadPrerequisite::ObservedCurrentExecutionRequest { observation, request, observed_until, .. } =
            &self.prerequisite
        else {
            return false;
        };
        let TelemetryRecord::Activity(original) = &original_row.record else { return false };
        let mut rows = rows.into_iter();
        let Some((reference, row)) = rows.next() else { return false };
        if rows.next().is_some() || reference != observation.as_ref() {
            return false;
        }
        let record = TelemetryRecord::Operational(row.clone());
        let Some(point) = super::relational_input::current_plan_point(source, &record) else { return false };
        point.focus == request.as_ref()
            && current_request_origin_is_eligible(point.context.origin)
            && point.focus.matches_parent(original_source, original, &self.basis)
            && source.observation.snapshot().is_some_and(|snapshot| &snapshot.observed_until == observed_until)
    }
}
impl DiagnosticEpisode {
    pub fn complete_current_execution_profile_read(
        &self,
        intent: &DiagnosticInvestigationIntent,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        if !intent.matches_episode(self) || !intent.matches_result_source(source) || !self.needs_question(&intent.focus)
        {
            return Err(invalid());
        }
        let DiagnosticFocus::SourceRead { focus } = &intent.focus else { return Err(invalid()) };
        let answer =
            CurrentExecutionProfileAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::CurrentExecutionProfile(Box::new(answer))),
            },
        )
    }
}

/// The original user-request contract admits selected operator observation; an unknown cache/module
/// origin grants no parameter/type proof. Explicit internal and Culprit work remain outside this question.
fn current_request_origin_is_eligible(origin: StatementOrigin) -> bool {
    matches!(origin, StatementOrigin::Application | StatementOrigin::Unknown)
}
