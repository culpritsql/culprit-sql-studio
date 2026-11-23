//! One adverse request may ask about its independently observed current cached plan. The retained plan
//! never becomes that request's execution, runtime values or a harmful-reuse verdict.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum RelationalInputGap {
    AffectedExecutionPlan,
    RuntimeParameterValues,
    RuntimeInputCardinality,
    ComparableExecutionWork,
    BlockingAndGrantDemand,
    StatisticsAndForcingContext,
}
impl RelationalInputGap {
    pub const ALL: &'static [Self] = &[
        Self::AffectedExecutionPlan,
        Self::RuntimeParameterValues,
        Self::RuntimeInputCardinality,
        Self::ComparableExecutionWork,
        Self::BlockingAndGrantDemand,
        Self::StatisticsAndForcingContext,
    ];
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum RelationalInputSelection {
    Observed {
        observation: Box<DiagnosticEvidenceRef>,
        /// Positive native classifications, independently bound to the retained body and module declaration.
        input_proof: Option<Box<MssqlRelationalInputProof>>,
        retained_plan_available: bool,
    },
    NotObserved,
    IncarnationChanged,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RelationalInputAnswer {
    pub basis: DiagnosticEvidenceRef,
    pub basis_context: NativeReadContext,
    pub work: DiagnosticEvidenceRef,
    pub current_cache: CachedStatementContext,
    pub cache_observed_from: String,
    pub cache_observed_until: String,
    pub result: DiagnosticCaptureRef,
    pub result_context: NativeReadContext,
    pub source_state: crate::SourceState,
    pub selection: RelationalInputSelection,
    /// These concern the affected original execution, even when another retained execution has measurements.
    pub unresolved: Vec<RelationalInputGap>,
}
impl RelationalInputAnswer {
    /// Every row and all EOF/receipt bytes are authenticated by the existing canonical reader before this call.
    pub fn from_originals(
        focus: &SourceReadFocus,
        original_source: &CollectionDescription,
        original: &ExportedRecord,
        source: &CollectionDescription,
        capture: &DiagnosticCaptureRef,
        rows: &[(&DiagnosticEvidenceRef, &ExportedRecord)],
    ) -> Result<Self> {
        let SourceReadPrerequisite::ObservedCurrentRequestPlan {
            observation: work,
            context,
            observed_from,
            observed_until,
        } = &focus.prerequisite
        else {
            return Err(invalid());
        };
        let SourceObservation::SourceRead { snapshot, focus_key, execution } = &source.observation else {
            return Err(invalid());
        };
        let authority = SourceReadExecutionScope::from_context(focus.question, execution)?;
        let basis_context = focus.original_context(original_source)?;
        if focus.question != SourceReadQuestion::RelationalInputContext
            || !focus.matches_basis(original_source, original, &authority)
            || !source.partition_is_consistent()
            || !source.observation.covers_request()
            || source.target.engine() != crate::Engine::Mssql
            || focus.key()? != *focus_key
            || !capture.is_valid()
            || capture.capture == focus.basis.capture
            || capture.capture == work.capture
            || source.captured_range()?.scope_id != capture.source_scope_id
            || snapshot.dataset != SnapshotDataset::RetainedPlans
            || snapshot.records as usize != rows.len()
            || rows.len() > 1
            || ObservationBounds::new(observed_until, &execution.observed_from).is_none()
        {
            return Err(invalid());
        }
        let selection = match rows {
            [] => RelationalInputSelection::NotObserved,
            [(reference, row)] => {
                reference.validate_canonical_record(source, &row.record)?;
                let TelemetryRecord::ObjectDefinition(definition) = &row.record else { return Err(invalid()) };
                let ObjectDefinitionFact::MssqlRetainedPlan(retained) = &definition.fact else {
                    return Err(invalid());
                };
                if row.capture != capture.capture
                    || reference.capture_ref() != *capture
                    || definition.source != crate::source_id::RETAINED_PLANS
                    || !retained.native_plan_proofs_match(source)
                    || !retained.context.matches_locator(context)
                {
                    return Err(invalid());
                }
                if retained.context.identity.key()? == context.identity.key()?
                    && retained.context.query_hash == context.query_hash
                {
                    RelationalInputSelection::Observed {
                        observation: Box::new((*reference).clone()),
                        input_proof: retained.relational_input_proof.as_ref().map(|proof| Box::new(proof.clone())),
                        retained_plan_available: retained.batch_plan.payload.is_available(),
                    }
                } else {
                    RelationalInputSelection::IncarnationChanged
                }
            }
            _ => return Err(invalid()),
        };
        let answer = Self {
            basis: focus.basis.as_ref().clone(),
            basis_context,
            work: work.as_ref().clone(),
            current_cache: context.as_ref().clone(),
            cache_observed_from: observed_from.clone(),
            cache_observed_until: observed_until.clone(),
            result: capture.clone(),
            result_context: execution.as_ref().clone(),
            source_state: snapshot.coverage_state()?,
            selection,
            unresolved: RelationalInputGap::ALL.to_vec(),
        };
        if !answer.is_valid() {
            return Err(invalid());
        }
        Ok(answer)
    }
    pub fn is_valid(&self) -> bool {
        self.basis.is_valid()
            && self.basis.stream == TelemetryStream::ActivityObservations
            && self.work.is_valid()
            && matches!(&self.work.native_key, NativeRecordIdentity::Operational { source, .. }
                if source == crate::source_id::MSSQL_CURRENT_REQUEST_WORK)
            && self.work.capture != self.basis.capture
            && self.result.is_valid()
            && self.result.capture != self.basis.capture
            && self.result.capture != self.work.capture
            && self.current_cache.is_valid()
            && self.current_cache.identity.has_plan_handle()
            && self.current_cache.identity.has_sql_handle()
            && self.current_cache.origin == StatementOrigin::Application
            && SourceReadExecutionScope::from_context(SourceReadQuestion::RelationalInputContext, &self.result_context)
                .is_ok_and(|scope| scope.matches_original_context(&self.basis_context))
            && self.result_context.target.engine() == crate::Engine::Mssql
            && ObservationBounds::new(&self.basis_context.observed_until, &self.cache_observed_from).is_some()
            && ObservationBounds::new(&self.cache_observed_from, &self.cache_observed_until).is_some()
            && ObservationBounds::new(&self.cache_observed_until, &self.result_context.observed_from).is_some()
            && self.unresolved == RelationalInputGap::ALL
            && match &self.selection {
                RelationalInputSelection::Observed { observation, input_proof, retained_plan_available } => {
                    observation.is_valid()
                        && observation.capture_ref() == self.result
                        && matches!(&observation.native_key, NativeRecordIdentity::ObjectDefinition { .. })
                        && input_proof.as_ref().is_none_or(|proof| proof.is_valid() && *retained_plan_available)
                        && matches!(self.source_state, crate::SourceState::Collected | crate::SourceState::Partial)
                }
                RelationalInputSelection::NotObserved => true,
                RelationalInputSelection::IncarnationChanged => {
                    matches!(self.source_state, crate::SourceState::Collected | crate::SourceState::Partial)
                }
            }
    }
    pub(super) fn matches_prerequisite(&self, prerequisite: &SourceReadPrerequisite) -> bool {
        matches!(prerequisite, SourceReadPrerequisite::ObservedCurrentRequestPlan {
            observation, context, observed_from, observed_until,
        } if observation.as_ref() == &self.work && context.as_ref() == &self.current_cache
            && observed_from == &self.cache_observed_from && observed_until == &self.cache_observed_until)
    }
    pub fn references(&self) -> impl Iterator<Item = &DiagnosticEvidenceRef> {
        let selected = match &self.selection {
            RelationalInputSelection::Observed { observation, .. } => Some(observation.as_ref()),
            _ => None,
        };
        [&self.basis, &self.work].into_iter().chain(selected)
    }
}

impl SourceReadFocus {
    /// Only an already adverse original episode can acquire this context question. A TVP type, cached plan,
    /// estimate, or another query's current-work record cannot enroll an incident.
    pub(super) fn from_current_work(
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        if !episode.is_valid() {
            return Err(invalid());
        }
        if episode.source_reads.get(SourceReadQuestion::RelationalInputContext).is_some()
            || !matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
            || !matches!(&episode.report, Some(DiagnosticReport::CurrentRequestWork { observation }) if observation == reference)
        {
            return Ok(None);
        }
        let Some(point) = current_plan_point(source, &row.record) else { return Ok(None) };
        if !episode.matches_current_request_work_focus(point.focus)
            || reference.validate_canonical_record(source, &row.record).is_err()
            || reference.capture != row.capture
            || reference.batch_id != row.batch_id
            || row.payload_sha256()? != reference.payload_sha256
            || point.context.origin != StatementOrigin::Application
        {
            return Err(invalid());
        }
        let focus = Self {
            basis: Box::new(point.focus.parent.clone()),
            target: episode.scope.target.clone(),
            question: SourceReadQuestion::RelationalInputContext,
            prerequisite: SourceReadPrerequisite::ObservedCurrentRequestPlan {
                observation: Box::new(reference.clone()),
                context: Box::new(point.context.clone()),
                observed_from: point.from.into(),
                observed_until: point.until.into(),
            },
        };
        if !focus.is_valid() {
            return Err(invalid());
        }
        Ok(Some(focus))
    }
    pub(super) fn matches_current_plan_input<'a>(
        &self,
        original_source: &CollectionDescription,
        original_row: &ExportedRecord,
        source: &CollectionDescription,
        rows: impl IntoIterator<Item = (&'a DiagnosticEvidenceRef, &'a OperationalObservation)>,
    ) -> bool {
        let SourceReadPrerequisite::ObservedCurrentRequestPlan { observation, context, observed_from, observed_until } =
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
        let Some(point) = current_plan_point(source, &record) else { return false };
        point.context == context.as_ref()
            && point.from == observed_from
            && point.until == observed_until
            && point.focus.matches_parent(original_source, original, &self.basis)
            && self.matches_original(original_source, original_row)
            && reference.validate_canonical_record(source, &record).is_ok()
    }
}

pub(super) struct CurrentPlanPoint<'a> {
    pub focus: &'a ActivityRequestFocus,
    pub context: &'a CachedStatementContext,
    pub from: &'a str,
    pub until: &'a str,
}
pub(super) fn current_plan_point<'a>(
    source: &'a CollectionDescription,
    record: &'a TelemetryRecord,
) -> Option<CurrentPlanPoint<'a>> {
    let SourceObservation::CurrentRequestWork { profile: CurrentRequestWorkProfile::MssqlRequest, snapshot, result } =
        &source.observation
    else {
        return None;
    };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::CurrentRequestWork(point), ..
    }) = record
    else {
        return None;
    };
    let CurrentRequestWorkNative::Mssql { work } = &point.native else { return None };
    let MssqlCurrentRequestPlan::Observed { context, observed_from, observed_until, .. } = &work.current_plan else {
        return None;
    };
    if !point.fits_source_scope(source)
        || snapshot.records != 1
        || snapshot.coverage_state().ok()? != crate::SourceState::Collected
        || result.binding != FocusedRequestBinding::Matched
        || result.membership != FocusedRequestMembership::Observed
    {
        return None;
    }
    Some(CurrentPlanPoint { focus: result.focus.as_deref()?, context, from: observed_from, until: observed_until })
}

impl DiagnosticEpisode {
    pub fn complete_relational_input_read(
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
        let answer = RelationalInputAnswer::from_originals(focus, original_source, original, source, capture, rows)?;
        self.transition_source_read(
            focus,
            SourceReadStageState::Answered { answer: Box::new(SourceReadAnswer::RelationalInputs(Box::new(answer))) },
        )
    }
}

#[cfg(test)]
mod tests;
