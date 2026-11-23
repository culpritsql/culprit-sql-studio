//! One live acquisition can retain an identical input context for two independent interpretations.
//! Canonical question order remains a storage rule, never an excuse to reacquire transient native evidence.
use super::*;

impl DiagnosticEpisode {
    /// The recorded sibling state stays intact. Only unadmitted identical retained-input work can be reused.
    /// The returned original reference is evidence to authenticate, never native admission or a retained result.
    pub fn source_read_context_reuse(&self, focus: &SourceReadFocus) -> Option<&DiagnosticEvidenceRef> {
        if !self.is_valid() {
            return None;
        }
        let retained = self.source_reads.get(SourceReadQuestion::RelationalInputContext)?;
        if retained.focus != *focus || !retained.needs_admission() {
            return None;
        }
        let SourceReadPrerequisite::ObservedCurrentRequestPlan { observation, context, .. } = &focus.prerequisite
        else {
            return None;
        };
        let profile = self.source_reads.get(SourceReadQuestion::CurrentExecutionProfile)?;
        let SourceReadStageState::Answered { answer } = &profile.state else { return None };
        let SourceReadAnswer::CurrentExecutionProfile(answer) = answer.as_ref() else { return None };
        if answer.basis != *focus.basis
            || &answer.current_work != observation.as_ref()
            || !answer.evidence.has_relational_input_context()
        {
            return None;
        }
        let inputs = answer.evidence.relational_inputs.as_deref()?;
        (inputs.context.as_ref() == context.as_ref()).then_some(&answer.observation)
    }

    /// Prefer transient current evidence when its actual selected route passes the existing admission owner.
    /// Unsupported/unqualified routes still fall through; this ordering grants no native work or new budget.
    pub fn source_read_admission_order(&self) -> Vec<&SourceReadStage> {
        let mut stages = self
            .source_reads
            .iter()
            .filter(|stage| stage.needs_admission() && self.owes_source_read(stage))
            .collect::<Vec<_>>();
        let retained =
            stages.iter().position(|stage| stage.focus.question == SourceReadQuestion::RelationalInputContext);
        let profile =
            stages.iter().position(|stage| stage.focus.question == SourceReadQuestion::CurrentExecutionProfile);
        if let (Some(retained), Some(profile)) = (retained, profile)
            && profile > retained
        {
            let selected = stages.remove(profile);
            stages.insert(retained, selected);
        }
        stages
    }
}
