//! The existing canonical worker authenticates inputs; this binds every emitted row reference to those inputs.
use super::*;
use std::collections::BTreeSet;

impl SynthesisInput {
    pub(crate) fn validate_diagnosis(&self, diagnosis: &DerivedDiagnosis, analyzer: &ComponentVersion) -> Result<()> {
        diagnosis.validate()?;
        let description = &diagnosis.description;
        if description.analyzer != *analyzer
            || description.recorded_at != self.recorded_at
            || self.derived_context_issues().iter().any(|issue| !description.context_issues.contains(issue))
            || !description.input_read_complete
            || description.inputs.len() != self.captures().count()
            || description.inputs.iter().any(|input| {
                !self.captures().any(|capture| capture.original() == &input.capture)
                    || input.catalog.as_ref().is_some_and(|catalog| {
                        self.captures().find(|capture| capture.original() == &input.capture).is_none_or(|capture| {
                            catalog.observed_from != capture.collected_from()
                                || catalog.observed_until != capture.collected_until()
                        })
                    })
            })
        {
            return Err(DeliveryProblem::Conflict.error());
        }
        let mut authenticated_rows = BTreeSet::new();
        for capture in self.captures() {
            for reference in capture.row_references() {
                authenticated_rows.insert(reference?.key()?);
            }
        }
        let selected = |reference: &DiagnosticEvidenceRef| -> Result<()> {
            if !authenticated_rows.contains(&reference.key()?) {
                return Err(DeliveryProblem::Integrity.error());
            }
            Ok(())
        };
        for incident in &diagnosis.incidents {
            selected(&incident.revision_reference)?;
            for query in
                incident.affected_queries.iter().chain(&incident.culprit_queries).chain(&incident.candidate_queries)
            {
                for reference in &query.evidence {
                    selected(reference)?;
                }
                if let Some(caller) = &query.caller {
                    selected(&caller.evidence)?;
                }
            }
            if let Some(caller) = &incident.caller {
                selected(&caller.evidence)?;
            }
        }
        for finding in &diagnosis.findings {
            for reference in finding.inputs.iter().chain(finding.cause.iter().flat_map(|cause| &cause.evidence)) {
                selected(reference)?;
            }
            if let Some(query) = &finding.query {
                selected(&query.reference)?;
            }
        }
        for change in &diagnosis.changes {
            for reference in &change.inputs {
                selected(reference)?;
            }
        }
        Ok(())
    }
}
