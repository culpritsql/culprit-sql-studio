//! Compare qualified retained configuration reads through the existing history owner, never a timestamp detector.
use super::*;
use crate::collected_evidence::catalogs;
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::{BTreeMap, BTreeSet};

type Configurations = BTreeMap<Vec<u8>, Setting>;
pub(super) struct SettingsComparison<'a> {
    previous: &'a dyn OriginalProjection,
    current: &'a dyn OriginalProjection,
    before: Configurations,
    after: Configurations,
    evaluation: DerivedChangeEvaluation,
}
pub(super) struct ComparisonPlan<'a> {
    comparison: Option<SettingsComparison<'a>>,
    pub issues: Vec<DerivedContextIssue>,
}
impl<'a> ComparisonPlan<'a> {
    pub fn select(primary: &dyn OriginalProjection, originals: &[&'a dyn OriginalProjection]) -> Result<Self> {
        let mut plan = Self { comparison: None, issues: vec![] };
        let as_of = time(primary.collected_from())?;
        let mut candidates = Vec::new();
        for (index, &original) in originals.iter().enumerate() {
            if catalogs::selection::dataset(original) != Some(SnapshotDataset::Settings) {
                continue;
            }
            if !catalogs::selection::same_scope(primary, original) {
                plan.issue(DerivedContextIssue::UnqualifiedCatalogScope);
                continue;
            }
            let until = time(original.collected_until())?;
            if original.capture() != primary.capture() && until > as_of {
                plan.issue(DerivedContextIssue::CatalogNotRetainedAsOf);
                continue;
            }
            candidates.push((until, index, original));
        }
        candidates.sort_by_key(|candidate| candidate.0);
        let count = candidates.len();
        if count < 2 {
            return Ok(plan);
        }
        let (before_at, previous_input, previous) = &candidates[count - 2];
        let (after_at, current_input, current) = &candidates[count - 1];
        if before_at == after_at || count > 2 && candidates[count - 3].0 == *before_at {
            plan.issue(DerivedContextIssue::AmbiguousCatalog);
            return Ok(plan);
        }
        if previous.source().source != current.source().source
            || previous.source().partition != current.source().partition
        {
            plan.issue(DerivedContextIssue::UnqualifiedCatalogScope);
            return Ok(plan);
        }
        if time(previous.collected_until())? > time(current.collected_from())? || !native_ordered(*previous, *current)?
        {
            plan.issue(DerivedContextIssue::UnknownCatalogClock);
            return Ok(plan);
        }
        let (before, after) =
            match configurations(*previous).and_then(|before| configurations(*current).map(|after| (before, after))) {
                Ok(pair) => pair,
                Err(issue) => {
                    plan.issue(issue);
                    return Ok(plan);
                }
            };
        if before.is_empty() || before.keys().ne(after.keys()) {
            plan.issue(DerivedContextIssue::PartialCatalog);
            return Ok(plan);
        }
        let evaluation = DerivedChangeEvaluation {
            scope: DerivedChangeScope::Settings,
            previous_input: (*previous_input).try_into().map_err(|_| capacity())?,
            current_input: (*current_input).try_into().map_err(|_| capacity())?,
            // A value was read somewhere within each capture. Use the earliest prior acquisition bound
            // and latest current bound, not a native DDL time or a falsely exact change instant.
            observed_after: canonical_time(previous.collected_from())?,
            observed_until: canonical_time(current.collected_until())?,
        };
        plan.comparison =
            Some(SettingsComparison { previous: *previous, current: *current, before, after, evaluation });
        Ok(plan)
    }
    pub fn evaluation(&self) -> Option<DerivedChangeEvaluation> {
        self.comparison.as_ref().map(|comparison| comparison.evaluation.clone())
    }
    pub fn project(
        &self,
        description: &DerivedDiagnosisDescription,
        display: &DerivedDisplayContext,
        pipeline: &culprit_checks::Pipeline,
        budget: &mut OutputBudget,
    ) -> Result<Vec<DerivedChange>> {
        let Some(comparison) = &self.comparison else { return Ok(vec![]) };
        let previous = crate::collected_evidence::settings_snapshot(
            comparison.previous,
            comparison.before.values().cloned().collect(),
            &comparison.evaluation.observed_after,
            pipeline,
        )?;
        let current = crate::collected_evidence::settings_snapshot(
            comparison.current,
            comparison.after.values().cloned().collect(),
            &comparison.evaluation.observed_until,
            pipeline,
        )?;
        let database = current.identity.as_ref().map(IncidentDatabaseSelector::from_route).transpose()?;
        let mut output = Vec::new();
        for change in crate::history::diff(Some(&previous), &current, vec![]) {
            let inputs = references(comparison, change.object.as_deref())?;
            let mut row = DerivedChange {
                analysis_id: description.analysis_id.clone(),
                database: database.clone(),
                display: display.clone(),
                change,
                inputs,
            };
            row.redact(current.server.engine);
            budget.admit(&row)?;
            output.push(row);
        }
        Ok(output)
    }
    fn issue(&mut self, issue: DerivedContextIssue) {
        if !self.issues.contains(&issue) {
            self.issues.push(issue);
        }
    }
}
fn time(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid())
}
fn native_ordered(previous: &dyn OriginalProjection, current: &dyn OriginalProjection) -> Result<bool> {
    let before = previous.source().observation.snapshot().ok_or_else(invalid)?;
    let after = current.source().observation.snapshot().ok_or_else(invalid)?;
    Ok(time(&before.observed_until)? <= time(&after.observed_from)?)
}
fn configurations(original: &dyn OriginalProjection) -> std::result::Result<Configurations, DerivedContextIssue> {
    if !catalogs::complete(original) {
        return Err(DerivedContextIssue::PartialCatalog);
    }
    let mut settings = BTreeMap::new();
    let mut names = BTreeSet::new();
    for row in original.records() {
        let TelemetryRecord::Setting(value) = &row.row().record else {
            return Err(DerivedContextIssue::PartialCatalog);
        };
        match value.nature {
            SettingNature::State | SettingNature::Measurement => continue,
            SettingNature::Unknown => return Err(DerivedContextIssue::PartialCatalog),
            SettingNature::Configuration => {}
        }
        let setting = catalogs::settings::project(original, value)?;
        let key = serde_json::to_vec(&(&value.source, &value.namespace, &value.name, value.scope))
            .map_err(|_| DerivedContextIssue::PartialCatalog)?;
        if settings.insert(key, setting.clone()).is_some() || !names.insert(setting.name.to_ascii_lowercase()) {
            return Err(DerivedContextIssue::AmbiguousCatalog);
        }
    }
    Ok(settings)
}
fn references(comparison: &SettingsComparison<'_>, name: Option<&str>) -> Result<Vec<DiagnosticEvidenceRef>> {
    // Folded many-setting events use the two authenticated capture-wide inputs, rather than an arbitrary row.
    let Some(name) = name else { return Ok(vec![]) };
    let mut references = Vec::new();
    for original in [comparison.previous, comparison.current] {
        for (position, row) in original.records().iter().enumerate() {
            let TelemetryRecord::Setting(setting) = &row.row().record else { continue };
            if setting.nature == SettingNature::Configuration
                && catalogs::settings::project(original, setting).is_ok_and(|setting| setting.name == name)
            {
                references.push(original.reference(position)?);
            }
        }
    }
    Ok(references)
}
