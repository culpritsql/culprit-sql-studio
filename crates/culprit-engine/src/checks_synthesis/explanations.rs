//! Existing native interpreters own qualification and wording; this adapter only selects their exact original.
use super::incidents::resolve;
use super::*;
use crate::{LocalCaptureAnalysisKind as Kind, LocalSnapshotReport};
use culprit_evidence::original_projection::OriginalProjection;
#[derive(Default)]
pub(super) struct Explanation {
    pub victim_summary: Option<String>,
    pub mechanism: Option<String>,
    pub action: Option<String>,
    pub verify: Option<String>,
    pub affected: Vec<DerivedIncidentQuery>,
    pub candidates: Vec<DerivedIncidentQuery>,
    pub culprits: Vec<DerivedIncidentQuery>,
    pub limitations: Vec<String>,
    pub context_evidence: Vec<IncidentHistoryEvidence>,
}
impl Explanation {
    fn merge_followup(&mut self, followup: Self) {
        append_paragraph(&mut self.victim_summary, followup.victim_summary);
        append_paragraph(&mut self.mechanism, followup.mechanism);
        append_paragraph(&mut self.action, followup.action);
        append_paragraph(&mut self.verify, followup.verify);
        self.affected.extend(followup.affected);
        self.candidates.extend(followup.candidates);
        self.culprits.extend(followup.culprits);
        self.limitations.extend(followup.limitations);
        self.context_evidence.extend(followup.context_evidence);
    }

    pub fn attribution_status(&self) -> DerivedAttributionStatus {
        if !self.culprits.is_empty() {
            DerivedAttributionStatus::Established
        } else if !self.candidates.is_empty() {
            DerivedAttributionStatus::CandidatesOnly
        } else {
            DerivedAttributionStatus::Unestablished
        }
    }
    fn words(explanation: &str, action: &str, verify: &str) -> Self {
        Self {
            victim_summary: None,
            mechanism: Some(explanation.into()),
            action: Some(action.into()),
            verify: Some(verify.into()),
            affected: vec![],
            candidates: vec![],
            culprits: vec![],
            limitations: vec![],
            context_evidence: vec![],
        }
    }
}

fn append_paragraph(existing: &mut Option<String>, additional: Option<String>) {
    let Some(additional) = additional else { return };
    match existing {
        Some(existing) => {
            existing.push_str("\n\n");
            existing.push_str(&additional);
        }
        None => *existing = Some(additional),
    }
}
mod commit_pipeline;
mod source_answers;

pub(super) fn for_episode(originals: &[&dyn OriginalProjection], episode: &DiagnosticEpisode) -> Result<Explanation> {
    let mut explanation = from_report(originals, episode)?;
    source_answers::append(originals, episode, &mut explanation)?;
    commit_pipeline::append(originals, episode, &mut explanation)?;
    explanation.candidates = super::incidents::distinct(explanation.candidates)?;
    Ok(explanation)
}

fn from_report(originals: &[&dyn OriginalProjection], episode: &DiagnosticEpisode) -> Result<Explanation> {
    let Some(report) = &episode.report else { return Ok(Explanation::default()) };
    let reference = match report {
        DiagnosticReport::OriginalObservation { observation, .. }
        | DiagnosticReport::Blocking { observation, .. }
        | DiagnosticReport::PageWait { observation, .. }
        | DiagnosticReport::PageWaitWriters { observation, .. }
        | DiagnosticReport::PageTaskHolders { observation, .. }
        | DiagnosticReport::RequestMemory { observation, .. }
        | DiagnosticReport::CurrentRequestWork { observation } => observation,
        DiagnosticReport::CompilationAdmission { report } => &report.observation,
    };
    let Some((original, position)) = resolve(originals, reference)? else { return Ok(Explanation::default()) };
    if !report.matches_observation(episode, original.source(), original.records()[position].row()) {
        return Err(invalid());
    }
    match report {
        DiagnosticReport::Blocking { .. } => blocking(original),
        DiagnosticReport::RequestMemory { .. } => request_memory(original, position),
        DiagnosticReport::CurrentRequestWork { .. } => current_work(original, position),
        DiagnosticReport::CompilationAdmission { .. } => compilation(original, position),
        DiagnosticReport::PageWait { .. } => page(original, position),
        DiagnosticReport::PageWaitWriters { .. } => writers(original, position),
        DiagnosticReport::PageTaskHolders { .. } => holders(original, position),
        DiagnosticReport::OriginalObservation { .. } => original_observation(original, position),
    }
}
fn native_rows(original: &dyn OriginalProjection) -> Result<CapturedOperationalRead> {
    original.operational_read()
}
fn source(read: &CapturedOperationalRead, name: &str) -> Result<SourceOutcome> {
    let coverage = read.source_coverage(name).ok_or_else(invalid)?;
    let rows = read.observations.iter().filter(|row| row.source == name).count();
    crate::collected_resources::read_outcome(coverage, rows, read.read_complete)
}
pub(super) fn request_memory(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let read = native_rows(original)?;
    let TelemetryRecord::Operational(point) = &original.records()[position].row().record else { return Err(invalid()) };
    let LocalSnapshotReport::RequestMemory(report) =
        crate::local_captures::explain_operational_report(Kind::RequestMemory, &read)?
    else {
        return Err(invalid());
    };
    let mut explanation = report
        .entries
        .iter()
        .find(|entry| entry.observation_key == point.key)
        .map(|entry| Explanation::words(&entry.explanation, &entry.investigation, &entry.verify))
        .unwrap_or_default();
    explanation.limitations = report.limitations.clone();
    let peers = report
        .admission_resources
        .iter()
        .filter(|resource| resource.participants.iter().any(|entry| entry.observation_key == point.key))
        .flat_map(|resource| &resource.participants)
        .filter(|entry| entry.observation_key != point.key)
        .collect::<Vec<_>>();
    for (candidate_position, row) in original.records().iter().enumerate() {
        let TelemetryRecord::Operational(candidate) = &row.row().record else { continue };
        let OperationalFact::ExecutionGrant(grant) = &candidate.fact else { continue };
        if grant.state != ExecutionGrantState::Granted
            || !peers.iter().any(|peer| peer.observation_key == candidate.key)
        {
            continue;
        }
        let Some(entry) = report.entries.iter().find(|entry| entry.observation_key == candidate.key) else { continue };
        let Some(request) =
            grant.request.as_ref().filter(|request| entry.qualified_request.as_ref() == Some(&request.key))
        else {
            continue;
        };
        let evidence = original.reference(candidate_position)?;
        if let Some(contributor) = super::causal_actors::reservation(original, candidate_position, entry, request)? {
            explanation.culprits.push(contributor);
        }
        explanation.candidates.push(queries::from_qualified_request(request, evidence));
    }
    explanation.candidates = super::incidents::distinct(explanation.candidates)?;
    explanation.culprits = super::incidents::distinct(explanation.culprits)?;
    Ok(explanation)
}
fn current_work(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let read = native_rows(original)?;
    let TelemetryRecord::Operational(point) = &original.records()[position].row().record else { return Err(invalid()) };
    let LocalSnapshotReport::CurrentRequestWork(report) =
        crate::local_captures::explain_operational_report(Kind::CurrentRequestWork, &read)?
    else {
        return Err(invalid());
    };
    let mut explanation = report
        .entries
        .iter()
        .find(|entry| entry.observation_key == point.key)
        .map(|entry| Explanation::words(&entry.explanation, &entry.investigation, &entry.verify))
        .unwrap_or_default();
    explanation.limitations = report.limitations;
    Ok(explanation)
}
fn compilation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let read = native_rows(original)?;
    let point = match &original.records()[position].row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let context = read.context()?;
    let outcome = source(&read, &point.source)?;
    let report = culprit_checks::compiler_admission::explain(
        &CompilerAdmissionSnapshot {
            source: point.source.clone(),
            context: Some(context),
            observations: read.observations.clone(),
        },
        Some(&outcome),
    )?;
    let mut explanation = report
        .waiters
        .iter()
        .find(|entry| entry.observation_key == point.key)
        .map(|entry| Explanation::words(&entry.explanation, &entry.investigation, &entry.verify))
        .unwrap_or_default();
    explanation.limitations = report.limitations;
    Ok(explanation)
}
fn page(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let read = native_rows(original)?;
    let point = match &original.records()[position].row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let rows = read
        .observations
        .iter()
        .map(|row| match &row.fact {
            OperationalFact::PageWait(page) => Ok(page.as_ref()),
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let outcome = source(&read, &point.source)?;
    let report = culprit_checks::page_waits::explain(&rows, Some(&outcome), None)?;
    let mut explanation = report
        .entries
        .iter()
        .find(|entry| entry.observation_key == point.key)
        .map(|entry| Explanation::words(&entry.explanation, &entry.investigation, &entry.verify))
        .unwrap_or_default();
    explanation.limitations = report.limitations;
    Ok(explanation)
}
fn writers(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let SourceObservation::PageWaitWriters { focus, .. } = &original.source().observation else {
        return Err(invalid());
    };
    let read = native_rows(original)?;
    let context = read.context()?;
    let point = match &original.records()[position].row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let OperationalFact::PageWaitWriters(observation) = &point.fact else { return Err(invalid()) };
    let outcome = source(&read, &point.source)?;
    let report = culprit_checks::page_waits::explain_writers(focus, &context, Some(observation), Some(&outcome))?;
    let mut explanation = Explanation {
        mechanism: Some(report.summary.clone()),
        limitations: report.limitations.clone(),
        ..Default::default()
    };
    for entry in &report.entries {
        if let Some(sql) = &entry.sql {
            let request = observation
                .candidates
                .iter()
                .find(|candidate| candidate.parent == entry.parent)
                .and_then(|candidate| candidate.request.as_ref())
                .filter(|request| {
                    request.key == entry.request && request.statement_identity.as_ref() == Some(&entry.statement)
                })
                .ok_or_else(invalid)?;
            let mut candidate = queries::from_qualified_request(request, original.reference(position)?);
            candidate.sql = sql.clone();
            explanation.candidates.push(candidate);
        }
    }
    explanation.candidates = super::incidents::distinct(explanation.candidates)?;
    if report.entries.len() == 1 {
        let entry = &report.entries[0];
        explanation.action = Some(entry.investigation.clone());
        explanation.verify = Some(entry.verify.clone());
    }
    Ok(explanation)
}
pub(super) fn holders(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let SourceObservation::PageTaskHolders { focus, .. } = &original.source().observation else {
        return Err(invalid());
    };
    let read = native_rows(original)?;
    let context = read.context()?;
    let point = match &original.records()[position].row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let OperationalFact::PageTaskHolders(observation) = &point.fact else { return Err(invalid()) };
    let outcome = source(&read, &point.source)?;
    let report = culprit_checks::page_waits::explain_task_holders(focus, &context, Some(observation), Some(&outcome))?;
    let mut explanation = Explanation {
        mechanism: Some(report.summary.clone()),
        limitations: report.limitations.clone(),
        ..Default::default()
    };
    for entry in &report.entries {
        if let Some(holder) = super::causal_actors::holder(original, position, &report, entry)? {
            explanation.culprits.push(holder);
        }
        if let (Some(actor), Some(statement), Some(sql)) = (&entry.request, &entry.statement_identity, &entry.statement)
        {
            let point = match entry.bound {
                culprit_core::page_wait_report::PageTaskHolderBound::Before => &observation.before,
                culprit_core::page_wait_report::PageTaskHolderBound::After => &observation.after,
            };
            let request = point
                .holder
                .request
                .as_ref()
                .filter(|request| &request.key == actor && request.statement_identity.as_ref() == Some(statement))
                .ok_or_else(invalid)?;
            let mut candidate = queries::from_qualified_request(request, original.reference(position)?);
            candidate.sql = sql.clone();
            explanation.candidates.push(candidate);
        }
    }
    explanation.candidates = super::incidents::distinct(explanation.candidates)?;
    explanation.culprits = super::incidents::distinct(explanation.culprits)?;
    Ok(explanation)
}
pub(super) fn blocking(original: &dyn OriginalProjection) -> Result<Explanation> {
    let rows = original
        .records()
        .iter()
        .filter_map(|row| match &row.row().record {
            TelemetryRecord::Activity(point) => Some(ActivityFrameRow { observation: point, reference: None }),
            _ => None,
        })
        .collect::<Vec<_>>();
    let report = culprit_checks::focused_blocking::explain(original.source(), &rows, original.is_read_complete())?;
    let actors = super::causal_actors::blocking(original, &report)?;
    let mut explanation = Explanation::words(&report.summary, &report.investigation, &report.verify);
    if let SourceObservation::FocusedBlocking { result, .. } = &original.source().observation
        && let Some(case) = report
            .metadata_queue
            .evidence
            .cascades
            .iter()
            .find(|case| case.affected.iter().any(|affected| affected.actor == result.focus.original.key))
        && let Some(words) = report.metadata_queue.explanations.iter().find(|words| words.key == case.key)
    {
        explanation.victim_summary = Some(words.victim_summary.clone());
        explanation.mechanism = Some(words.mechanism.clone());
        explanation.action = Some(words.investigation.clone());
        explanation.verify = Some(words.verify.clone());
    }
    if let SourceObservation::FocusedBlocking { result, .. } = &original.source().observation
        && let Some(case) =
            report.output_retention.evidence.cases.iter().find(|case| case.waiter == result.focus.original.key)
        && let Some(words) = report.output_retention.explanations.iter().find(|words| words.key == case.key)
    {
        explanation.victim_summary = Some(words.victim_summary.clone());
        explanation.mechanism = Some(words.mechanism.clone());
        explanation.action = Some(words.investigation.clone());
        explanation.verify = Some(words.verify.clone());
    }
    explanation.culprits = actors.culprits;
    explanation.candidates = actors.candidates;
    explanation.limitations = report.limitations;
    explanation.limitations.extend(report.metadata_queue.limitations);
    explanation.limitations.extend(report.output_retention.limitations);
    Ok(explanation)
}
pub(super) fn original_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let Some(header) = original.source().observation.snapshot() else { return Ok(Explanation::default()) };
    if header.dataset != SnapshotDataset::TaskSpace {
        return Ok(Explanation::default());
    }
    let report = task_space_report(original)?;
    let point = match &original.records()[position].row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let Some(entry) = report.entries.iter().find(|entry| entry.observation_key == point.key) else {
        return Ok(Explanation::default());
    };
    let mut explanation = Explanation::words(&entry.explanation, &entry.investigation, &entry.verify);
    explanation.limitations = report.limitations.clone();
    if let Some(request) = task_space_request(original, position, entry)? {
        explanation.candidates.push(request);
    }
    Ok(explanation)
}

fn task_space_report(original: &dyn OriginalProjection) -> Result<Box<TaskSpaceReport>> {
    let read = native_rows(original)?;
    let LocalSnapshotReport::TaskSpace(report) =
        crate::local_captures::explain_operational_report(Kind::TaskSpace, &read)?
    else {
        return Err(invalid());
    };
    Ok(report)
}

fn task_space_request(
    original: &dyn OriginalProjection,
    position: usize,
    entry: &TaskSpaceExplanation,
) -> Result<Option<DerivedIncidentQuery>> {
    let Some(qualified) = &entry.current_request else { return Ok(None) };
    let Some(TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TaskSpace(task), .. })) =
        original.records().get(position).map(|row| &row.row().record)
    else {
        return Err(invalid());
    };
    let request = task.request.as_ref().filter(|request| request.key == qualified.request).ok_or_else(invalid)?;
    let mut candidate = queries::from_qualified_request(request, original.reference(position)?);
    candidate.sql = qualified.statement.clone();
    Ok(Some(candidate))
}

#[cfg(test)]
mod context_tests;
