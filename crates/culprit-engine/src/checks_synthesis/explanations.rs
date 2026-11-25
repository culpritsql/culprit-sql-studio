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
    let mut mechanisms = vec![report.summary.clone(), mapped_target_detail(&report.original_target)];
    for entry in report.entries.iter().take(8) {
        push_unique(&mut mechanisms, entry.explanation.clone());
    }
    let has_target_writer = report.entries.iter().any(|entry| entry.relation == PageWriterRelation::TargetObjectWrite);
    let mut actions = Vec::new();
    let mut verifies = Vec::new();
    for entry in report
        .entries
        .iter()
        .filter(|entry| !has_target_writer || entry.relation == PageWriterRelation::TargetObjectWrite)
        .take(8)
    {
        push_unique(&mut actions, entry.investigation.clone());
        push_unique(&mut verifies, entry.verify.clone());
    }
    let mut explanation = Explanation {
        mechanism: Some(mechanisms.join("\n\n")),
        action: (!actions.is_empty()).then(|| actions.join("\n\n")),
        verify: (!verifies.is_empty()).then(|| verifies.join("\n\n")),
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
    let mut mechanisms = vec![report.summary.clone(), mapped_target_detail(&focus.target)];
    for entry in &report.entries {
        push_unique(&mut mechanisms, entry.explanation.clone());
    }
    let is_qualified = |entry: &culprit_core::page_wait_report::PageTaskHolderExplanation| {
        entry.request_state == PageWaitRequestState::SameStatement
            && entry.wait_state == PageWaitRelation::OriginalObserved
            && entry.relation == PageTaskHolderRelation::QualifiedRequest
    };
    let has_qualified = report.entries.iter().any(&is_qualified);
    let mut actions = Vec::new();
    let mut verifies = Vec::new();
    for entry in report.entries.iter().filter(|entry| !has_qualified || is_qualified(entry)) {
        push_unique(&mut actions, entry.investigation.clone());
        push_unique(&mut verifies, entry.verify.clone());
    }
    let mut explanation = Explanation {
        mechanism: Some(mechanisms.join("\n\n")),
        action: (!actions.is_empty()).then(|| actions.join("\n\n")),
        verify: (!verifies.is_empty()).then(|| verifies.join("\n\n")),
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

fn mapped_target_detail(target: &MappedPageTarget) -> String {
    let kind = match target.kind {
        PageWaitTargetKind::IndexLeaf => key!("view.page_waits.kind.leaf"),
        PageWaitTargetKind::IndexBranch => key!("view.page_waits.kind.branch"),
        PageWaitTargetKind::HeapData => key!("view.page_waits.kind.heap"),
        PageWaitTargetKind::Pfs => key!("view.page_waits.kind.pfs"),
        PageWaitTargetKind::Gam => key!("view.page_waits.kind.gam"),
        PageWaitTargetKind::Sgam => key!("view.page_waits.kind.sgam"),
        PageWaitTargetKind::Iam => key!("view.page_waits.kind.iam"),
        PageWaitTargetKind::Unclassified => key!("view.page_waits.kind.unclassified"),
    };
    let kind_text = culprit_core::i18n::t(kind, &[]);
    match &target.object {
        Some(object) => {
            let object_text = match &object.index_name {
                Some(index) => t!(
                    "view.page_waits.target.index",
                    schema = object.schema.as_str(),
                    object = object.name.as_str(),
                    index = index.as_str()
                ),
                None => {
                    t!("view.page_waits.target.object", schema = object.schema.as_str(), object = object.name.as_str())
                }
            };
            format!("{kind_text} {object_text}")
        }
        None => kind_text,
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.contains(&value) {
        values.push(value);
    }
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
    let mut specialized = false;
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
        specialized = true;
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
        specialized = true;
    }
    if !specialized && let Some(blocking) = &report.blocking {
        enrich_blocking_root_cause(original, &report.context, &rows, blocking, &mut explanation);
    }
    explanation.culprits = actors.culprits;
    explanation.candidates = actors.candidates;
    explanation.limitations = report.limitations;
    explanation.limitations.extend(report.metadata_queue.limitations);
    explanation.limitations.extend(report.output_retention.limitations);
    Ok(explanation)
}

fn enrich_blocking_root_cause(
    original: &dyn OriginalProjection,
    context: &NativeReadContext,
    rows: &[ActivityFrameRow<'_>],
    blocking: &Blocking,
    explanation: &mut Explanation,
) {
    if !original.is_read_complete() || !context.has_qualified_local_owner() {
        return;
    }
    let chain = match &original.source().observation {
        SourceObservation::FocusedBlocking { snapshot, result } => {
            if snapshot.coverage.error_code == Some(culprit_core::ErrorCode::PermissionDenied)
                || result.binding != FocusedRequestBinding::Matched
                || result.membership != FocusedRequestMembership::Observed
            {
                return;
            }
            let victim = result.focus.original.key.session.id.as_str();
            blocking.chains.iter().find(|chain| chain_contains_session(&chain.tree, victim))
        }
        _ => blocking.chains.first(),
    };
    let Some(chain) = chain else { return };

    let head_obs = chain.tree.owner.as_ref().and_then(ActivityWaitOwner::session).and_then(|identity| {
        let mut matches = rows.iter().filter(|row| row.observation.key.session == identity);
        let first = matches.next()?;
        matches.next().is_none().then_some(first.observation)
    });
    let idle_with_open_tx = chain.head_kind == culprit_core::blocking::HeadKind::IdleInTransaction
        || head_obs.is_some_and(|obs| {
            let state = obs.state.to_ascii_lowercase();
            (obs.idle || state == "sleeping" || state.contains("idle in transaction"))
                && (obs.open_transactions.unwrap_or(0) > 0 || obs.transaction_age_ms.is_some())
        });

    if idle_with_open_tx {
        let tx_ms = chain
            .transaction_ms
            .or_else(|| head_obs.and_then(|obs| obs.transaction_age_ms.as_deref()?.parse::<f64>().ok()));
        let (has_age, duration) = match tx_ms.filter(|ms| ms.is_finite() && *ms >= 0.0) {
            Some(ms) => ("yes", format::duration(ms)),
            None => ("no", String::new()),
        };
        explanation.victim_summary = Some(t!(
            "view.blocking.synthesis.idle_transaction.victim",
            head = chain.head.as_str(),
            has_age = has_age,
            duration = duration.as_str()
        ));
        append_paragraph(
            &mut explanation.mechanism,
            Some(t!(
                "view.blocking.synthesis.idle_transaction.mechanism",
                head = chain.head.as_str(),
                n = chain.waiters,
                count = format::digits(chain.waiters.to_string())
            )),
        );
        append_paragraph(
            &mut explanation.action,
            Some(t!("view.blocking.synthesis.idle_transaction.action", head = chain.head.as_str())),
        );
    }

    let is_object_data_lock = original.source().target.engine() == Engine::Mssql
        && chain
            .lock
            .as_ref()
            .and_then(|lock| lock.resource.as_deref())
            .is_some_and(|res| res.eq_ignore_ascii_case("OBJECT"))
        && !chain.wait_type.as_deref().is_some_and(|w| w.starts_with("LCK_M_SCH_"));
    if is_object_data_lock {
        let object = chain.lock.as_ref().and_then(|lock| lock.object.as_deref()).unwrap_or_default();
        let has_object = if object.is_empty() { "no" } else { "yes" };
        append_paragraph(
            &mut explanation.mechanism,
            Some(t!(
                "view.blocking.synthesis.object_escalation.mechanism",
                head = chain.head.as_str(),
                has_object = has_object,
                object = object
            )),
        );
        append_paragraph(&mut explanation.action, Some(t!("view.blocking.synthesis.object_escalation.action")));
    }
}

fn chain_contains_session(node: &culprit_core::blocking::BlockNode, session_id: &str) -> bool {
    node.session == session_id || node.children.iter().any(|child| chain_contains_session(child, session_id))
}
pub(super) fn original_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let Some(header) = original.source().observation.snapshot() else { return Ok(Explanation::default()) };
    match header.dataset {
        SnapshotDataset::TaskSpace => task_space_observation(original, position),
        SnapshotDataset::Cleanup => cleanup_observation(original, position),
        SnapshotDataset::MemoryFailures => memory_failure_observation(original, position),
        SnapshotDataset::NativeFailures => native_failure_observation(original, position),
        _ => Ok(Explanation::default()),
    }
}

fn memory_failure_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let OperationalFact::MssqlOutOfMemory(failure) =
        &match &original.records().get(position).ok_or_else(invalid)?.row().record {
            TelemetryRecord::Operational(point) => point,
            _ => return Err(invalid()),
        }
        .fact
    else {
        return Err(invalid());
    };
    let report = crate::collected_resources::memory_failures_from_original(original)?;
    let Some(entry) = report.entries.iter().find(|entry| entry.position == failure.position) else {
        return Ok(Explanation::default());
    };
    let mut explanation = Explanation::words(&entry.explanation, &entry.investigation, &entry.verify);
    explanation.victim_summary = Some(entry.title.clone());
    explanation.limitations = report.limitations;
    Ok(explanation)
}

fn native_failure_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let point = match &original.records().get(position).ok_or_else(invalid)?.row().record {
        TelemetryRecord::Operational(point) => point,
        _ => return Err(invalid()),
    };
    let (row_position, candidate_sql, candidate_client) = match &point.fact {
        OperationalFact::MysqlErrorLog(row) => {
            (row.position, DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected }, None)
        }
        OperationalFact::PostgresLog(row) => (row.position, row.statement.clone(), None),
        OperationalFact::MssqlExtendedEvent(row) => (row.position, row.statement.clone(), row.client.clone()),
        _ => return Err(invalid()),
    };
    let report = crate::collected_resources::native_failures_from_original(original)?;
    let Some(entry) = report.entries.iter().find(|entry| entry.position == row_position) else {
        return Ok(Explanation::default());
    };
    let mut explanation = Explanation::words(&entry.explanation, &entry.investigation, &entry.verify);
    explanation.victim_summary = Some(entry.title.clone());
    explanation.limitations = report.limitations;
    if candidate_sql.is_available() || candidate_client.is_some() {
        let reference = original.reference(position)?;
        let caller = candidate_client
            .filter(|client| client.application.is_some() || client.user.is_some() || client.host.is_some())
            .map(|client| DerivedCaller { client: Some(client), session_context: None, evidence: reference.clone() });
        if candidate_sql.is_available() || caller.is_some() {
            explanation.candidates.push(DerivedIncidentQuery {
                causal_role: None,
                identity: None,
                sql: candidate_sql,
                caller,
                evidence: vec![reference],
            });
        }
    }
    Ok(explanation)
}

fn task_space_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let report = task_space_report(original)?;
    let point = match &original.records().get(position).ok_or_else(invalid)?.row().record {
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

fn cleanup_observation(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let point = match &original.records().get(position).ok_or_else(invalid)?.row().record {
        TelemetryRecord::Operational(point) if matches!(point.fact, OperationalFact::MssqlCleanupHorizon(_)) => point,
        _ => return Err(invalid()),
    };
    let report = crate::collected_resources::cleanup::from_original(original)?;
    let Some(entry) = report
        .entries
        .iter()
        .find(|entry| entry.reference.source == point.source && entry.reference.observation_key == point.key)
    else {
        return Ok(Explanation::default());
    };
    let mut explanation = Explanation::words(&entry.explanation, &entry.investigation, &entry.verify);
    for sibling in report.entries.iter().filter(|sibling| sibling.reference != entry.reference) {
        let sibling_position = original
            .records()
            .iter()
            .position(|row| {
                matches!(
                    &row.row().record,
                    TelemetryRecord::Operational(candidate)
                        if candidate.source == sibling.reference.source
                            && candidate.key == sibling.reference.observation_key
                )
            })
            .ok_or_else(invalid)?;
        let reference = original.reference(sibling_position)?;
        explanation.merge_followup(Explanation::words(&sibling.explanation, &sibling.investigation, &sibling.verify));
        if !explanation.context_evidence.iter().any(|evidence| evidence.reference.as_ref() == Some(&reference)) {
            explanation.context_evidence.push(IncidentHistoryEvidence {
                label: sibling.title.clone(),
                capture: reference.capture_ref(),
                reference: Some(reference),
                availability: t!("view.incident_history.evidence.referenced"),
            });
        }
    }
    explanation.limitations = report.limitations;
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
