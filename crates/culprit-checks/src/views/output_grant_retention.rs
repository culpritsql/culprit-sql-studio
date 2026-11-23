//! The existing reservation interpreter owns the native mechanism; this question adds original scope and gaps.
use culprit_core::{MemoryParticipantMechanism, Result, SourceOutcome, format, telemetry::*};

pub fn explain(
    answer: &OutputGrantRetentionAnswer,
    read: &CapturedOperationalRead,
    source: Option<&SourceOutcome>,
) -> Result<OutputGrantRetentionReport> {
    if !answer.is_valid()
        || !read.is_valid()
        || read.origin.capture != answer.result
        || RequestMemorySnapshot::acquisition_context(&read.description)? != answer.result_context
        || read.description.observation.snapshot().ok_or_else(invalid)?.coverage_state()? != answer.source_state
    {
        return Err(invalid());
    }
    let references = read.captured_references()?;
    let expected = answer.subject.iter().chain(&answer.peers).collect::<Vec<_>>();
    if references.iter().any(|reference| !expected.contains(&reference))
        || read.read_complete && references.len() != expected.len()
    {
        return Err(invalid());
    }
    let grants = read
        .observations
        .iter()
        .map(|row| {
            let OperationalFact::ExecutionGrant(grant) = &row.fact else { return Err(invalid()) };
            Ok(grant.as_ref())
        })
        .collect::<Result<Vec<_>>>()?;
    let expected_state = if read.read_complete { answer.source_state } else { culprit_core::SourceState::Partial };
    let fallback = SourceOutcome { source: read.description.source.clone(), state: expected_state, reason: None };
    if source.is_some_and(|outcome| outcome.source != fallback.source || outcome.state != expected_state) {
        return Err(invalid());
    }
    let source = source.or(Some(&fallback));
    let memory = super::request_memory::explain(
        super::request_memory::RequestMemoryEvidence { context: &answer.result_context, observations: &grants },
        source,
    )?;
    let subject_key = answer.subject.as_ref().and_then(|subject| {
        let NativeRecordIdentity::Operational { key, .. } = &subject.native_key else { return None };
        read.observations.iter().any(|row| &row.key == key).then(|| key.clone())
    });
    if let Some(key) = &subject_key {
        let grant = read
            .observations
            .iter()
            .find(|row| &row.key == key)
            .and_then(|row| {
                let OperationalFact::ExecutionGrant(grant) = &row.fact else { return None };
                Some(grant.as_ref())
            })
            .ok_or_else(invalid)?;
        if !answer.matches_subject_wait(grant, read.description.target.engine()) {
            return Err(invalid());
        }
    }
    let entry = subject_key.as_ref().and_then(|key| memory.entries.iter().find(|entry| &entry.observation_key == key));
    let status = if entry
        .is_some_and(|entry| entry.mechanism == Some(MemoryParticipantMechanism::ReservationDuringClientOutputWait))
    {
        OutputGrantRetentionStatus::Observed
    } else if entry.is_some() {
        OutputGrantRetentionStatus::NotEstablished
    } else {
        OutputGrantRetentionStatus::ContextUnavailable
    };
    let (summary, investigation, verify) = if status == OutputGrantRetentionStatus::Observed {
        let entry = entry.ok_or_else(invalid)?;
        (entry.explanation.clone(), entry.investigation.clone(), entry.verify.clone())
    } else {
        (
            culprit_core::t!("view.output_grant_retention.not_established"),
            culprit_core::t!("view.output_grant_retention.action"),
            culprit_core::t!("view.output_grant_retention.verify"),
        )
    };
    let mut limitations = memory.limitations.clone();
    limitations.extend([
        culprit_core::t!("view.output_grant_retention.limit.cause"),
        culprit_core::t!("view.output_grant_retention.limit.points"),
        culprit_core::t!("view.output_grant_retention.limit.locks"),
    ]);
    if !read.read_complete {
        limitations.push(culprit_core::t!("view.output_grant_retention.limit.read"));
    }
    if status != OutputGrantRetentionStatus::Observed {
        limitations.push(culprit_core::t!("view.output_grant_retention.limit.absence"));
    }
    if let Some(value) = &answer.original_wait_ms {
        limitations
            .push(culprit_core::t!("view.output_grant_retention.original_wait", value = format::digits(value.clone())));
    }
    if let Some(value) = &answer.observed_output_wait_ms {
        limitations
            .push(culprit_core::t!("view.output_grant_retention.observed_wait", value = format::digits(value.clone())));
    }
    if answer.original_wait_ms.is_none() || answer.observed_output_wait_ms.is_none() {
        limitations.push(culprit_core::t!("view.output_grant_retention.limit.duration"));
    }
    Ok(OutputGrantRetentionReport { status, subject_key, memory, summary, investigation, verify, limitations })
}
fn invalid() -> culprit_core::Error {
    CollectionStop::InvalidPage.error()
}
