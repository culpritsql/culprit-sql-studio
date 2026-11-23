//! One pure point interpretation over supplied originals. Source authentication and acquisition remain upstream.
use culprit_core::{telemetry::*, *};
use std::collections::BTreeSet;
mod facts;
mod increases;
mod intervals;
mod words;

/// Borrowed canonical bodies plus their existing exact references; no warehouse transport or live handle.
pub struct OriginalRow<'a> {
    pub record: &'a TelemetryRecord,
    pub reference: DiagnosticEvidenceRef,
}
pub struct Original<'a> {
    pub source: &'a CollectionDescription,
    pub original: &'a DiagnosticCaptureRef,
    pub rows: Vec<OriginalRow<'a>>,
    pub read_complete: bool,
}
impl Original<'_> {
    fn validate(&self) -> Result<()> {
        if !self.original.is_valid()
            || !self.source.partition_is_consistent()
            || self.original.source_scope_id != self.source.captured_range()?.scope_id
        {
            return Err(invalid());
        }
        let mut ordinals = BTreeSet::new();
        for row in &self.rows {
            if row.reference.capture_ref() != *self.original
                || !ordinals.insert((row.reference.batch_id.clone(), row.reference.row_ordinal))
            {
                return Err(invalid());
            }
            row.reference.validate_canonical_record(self.source, row.record)?;
        }
        Ok(())
    }
    fn context(&self) -> Result<NativeReadContext> {
        // This existing owner retains SourceRead logical data namespace separately from actual acquisition.
        SourceReadExecutionScope::context_from_source(self.source)
    }
    fn operational(&self) -> Result<CapturedOperationalRead> {
        let positions = self
            .rows
            .iter()
            .map(|row| {
                Ok(CapturedSnapshotPosition {
                    row_ordinal: row.reference.row_ordinal,
                    payload_sha256: row.reference.payload_sha256.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let observations = self
            .rows
            .iter()
            .map(|row| match row.record {
                TelemetryRecord::Operational(point) => Ok(point.clone()),
                _ => Err(invalid()),
            })
            .collect::<Result<Vec<_>>>()?;
        CapturedOperationalRead::captured(
            self.source.clone(),
            self.original.clone(),
            positions,
            observations,
            self.read_complete,
        )
    }
}

/// `primary` is one exact native Activity row, not a caller-selected display key or a SQL-text match.
pub fn explain(originals: &[Original<'_>], primary: &DiagnosticEvidenceRef) -> Result<CommitPipelineReport> {
    if originals.len() > MAX_DERIVED_CAPTURES || !primary.is_valid() {
        return Err(invalid());
    }
    let mut captures = BTreeSet::new();
    for original in originals {
        original.validate()?;
        if !captures.insert(original.original.key()?) {
            return Err(invalid());
        }
    }
    let selected = originals.iter().filter(|original| *original.original == primary.capture_ref()).collect::<Vec<_>>();
    let [original] = selected.as_slice() else { return Err(invalid()) };
    let row = original.rows.iter().find(|row| row.reference == *primary).ok_or_else(invalid)?;
    let TelemetryRecord::Activity(actor) = row.record else { return Err(invalid()) };
    let rows = original
        .rows
        .iter()
        .map(|row| match row.record {
            TelemetryRecord::Activity(observation) => {
                Ok(ActivityFrameRow { observation, reference: Some(&row.reference) })
            }
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let frame = ActivityFrameEvidence::new(original.source, &rows, original.read_complete)?;
    let context = frame.context();
    let native_wait = actor.wait.as_ref().and_then(|wait| wait.native_type.clone());
    let qualified = original.read_complete
        && frame.is_qualified()
        && context.has_qualified_local_owner()
        && frame.has_session_generation_authority(&actor.key.session)
        && actor.key.request.is_some()
        && actor.statement_identity.is_some()
        && !actor.idle
        && !matches!(actor.scope, ActivityScopeBasis::OtherDatabase | ActivityScopeBasis::BlockingReference)
        && matches!(frame.native().coverage.state, SourceState::Collected | SourceState::Partial)
        && frame.native().coverage.error_code != Some(ErrorCode::PermissionDenied);
    let stage = qualified
        .then(|| {
            native_wait
                .as_deref()
                .and_then(|native| original.source.target.engine().spec().commit_wait_stage(native))
                .or_else(|| {
                    actor
                        .wait
                        .as_ref()
                        .filter(|wait| wait.class == Some(ActivityWaitClass::LogRateGovernance))
                        .map(|_| CommitPipelineStage::RateGovernance { component: None })
                })
        })
        .flatten();
    let mut report = CommitPipelineReport {
        original: primary.clone(),
        context,
        observed_at: frame.native().at.clone(),
        source_state: frame.native().coverage.state,
        read_complete: original.read_complete,
        native_rows: frame.native().native_rows,
        observed_rows: u32::try_from(rows.len()).map_err(|_| invalid())?,
        actor: actor.key.clone(),
        statement: actor.statement_identity.clone(),
        native_wait,
        wait_ms: actor.wait.as_ref().and_then(|wait| wait.elapsed_ms.clone()),
        stage,
        inputs: vec![],
        facts: vec![],
        increases: vec![],
        intervals: vec![],
        interval_gaps: vec![],
        context_summaries: vec![],
        examined_rows: 0,
        facts_limited: false,
        title: t!("view.commit_pipeline.title"),
        summary: String::new(),
        mechanism: None,
        investigation: String::new(),
        verify: t!("view.commit_pipeline.verify"),
        limitations: vec![],
    };
    facts::append(originals, &mut report)?;
    increases::append(originals, &mut report)?;
    intervals::append(originals, &mut report)?;
    words::render(&mut report, qualified);
    Ok(report)
}
fn context_binding(context: &NativeReadContext, primary: &NativeReadContext) -> CommitPipelineContextBinding {
    if !context.has_qualified_local_owner() || !primary.has_qualified_local_owner() {
        return CommitPipelineContextBinding::Unqualified;
    }
    if context.partition != primary.partition {
        return CommitPipelineContextBinding::Foreign;
    }
    match (&context.target, &primary.target) {
        (CollectionTarget::Database(_), CollectionTarget::Database(_)) if context.target == primary.target => {
            CommitPipelineContextBinding::SameNativeOwner
        }
        (CollectionTarget::Shared(owner), _) if matches!(&primary.partition,SourcePartition::InstanceLocal {member:Some(member),..} if member==owner) => {
            CommitPipelineContextBinding::SharedNativeOwner
        }
        _ => CommitPipelineContextBinding::Foreign,
    }
}
// Eligibility for this interpretation is a declared data resource, not native owner/generation admission.
// A same-target capture with a replaced or missing acquisition owner must still reach endpoint selection.
fn relevant_target(target: &CollectionTarget, primary: &NativeReadContext) -> bool {
    target == &primary.target
        || matches!((target, &primary.partition),
            (CollectionTarget::Shared(resource), SourcePartition::InstanceLocal { member: Some(member), .. })
                if resource == member)
}

fn compatible_owner(context: &NativeReadContext, primary: &NativeReadContext) -> bool {
    matches!(
        context_binding(context, primary),
        CommitPipelineContextBinding::SameNativeOwner | CommitPipelineContextBinding::SharedNativeOwner
    )
}

fn invalid() -> Error {
    CollectionStop::InvalidPage.error()
}
