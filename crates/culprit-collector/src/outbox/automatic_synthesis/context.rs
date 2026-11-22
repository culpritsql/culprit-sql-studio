//! Exact native ancestry and bounded catalog hints share the original EOF verifier and cumulative read allowance.
use super::*;
use culprit_core::DiagnosticAncestryTarget;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContextPlan {
    pub context: Vec<DiagnosticCaptureRef>,
    pub catalogs: Vec<String>,
    pub issues: Vec<SynthesisContextIssue>,
    pub recorded_at: String,
}
impl ContextPlan {
    fn validate(&self) -> Result<()> {
        let mut identities = BTreeSet::new();
        let mut catalog_keys = BTreeSet::new();
        if self.context.len() > MAX_SYNTHESIS_CONTEXT_CAPTURES
            || self.issues.len() > MAX_SYNTHESIS_CONTEXT_ISSUES
            || canonical_time(&self.recorded_at)? != self.recorded_at
            || self
                .context
                .iter()
                .any(|capture| !capture.is_valid() || !identities.insert(&capture.capture.observation_id))
            || self.catalogs.len() > self.context.len()
            || self.catalogs.iter().any(|key| {
                !catalog_keys.insert(key)
                    || !self.context.iter().any(|capture| capture.key().as_ref().is_ok_and(|capture| capture == key))
            })
            || self.issues.iter().any(|issue| match issue {
                SynthesisContextIssue::MissingLocalOriginal { capture }
                | SynthesisContextIssue::UnavailableOriginal { capture } => !capture.is_valid(),
                _ => false,
            })
        {
            return Err(integrity());
        }
        Ok(())
    }
}
pub(super) fn encode_plan(plan: &ContextPlan) -> Result<String> {
    plan.validate()?;
    let bytes = crate::durable_fs::json_bytes(plan, MAX_SYNTHESIS_PLAN_BYTES).map_err(storage)?;
    String::from_utf8(bytes).map_err(|_| integrity())
}
pub(super) fn decode_plan(value: &str) -> Result<ContextPlan> {
    if value.len() > MAX_SYNTHESIS_PLAN_BYTES {
        return Err(integrity());
    }
    let plan: ContextPlan = capture_journal::decode(value)?;
    plan.validate()?;
    Ok(plan)
}

enum Availability {
    Ready,
    Missing,
    Unavailable,
}
fn availability(conn: &Connection, original: &DiagnosticCaptureRef) -> Result<Availability> {
    let current: Option<(String, String, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT state,source_work,manifest_sha,range_json FROM captures WHERE observation_id=?1",
            [&original.capture.observation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(storage)?;
    let Some((state, work, manifest, range)) = current else { return Ok(Availability::Missing) };
    if capture_journal::decode::<OutboxCaptureState>(&state)? != OutboxCaptureState::Ready
        || capture_journal::decode::<SourceWorkState>(&work)? != SourceWorkState::Quiescent
    {
        return Ok(Availability::Unavailable);
    }
    let range: CapturedRange = capture_journal::decode(&range.ok_or_else(integrity)?)?;
    if manifest.as_deref() != Some(&original.manifest_sha256) || range.scope_id != original.source_scope_id {
        return Err(integrity());
    }
    Ok(Availability::Ready)
}

/// Structural enumeration is Core-owned. Every retained row link is independently checked against the
/// canonical body at its original stream ordinal; an unavailable original is an explicit context gap.
struct ReferencedTargets<'a> {
    values: Vec<DiagnosticAncestryTarget<'a>>,
    limited: bool,
}
fn references(capture: &SynthesisCapture) -> Result<ReferencedTargets<'_>> {
    let mut references = Vec::new();
    for row in capture.records() {
        let TelemetryRecord::Operational(sample) = &row.record else { continue };
        let OperationalFact::DiagnosticRevision(revision) = &sample.fact else { continue };
        for reference in revision.referenced_evidence()? {
            if references.len() >= culprit_core::MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                return Ok(ReferencedTargets { values: references, limited: true });
            }
            references.push(reference.target);
        }
    }
    Ok(ReferencedTargets { values: references, limited: false })
}

struct Selection {
    context: Vec<SynthesisCapture>,
    issues: Vec<SynthesisContextIssue>,
    known: BTreeMap<String, DiagnosticCaptureRef>,
    catalogs: Vec<String>,
}
impl Selection {
    fn issue(&mut self, issue: SynthesisContextIssue) {
        if self.issues.contains(&issue) {
            return;
        }
        if self.issues.len() < MAX_SYNTHESIS_CONTEXT_ISSUES - 1 {
            self.issues.push(issue);
        } else if !self.issues.contains(&SynthesisContextIssue::ContextLimit) {
            self.issues.push(SynthesisContextIssue::ContextLimit);
        }
    }
    fn register_reference(&mut self, reference: &DiagnosticCaptureRef) -> Result<bool> {
        let id = &reference.capture.observation_id;
        if let Some(previous) = self.known.get(id) {
            if previous != reference {
                return Err(integrity());
            }
            return Ok(false);
        }
        if self.context.len() >= MAX_SYNTHESIS_CONTEXT_CAPTURES || self.known.len() > MAX_SYNTHESIS_CONTEXT_CAPTURES * 2
        {
            self.issue(SynthesisContextIssue::ContextLimit);
            return Ok(false);
        }
        self.known.insert(id.clone(), reference.clone());
        Ok(true)
    }
}

impl Files {
    pub(super) fn plan_synthesis_context(
        &mut self,
        primary: SynthesisCapture,
        read: &mut CompletedCaptureRead,
    ) -> Result<(SynthesisInput, ContextPlan)> {
        let mut selection = Selection {
            context: vec![],
            issues: vec![],
            known: [(primary.original().capture.observation_id.clone(), primary.original().clone())].into(),
            catalogs: vec![],
        };
        let primary_references = references(&primary)?;
        if primary_references.limited {
            selection.issue(SynthesisContextIssue::ContextLimit);
        }
        let mut pending = primary_references.values.into_iter().map(owned_target).collect::<Vec<_>>();
        let mut scanned = 0;
        // Each capture is decoded once. Its direct closed ancestry may add at most the named context allowance.
        while scanned < pending.len() {
            read.check()?;
            let reference = pending[scanned].capture();
            scanned += 1;
            if !selection.register_reference(&reference)? {
                continue;
            }
            match availability(&self.journal.conn, &reference)? {
                Availability::Missing => {
                    selection.issue(SynthesisContextIssue::MissingLocalOriginal { capture: reference });
                    continue;
                }
                Availability::Unavailable => {
                    selection.issue(SynthesisContextIssue::UnavailableOriginal { capture: reference });
                    continue;
                }
                Availability::Ready => (),
            }
            read.select_synthesis_reference(reference)?;
            let capture = self.synthesis_capture(read)?;
            if capture.source().observation.collection() == TelemetryCollection::DerivedDiagnosis {
                return Err(integrity());
            }
            let found = references(&capture)?;
            if found.limited {
                selection.issue(SynthesisContextIssue::ContextLimit);
            }
            for reference in found.values {
                if pending.len() >= culprit_core::MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                    selection.issue(SynthesisContextIssue::ContextLimit);
                    break;
                }
                pending.push(owned_target(reference));
            }
            selection.context.push(capture);
        }
        validate_row_links(&primary, &selection.context, &pending)?;
        // Catalogs accompany native originals, never the local diagnostic capture's later synthesis clock.
        let anchors = std::iter::once(&primary)
            .chain(&selection.context)
            .filter(|capture| !matches!(capture.source().observation, SourceObservation::Diagnostics(_)))
            .collect::<Vec<_>>();
        let mut hints = Vec::new();
        let mut unknown_scope = false;
        for anchor in anchors {
            read.check()?;
            match catalog::select(&self.journal.conn, anchor)? {
                catalog::CatalogSelection::UnknownScope => unknown_scope = true,
                catalog::CatalogSelection::Known(found) => {
                    for hint in found {
                        if hints.len() >= crate::synthesis::MAX_SYNTHESIS_CATALOG_HEADS as usize {
                            break;
                        }
                        if !hints.iter().any(|before: &catalog::CatalogHint| before.capture == hint.capture) {
                            hints.push(hint);
                        }
                    }
                }
            }
        }
        if unknown_scope {
            selection.issue(SynthesisContextIssue::UnqualifiedCatalogScope);
        }
        for hint in hints {
            read.check()?;
            if !selection.register_reference(&hint.capture)? {
                continue;
            }
            if !matches!(availability(&self.journal.conn, &hint.capture)?, Availability::Ready) {
                selection.issue(SynthesisContextIssue::UnavailableOriginal { capture: hint.capture });
                continue;
            }
            read.select_synthesis_reference(hint.capture.clone())?;
            let capture = self.synthesis_capture(read)?;
            if !hint.matches(&capture) {
                return Err(integrity());
            }
            selection.catalogs.push(capture.original().key()?);
            selection.context.push(capture);
        }
        if selection.catalogs.is_empty() {
            selection.issue(SynthesisContextIssue::CatalogNotRetainedAsOf);
        }
        let plan = ContextPlan {
            context: selection.context.iter().map(|capture| capture.original().clone()).collect(),
            catalogs: selection.catalogs,
            issues: selection.issues,
            recorded_at: canonical_time(&chrono::Utc::now().to_rfc3339())?,
        };
        plan.validate()?;
        let input = SynthesisInput::new_with_context_issues(
            primary,
            selection.context,
            plan.recorded_at.clone(),
            plan.issues.clone(),
        )?;
        Ok((input, plan))
    }

    pub(super) fn replay_synthesis_plan(
        &mut self,
        primary: SynthesisCapture,
        plan: &ContextPlan,
        read: &mut CompletedCaptureRead,
    ) -> Result<SynthesisInput> {
        plan.validate()?;
        let mut context = Vec::with_capacity(plan.context.len());
        for reference in &plan.context {
            read.select_synthesis_reference(reference.clone())?;
            context.push(self.synthesis_capture(read)?);
        }
        let references = std::iter::once(&primary)
            .chain(&context)
            .map(references)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|found| found.values)
            .map(owned_target)
            .collect::<Vec<_>>();
        validate_row_links(&primary, &context, &references)?;
        for capture in &context {
            if plan.catalogs.contains(&capture.original().key()?)
                && !std::iter::once(&primary).chain(&context).any(|anchor| {
                    anchor.original() != capture.original()
                        && !matches!(anchor.source().observation, SourceObservation::Diagnostics(_))
                        && catalog::matches_anchor(capture, anchor)
                })
            {
                return Err(integrity());
            }
        }
        SynthesisInput::new_with_context_issues(primary, context, plan.recorded_at.clone(), plan.issues.clone())
    }
}

enum RequiredTarget {
    Capture(DiagnosticCaptureRef),
    Row(Box<DiagnosticEvidenceRef>),
}
impl RequiredTarget {
    fn capture(&self) -> DiagnosticCaptureRef {
        match self {
            Self::Capture(capture) => capture.clone(),
            Self::Row(row) => row.capture_ref(),
        }
    }
}
fn owned_target(reference: DiagnosticAncestryTarget<'_>) -> RequiredTarget {
    match reference {
        DiagnosticAncestryTarget::Capture(capture) => RequiredTarget::Capture(capture.clone()),
        DiagnosticAncestryTarget::Record(row) => RequiredTarget::Row(Box::new(row.clone())),
    }
}
fn validate_row_links(
    primary: &SynthesisCapture,
    context: &[SynthesisCapture],
    required: &[RequiredTarget],
) -> Result<()> {
    let mut authenticated = BTreeSet::new();
    for capture in std::iter::once(primary).chain(context) {
        for reference in capture.row_references() {
            authenticated.insert(reference?.key()?);
        }
    }
    for target in required {
        let RequiredTarget::Row(row) = target else { continue };
        if std::iter::once(primary).chain(context).any(|capture| capture.original() == &row.capture_ref())
            && !authenticated.contains(&row.key()?)
        {
            return Err(integrity());
        }
    }
    Ok(())
}
