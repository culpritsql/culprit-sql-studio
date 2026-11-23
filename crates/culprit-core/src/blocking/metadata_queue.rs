//! Point-only native queue proof. Display names, current SQL and wait durations authorize no edge.
use super::*;
use crate::{Result, telemetry::*};
use std::collections::{BTreeMap, BTreeSet};
mod candidates;
mod native;
use native::{QueueEdge, QueuePoint, point};

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MetadataQueueIssue {
    UnqualifiedOriginal,
    IncompleteRead,
    NativeDetailsNotRecorded,
    NativeVisibilityUnavailable,
    UnqualifiedParticipant,
    PointChanged,
    LockEvidenceLimited,
    UnsupportedResource,
    UnknownModeOrEdge,
    MixedGrantedAndPending,
    PendingBlockerNotExposed,
    NativeLifetimeNotRecorded,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", content = "native", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MetadataQueueResource {
    PostgresRelation(PostgresRelationLockTag),
    MssqlObject(MssqlObjectLockResource),
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueHolder {
    pub actor: ActivityKey,
    pub granted_modes: Vec<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueAffected {
    pub actor: ActivityKey,
    pub requested_mode: String,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueCascade {
    pub key: String,
    pub observed_at: String,
    pub resource: MetadataQueueResource,
    /// Native name captured with this point; never a historical/current schema or action authority.
    pub object_label: Option<String>,
    pub gatekeeper: ActivityKey,
    pub requested_mode: String,
    pub holders: Vec<MetadataQueueHolder>,
    pub affected: Vec<MetadataQueueAffected>,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MetadataQueueStatus {
    Observed,
    NotEstablished,
    Unavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueEvidence {
    pub status: MetadataQueueStatus,
    pub candidates: Vec<MetadataQueueCandidate>,
    pub cascades: Vec<MetadataQueueCascade>,
    pub issues: Vec<MetadataQueueIssue>,
}

/// A same-object cohort is a candidate mechanism, never a native pending-blocker edge.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueCandidateActor {
    pub actor: ActivityKey,
    pub modes: Vec<String>,
    pub durations: Vec<String>,
    pub requesting_events: Vec<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataQueueCandidate {
    pub key: String,
    pub observed_at: String,
    pub resource: MysqlMetadataLockResource,
    pub affected: ActivityKey,
    pub requested_mode: String,
    pub pending_exclusive: Vec<MetadataQueueCandidateActor>,
    pub granted_holders: Vec<MetadataQueueCandidateActor>,
}

impl MetadataQueueCandidate {
    pub fn is_valid(&self) -> bool {
        let actor = |actor: &MetadataQueueCandidateActor| {
            actor.actor.is_valid()
                && actor.actor != self.affected
                && !actor.modes.is_empty()
                && actor.modes.len() <= MAX_NATIVE_RELATION_LOCK_MODES
                && actor.modes.iter().all(|mode| !mode.is_empty() && mode.len() <= 64)
                && !actor.durations.is_empty()
                && actor.durations.len() <= 3
                && actor
                    .durations
                    .iter()
                    .all(|duration| matches!(duration.as_str(), "STATEMENT" | "TRANSACTION" | "EXPLICIT"))
                && !actor.requesting_events.is_empty()
                && actor.requesting_events.len() <= MAX_BLOCKING_PARTICIPANTS as usize
                && actor
                    .requesting_events
                    .iter()
                    .all(|id| id.parse::<u64>().is_ok_and(|value| value > 0 && value.to_string() == *id))
        };
        self.key.len() == 64
            && self.key.bytes().all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
            && canonical_time(&self.observed_at).is_ok()
            && self.resource.is_valid()
            && self.affected.is_valid()
            && self.affected.request.is_some()
            && !self.requested_mode.is_empty()
            && self.requested_mode.len() <= 64
            && !self.pending_exclusive.is_empty()
            && !self.granted_holders.is_empty()
            && self.pending_exclusive.len() + self.granted_holders.len() < MAX_BLOCKING_PARTICIPANTS as usize
            && self.pending_exclusive.iter().chain(&self.granted_holders).all(actor)
            && self.pending_exclusive.iter().all(|actor| actor.modes.iter().all(|mode| mode == "EXCLUSIVE"))
            && self
                .pending_exclusive
                .iter()
                .chain(&self.granted_holders)
                .map(|actor| &actor.actor)
                .collect::<BTreeSet<_>>()
                .len()
                == self.pending_exclusive.len() + self.granted_holders.len()
    }
}

pub fn metadata_queue(frame: &ActivityFrameEvidence<'_>) -> Result<MetadataQueueEvidence> {
    if let SourceObservation::MetadataLockContext { result, .. } = &frame.source().observation {
        return candidates::explain(frame, result);
    }
    let mut issues = BTreeSet::new();
    if !qualified_original(frame) {
        issues.insert(MetadataQueueIssue::UnqualifiedOriginal);
    }
    if !frame.read_complete() {
        issues.insert(MetadataQueueIssue::IncompleteRead);
    }
    let mut cases = BTreeMap::new();
    if issues.is_empty() {
        let points = points(frame, &mut issues);
        for (victim_key, victim) in &points {
            append_cascades(frame, victim_key, victim, &points, &mut cases, &mut issues)?;
        }
    }
    for case in cases.values_mut() {
        case.affected.sort_by(|a, b| a.actor.cmp(&b.actor));
        case.affected.dedup_by(|a, b| a.actor == b.actor);
    }
    let status = if !cases.is_empty() {
        MetadataQueueStatus::Observed
    } else if issues.is_empty() {
        MetadataQueueStatus::NotEstablished
    } else {
        MetadataQueueStatus::Unavailable
    };
    Ok(MetadataQueueEvidence {
        status,
        candidates: vec![],
        cascades: cases.into_values().collect(),
        issues: issues.into_iter().collect(),
    })
}
fn qualified_original(frame: &ActivityFrameEvidence<'_>) -> bool {
    matches!(&frame.source().observation,SourceObservation::FocusedBlocking {result,..}
        if result.binding==FocusedRequestBinding::Matched && result.membership==FocusedRequestMembership::Observed
        && result.request_end==BlockingRequestState::SameStatement && result.closure.status==BlockingClosureStatus::Observed)
        && frame.context().has_qualified_local_owner()
        && frame.is_qualified()
        && frame.native().coverage.error_code != Some(crate::ErrorCode::PermissionDenied)
}
fn points(
    frame: &ActivityFrameEvidence<'_>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> BTreeMap<ActivityKey, QueuePoint> {
    let mut points = BTreeMap::new();
    for row in frame.rows().filter(|row| row.observation.waiting_on_lock == Some(true)) {
        let issue = match point(row.observation) {
            Some(point) if point.unavailable_issue.is_some() => point.unavailable_issue,
            Some(point) if !point.participant_matches => Some(MetadataQueueIssue::PointChanged),
            Some(point) if !point.snapshot_complete || !point.blockers_complete => {
                Some(MetadataQueueIssue::LockEvidenceLimited)
            }
            Some(point) if point.resource.is_none() => Some(MetadataQueueIssue::UnsupportedResource),
            Some(point) => {
                points.insert(row.observation.key.clone(), point);
                None
            }
            None => Some(MetadataQueueIssue::NativeDetailsNotRecorded),
        };
        if let Some(issue) = issue {
            issues.insert(issue);
        }
    }
    points
}
fn append_cascades(
    frame: &ActivityFrameEvidence<'_>,
    victim_key: &ActivityKey,
    victim: &QueuePoint,
    points: &BTreeMap<ActivityKey, QueuePoint>,
    cases: &mut BTreeMap<String, MetadataQueueCascade>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Result<()> {
    let Some(victim_row) = qualified_row(frame, victim_key) else {
        issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
        return Ok(());
    };
    for edge in &victim.blockers {
        if !edge_is(edge, NativeRelationLockEdge::PendingOnlyConflict, issues) {
            continue;
        }
        let Some(gate_key) = edge.actor.as_ref().filter(|key| *key != victim_key) else {
            continue;
        };
        if !reported(victim_row, edge) {
            issues.insert(MetadataQueueIssue::PointChanged);
            continue;
        }
        let Some(gate) = points.get(gate_key) else {
            issues.insert(MetadataQueueIssue::NativeDetailsNotRecorded);
            continue;
        };
        let Some(gate_row) = qualified_row(frame, gate_key) else {
            issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
            continue;
        };
        if !same_point(victim, gate) || edge.waiting_mode.as_ref() != gate.requested_mode.as_ref() {
            issues.insert(MetadataQueueIssue::PointChanged);
            continue;
        }
        let holders = holders(frame, gate_row, gate, victim_key, issues);
        if !holders.is_empty() {
            append_case(frame, gate_row, gate, (victim_key, victim), holders, cases, issues)?;
        }
    }
    Ok(())
}
fn holders(
    frame: &ActivityFrameEvidence<'_>,
    gate_row: &ActivityObservation,
    gate: &QueuePoint,
    victim: &ActivityKey,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Vec<MetadataQueueHolder> {
    let mut holders = Vec::new();
    for edge in &gate.blockers {
        if !edge_is(edge, NativeRelationLockEdge::GrantedConflict, issues) {
            continue;
        }
        let Some(actor) = edge.actor.as_ref().filter(|key| *key != &gate_row.key && *key != victim) else {
            continue;
        };
        if qualified_row(frame, actor).is_none() {
            issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
            continue;
        }
        if !reported(gate_row, edge) {
            issues.insert(MetadataQueueIssue::PointChanged);
            continue;
        }
        holders
            .push(MetadataQueueHolder { actor: actor.clone(), granted_modes: edge.conflicting_granted_modes.clone() });
    }
    holders.sort_by(|a, b| a.actor.cmp(&b.actor));
    holders.dedup_by(|a, b| a.actor == b.actor);
    holders
}
fn append_case(
    frame: &ActivityFrameEvidence<'_>,
    gate_row: &ActivityObservation,
    gate: &QueuePoint,
    victim: (&ActivityKey, &QueuePoint),
    holders: Vec<MetadataQueueHolder>,
    cases: &mut BTreeMap<String, MetadataQueueCascade>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Result<()> {
    let resource = gate.resource.as_ref().ok_or_else(invalid)?.clone();
    let key = digest(
        &serde_json::to_vec(&(
            "metadata-queue-point-v1",
            frame.source().captured_range()?.scope_id,
            &gate.observed_at,
            &gate_row.key,
            &resource,
        ))
        .map_err(|_| invalid())?,
    );
    let object_label = gate.object_label.clone();
    let entry = cases.entry(key.clone()).or_insert_with(|| MetadataQueueCascade {
        key,
        observed_at: gate.observed_at.clone(),
        resource,
        object_label,
        gatekeeper: gate_row.key.clone(),
        requested_mode: gate.requested_mode.as_ref().unwrap().clone(),
        holders: holders.clone(),
        affected: Vec::new(),
    });
    if entry.holders != holders {
        issues.insert(MetadataQueueIssue::PointChanged);
        return Ok(());
    }
    entry.affected.push(MetadataQueueAffected {
        actor: victim.0.clone(),
        requested_mode: victim.1.requested_mode.as_ref().unwrap().clone(),
    });
    Ok(())
}
fn edge_is(edge: &QueueEdge, expected: NativeRelationLockEdge, issues: &mut BTreeSet<MetadataQueueIssue>) -> bool {
    match edge.relation {
        NativeRelationLockEdge::Unknown => {
            issues.insert(MetadataQueueIssue::UnknownModeOrEdge);
            false
        }
        NativeRelationLockEdge::GrantedAndPendingConflict => {
            issues.insert(MetadataQueueIssue::MixedGrantedAndPending);
            false
        }
        actual => actual == expected,
    }
}
fn qualified_row<'a>(frame: &ActivityFrameEvidence<'a>, key: &ActivityKey) -> Option<&'a ActivityObservation> {
    let row = frame.row(key)?.observation;
    (frame.has_session_generation_authority(&key.session) && frame.session_rows(&key.session).count() == 1)
        .then_some(row)
}
fn reported(row: &ActivityObservation, edge: &QueueEdge) -> bool {
    !edge.owner().session().is_some_and(|owner| owner == row.key.session)
        && row.wait.as_ref().is_some_and(|wait| wait.owners.contains(&edge.owner()))
}
fn same_point(a: &QueuePoint, b: &QueuePoint) -> bool {
    a.resource == b.resource
        && a.snapshot_rows == b.snapshot_rows
        && a.snapshot_limit == b.snapshot_limit
        && canonical_time(&a.observed_at).ok() == canonical_time(&b.observed_at).ok()
        && a.settled_at == b.settled_at
}
fn invalid() -> crate::Error {
    CollectionStop::InvalidPage.error()
}
