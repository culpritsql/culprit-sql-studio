//! Point-only native queue proof. Display names, current SQL and wait durations authorize no edge.
use super::*;
use crate::{Result, telemetry::*};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MetadataQueueIssue {
    UnqualifiedOriginal,
    IncompleteRead,
    NativeDetailsNotRecorded,
    UnqualifiedParticipant,
    PointChanged,
    LockEvidenceLimited,
    UnsupportedResource,
    UnknownModeOrEdge,
    MixedGrantedAndPending,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", content = "native", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MetadataQueueResource {
    PostgresRelation(PostgresRelationLockTag),
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
    pub cascades: Vec<MetadataQueueCascade>,
    pub issues: Vec<MetadataQueueIssue>,
}

pub fn metadata_queue(frame: &ActivityFrameEvidence<'_>) -> Result<MetadataQueueEvidence> {
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
    Ok(MetadataQueueEvidence { status, cascades: cases.into_values().collect(), issues: issues.into_iter().collect() })
}
fn qualified_original(frame: &ActivityFrameEvidence<'_>) -> bool {
    matches!(&frame.source().observation,SourceObservation::FocusedBlocking {result,..}
        if result.binding==FocusedRequestBinding::Matched && result.membership==FocusedRequestMembership::Observed
        && result.request_end==BlockingRequestState::SameStatement && result.closure.status==BlockingClosureStatus::Observed)
        && frame.context().has_qualified_local_owner()
        && frame.is_qualified()
        && frame.native().coverage.error_code != Some(crate::ErrorCode::PermissionDenied)
}
fn points<'a>(
    frame: &ActivityFrameEvidence<'a>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> BTreeMap<ActivityKey, &'a PostgresLockPoint> {
    let mut points = BTreeMap::new();
    for row in frame.rows().filter(|row| row.observation.waiting_on_lock == Some(true)) {
        let issue = match point(row.observation) {
            Some(point) if !point.waiter_matches || !point.single_backend => Some(MetadataQueueIssue::PointChanged),
            Some(point)
                if !point.snapshot_complete
                    || !point.blockers_complete
                    || point.actor_rows > MAX_BLOCKING_PARTICIPANTS =>
            {
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
    victim: &PostgresLockPoint,
    points: &BTreeMap<ActivityKey, &PostgresLockPoint>,
    cases: &mut BTreeMap<String, MetadataQueueCascade>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Result<()> {
    let Some(victim_row) = qualified_row(frame, victim_key) else {
        issues.insert(MetadataQueueIssue::UnqualifiedParticipant);
        return Ok(());
    };
    for edge in &victim.blockers {
        if !edge_is(victim, edge, NativeRelationLockEdge::PendingOnlyConflict, issues) {
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
    gate: &PostgresLockPoint,
    victim: &ActivityKey,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Vec<MetadataQueueHolder> {
    let mut holders = Vec::new();
    for edge in &gate.blockers {
        if !edge_is(gate, edge, NativeRelationLockEdge::GrantedConflict, issues) {
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
        holders.push(MetadataQueueHolder {
            actor: actor.clone(),
            granted_modes: edge
                .granted_modes
                .iter()
                .filter(|mode| {
                    gate.requested_mode.as_ref().and_then(|requested| requested.conflicts_with(mode)) == Some(true)
                })
                .map(|mode| mode.native().to_owned())
                .collect(),
        });
    }
    holders.sort_by(|a, b| a.actor.cmp(&b.actor));
    holders.dedup_by(|a, b| a.actor == b.actor);
    holders
}
fn append_case(
    frame: &ActivityFrameEvidence<'_>,
    gate_row: &ActivityObservation,
    gate: &PostgresLockPoint,
    victim: (&ActivityKey, &PostgresLockPoint),
    holders: Vec<MetadataQueueHolder>,
    cases: &mut BTreeMap<String, MetadataQueueCascade>,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> Result<()> {
    let resource = MetadataQueueResource::PostgresRelation(gate.resource.as_ref().ok_or_else(invalid)?.clone());
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
    let object_label = gate_row
        .wait
        .as_ref()
        .and_then(|wait| wait.resources.as_ref())
        .and_then(|rows| rows.iter().find(|resource| resource.native.is_some()))
        .and_then(|row| row.object.clone());
    let entry = cases.entry(key.clone()).or_insert_with(|| MetadataQueueCascade {
        key,
        observed_at: gate.observed_at.clone(),
        resource,
        object_label,
        gatekeeper: gate_row.key.clone(),
        requested_mode: gate.requested_mode.as_ref().unwrap().native().into(),
        holders: holders.clone(),
        affected: Vec::new(),
    });
    if entry.holders != holders {
        issues.insert(MetadataQueueIssue::PointChanged);
        return Ok(());
    }
    entry.affected.push(MetadataQueueAffected {
        actor: victim.0.clone(),
        requested_mode: victim.1.requested_mode.as_ref().unwrap().native().into(),
    });
    Ok(())
}
fn edge_is(
    point: &PostgresLockPoint,
    edge: &PostgresLockBlocker,
    expected: NativeRelationLockEdge,
    issues: &mut BTreeSet<MetadataQueueIssue>,
) -> bool {
    match point.edge(edge) {
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
fn point(row: &ActivityObservation) -> Option<&PostgresLockPoint> {
    let mut points =
        row.wait.as_ref()?.resources.as_ref()?.iter().filter_map(|resource| {
            resource.native.as_deref().map(|NativeLockEvidence::Postgres(point)| point.as_ref())
        });
    let point = points.next()?;
    points.next().is_none().then_some(point)
}
fn qualified_row<'a>(frame: &ActivityFrameEvidence<'a>, key: &ActivityKey) -> Option<&'a ActivityObservation> {
    let row = frame.row(key)?.observation;
    (frame.has_session_generation_authority(&key.session) && frame.session_rows(&key.session).count() == 1)
        .then_some(row)
}
fn reported(row: &ActivityObservation, edge: &PostgresLockBlocker) -> bool {
    !edge.owner().session().is_some_and(|owner| owner == row.key.session)
        && row.wait.as_ref().is_some_and(|wait| wait.owners.contains(&edge.owner()))
}
fn same_point(a: &PostgresLockPoint, b: &PostgresLockPoint) -> bool {
    a.resource == b.resource
        && a.snapshot_rows == b.snapshot_rows
        && a.snapshot_limit == b.snapshot_limit
        && canonical_time(&a.observed_at).ok() == canonical_time(&b.observed_at).ok()
}
fn invalid() -> crate::Error {
    CollectionStop::InvalidPage.error()
}
