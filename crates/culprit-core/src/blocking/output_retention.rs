//! Native blocker session plus its qualified same-point output request. No acquiring statement or client/network cause.
use super::*;
use crate::{Result, telemetry::*};

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum OutputBlockingIssue {
    UnqualifiedOriginal,
    IncompleteRead,
    PointChanged,
    NativeDetailsNotRecorded,
    LockEvidenceLimited,
    OwnerContextMissing,
    UnknownRequestPopulation,
    MultipleCurrentRequests,
    UnqualifiedStatement,
    UnknownNativeWait,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum OutputBlockingDependency {
    /// Native blocker/session edge only. Exact held lock, mode and acquiring statement remain unknown.
    NativeSession,
    GrantedRelation {
        resource: PostgresRelationLockTag,
        requested_mode: String,
        granted_modes: Vec<String>,
    },
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OutputBlockingCase {
    pub key: String,
    pub observed_at: String,
    pub waiter: ActivityKey,
    pub owner: NativeBlockingOwnerContext,
    pub dependency: OutputBlockingDependency,
    pub object_label: Option<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum OutputBlockingStatus {
    Observed,
    NotEstablished,
    Unavailable,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct OutputBlockingEvidence {
    pub status: OutputBlockingStatus,
    pub cases: Vec<OutputBlockingCase>,
    pub issues: Vec<OutputBlockingIssue>,
}

pub fn output_blocking(frame: &ActivityFrameEvidence<'_>) -> Result<OutputBlockingEvidence> {
    let mut issues = BTreeSet::new();
    let mut cases = Vec::new();
    let result = match &frame.source().observation {
        SourceObservation::FocusedBlocking { result, .. } => result.as_ref(),
        SourceObservation::MetadataLockContext { result, .. } => &result.request,
        _ => return Err(invalid_output()),
    };
    if result.binding != FocusedRequestBinding::Matched
        || result.membership != FocusedRequestMembership::Observed
        || result.request_end != BlockingRequestState::SameStatement
        || result.closure.status != BlockingClosureStatus::Observed
        || !frame.context().has_qualified_local_owner()
        || !frame.is_qualified()
        || frame.native().coverage.error_code == Some(crate::ErrorCode::PermissionDenied)
    {
        issues.insert(OutputBlockingIssue::UnqualifiedOriginal);
    }
    if !frame.read_complete() {
        issues.insert(OutputBlockingIssue::IncompleteRead);
    }
    if issues.is_empty() {
        let waiter = &result.focus.original.key;
        let Some(row) = frame.row(waiter).filter(|row| row.observation.waiting_on_lock == Some(true)) else {
            return Ok(OutputBlockingEvidence {
                status: OutputBlockingStatus::Unavailable,
                cases,
                issues: vec![OutputBlockingIssue::PointChanged],
            });
        };
        for edge in ActivityEvidence::blocking_for_frame(frame)
            .iter()
            .filter(|edge| edge.waiter == *waiter && !edge.reports_waiter_session_id())
        {
            if !edge.owner_observed || edge.owner.session().is_none() {
                issues.insert(OutputBlockingIssue::OwnerContextMissing);
                continue;
            }
            let Some(point) = dependency(row.observation, &edge.owner, &mut issues) else { continue };
            if let Some(case) = qualify(frame, waiter, point, &mut issues)? {
                cases.push(case);
            }
        }
    }
    let status = if !cases.is_empty() {
        OutputBlockingStatus::Observed
    } else if issues.is_empty() {
        OutputBlockingStatus::NotEstablished
    } else {
        OutputBlockingStatus::Unavailable
    };
    Ok(OutputBlockingEvidence { status, cases, issues: issues.into_iter().collect() })
}
struct Dependency<'a> {
    at: &'a str,
    owner: &'a NativeBlockingOwnerContext,
    kind: OutputBlockingDependency,
    label: Option<String>,
}
fn dependency<'a>(
    waiter: &'a ActivityObservation,
    owner: &ActivityWaitOwner,
    issues: &mut BTreeSet<OutputBlockingIssue>,
) -> Option<Dependency<'a>> {
    let mut found = Vec::new();
    for lock in waiter.wait.iter().flat_map(|wait| wait.resources.iter().flatten()) {
        match lock.native.as_deref() {
            Some(NativeLockEvidence::MssqlBlockingSession(point)) => {
                for context in point.owners.iter().filter(|context| &context.owner == owner) {
                    found.push(Dependency {
                        at: &point.observed_at,
                        owner: context,
                        kind: OutputBlockingDependency::NativeSession,
                        label: None,
                    });
                }
            }
            Some(NativeLockEvidence::Postgres(point)) => {
                if !point.waiter_matches
                    || !point.single_backend
                    || !point.snapshot_complete
                    || !point.blockers_complete
                {
                    issues.insert(OutputBlockingIssue::LockEvidenceLimited);
                    continue;
                }
                for blocker in point.blockers.iter().filter(|blocker| &blocker.owner() == owner) {
                    if !blocker.single_backend {
                        issues.insert(OutputBlockingIssue::PointChanged);
                        continue;
                    }
                    let Some(context) = blocker.output_context.as_ref() else {
                        issues.insert(OutputBlockingIssue::OwnerContextMissing);
                        continue;
                    };
                    let kind = match point.edge(blocker) {
                        NativeRelationLockEdge::GrantedConflict => OutputBlockingDependency::GrantedRelation {
                            resource: point.resource.clone()?,
                            requested_mode: point.requested_mode.as_ref()?.native().into(),
                            granted_modes: blocker.granted_modes.iter().map(|mode| mode.native().into()).collect(),
                        },
                        NativeRelationLockEdge::Unknown if blocker.waiting_mode.is_none() => {
                            OutputBlockingDependency::NativeSession
                        }
                        _ => {
                            issues.insert(OutputBlockingIssue::PointChanged);
                            continue;
                        }
                    };
                    let label = match &kind {
                        OutputBlockingDependency::GrantedRelation { .. } => lock.object.clone(),
                        OutputBlockingDependency::NativeSession => None,
                    };
                    found.push(Dependency { at: &point.observed_at, owner: context, kind, label });
                }
            }
            Some(NativeLockEvidence::MssqlObject(_) | NativeLockEvidence::MysqlMetadata(_)) | None => {}
        }
    }
    if found.len() != 1 {
        issues.insert(if found.is_empty() {
            OutputBlockingIssue::NativeDetailsNotRecorded
        } else {
            OutputBlockingIssue::PointChanged
        });
        return None;
    }
    found.pop()
}
fn qualify(
    frame: &ActivityFrameEvidence<'_>,
    waiter: &ActivityKey,
    point: Dependency<'_>,
    issues: &mut BTreeSet<OutputBlockingIssue>,
) -> Result<Option<OutputBlockingCase>> {
    let owner = point.owner;
    if !owner.complete_request_population || owner.current_requests.is_none() {
        issues.insert(OutputBlockingIssue::UnknownRequestPopulation);
        return Ok(None);
    }
    if owner.current_requests.as_deref() != Some("1") {
        if owner.current_requests.as_deref() != Some("0") {
            issues.insert(OutputBlockingIssue::MultipleCurrentRequests);
        }
        return Ok(None);
    }
    if !owner.has_single_current_request() || owner.statement.is_none() {
        issues.insert(OutputBlockingIssue::UnqualifiedStatement);
        return Ok(None);
    }
    let actor = owner.actor.as_ref().ok_or_else(invalid_output)?;
    let known = frame.session_rows(&actor.session).collect::<Vec<_>>();
    if !frame.has_session_generation_authority(&actor.session)
        || known.is_empty()
        || known.iter().any(|row| row.observation.key.request.is_some() && !owner.matches_row(row.observation))
    {
        issues.insert(OutputBlockingIssue::PointChanged);
        return Ok(None);
    }
    let Some(native) = owner.native_wait.as_deref() else {
        issues.insert(OutputBlockingIssue::UnknownNativeWait);
        return Ok(None);
    };
    if frame.source().target.engine().spec().native_wait_mechanism(native)
        != Some(crate::telemetry::WaitMechanism::ClientResultOutput)
    {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(&(
        "native-output-blocking-v1",
        frame.source().captured_range()?.scope_id,
        point.at,
        waiter,
        owner,
    ))
    .map_err(|_| invalid_output())?;
    Ok(Some(OutputBlockingCase {
        key: digest(&bytes),
        observed_at: point.at.into(),
        waiter: waiter.clone(),
        owner: owner.clone(),
        dependency: point.kind,
        object_label: point.label,
    }))
}
fn invalid_output() -> crate::Error {
    CollectionStop::InvalidPage.error()
}
