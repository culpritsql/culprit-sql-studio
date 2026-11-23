//! One closed native-to-neutral projection. Native mode conflict owners remain with their original typed models.
use super::*;
pub(super) struct QueuePoint {
    pub observed_at: String,
    pub object_label: Option<String>,
    pub settled_at: Option<String>,
    pub participant_matches: bool,
    pub unavailable_issue: Option<MetadataQueueIssue>,
    pub resource: Option<MetadataQueueResource>,
    pub requested_mode: Option<String>,
    pub snapshot_rows: u32,
    pub snapshot_limit: u32,
    pub snapshot_complete: bool,
    pub blockers_complete: bool,
    pub blockers: Vec<QueueEdge>,
}
pub(super) struct QueueEdge {
    pub actor: Option<ActivityKey>,
    pub native_owner: ActivityWaitOwner,
    pub waiting_mode: Option<String>,
    pub conflicting_granted_modes: Vec<String>,
    pub relation: NativeRelationLockEdge,
}
impl QueueEdge {
    pub fn owner(&self) -> ActivityWaitOwner {
        self.native_owner.clone()
    }
}
impl QueuePoint {
    fn postgres(point: &PostgresLockPoint) -> Self {
        Self {
            observed_at: point.observed_at.clone(),
            object_label: None,
            settled_at: None,
            participant_matches: point.waiter_matches && point.single_backend,
            unavailable_issue: None,
            resource: point.resource.clone().map(MetadataQueueResource::PostgresRelation),
            requested_mode: point.requested_mode.as_ref().map(|mode| mode.native().into()),
            snapshot_rows: point.snapshot_rows,
            snapshot_limit: point.snapshot_limit,
            snapshot_complete: point.snapshot_complete && point.actor_rows <= MAX_BLOCKING_PARTICIPANTS,
            blockers_complete: point.blockers_complete,
            blockers: point
                .blockers
                .iter()
                .map(|owner| QueueEdge {
                    actor: owner.actor.clone(),
                    native_owner: owner.owner(),
                    waiting_mode: owner.waiting_mode.as_ref().map(|mode| mode.native().into()),
                    conflicting_granted_modes: owner
                        .granted_modes
                        .iter()
                        .filter(|mode| {
                            point.requested_mode.as_ref().and_then(|wanted| wanted.conflicts_with(mode)) == Some(true)
                        })
                        .map(|mode| mode.native().into())
                        .collect(),
                    relation: point.edge(owner),
                })
                .collect(),
        }
    }
    fn mssql(point: &MssqlObjectLockPoint) -> Self {
        Self {
            observed_at: point.observed_at.clone(),
            object_label: None,
            settled_at: Some(point.settled_at.clone()),
            participant_matches: point.waiter_matches && point.waiter_settled,
            unavailable_issue: match point.availability {
                MssqlObjectLockAvailability::Observed => None,
                MssqlObjectLockAvailability::PermissionDenied => Some(MetadataQueueIssue::NativeVisibilityUnavailable),
                MssqlObjectLockAvailability::Limited => Some(MetadataQueueIssue::LockEvidenceLimited),
                MssqlObjectLockAvailability::Unsupported | MssqlObjectLockAvailability::Unavailable => {
                    Some(MetadataQueueIssue::NativeDetailsNotRecorded)
                }
            },
            resource: point
                .resource
                .clone()
                .filter(|resource| resource.created_without_offset.is_some())
                .map(MetadataQueueResource::MssqlObject),
            requested_mode: point.requested_mode.as_ref().map(|mode| mode.native().into()),
            snapshot_rows: point.snapshot_rows,
            snapshot_limit: point.snapshot_limit,
            snapshot_complete: point.snapshot_complete && point.availability == MssqlObjectLockAvailability::Observed,
            blockers_complete: point.blockers_complete,
            blockers: point
                .blockers
                .iter()
                .map(|owner| QueueEdge {
                    actor: owner.context.actor.clone(),
                    native_owner: owner.context.owner.clone(),
                    waiting_mode: owner.waiting_mode.as_ref().map(|mode| mode.native().into()),
                    conflicting_granted_modes: owner
                        .granted_modes
                        .iter()
                        .filter(|mode| {
                            point.requested_mode.as_ref().and_then(|wanted| wanted.conflicts_with(mode)) == Some(true)
                        })
                        .map(|mode| mode.native().into())
                        .collect(),
                    relation: point.edge(owner),
                })
                .collect(),
        }
    }
}
pub(super) fn point(row: &ActivityObservation) -> Option<QueuePoint> {
    let mut points =
        row.wait.as_ref()?.resources.as_ref()?.iter().filter_map(|resource| match resource.native.as_deref() {
            Some(NativeLockEvidence::Postgres(point)) => {
                let mut point = QueuePoint::postgres(point);
                point.object_label = resource.object.clone();
                Some(point)
            }
            Some(NativeLockEvidence::MssqlObject(point)) => {
                let mut point = QueuePoint::mssql(point);
                point.object_label = resource.object.clone();
                Some(point)
            }
            Some(NativeLockEvidence::MssqlBlockingSession(_) | NativeLockEvidence::MysqlMetadata(_)) | None => None,
        });
    let point = points.next()?;
    points.next().is_none().then_some(point)
}
