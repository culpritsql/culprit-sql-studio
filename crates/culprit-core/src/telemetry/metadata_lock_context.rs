//! A selected ticket cohort admits native candidates without inventing blocking reachability.
use super::*;
use crate::blocking::{MysqlMetadataLockPoint, NativeLockEvidence};
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MetadataLockContextDescription {
    pub request: ActivityBlockingFocusResult,
    /// Exact selected native actor keys, not a wait-for graph or a complete database activity census.
    pub cohort: Vec<ActivityKey>,
}
impl MetadataLockContextDescription {
    pub fn is_valid_header(&self, snapshot: &ActivitySnapshotDescription) -> bool {
        self.request.is_valid_header(snapshot)
            && self.request.focus.is_metadata_lock_focus()
            && self.cohort.len() <= MAX_BLOCKING_PARTICIPANTS as usize
            && self.cohort.len() == snapshot.observations as usize
            && self.cohort.iter().all(ActivityKey::is_valid)
            && self.cohort.iter().collect::<BTreeSet<_>>().len() == self.cohort.len()
            && (self.request.membership != FocusedRequestMembership::Observed
                || self.cohort.contains(&self.request.focus.original.key))
    }
    pub fn is_valid_source(&self, source: &CollectionDescription, snapshot: &ActivitySnapshotDescription) -> bool {
        self.is_valid_header(snapshot)
            && self.request.is_valid_source(source, snapshot)
            && source.target.engine() == crate::Engine::Mysql
    }
    pub fn accepts_row(&self, row: &ActivityObservation) -> bool {
        self.request.accepts_row(row) && self.cohort.contains(&row.key)
    }
    pub fn point<'a>(&self, frame: &'a ActivityFrameEvidence<'_>) -> Option<&'a MysqlMetadataLockPoint> {
        let row = frame.row(&self.request.focus.original.key)?.observation;
        let mut points = row.wait.iter().flat_map(|wait| wait.resources.iter().flatten()).filter_map(|lock| match lock
            .native
            .as_deref()
        {
            Some(NativeLockEvidence::MysqlMetadata(point)) => Some(point.as_ref()),
            _ => None,
        });
        let point = points.next()?;
        points.next().is_none().then_some(point)
    }
    pub fn accepts_frame(&self, frame: &ActivityFrameEvidence<'_>) -> bool {
        if !self.is_valid_source(frame.source(), frame.native())
            || frame.rows().any(|row| !self.accepts_row(row.observation))
        {
            return false;
        }
        if !frame.read_complete() {
            return true;
        }
        if self.request.membership != FocusedRequestMembership::Observed {
            return frame.rows().next().is_none();
        }
        let Some(point) = self.point(frame).filter(|point| point.is_valid_for(&self.request.focus.original.key)) else {
            return false;
        };
        let rows = frame.rows().map(|row| row.observation.clone()).collect::<Vec<_>>();
        let Ok(reached) =
            ActivityEvidence::context_for(&rows, &BTreeSet::from([self.request.focus.original.key.clone()]))
        else {
            return false;
        };
        rows.len() == self.cohort.len()
            && rows.iter().all(|row| reached.contains(&row.key) || point.accepts_actor(&row.key))
    }
}
