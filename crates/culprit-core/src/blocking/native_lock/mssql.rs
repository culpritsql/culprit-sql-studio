//! SQL Server reports a blocking SESSION, not its acquiring request or exact locked object/mode.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlBlockingSessionPoint {
    pub observed_at: String,
    pub waiter: ActivityKey,
    pub owners: Vec<super::super::NativeBlockingOwnerContext>,
}
impl MssqlBlockingSessionPoint {
    pub fn is_valid_for(&self, key: &ActivityKey) -> bool {
        &self.waiter == key
            && key.is_valid()
            && key.request.is_some()
            && canonical_time(&self.observed_at).is_ok()
            && self.owners.len() <= MAX_BLOCKING_LOOKUPS as usize
            && self.owners.iter().all(super::super::NativeBlockingOwnerContext::is_valid)
            && self.owners.iter().map(|context| &context.owner).collect::<BTreeSet<_>>().len() == self.owners.len()
    }
}
