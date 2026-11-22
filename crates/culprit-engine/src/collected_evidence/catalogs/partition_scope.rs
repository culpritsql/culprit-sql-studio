//! One complete retained partition population must contain a coherent, dense native boundary roster per index.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

struct Group {
    descriptor: Vec<u8>,
    fanout: u32,
    boundaries: BTreeSet<Option<u32>>,
}
pub(super) fn complete(original: &dyn OriginalProjection) -> bool {
    let mut groups = BTreeMap::new();
    let mut functions = BTreeMap::new();
    for row in original.records() {
        let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
            fact: ObjectDefinitionFact::MssqlPartitionBoundary(value),
            ..
        }) = &row.row().record
        else {
            return false;
        };
        if !value.value_identity_complete() {
            return false;
        }
        let key =
            (value.table.database_id, value.table.object_id, value.index_id, value.function_id, value.parameter_id);
        let Some(function) = function_descriptor(value) else {
            return false;
        };
        let function_key = (value.table.database_id, value.function_id, value.parameter_id);
        if functions.insert(function_key, function.clone()).is_some_and(|previous| previous != function) {
            return false;
        }
        let Some(descriptor) = descriptor(value, &function) else {
            return false;
        };
        let group = groups.entry(key).or_insert_with(|| Group {
            descriptor: descriptor.clone(),
            fanout: value.fanout,
            boundaries: BTreeSet::new(),
        });
        if group.descriptor != descriptor || !group.boundaries.insert(value.boundary_id) {
            return false;
        }
    }
    groups.values().all(Group::complete)
}
impl Group {
    fn complete(&self) -> bool {
        if self.fanout == 1 {
            return self.boundaries.len() == 1 && self.boundaries.contains(&None);
        }
        self.boundaries.len() == (self.fanout - 1) as usize
            && self.boundaries.iter().enumerate().all(|(index, boundary)| *boundary == u32::try_from(index + 1).ok())
    }
}
fn function_descriptor(value: &MssqlPartitionBoundary) -> Option<Vec<u8>> {
    let created = super::subjects::local(&value.function_created_without_offset)?.to_string();
    let modified = super::subjects::local(&value.function_modified_without_offset)?.to_string();
    let function = (value.function_id, &value.function_name, created, modified, value.fanout, value.boundary_on_right);
    let parameter = (
        &value.parameter_type_schema,
        &value.parameter_type,
        value.parameter_system_type_id,
        value.parameter_user_type_id,
        value.parameter_maximum_bytes,
        value.parameter_precision,
        value.parameter_scale,
        &value.parameter_collation,
    );
    serde_json::to_vec(&(function, parameter)).ok()
}
fn descriptor(value: &MssqlPartitionBoundary, function: &[u8]) -> Option<Vec<u8>> {
    let scheme = (value.scheme_id, &value.scheme_name);
    let index = (&value.index_name, value.index_disabled, value.index_hypothetical);
    serde_json::to_vec(&(scheme, function, index)).ok()
}
