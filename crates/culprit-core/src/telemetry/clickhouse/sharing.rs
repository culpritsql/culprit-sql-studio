use super::*;
impl ClickHouseObservation {
    /// Keep measurements and logical database/table names. Hide physical nodes and caller-controlled identifiers
    /// in a shared report while preserving equality across its evidence.
    pub(crate) fn redact_for_sharing(&mut self, opaque: fn(&str) -> String) {
        fn mask(value: &mut String, opaque: fn(&str) -> String) {
            if !value.is_empty() {
                *value = opaque(value);
            }
        }
        let node = match self {
            Self::Failure(row) => {
                mask(&mut row.query_id, opaque);
                mask(&mut row.initial_query_id, opaque);
                &mut row.node
            }
            Self::Execution(row) => {
                mask(&mut row.focus.query_id, opaque);
                &mut row.focus.node
            }
            Self::PartEvent(row) => {
                for value in [&mut row.part_name, &mut row.partition_id, &mut row.query_id] {
                    mask(value, opaque);
                }
                &mut row.node
            }
            Self::QueryFamily(row) => {
                row.redact_statement();
                &mut row.node
            }
            Self::Parts(row) => {
                mask(&mut row.partition_id, opaque);
                &mut row.node
            }
            Self::Merge(row) => {
                mask(&mut row.partition_id, opaque);
                mask(&mut row.result_part_name, opaque);
                &mut row.node
            }
            Self::Mutation(row) => {
                mask(&mut row.mutation_id, opaque);
                mask(&mut row.latest_failed_part, opaque);
                &mut row.node
            }
            Self::Replica(row) => {
                mask(&mut row.replica_name, opaque);
                &mut row.node
            }
            Self::ReplicationTask(row) => {
                mask(&mut row.replica_name, opaque);
                mask(&mut row.node_name, opaque);
                &mut row.node
            }
            Self::DistributedQueue(row) => &mut row.node,
            Self::AsyncInsert(row) => {
                mask(&mut row.query_id, opaque);
                if let Some(id) = &mut row.flush_query_id {
                    mask(id, opaque);
                }
                &mut row.node
            }
        };
        mask(node, opaque);
    }
}
