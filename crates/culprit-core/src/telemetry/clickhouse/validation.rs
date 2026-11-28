use super::*;
impl ClickHouseObservation {
    pub fn identity(&self) -> NativeIdentity {
        match self {
            Self::QueryFamily(row) => NativeIdentity::new("clickhouse-family-window-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("family", &row.family)
                .qualified("from", &row.from)
                .qualified("until", &row.until),
            Self::Execution(row) => row.focus.identity(),
            Self::PartEvent(row) => NativeIdentity::new("clickhouse-part-event-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("part", &row.part_name)
                .qualified("event", &row.event)
                .qualified("at", row.occurred_us.to_string()),
            Self::Failure(row) => NativeIdentity::new("clickhouse-failure-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("query_id", &row.query_id)
                .qualified("started_us", row.started_us.to_string())
                .qualified("ended_us", row.ended_us.to_string()),
            Self::Parts(row) => NativeIdentity::new("clickhouse-parts-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("partition_id", &row.partition_id),
            Self::Merge(row) => NativeIdentity::new("clickhouse-merge-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("result_part_name", &row.result_part_name),
            Self::Mutation(row) => NativeIdentity::new("clickhouse-mutation-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("mutation_id", &row.mutation_id),
            Self::Replica(row) => NativeIdentity::new("clickhouse-replica-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("replica_name", &row.replica_name),
            Self::ReplicationTask(row) => NativeIdentity::new("clickhouse-replicationtask-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("replica_name", &row.replica_name)
                .qualified("node_name", &row.node_name),
            Self::DistributedQueue(row) => NativeIdentity::new("clickhouse-distribution-queue-v1")
                .qualified("node", &row.node)
                .qualified("database", &row.database)
                .qualified("table", &row.table)
                .qualified("queue", &row.queue_id),
            Self::AsyncInsert(row) => {
                let queued = NativeIdentity::new("clickhouse-asyncinsert-v1")
                    .qualified("node", &row.node)
                    .qualified("database", &row.database)
                    .qualified("table", &row.table)
                    .qualified("query_id", &row.query_id)
                    .qualified("queued_us", row.queued_us.to_string());
                match &row.flush_query_id {
                    Some(id) => queued.qualified("flush_query_id", id),
                    None => queued,
                }
            }
        }
    }
    pub fn is_valid(&self) -> bool {
        self.identity().is_valid()
            && match self {
                Self::QueryFamily(row) => {
                    row.sample.is_valid()
                        && row.population.executions >= row.executions
                        && row.population.elapsed_ms >= row.elapsed_ms
                        && row.population.families > 0
                        && row.population.executions >= row.population.families
                        && row.max_ms <= row.elapsed_ms
                        && DateTime::parse_from_rfc3339(&row.first_seen)
                            .is_ok_and(|a| DateTime::parse_from_rfc3339(&row.last_seen).is_ok_and(|b| a <= b))
                        && row.executions > 0
                        && row.failures <= row.executions
                        && row.family.parse::<u64>().is_ok()
                        && ObservationBounds::new(&row.from, &row.until).is_some()
                }
                Self::PartEvent(row) => row.occurred_us > 0 && valid_key(&row.event),
                Self::Execution(row) => {
                    row.focus.is_valid()
                        && row.family.parse::<u64>().is_ok()
                        && row.exception_code > 0
                        && matches!(row.outcome.as_str(), "ExceptionBeforeStart" | "ExceptionWhileProcessing")
                        && valid_profile(&row.profile)
                }
                Self::Failure(row) => {
                    valid_profile(&row.profile)
                        && row.ended_us >= row.started_us
                        && row.started_us > 0
                        && row.exception_code > 0
                        && matches!(row.outcome.as_str(), "ExceptionWhileProcessing" | "ExceptionBeforeStart")
                        && row.family.parse::<u64>().is_ok()
                        && row.tables.len() <= 32
                        && row.total_tables >= row.tables.len() as u64
                        && valid_key(&row.exception_name)
                        && row.tables.iter().all(|t| valid_key(t))
                }
                Self::Merge(row) => {
                    row.elapsed.is_finite()
                        && row.elapsed >= 0.0
                        && (0.0..=1.0).contains(&row.progress)
                        && row.is_mutation <= 1
                }
                Self::Replica(row) => row.is_readonly <= 1 && row.is_session_expired <= 1,
                Self::ReplicationTask(row) => row.has_exception <= 1 && valid_key(&row.task_type),
                Self::DistributedQueue(row) => {
                    row.is_blocked <= 1
                        && row.queue_id.len() == 64
                        && row.queue_id.bytes().all(|b| b.is_ascii_hexdigit())
                }
                Self::AsyncInsert(row) => valid_key(&row.status) && row.queued_us > 0,
                Self::Parts(row) => row.active_parts > 0,
                Self::Mutation(row) => row.created_seconds > 0,
            }
    }
}
impl ClickHouseObservation {
    pub fn fits_source(&self, source: &CollectionDescription) -> bool {
        let CollectionTarget::Database(target) = &source.target else {
            return false;
        };
        let crate::DatabaseLocation::Server { instance: Some(node), .. } = &target.location else {
            return false;
        };
        let id = self.identity();
        target.engine == crate::Engine::Clickhouse
            && source.source == CLICKHOUSE_OPERATIONAL_SOURCE
            && source.partition == SourcePartition::Target
            && source.observation.snapshot().is_some_and(|s| s.dataset == SnapshotDataset::Operational)
            && id.qualifiers.get("node") == Some(node)
            && id.qualifiers.get("database") == Some(&target.database)
            && match (&source.observation, self) {
                (SourceObservation::ClickHouseInvestigation { focus, .. }, Self::Execution(row)) => {
                    &focus.target == target && focus.request == row.focus
                }
                (SourceObservation::ClickHouseInvestigation { focus, .. }, Self::PartEvent(row)) => {
                    &focus.target == target
                        && focus.table.as_ref() == Some(&row.table)
                        && (focus.request.ended_us.saturating_sub(CLICKHOUSE_PART_CONTEXT_US)..=focus.request.ended_us)
                            .contains(&row.occurred_us)
                }
                (SourceObservation::Snapshot(_), Self::Execution(_) | Self::PartEvent(_)) => false,
                (SourceObservation::Snapshot(header), Self::QueryFamily(family)) => {
                    family.until == header.observed_from
                }
                (SourceObservation::Snapshot(_), _) => true,
                _ => false,
            }
    }
}

fn valid_profile(profile: &std::collections::BTreeMap<String, u64>) -> bool {
    profile.keys().all(|name| CLICKHOUSE_PROFILE_EVENTS.contains(&name.as_str()))
}
