//! A controlled source/row contract for canonical publication tests; no native readiness/qualification is issued.
use super::*;
pub fn readable_resource_history_case(member: Option<&str>) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, mut records) = resource_io_evidence(DataIoAccounting::ComputeLocalSsd);
    let CollectionTarget::Database(database) = &mut source.target else { panic!() };
    database.incarnation = Some("11111111-1111-1111-1111-111111111111".into());
    let member = member.map(|id| SharedResourceIdentity {
        engine: culprit_core::Engine::Mssql,
        kind: SharedResourceKind::Node,
        namespace: NativeIdentity::new(database.incarnation.as_ref().unwrap())
            .qualified("kind", "azure-sql-logical-database"),
        native: NativeIdentity::new(id).qualified("kind", "azure-sql-hyperscale-replica"),
    });
    let SourceObservation::RetainedSamples(history) = source.observation else { panic!() };
    let reader = NativeReadContext {
        target: source.target.clone(),
        partition: SourcePartition::InstanceLocal { member, epoch: Some("a".repeat(64)) },
        observed_from: history.observed_at.clone(),
        observed_until: history.observed_at.clone(),
        database_writable: Some(false),
    };
    source.source = MSSQL_READABLE_RESOURCE_HISTORY_SOURCE.into();
    source.partition = SourcePartition::Target;
    source.continuity = SourceContinuity::Unverified;
    source.observation = SourceObservation::MssqlReadableResources(Box::new(MssqlReadableResourceHistory {
        history,
        current_reader: Box::new(reader),
        observed_role: Some(MssqlResourceReplicaRole::HighAvailabilitySecondary),
    }));
    for row in &mut records {
        let TelemetryRecord::ResourceSample(row) = row else { panic!() };
        row.replica_id = None;
        row.replica_role =
            Some(NativeResourceReplicaRole::MssqlDbResourceStats(MssqlResourceReplicaRole::HighAvailabilitySecondary));
    }
    assert!(source.partition_is_consistent());
    (source, records)
}
