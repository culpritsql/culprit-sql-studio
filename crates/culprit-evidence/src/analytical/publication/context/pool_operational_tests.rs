use super::*;
use crate::analytical::test_support::{PoolWaitCounterCase, pool_wait_counter_evidence};
#[test]
fn public_pool_coverage_keeps_dataset_and_gaps_without_minting_native_member_or_process_authority() {
    let (mut source, _) = pool_wait_counter_evidence(PoolWaitCounterCase::GeneralPurpose);
    source.source = MSSQL_POOL_OPERATIONAL_SOURCE.into();
    let SourceObservation::MssqlPoolCurrent { snapshot, authority } = &mut source.observation else { panic!() };
    let scope = MssqlPoolNativeScope::Observed {
        process_epoch: "a".repeat(64),
        anchor_replica_id: None,
        members: vec![MssqlPoolNativeMember {
            database_id: authority.before.database_id,
            database_name: authority.before.database_name.clone(),
            identity: authority.before.member.clone(),
            workload_group_id: 3,
        }],
    };
    authority.native = Some(Box::new(MssqlPoolNativeAuthority { after: scope.key().unwrap(), before: scope }));
    snapshot.dataset = SnapshotDataset::Operational;
    snapshot.records = 0;
    snapshot.sources = MSSQL_POOL_OPERATIONAL_SOURCES
        .iter()
        .map(|name| StateSourceCoverage::unavailable((*name).into(), culprit_core::ErrorCode::Unsupported, None))
        .collect();
    assert!(source.partition_is_consistent());
    let public = observation(&source.observation).unwrap();
    assert_eq!(public["data"]["snapshot"]["dataset"], "operational");
    assert_eq!(public["data"]["snapshot"]["sources"].as_array().unwrap().len(), 5);
    assert_eq!(public["data"].as_object().unwrap().len(), 1);
    let text = public.to_string();
    for private in [
        "process_epoch",
        "logical_database_guid",
        "physical_database_guid",
        "workload_group_id",
        "server_performance_state",
        &"a".repeat(64),
    ] {
        assert!(!text.contains(private), "{private}");
    }
    assert!(serde_json::from_value::<SourceObservation>(public).is_err());
}
