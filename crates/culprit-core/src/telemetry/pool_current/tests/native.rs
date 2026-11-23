use super::*;
fn native_source() -> CollectionDescription {
    let mut original = source();
    original.source = MSSQL_POOL_OPERATIONAL_SOURCE.into();
    let SourceObservation::MssqlPoolCurrent { snapshot, authority } = &mut original.observation else { panic!() };
    let before = MssqlPoolNativeScope::Observed {
        process_epoch: "a".repeat(64),
        anchor_replica_id: None,
        members: vec![MssqlPoolNativeMember {
            database_id: authority.before.database_id,
            database_name: authority.before.database_name.clone(),
            identity: authority.before.member.clone(),
            workload_group_id: 3,
        }],
    };
    authority.native = Some(Box::new(MssqlPoolNativeAuthority { after: before.key().unwrap(), before }));
    snapshot.dataset = SnapshotDataset::Operational;
    snapshot.records = 0;
    snapshot.sources = MSSQL_POOL_OPERATIONAL_SOURCES
        .iter()
        .map(|source| StateSourceCoverage::unavailable((*source).into(), crate::ErrorCode::Unsupported, None))
        .collect();
    assert!(original.partition_is_consistent());
    original
}
#[test]
fn pool_current_native_scope_roundtrips_and_cannot_be_replaced_by_a_public_or_generic_pool_header() {
    let original = native_source();
    let restored: CollectionDescription = serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    assert_eq!(restored, original);
    assert!(restored.partition_is_consistent());
    let mut bare = original.clone();
    bare.observation = SourceObservation::Snapshot(bare.observation.snapshot().unwrap().clone());
    assert!(!bare.partition_is_consistent());
    let mut forged = original.clone();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut forged.observation else { panic!() };
    authority.native = None;
    assert!(!forged.partition_is_consistent());
}
#[test]
fn final_digest_membership_reused_ids_duplicate_groups_and_wrong_epoch_are_not_repaired() {
    for change in 0..6 {
        let mut original = native_source();
        let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut original.observation else { panic!() };
        let native = authority.native.as_mut().unwrap();
        let MssqlPoolNativeScope::Observed { process_epoch, members, .. } = &mut native.before else { panic!() };
        match change {
            0 => *process_epoch = "b".repeat(64),
            1 => members[0].database_id += 1,
            2 => assert!(
                members[0]
                    .identity
                    .qualifiers
                    .insert("physical_database_guid".into(), "33333333-3333-3333-3333-333333333333".into())
                    .is_some()
            ),
            3 => members.push(members[0].clone()),
            4 => native.after = "c".repeat(64),
            5 => members[0].workload_group_id = 0,
            _ => unreachable!(),
        }
        assert!(!original.partition_is_consistent(), "{change}");
    }
}
#[test]
fn actual_readonly_role_is_operational_context_and_cannot_promote_the_primary_only_wait_contract() {
    let mut original = native_source();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut original.observation else { panic!() };
    authority.before.database_writable = false;
    authority.after.database_writable = false;
    assert!(!authority.before.is_valid());
    assert!(authority.before.is_valid_for_operational());
    assert!(original.partition_is_consistent());
    assert_eq!(NativeReadContext::from_snapshot(&original).unwrap().database_writable, Some(false));
    let mut wait = source();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut wait.observation else { panic!() };
    authority.before.database_writable = false;
    authority.after.database_writable = false;
    assert!(!wait.partition_is_consistent());
}
#[test]
fn hyperscale_requires_positive_native_replica_identity_not_role_name_or_startup_key() {
    let mut original = native_source();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut original.observation else { panic!() };
    authority.before.native_tier = Some("Hyperscale".into());
    authority.after.native_tier = Some("Hyperscale".into());
    let native = authority.native.as_mut().unwrap();
    assert!(matches!(&native.before, MssqlPoolNativeScope::Observed { anchor_replica_id: None, .. }));
    assert!(!native.before.is_valid_for(&authority.before));
    let MssqlPoolNativeScope::Observed { anchor_replica_id, .. } = &mut native.before else { panic!() };
    *anchor_replica_id = Some("55555555-5555-5555-5555-555555555555".into());
    native.after = native.before.key().unwrap();
    assert!(original.partition_is_consistent());
}
