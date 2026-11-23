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

fn tempdb_row() -> OperationalObservation {
    OperationalObservation {
        source: crate::source_id::TEMP_CAPACITY.into(),
        key: "shared-tempdb".into(),
        fact: OperationalFact::Space(OperationalSpace {
            kind: crate::FileKind::Temp,
            scope: crate::FileScope::Server,
            population: StoragePopulation::SharedTempdbDataFiles,
            observed_at: "2026-11-21T00:00:01Z".into(),
            database: None,
            native_name: None,
            total_bytes: Some("1048576".into()),
            used_bytes: Some("65536".into()),
            limit_bytes: None,
            growth_enabled: None,
            reuse_wait: None,
            parts: vec![],
        }),
    }
}
fn tempdb_source() -> CollectionDescription {
    let mut source = native_source();
    let SourceObservation::MssqlPoolCurrent { snapshot, .. } = &mut source.observation else { panic!() };
    snapshot.records = 1;
    *snapshot.sources.iter_mut().find(|source| source.source == crate::source_id::TEMP_CAPACITY).unwrap() =
        StateSourceCoverage::collected(crate::source_id::TEMP_CAPACITY.into(), 1);
    source
}
#[test]
fn operational_rows_require_the_exact_current_header_with_canonical_utc_bounds() {
    let original = tempdb_source();
    let row = tempdb_row();
    let SourceObservation::MssqlPoolCurrent { authority, snapshot } = &original.observation else { panic!() };
    assert!(authority.accepts_operational(snapshot, &row));

    let mut equivalent = snapshot.clone();
    equivalent.observed_until = chrono::DateTime::parse_from_rfc3339(&snapshot.observed_until)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, false);
    assert_ne!(equivalent.observed_until, snapshot.observed_until);
    assert!(authority.accepts_operational(&equivalent, &row));

    let mut different = snapshot.clone();
    different.observed_until = chrono::DateTime::parse_from_rfc3339(&snapshot.observed_until)
        .unwrap()
        .checked_add_signed(chrono::TimeDelta::milliseconds(1))
        .unwrap()
        .to_rfc3339();
    assert!(different.is_valid(), "a valid header can still belong to a different acquisition");
    assert!(!authority.accepts_operational(&different, &row));
}
#[test]
fn pool_tempdb_preserves_shared_population_and_exact_native_authority_through_original_and_storage_contracts() {
    let source = tempdb_source();
    let row = tempdb_row();
    assert!(MssqlPoolOperationalRead { source: source.clone(), observations: vec![row.clone()] }.is_valid());
    ExportedRecord::new(
        &source,
        &CaptureIdentity { observation_id: "pool-tempdb-original".into(), revision: None },
        TelemetryRecord::Operational(row.clone()),
    )
    .unwrap();
    let read = StorageRead::from_source(&source, vec![row], true).unwrap();
    assert_eq!(read.target, source.target);
    assert_eq!(read.context.native().unwrap().database_writable, Some(true));
    assert!(serde_json::from_value::<StorageRead>(serde_json::to_value(&read).unwrap()).unwrap().is_valid());
    let mut readonly = source.clone();
    let SourceObservation::MssqlPoolCurrent { authority, .. } = &mut readonly.observation else { panic!() };
    authority.before.database_writable = false;
    authority.after.database_writable = false;
    let read = StorageRead::from_source(&readonly, vec![tempdb_row()], true).unwrap();
    assert_eq!(read.context.native().unwrap().database_writable, Some(false));
}
#[test]
fn a_selected_database_label_different_population_or_missing_pool_authority_cannot_mint_pool_allocation() {
    let source = tempdb_source();
    for change in 0..7 {
        let mut row = tempdb_row();
        let OperationalFact::Space(space) = &mut row.fact else { panic!() };
        match change {
            0 => space.database = Some("anchor".into()),
            1 => space.native_name = Some("anchor".into()),
            2 => {
                space.kind = crate::FileKind::Log;
                space.population = StoragePopulation::SharedTempdbTransactionLog;
            }
            3 => {
                space.kind = crate::FileKind::Data;
                space.scope = crate::FileScope::Database;
                space.population = StoragePopulation::WritableOnlineDataFilegroups;
            }
            4 => row.source = crate::source_id::LOG_CAPACITY.into(),
            5 => space.observed_at = "2026-11-20T23:59:59Z".into(),
            6 => space.observed_at = "2026-11-21T00:00:00.500Z".into(),
            _ => unreachable!(),
        }
        assert!(
            ExportedRecord::new(
                &source,
                &CaptureIdentity { observation_id: "refused-pool-allocation".into(), revision: None },
                TelemetryRecord::Operational(row)
            )
            .is_err(),
            "{change}"
        );
    }
    for change in 0..4 {
        let mut original = source.clone();
        let SourceObservation::MssqlPoolCurrent { authority, snapshot } = &mut original.observation else { panic!() };
        match change {
            0 => authority.native = None,
            1 => authority.native.as_mut().unwrap().after = "f".repeat(64),
            2 => {
                snapshot.sources.retain(|source| source.source != crate::source_id::TEMP_CAPACITY);
            }
            3 => {
                authority.before.native_tier = Some("Hyperscale".into());
                authority.after.native_tier = Some("Hyperscale".into());
            }
            _ => unreachable!(),
        }
        assert!(
            ExportedRecord::new(
                &original,
                &CaptureIdentity { observation_id: "refused-pool-owner".into(), revision: None },
                TelemetryRecord::Operational(tempdb_row())
            )
            .is_err(),
            "{change}"
        );
    }
}
