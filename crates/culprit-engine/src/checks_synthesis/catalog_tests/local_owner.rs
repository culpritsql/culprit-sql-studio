//! Canonical retained captures prove read ownership; these controlled facts are not native admission/cost tests.
use super::*;

async fn primary(known_role: bool) -> EvidenceCapture {
    let (mut source, records) = test_support::replica_activity_evidence_cases().remove(0);
    let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut source.partition else { panic!() };
    member.kind = SharedResourceKind::Instance;
    let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
    if !known_role {
        frame.database_writable = None;
    }
    assert!(source.target.database().unwrap().incarnation.is_none());
    assert!(source.partition_is_consistent());
    capture(source, records, 90, "2020-01-01T12:01:00Z", EvidenceFamily::ActivityObservations).await
}

fn local_table(primary: &dyn OriginalProjection) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, rows) = table_rows(primary, "2020-01-01T10:00:00Z", false);
    source.partition = primary.source().partition.clone();
    let SourceObservation::Snapshot(snapshot) = source.observation else { panic!() };
    source.observation = SourceObservation::LocalResource { snapshot, database_writable: true };
    assert!(source.partition_is_consistent());
    (source, rows)
}

#[tokio::test]
async fn a_canonical_local_catalog_keeps_exact_db_epoch_role_capture_and_acquisition_time_without_a_global_namespace() {
    let original = primary(true).await;
    let primary = original.verified_original().unwrap();
    let (source, rows) = local_table(&primary);
    let bytes = serde_json::to_vec(&source).unwrap();
    let retained = capture(source, rows, 91, "2020-01-01T10:00:00Z", EvidenceFamily::OperationalSamples).await;
    assert!(retained.is_read_complete(), "{:?}", retained.gaps());
    let catalog = retained.verified_original().unwrap();
    let context = CatalogContext::from_originals(&primary, &[&primary, &catalog]).unwrap();
    assert_eq!(context.inputs(), [1]);
    assert!(context.issues().contains(&DerivedContextIssue::AcquisitionCatalogOnly));
    let output = policy().analyze_originals(&[&primary, &catalog], "2020-01-01T12:02:00Z", &display(), vec![]).unwrap();
    let input = output.description.inputs.iter().find(|input| input.capture == *catalog.original()).unwrap();
    assert_eq!(input.role, DerivedInputRole::Catalog);
    assert!(matches!(&input.catalog.as_ref().unwrap().native_time, Some(DerivedCatalogTime::Utc { at })
        if *at == canonical_time("2020-01-01T10:00:01Z").unwrap()));
    assert_eq!(serde_json::to_vec(catalog.source()).unwrap(), bytes);
    assert!(catalog.source().target.database().unwrap().incarnation.is_none());
    output.validate().unwrap();
}

#[tokio::test]
async fn canonical_local_catalogs_with_changed_identity_role_missing_role_partial_or_future_bounds_remain_unqualified_context()
 {
    for case in
        ["recovery-epoch", "role", "missing-catalog-role", "missing-primary-role", "foreign-db", "partial", "future"]
    {
        let original = primary(case != "missing-primary-role").await;
        let primary = original.verified_original().unwrap();
        let (mut source, rows) = local_table(&primary);
        match case {
            "recovery-epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut source.partition else { panic!() };
                *epoch = Some(digest(b"same-member-different-local-database-file-recovery-epoch"));
            }
            "role" => {
                let SourceObservation::LocalResource { database_writable, .. } = &mut source.observation else {
                    panic!()
                };
                *database_writable = false;
            }
            "missing-catalog-role" => {
                let SourceObservation::LocalResource { snapshot, .. } = source.observation else { panic!() };
                source.observation = SourceObservation::Snapshot(snapshot);
            }
            "foreign-db" => {
                let CollectionTarget::Database(database) = &mut source.target else { panic!() };
                database.database = "another-database".into();
            }
            "partial" => {
                let SourceObservation::LocalResource { snapshot, .. } = &mut source.observation else { panic!() };
                snapshot.sources[0] = StateSourceCoverage::observed(source_id::TABLES.into(), 1, false);
            }
            "future" => {
                let SourceObservation::LocalResource { snapshot, .. } = &mut source.observation else { panic!() };
                snapshot.observed_from = "2020-01-01T13:00:00Z".into();
                snapshot.observed_until = "2020-01-01T13:00:01Z".into();
            }
            _ => (),
        }
        assert!(source.partition_is_consistent(), "{case}");
        let captured_at = if case == "future" { "2020-01-01T13:00:00Z" } else { "2020-01-01T10:00:00Z" };
        let retained = capture(source, rows, 92, captured_at, EvidenceFamily::OperationalSamples).await;
        assert!(retained.is_read_complete(), "{case}: {:?}", retained.gaps());
        let catalog = retained.verified_original().unwrap();
        let context = CatalogContext::from_originals(&primary, &[&primary, &catalog]).unwrap();
        assert!(context.inputs().is_empty(), "{case}");
        let expected = match case {
            "partial" => DerivedContextIssue::PartialCatalog,
            "future" => DerivedContextIssue::CatalogNotRetainedAsOf,
            _ => DerivedContextIssue::UnqualifiedCatalogScope,
        };
        assert!(context.issues().contains(&expected), "{case}: {:?}", context.issues());
    }
}
