use crate::{
    analytical::{
        RowProjection,
        evidence::{EvidenceFamily, EvidenceReadBudget, EvidenceReader},
        indexes::ProjectionIndexes,
        projection::ScalarType,
        read_store::AnalyticalReadStore,
        test_support::{self, InstanceResourceCase, ReadOnlyFixture},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::reopen},
};
use std::sync::Arc;

#[tokio::test]
async fn read_only_root_index_manifest_and_projection_preserve_the_exact_original() {
    let (source, rows) = test_support::instance_resource_evidence(InstanceResourceCase::Exact);
    let fixture = ReadOnlyFixture::capture(source.clone(), rows.clone()).await;
    let before = fixture.objects.objects.lock().unwrap().clone();
    let recovery = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let catalog = fixture.store.catalog();
    let publication = fixture.store.publication_rows(&range, 64 * 1024).await.unwrap();
    let schema = catalog.publication();
    let expected = schema.project(&crate::analytical::publication::input(catalog.as_ref(), &range).unwrap()).unwrap();
    assert_eq!(
        schema.returned_digest(&publication.rows[0]).unwrap(),
        crate::analytical::projection::digest(&expected).unwrap(),
        "publication proof must bind this actual recovery reader's store identity"
    );
    let reader = EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport())));
    let captured = reader
        .read_capture(&range, &[EvidenceFamily::OperationalSamples], EvidenceReadBudget::default(), None)
        .await
        .unwrap();
    assert!(captured.is_read_complete(), "{:?}", captured.gaps());
    assert_eq!(captured.source(), &source);
    assert_eq!(captured.capture(), &fixture.manifest.capture);
    assert_eq!(captured.capture_manifest_sha256(), fixture.manifest.capture_manifest_sha256);
    assert_eq!(captured.records().len(), rows.len());
    for (index, (actual, row)) in captured.records().iter().zip(rows).enumerate() {
        assert_eq!(actual.row_ordinal() as usize, index + 1);
        assert_eq!(actual.row().record, row);
    }
    assert_eq!(*fixture.objects.objects.lock().unwrap(), before);
}

#[tokio::test]
async fn root_store_identity_and_immutable_manifest_corruption_are_not_read_capabilities() {
    let (source, rows) = test_support::instance_resource_evidence(InstanceResourceCase::Exact);
    let fixture = ReadOnlyFixture::capture(source, rows).await;
    let recovery = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    {
        let mut stored = fixture.objects.objects.lock().unwrap();
        let key = stored.keys().find(|key| key.starts_with("manifests/")).unwrap().clone();
        stored.get_mut(&key).unwrap().push(b' ');
    }
    assert!(reopen(&recovery, &fixture.manifest.range_id).await.is_err());
    {
        let mut stored = fixture.objects.objects.lock().unwrap();
        let root = stored.get_mut("recovery.json").unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(root).unwrap();
        value["store_id"] = serde_json::json!(culprit_core::telemetry::digest(b"foreign store"));
        *root = serde_json::to_vec(&value).unwrap();
    }
    assert!(reopen(&recovery, &fixture.manifest.range_id).await.is_err());
}

#[tokio::test]
async fn read_only_fixture_uses_encoded_physical_cells_and_bare_json_containers_cannot_verify() {
    const READBACK_BYTES: usize = 64 * 1024;
    let (source, rows) = test_support::instance_resource_evidence(InstanceResourceCase::Exact);
    let fixture = ReadOnlyFixture::capture(source, rows).await;
    let recovery = ObjectRecoveryReader::open(fixture.transport(), fixture.contract.clone()).await.unwrap();
    let range = reopen(&recovery, &fixture.manifest.range_id).await.unwrap();
    let catalog = fixture.store.catalog();
    let publication = fixture.store.publication_rows(&range, READBACK_BYTES).await.unwrap();
    assert_eq!(publication.rows.len(), 1);
    assert!(physical_container_cells(catalog.publication(), &publication.rows[0]) > 0);
    let mut native_containers = 0;
    for batch in range.manifest().batches.iter().filter(|batch| batch.delivery_required) {
        let page = fixture.store.rows(&range, &batch.batch_id, 0, batch.rows.len(), READBACK_BYTES).await.unwrap();
        assert_eq!(page.rows.len(), batch.rows.len());
        let schema = catalog.schema(batch.stream).unwrap();
        for row in &page.rows {
            native_containers += physical_container_cells(schema, row);
        }
    }
    assert!(native_containers > 0, "the original operational body must exercise a non-null JSON cell");
}

fn physical_container_cells(schema: &dyn RowProjection, row: &[serde_json::Value]) -> usize {
    assert!(schema.returned_digest(row).is_ok());
    let mut containers = 0;
    for (index, column) in schema.columns().iter().enumerate() {
        if column.kind != ScalarType::JsonContainer || row[index].is_null() {
            continue;
        }
        let text = row[index].as_str().expect("physical JSON cells retain encoded text");
        let mut bare = row.to_vec();
        bare[index] = serde_json::from_str(text).unwrap();
        assert!(bare[index].is_object() || bare[index].is_array());
        assert!(schema.returned_digest(&bare).is_err(), "a logical value is not a returned physical cell");
        containers += 1;
    }
    containers
}
