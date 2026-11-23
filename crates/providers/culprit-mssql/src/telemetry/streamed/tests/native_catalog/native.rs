//! Real catalog readers and canonical reopening on the nonce-owned seeded loopback SQL Server only.
use crate::test_support::{
    OwnedSourceFixture, owned_captures::OwnedCaptures, owned_case, receipt_directory, save_cost_receipt,
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use std::time::Duration;

const CASE_TIMEOUT: Duration = Duration::from_secs(45);
const READER_QUERY_CAP: u32 = 12;

#[tokio::test]
#[ignore = "requires the nonce-owned loopback SQL16 supervisor and private receipt directory; correctness only, no production cost or automatic admission"]
async fn every_native_catalog_reopens_the_completed_original_with_the_same_local_owner_and_caller_budget() {
    let cancellation = CollectionCancellation::default();
    let deadline = tokio::time::Instant::now() + CASE_TIMEOUT;
    let (expired, result) = owned_case::finish_case(deadline, &cancellation, prove(&cancellation)).await;
    result.expect("owned native catalog capture and reopening");
    assert!(!expired, "owned native catalog body exceeded its deadline");
}

async fn prove(cancellation: &CollectionCancellation) -> Result<()> {
    let fixture = OwnedSourceFixture::open("native-catalog-correctness").await?;
    prepare_catalog_fixture(&fixture).await?;
    let mut captures = OwnedCaptures::from_pool(fixture.pool, fixture.port)?;
    let datasets = [
        SnapshotDataset::Tables,
        SnapshotDataset::Columns,
        SnapshotDataset::Indexes,
        SnapshotDataset::IndexOperations,
        SnapshotDataset::Statistics,
        SnapshotDataset::ForeignKeys,
        SnapshotDataset::CheckConstraints,
        SnapshotDataset::Objects,
        SnapshotDataset::Triggers,
        SnapshotDataset::Partitions,
        SnapshotDataset::MissingIndexes,
    ];
    let mut observations = Vec::new();
    for dataset in datasets {
        owned_case::require_active(cancellation)?;
        eprintln!("owned native catalog phase: {dataset:?}");
        let captured =
            captures.capture_with_cancellation(CollectionSelection::Snapshot { dataset }, cancellation).await?;
        let source = captured.source()?;
        let context = NativeReadContext::from_snapshot(source)?;
        assert_eq!(captured.manifest.status, CollectionStatus::Complete, "{dataset:?}");
        assert!(context.has_qualified_local_owner(), "{dataset:?}");
        assert_eq!(context.database_writable, Some(true), "{dataset:?}");
        assert!(source.target.database().is_some_and(|database| database.incarnation.is_none()));
        assert!(matches!(source.observation, SourceObservation::LocalResource { .. }));
        assert!(captured.manifest.source_queries <= READER_QUERY_CAP, "{dataset:?}");
        let metrics = captured.manifest.read_metrics.as_ref().expect("native owner settlement metrics");
        assert!(!metrics.has_unconfirmed_work(), "{dataset:?}");
        assert_eq!(metrics.active_tasks, 0);
        assert_eq!(metrics.untracked_tasks, 0);
        if dataset != SnapshotDataset::MissingIndexes {
            assert!(!captured.records.is_empty(), "seeded native {dataset:?} was unexpectedly empty");
        }
        captures.verify_retained(&captured).await?;
        observations.push(serde_json::json!({
            "dataset": dataset,
            "source": source,
            "manifest": captured.manifest,
            "records": captured.records.len(),
        }));
    }
    save_cost_receipt(
        &receipt_directory(),
        "mssql-native-catalogs-correctness",
        &serde_json::json!({
            "observations": observations,
            "reader_query_cap_per_capture": READER_QUERY_CAP,
            "fixture_setup_dispatches": 1,
            "total_query_cap_per_capture_including_setup": READER_QUERY_CAP + 1,
            "qualification": "unqualified",
            "native_cost_qualified": false,
            "automatic_admission": false,
            "limitations": "SQL16 edition3 writable fixture only; not MI/Azure/readable/cardinality/load/cancellation cost qualification",
        }),
    )
}

async fn prepare_catalog_fixture(fixture: &OwnedSourceFixture) -> Result<()> {
    use crate::{
        conn::SessionKind,
        test_support::{config, connect},
    };
    const SETUP: &str = "/* culprit fixture */
        EXEC(N'CREATE SCHEMA culprit_catalog_native');
        CREATE TABLE culprit_catalog_native.parent(id int NOT NULL PRIMARY KEY,CONSTRAINT native_parent_check CHECK(id>0));
        CREATE TABLE culprit_catalog_native.child(id int NOT NULL PRIMARY KEY,parent_id int NOT NULL,
            CONSTRAINT native_child_parent FOREIGN KEY(parent_id) REFERENCES culprit_catalog_native.parent(id));
        CREATE TABLE culprit_catalog_native.heap(id int NOT NULL);
        CREATE PARTITION FUNCTION culprit_native_function(int) AS RANGE RIGHT FOR VALUES(10);
        CREATE PARTITION SCHEME culprit_native_scheme AS PARTITION culprit_native_function ALL TO([PRIMARY]);
        CREATE TABLE culprit_catalog_native.partitioned(id int NOT NULL PRIMARY KEY CLUSTERED) ON culprit_native_scheme(id);
        EXEC(N'CREATE TRIGGER culprit_catalog_native.child_trigger ON culprit_catalog_native.child AFTER INSERT AS BEGIN SET NOCOUNT ON; END');";
    let mut client = connect(
        &config(fixture.port, &fixture.database, fixture.password(), "native-catalog-fixture-setup"),
        SessionKind::User,
    )
    .await?;
    let result = crate::conn::query(&mut client, SETUP).await.map_err(crate::conn::map_error);
    let retired = owned_case::retire(client).await;
    result?;
    retired
}
