//! Genuine existing FILE_CAPACITY acquisition; no application workload or cost qualification.
use super::*;
use crate::test_support::{owned_captures::OwnedCaptures, receipt_directory, save_cost_receipt};

const FILE_ORACLE: &str = r#"SELECT /* culprit fixture */ DB_ID() AS database_id,
 SUM(CASE WHEN type=0 THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END) AS data_files,
 SUM(CASE WHEN type=1 THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END) AS log_files,
 SUM(CASE WHEN type=0 THEN CONVERT(bigint,size)*8192 ELSE CONVERT(bigint,0) END) AS data_bytes,
 SUM(CASE WHEN type=1 THEN CONVERT(bigint,size)*8192 ELSE CONVERT(bigint,0) END) AS log_catalog_bytes
 FROM sys.database_files OPTION(MAXDOP 1)"#;
struct FileOracle {
    database_id: String,
    data_files: u32,
    log_files: u32,
    data_bytes: String,
    log_catalog_bytes: String,
}
async fn oracle(provider: &MssqlProvider) -> FileOracle {
    let mut reader = Box::new(
        ReadSession::collection(
            provider.pool.clone(),
            CollectionBudget {
                max_queries: 2,
                max_snapshot_rows: 1,
                page_rows: 1,
                page_bytes: 8192,
                total_bytes: 8192,
                statement_timeout_ms: 1000,
                total_timeout_ms: 5000,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let result = Box::pin(reader.query(FILE_ORACLE, vec![], 1)).await;
    let queries = reader.queries();
    let shutdown = Box::pin(reader.close()).await;
    assert_eq!(shutdown, SourceShutdown::Quiescent);
    assert_eq!(queries, 1, "the fixture oracle has its own reader1 + SET1 allowance");
    let rows = result.unwrap();
    let [row] = rows.as_slice() else { panic!("one native selected-database file oracle") };
    let unsigned = |name| u32::try_from(integer(row, name).unwrap()).unwrap();
    FileOracle {
        database_id: integer(row, "database_id").unwrap().to_string(),
        data_files: unsigned("data_files"),
        log_files: unsigned("log_files"),
        data_bytes: integer(row, "data_bytes").unwrap().to_string(),
        log_catalog_bytes: integer(row, "log_catalog_bytes").unwrap().to_string(),
    }
}

#[tokio::test]
#[ignore = "requires the positively nonce-owned loopback SQL Server fixture and private receipt directory"]
async fn owned_database_files_retain_real_local_generation_without_a_global_incarnation_or_extra_families() {
    Box::pin(capture_native_file_census()).await;
}
async fn capture_native_file_census() {
    let fixture = Box::pin(OwnedSourceFixture::open("culprit")).await.unwrap();
    let member = provider(fixture.port, &fixture.database, fixture.password());
    let expected = Box::pin(oracle(&member)).await;
    assert!(expected.data_files > 0 && expected.log_files > 0, "owned fixture must expose real data and log files");
    let mut originals = Box::new(OwnedCaptures::from_pool(member.pool.clone(), fixture.port).unwrap());
    let captured = Box::pin(originals.capture(CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational }))
        .await
        .unwrap();
    Box::pin(originals.verify_retained(&captured)).await.unwrap();
    assert_eq!(member.source_shutdown(), SourceShutdown::Quiescent);
    assert_eq!(captured.manifest.status, CollectionStatus::Complete);
    assert_eq!(captured.manifest.source_queries, 11, "existing profile/context/clocks/admission/files/final roster");
    let source = captured.source().unwrap();
    let context = NativeReadContext::from_snapshot(source).unwrap();
    assert!(context.has_qualified_local_owner());
    assert!(source.target.database().unwrap().incarnation.is_none());
    assert_eq!(context.database_writable, None, "original Snapshot does not borrow an acquisition role");
    let header = source.observation.snapshot().unwrap();
    let files = header.sources.iter().find(|source| source.source == crate::sources::FILE_CAPACITY.name).unwrap();
    assert_eq!(files.state, SourceState::Collected);
    assert!(files.complete_scope && files.records > 0);
    assert!(header.sources.iter().filter(|source| source.source != crate::sources::FILE_CAPACITY.name)
        .all(|source| source.state == SourceState::Unavailable && source.error_code == Some(ErrorCode::Unsupported)));
    let mut rows = Vec::new();
    for (position, record) in captured.records.iter().enumerate() {
        captured.reference(position).unwrap().validate_canonical_record(source, &record.record).unwrap();
        let TelemetryRecord::Operational(row) = &record.record else { panic!("native original file row") };
        assert_eq!(row.source, crate::sources::FILE_CAPACITY.name);
        rows.push(row.clone());
    }
    let read = StorageRead::from_source(source, rows, true).unwrap();
    let report = culprit_checks::storage::explain(&read).unwrap();
    assert_eq!(report.source_state, SourceState::Partial, "uncollected DMV log capacity stays explicit");
    let summary = report.file_summary.as_ref().unwrap();
    assert_eq!(summary.source_state, SourceState::Collected);
    assert_eq!(summary.native_database_id.as_deref(), Some(expected.database_id.as_str()));
    assert_eq!(summary.data_file_count, Some(expected.data_files));
    assert_eq!(summary.log_file_count, Some(expected.log_files));
    assert_eq!(summary.data_allocated_bytes.as_deref(), Some(expected.data_bytes.as_str()));
    assert_eq!(summary.log_catalog_allocated_bytes.as_deref(), Some(expected.log_catalog_bytes.as_str()));
    save_cost_receipt(
        &receipt_directory(),
        "boxed-database-file-census",
        &serde_json::json!({
            "manifest": captured.manifest,
            "records": captured.records,
            "storage": report,
            "source_reader_queries": 11,
            "source_setup_query_allowance": 1,
            "independent_fixture_oracle_reader_queries": 1,
            "independent_fixture_oracle_setup_query_allowance": 1,
            "total_paid_query_allowance": 14,
            "oracle_clock_is_not_the_original_source_clock": true,
            "catalog_log_allocation_is_not_effective_log_capacity": true,
            "native_cost_approved": false,
            "automatic_qualification": false,
        }),
    )
    .unwrap();
}

#[test]
fn actual_owned_file_capture_future_has_a_bounded_inline_frame() {
    fn assert_bounded<F: std::future::Future>(_: impl FnOnce() -> F) {
        const MAX_INLINE_PHASE_BYTES: usize = 4096;
        assert!(
            std::mem::size_of::<F>() <= MAX_INLINE_PHASE_BYTES,
            "actual file-capture future stores {} bytes inline",
            std::mem::size_of::<F>()
        );
    }
    assert_bounded(capture_native_file_census);
}
