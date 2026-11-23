//! Pool allocation reuses the existing native tempdb reader under the same held and rechecked pool owner.
use super::*;
use crate::operations::native::{self, Kind};

fn allocation(rows: &[TelemetryRecord]) -> &OperationalSpace {
    rows.iter()
        .find_map(|row| match row {
            TelemetryRecord::Operational(OperationalObservation {
                source,
                fact: OperationalFact::Space(space),
                ..
            }) if source == culprit_core::source_id::TEMP_CAPACITY => Some(space),
            _ => None,
        })
        .unwrap()
}
#[tokio::test]
async fn pool_allocation_reuses_the_exact_native_sql_and_decoder_without_selected_database_attribution() {
    let mut source = scripted_source(replies(), 22);
    let original = source.describe().await.unwrap();
    let rows = drain(&mut source).await;
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 21);
    let space = allocation(&rows);
    assert_eq!(space.population, StoragePopulation::SharedTempdbDataFiles);
    assert_eq!(space.scope, culprit_core::FileScope::Server);
    assert_eq!(space.kind, culprit_core::FileKind::Temp);
    assert!(space.database.is_none() && space.native_name.is_none());
    assert_eq!(space.total_bytes.as_deref(), Some("1048576"));
    assert_eq!(space.used_bytes.as_deref(), Some("327680"));
    assert_eq!(space.parts.len(), 3);
    let mut native = FakeRead::new(vec![Ok(vec![tempdb_row()])]);
    let expected = native::read(&mut native, Kind::Temp, 10).await.unwrap().observations(Kind::Temp).unwrap();
    let captured = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::Operational(row) if row.source == culprit_core::source_id::TEMP_CAPACITY => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(serde_json::to_value(captured).unwrap(), serde_json::to_value(&expected[0]).unwrap());
    let calls = source
        .io
        .calls
        .iter()
        .filter(|call| call.sql.contains("tempdb.sys.dm_db_file_space_usage"))
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].sql, native.calls[0].sql);
    assert!(calls[0].parameters.is_empty());
    assert!(calls[0].sql.contains("CAST(NULL AS nvarchar(128)) AS database_name"));
    let read = StorageRead::from_source(&original, vec![captured.clone()], true).unwrap();
    assert!(read.is_valid());
    assert_eq!(read.target, original.target);
    assert_eq!(read.sources.len(), 1);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn native_null_allocations_remain_unknown_and_known_zero_remains_a_distinct_collected_point() {
    for value in [Val::Null, Val::Int(0)] {
        let mut data = replies();
        data[14] = Ok(vec![replace(
            &tempdb_row(),
            &[
                ("total", value.clone()),
                ("used", value.clone()),
                ("users", value.clone()),
                ("internals", value.clone()),
                ("versions", value.clone()),
            ],
        )]);
        let mut source = scripted_source(data, 22);
        let original = source.describe().await.unwrap();
        let rows = drain(&mut source).await;
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        let space = allocation(&rows);
        let expected = if matches!(value, Val::Null) { None } else { Some("0") };
        assert_eq!(space.total_bytes.as_deref(), expected);
        assert_eq!(space.used_bytes.as_deref(), expected);
        assert!(space.parts.iter().all(|part| part.bytes.as_deref() == expected));
        let coverage = original
            .observation
            .snapshot()
            .unwrap()
            .sources
            .iter()
            .find(|source| source.source == culprit_core::source_id::TEMP_CAPACITY)
            .unwrap();
        assert_eq!(coverage.state, SourceState::Collected);
        assert_eq!(coverage.records, 1);
        assert!(coverage.complete_scope);
        source.close().await;
    }
}
#[tokio::test]
async fn unavailable_tempdb_does_not_become_zero_and_malformed_or_mislabelled_rows_do_not_publish() {
    let mut data = replies();
    data[14] =
        Err(culprit_core::Error::new(culprit_core::ErrorCode::PermissionDenied, "scripted tempdb permission failure")
            .into());
    let mut source = scripted_source(data, 22);
    let original = source.describe().await.unwrap();
    let coverage = original
        .observation
        .snapshot()
        .unwrap()
        .sources
        .iter()
        .find(|source| source.source == culprit_core::source_id::TEMP_CAPACITY)
        .unwrap();
    assert_eq!(coverage.state, SourceState::Unavailable);
    assert_eq!(coverage.records, 0);
    assert!(!coverage.complete_scope);
    let rows = drain(&mut source).await;
    assert!(!rows.iter().any(
        |row| matches!(row, TelemetryRecord::Operational(row) if row.source == culprit_core::source_id::TEMP_CAPACITY)
    ));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    source.close().await;
    for rows in [vec![], vec![replace(&tempdb_row(), &[("used", Val::Float(1.0))])]] {
        let mut data = replies();
        data[14] = Ok(rows);
        let mut source = scripted_source(data, 22);
        let original = source.describe().await.unwrap();
        let header = original.observation.snapshot().unwrap();
        let coverage =
            header.sources.iter().find(|source| source.source == culprit_core::source_id::TEMP_CAPACITY).unwrap();
        assert_eq!(coverage.state, SourceState::Unavailable);
        assert_eq!(coverage.stopped, Some(CollectionStop::InvalidPage));
        assert_eq!(coverage.records, 0);
        assert!(!coverage.complete_scope);
        assert_eq!(header.coverage_state().unwrap(), SourceState::Partial);
        drain(&mut source).await;
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        source.close().await;
    }
    for row in [
        replace(&tempdb_row(), &[("database_name", Val::Text("fixture-db".into()))]),
        replace(&tempdb_row(), &[("observed_at", Val::Text("2020-01-01T05:59:59Z".into()))]),
    ] {
        let mut data = replies();
        data[14] = Ok(vec![row]);
        let mut source = scripted_source(data, 22);
        assert!(source.describe().await.is_err());
        assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
        source.close().await;
    }
}
#[tokio::test]
async fn tempdb_pressure_cancellation_and_reader_retirement_stop_before_final_owner_dispatch() {
    let mut data = replies();
    data[13] = Ok(vec![replace(&pool_pressure(), &[("avg_cpu_percent", Val::Float(95.0))])]);
    let mut source = scripted_source(data, 22);
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 14);
    assert!(!source.io.calls.iter().any(|call| call.sql.contains("tempdb.sys.dm_db_file_space_usage")));
    source.close().await;
    for stop in [
        CollectionStop::Cancelled,
        CollectionStop::TimedOut,
        CollectionStop::CancellationUnconfirmed,
        CollectionStop::ByteLimit,
    ] {
        let mut data = replies();
        data[14] = Err(crate::read::ReadError::Stopped(stop));
        let mut source = scripted_source(data, 22);
        let error = source.describe().await.unwrap_err();
        assert_eq!(CollectionStop::from_error(&error), stop);
        assert_eq!(source.query_count(), 15);
        assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
        source.close().await;
    }
    let mut data = replies();
    data[14] = Err(culprit_core::Error::new(
        culprit_core::ErrorCode::PermissionDenied,
        "scripted retiring tempdb read",
    )
    .into());
    let mut source = scripted_source(data, 22);
    source.io = source.io.change_generation_at(15);
    let error = source.describe().await.unwrap_err();
    assert_eq!(error.code, culprit_core::ErrorCode::PermissionDenied);
    assert_eq!(source.query_count(), 15);
    assert!(source.read_page(TelemetryStream::OperationalSamples, None).await.is_err());
    source.close().await;
}
