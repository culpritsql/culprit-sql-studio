use super::*;
use crate::{
    activity::database_context::tests::row as authority,
    instance::test_support::{epoch, metadata},
    query_store::options::tests::row,
};
use culprit_core::{Error, ErrorCode, telemetry::*};

fn framed(row: &Rec) -> Rec {
    let mut columns = row.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    columns.extend([
        ("culprit_settings_frame", Val::Int(1)),
        ("culprit_settings_database_id", Val::Int(7)),
        ("culprit_settings_database_name", Val::Text("shop".into())),
        ("culprit_settings_updateability", Val::Text("READ_WRITE".into())),
    ]);
    Rec::of(&columns)
}

pub(crate) fn assert_row_contract(context: &NativeReadContext, values: &[SettingObservation], name: &str) {
    let description = CollectionDescription {
        source: "settings-state".into(),
        target: context.target.clone(),
        partition: context.partition.clone(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Snapshot(StateSnapshotDescription {
            dataset: SnapshotDataset::Settings,
            observed_from: context.observed_from.clone(),
            observed_until: context.observed_until.clone(),
            records: values.len().try_into().unwrap(),
            sources: vec![StateSourceCoverage::collected(
                crate::sources::QUERY_STORE_SETTINGS.name.into(),
                values.len().try_into().unwrap(),
            )],
        }),
    };
    assert!(description.target.database().unwrap().incarnation.is_none());
    let capture = CaptureIdentity { observation_id: name.into(), revision: None };
    for value in values {
        let original = ExportedRecord::new(&description, &capture, TelemetryRecord::Setting(value.clone())).unwrap();
        assert_eq!(
            serde_json::from_slice::<ExportedRecord>(&serde_json::to_vec(&original).unwrap()).unwrap(),
            original
        );
    }
}

#[tokio::test]
async fn scan_reuses_the_probe_read_and_matches_the_single_dispatch_retained_decoder() {
    for (waits, policy) in [(false, false), (true, false), (false, true), (true, true)] {
        let native = row(waits, policy);
        let mut probe_reader = FakeRead::new(vec![
            Ok(vec![Rec::of(&[
                ("version", Val::Text("16.0.4135.4".into())),
                ("major", Val::Text("16".into())),
                ("engine_edition", Val::Int(3)),
                ("database_name", Val::Text("sample".into())),
            ])]),
            Ok(vec![Rec::of(&[("started_at", Val::Text("2020-01-01T04:00:00Z".into()))])]),
            Ok(vec![native.clone()]),
        ]);
        let probe = crate::probe::run(&mut probe_reader).await.unwrap();
        assert_eq!(probe_reader.queries(), 3, "the existing probe uses the same one options dispatch");
        assert_eq!(probe_reader.calls[2].sql, crate::query_store::catalog_sql::options());
        let options = probe.query_store.unwrap();
        assert!(options.readable());
        let mut scan_reader = FakeRead::new(vec![Ok(vec![database()]), Ok(vec![scoped()]), Ok(vec![server()])]);
        let mut coverage = Coverage::default();
        let scan =
            collect_with(&mut scan_reader, target(), false, Some(&options.options), &mut coverage, "test grant").await;
        assert_eq!(scan_reader.queries(), 3, "scan must reuse Probe options without another native catalog read");
        assert_eq!(scan_reader.closed, 1);
        assert!(scan_reader.calls.iter().all(|call| !call.sql.contains("FROM sys.database_query_store_options o")));
        let mut retained_reader = FakeRead::new(vec![Ok(vec![native])]);
        let retained = read(&mut retained_reader, Kind::QueryStore, 512).await.unwrap();
        assert_eq!(retained_reader.queries(), 1);
        let projected = retained.iter().filter_map(application).collect::<Vec<_>>();
        let from_scan = scan
            .settings
            .into_iter()
            .filter(|value| value.name.starts_with("query_store_options."))
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::to_value(from_scan).unwrap(),
            serde_json::to_value(projected).unwrap(),
            "both consumers must retain the same raw native option values"
        );
        let outcomes = coverage.outcomes();
        let outcome =
            outcomes.iter().find(|outcome| outcome.source == crate::sources::QUERY_STORE_SETTINGS.name).unwrap();
        assert_eq!(outcome.state, SourceState::Collected);
    }
}

#[tokio::test]
async fn framed_options_preserve_the_same_decoder_and_canonical_local_owner_without_control_settings() {
    for (waits, policy) in [(false, false), (true, false), (false, true), (true, true)] {
        let native = row(waits, policy);
        let mut reader = FakeRead::new(vec![
            Ok(vec![metadata()]),
            Ok(vec![epoch(100)]),
            Ok(vec![authority(3)]),
            Ok(vec![framed(&native)]),
            Ok(vec![metadata()]),
            Ok(vec![epoch(100)]),
            Ok(vec![authority(3)]),
        ])
        .with_generation(1);
        let before = crate::local_source::Context::read(&mut reader, "fixture.invalid", 1433).await.unwrap();
        let values = read_current(&mut reader, Kind::QueryStore, 512, &before).await.unwrap();
        let after = before.recheck(&mut reader, "fixture.invalid", 1433).await.unwrap();
        let context = before.completed(&after).unwrap();
        assert_eq!(values, query_store_options(&native).unwrap());
        assert!(values.iter().all(|value| !value.name.starts_with("culprit_settings_")));
        assert!(context.has_qualified_local_owner());
        assert_eq!(reader.queries(), 7, "one selected catalog read between original and final source context");
        assert_eq!(reader.calls[3].parameters.len(), 0, "the fixed batch binds native frame variables internally");
        assert!(reader.calls[3].sql.contains(frame::EXECUTION_PARAMETERS));
        assert_row_contract(&context, &values, "query-store-settings-options");
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn native_failures_ambiguity_and_caller_limits_do_not_retry_or_replace_unavailable_options() {
    for error in [
        ReadError::Source(Error::new(ErrorCode::PermissionDenied, "native catalog denied")),
        ReadError::Stopped(CollectionStop::Cancelled),
        ReadError::Stopped(CollectionStop::ByteLimit),
        ReadError::Stopped(CollectionStop::TimedOut),
    ] {
        let expected = match &error {
            ReadError::Source(error) => error.code,
            ReadError::Stopped(stop) => stop.error().code,
        };
        let mut reader = FakeRead::new(vec![Err(error)]);
        assert_eq!(read(&mut reader, Kind::QueryStore, 512).await.unwrap_err().code, expected);
        assert_eq!(reader.queries(), 1);
        assert_eq!(reader.closed, 0, "the existing caller retains close/debt ownership");
    }
    for rows in [vec![], vec![row(true, true), row(true, true)]] {
        let mut reader = FakeRead::new(vec![Ok(rows)]);
        assert!(read(&mut reader, Kind::QueryStore, 512).await.is_err());
        assert_eq!(reader.queries(), 1);
    }
    let mut reader = FakeRead::new(vec![Ok(vec![row(true, true)])]);
    let error = read(&mut reader, Kind::QueryStore, 1).await.unwrap_err();
    assert_eq!(CollectionStop::from_error(&error), CollectionStop::RowLimit);
    assert_eq!(reader.queries(), 1);
    assert_eq!(reader.calls[0].limit, 2, "the caller's tighter native row allowance is unchanged");
}

#[test]
fn declared_null_is_preserved_but_missing_or_duplicate_option_aliases_are_invalid() {
    let native = row(true, true);
    let mut missing = native.clone();
    missing.retain_columns(|name| name != "readonly_reason");
    assert!(query_store_options(&missing).is_err(), "missing native projection is not SQL NULL");
    let mut columns = native.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    columns.push(("readonly_reason", Val::Int(0)));
    assert!(query_store_options(&Rec::of(&columns)).is_err(), "duplicate aliases cannot choose native evidence");
    let null = replace(&native, &[("readonly_reason", Val::Null)]);
    let values = query_store_options(&null).unwrap();
    assert_eq!(values.len(), 21);
    assert_eq!(values.iter().find(|value| value.name == "readonly_reason").unwrap().value, None);
    let mut columns = native.columns().map(|(name, value)| (name, value.clone())).collect::<Vec<_>>();
    columns.push(("future_usage_counter", Val::Int(8)));
    columns.push(("culprit_settings_frame", Val::Int(1)));
    assert_eq!(
        query_store_options(&Rec::of(&columns)).unwrap(),
        query_store_options(&native).unwrap(),
        "undeclared layout, control and future fields cannot impersonate settings"
    );
}
