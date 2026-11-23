//! Native-shaped MI controls reuse the existing Settings script, frame, decoder and capture owner.
use super::*;

const EDITION: i64 = crate::probe::ENGINE_EDITION_MANAGED_INSTANCE;

fn replace_response(rows: &mut [ReadResult<Vec<Rec>>], index: usize, fields: &[(&str, Val)]) {
    let Ok(body) = &mut rows[index] else { panic!("original successful native phase") };
    let [row] = body.as_mut_slice() else { panic!("one native control row") };
    *row = replace(row, fields);
}
fn responses(major: u32) -> Vec<ReadResult<Vec<Rec>>> {
    let mut rows = replies();
    replace_response(&mut rows, 0, &[("edition", Val::Int(EDITION))]);
    for index in [1, 14] {
        replace_response(&mut rows, index, &[("edition", Val::Int(EDITION)), ("major", Val::Text(major.to_string()))]);
    }
    for index in [3, 16] {
        replace_response(
            &mut rows,
            index,
            &[
                ("edition", Val::Int(EDITION)),
                ("major", Val::Int(i64::from(major))),
                ("server_version", Val::Text(format!("{major}.0.2000.8"))),
                ("incarnation", Val::Null),
                ("physical_incarnation", Val::Null),
                ("replica_id", Val::Null),
            ],
        );
    }
    rows[6] = Ok(vec![framed(replace(&properties(), &[("native_engine_edition", Val::Int(EDITION))]))]);
    rows[12] = Ok(vec![framed(replace(&crate::settings::tests::options(), &[("engine_edition", Val::Int(EDITION))]))]);
    rows
}
fn source(rows: Vec<ReadResult<Vec<Rec>>>, request: &CollectionRequest) -> StateSource<Reader> {
    let mut source = settings_source(rows, request);
    // Pay the actual existing session SET before the reader's17 native phases; never a synthetic filler query.
    let native = ReadSession::budget_after_setup(settings_budget(&request.budget)).unwrap();
    source.io.inner = source.io.inner.with_query_limit(native.max_queries);
    source
}

#[tokio::test]
async fn mi_current_settings_preserve_database_and_server_scopes_in_eighteen_total_phases() {
    for major in [12, 16] {
        let requested = request();
        let mut source = source(responses(major), &requested);
        let original = source.describe().await.unwrap();
        assert!(original.matches_request(&requested));
        assert!(original.target.database().unwrap().incarnation.is_none());
        let context = NativeReadContext::from_snapshot(&original).unwrap();
        assert!(context.has_qualified_local_owner());
        let SourcePartition::InstanceLocal { member: Some(instance), epoch: Some(epoch) } = &original.partition else {
            panic!()
        };
        assert_eq!(instance.kind, SharedResourceKind::Instance);
        assert!(valid_digest(epoch));
        let snapshot = original.observation.snapshot().unwrap();
        assert_eq!(snapshot.sources.len(), crate::settings::Kind::ALL.len());
        assert!(snapshot.sources.iter().all(|source| source.state == SourceState::Collected));
        let records = drain(&mut source).await;
        let capture = CaptureIdentity { observation_id: format!("original-mi-settings-{major}"), revision: None };
        for record in &records {
            let exported = ExportedRecord::new(&original, &capture, record.clone()).unwrap();
            assert_eq!(
                serde_json::from_slice::<ExportedRecord>(&serde_json::to_vec(&exported).unwrap()).unwrap(),
                exported
            );
        }
        let values = records
            .iter()
            .map(|record| match record {
                TelemetryRecord::Setting(value) => value,
                _ => panic!("original native Setting row"),
            })
            .collect::<Vec<_>>();
        let server = values.iter().find(|value| value.namespace == "sys.configurations").unwrap();
        assert_eq!(server.scope, MeasurementScope::Server);
        assert_eq!(server.value.as_deref(), Some("8"));
        assert_eq!(server.value_in_use.as_deref(), Some("0"));
        let scoped = values.iter().find(|value| value.namespace == "sys.database_scoped_configurations").unwrap();
        assert_eq!(scoped.scope, MeasurementScope::Database);
        assert_eq!(scoped.value_for_secondary, None, "native NULL is not current or primary value");
        let query_store = values.iter().find(|value| value.name == "actual_state_desc").unwrap();
        assert_eq!(query_store.namespace, "sys.database_query_store_options");
        // A writable database may still have a read-only Query Store; preserve both raw states.
        assert_eq!(query_store.value.as_deref(), Some("READ_ONLY"));
        assert_eq!(query_store.nature, SettingNature::State);
        assert_eq!(
            values.iter().find(|value| value.name == "desired_state_desc").unwrap().value.as_deref(),
            Some("READ_WRITE")
        );
        assert_eq!(
            values.iter().find(|value| value.name == "readonly_reason").unwrap().value.as_deref(),
            Some("65544")
        );
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count() + ReadSession::SESSION_SETUP_QUERIES, super::super::MAX_QUERIES);
        assert_eq!(
            source.io.inner.calls.iter().filter(|call| call.sql.contains("AS culprit_settings_frame")).count(),
            4
        );
        assert!(!source.io.inner.calls.iter().any(
            |call| call.sql.contains("dm_user_db_resource_governance") || call.sql.contains("dm_db_resource_stats")
        ));
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.io.inner.closed, 1);
    }
}

#[tokio::test]
async fn mi_missing_recovery_process_or_known_primary_role_refuses_before_any_setting_batch() {
    for (field, value) in [
        ("database_guid", Val::Null),
        ("recovery_fork_guid", Val::Null),
        ("database_file_guid", Val::Null),
        ("updateability", Val::Null),
        ("updateability", Val::Text("READ_ONLY".into())),
    ] {
        let mut rows = responses(12);
        replace_response(&mut rows, 3, &[(field, value)]);
        let mut source = source(rows, &request());
        assert!(source.describe().await.is_err(), "{field}");
        assert_eq!(source.query_count(), 4);
        assert!(source.io.inner.calls.iter().all(|call| !call.sql.contains("culprit_settings_frame")));
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
    let mut rows = responses(12);
    rows[2] = Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "native process epoch denied")));
    let mut source = source(rows, &request());
    assert!(source.describe().await.is_err());
    assert_eq!(source.query_count(), 4, "a completed denied epoch leaves the ordinary authority unqualified");
    assert!(source.io.inner.calls.iter().all(|call| !call.sql.contains("culprit_settings_frame")));
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn mi_native_permission_or_null_family_preserves_independent_raw_values_without_a_healthy_default() {
    for denied in [false, true] {
        let mut rows = responses(12);
        rows[8] = if denied {
            Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "native scoped catalog denied")))
        } else {
            Ok(vec![framed(replace(
                &crate::settings::tests::scoped(),
                &[("value_text", Val::Null), ("is_value_default", Val::Null)],
            ))])
        };
        let mut source = source(rows, &request());
        let original = source.describe().await.unwrap();
        let sources = &original.observation.snapshot().unwrap().sources;
        assert_eq!(sources[1].state, if denied { SourceState::Unavailable } else { SourceState::Collected });
        assert_eq!(sources[1].error_code, denied.then_some(ErrorCode::PermissionDenied));
        assert_eq!(sources[2].state, SourceState::Collected);
        let records = drain(&mut source).await;
        if !denied {
            let TelemetryRecord::Setting(value) = records.iter().find(|record| matches!(record, TelemetryRecord::Setting(value) if value.namespace == "sys.database_scoped_configurations")).unwrap() else { panic!() };
            assert_eq!(value.value, None);
            assert_eq!(value.native_is_default, None);
            assert!(value.to_mssql_setting().is_none());
        }
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn mi_foreign_database_or_changed_role_in_any_catalog_frame_stops_before_siblings() {
    for (field, value) in [
        ("culprit_settings_database_id", Val::Int(8)),
        ("culprit_settings_database_name", Val::Text("another-database".into())),
        ("culprit_settings_updateability", Val::Text("READ_ONLY".into())),
    ] {
        let mut rows = responses(12);
        rows[8] = Ok(vec![framed_with(crate::settings::tests::scoped(), replace(&control(), &[(field, value)]))]);
        let mut source = source(rows, &request());
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
        assert_eq!(source.query_count(), 9);
        assert!(source.read_page(TelemetryStream::SettingObservations, None).await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn mi_recreated_database_restarted_process_changed_role_or_retired_final_lease_cannot_verify() {
    for change in ["database", "fork", "process", "role", "socket"] {
        let mut rows = responses(12);
        match change {
            "database" => replace_response(&mut rows, 16, &[("database_guid", Val::Text("recreated-database".into()))]),
            "fork" => replace_response(&mut rows, 16, &[("recovery_fork_guid", Val::Text("new-fork".into()))]),
            "process" => rows[15] = Ok(vec![epoch(101)]),
            "role" => replace_response(&mut rows, 16, &[("updateability", Val::Text("READ_ONLY".into()))]),
            "socket" => {}
            _ => unreachable!(),
        }
        let mut source = source(rows, &request());
        if change == "socket" {
            source.io.change_at = Some(15);
        }
        source.describe().await.unwrap();
        drain(&mut source).await;
        assert!(!matches!(source.finish().await, Ok(SourceCompletion::Verified)), "{change}");
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn mi_cancellation_and_stricter_query_caps_cannot_skip_final_native_authority() {
    for stop in [CollectionStop::Cancelled, CollectionStop::CancellationUnconfirmed, CollectionStop::TimedOut] {
        let mut rows = responses(12);
        rows[8] = Err(ReadError::Stopped(stop));
        let mut source = source(rows, &request());
        assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stop);
        assert_eq!(source.query_count(), 9);
        source.io.inner.shutdown = if stop == CollectionStop::CancellationUnconfirmed {
            SourceShutdown::Unconfirmed
        } else {
            SourceShutdown::Quiescent
        };
        let shutdown = source.io.inner.shutdown;
        assert_eq!(source.close().await, shutdown);
        assert_eq!(source.io.inner.closed, 1);
    }
    let mut request = request();
    request.budget.max_queries = super::super::MAX_QUERIES - 1;
    let mut source = source(responses(12), &request);
    source.describe().await.unwrap();
    drain(&mut source).await;
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count() + ReadSession::SESSION_SETUP_QUERIES, request.budget.max_queries);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}

#[tokio::test]
async fn a_settings_family_that_loses_its_socket_preserves_the_error_and_never_dispatches_sibling_pressure() {
    for mi in [false, true] {
        let mut rows = if mi { responses(12) } else { replies() };
        rows[8] = Err(ReadError::Source(Error::new(ErrorCode::PermissionDenied, "original native catalog denial")));
        let mut source = source(rows, &request());
        source.io.change_at = Some(9);
        assert_eq!(source.describe().await.unwrap_err().code, ErrorCode::PermissionDenied);
        assert_eq!(source.query_count(), 9, "retired original cannot reopen to admit the next catalog");
        assert!(source.read_page(TelemetryStream::SettingObservations, None).await.is_err());
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        for retired_after in [5, 6, 9, 14] {
            let mut source = self::source(if mi { responses(12) } else { replies() }, &request());
            source.io.change_at = Some(retired_after);
            assert_eq!(
                CollectionStop::from_error(&source.describe().await.unwrap_err()),
                CollectionStop::SourceChanged
            );
            assert_eq!(source.query_count(), retired_after, "no dispatch after lost native settings lease");
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
        }
    }
}
