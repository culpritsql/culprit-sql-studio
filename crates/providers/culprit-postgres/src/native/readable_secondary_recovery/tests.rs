use super::*;
use crate::progress::test_support::{self as fixture, AT, BACKEND, REQUEST};
use serde_json::json;

pub(crate) fn actor_row() -> Row {
    fixture::row(&[
        ("observed_at", Some(AT)),
        ("pid", Some("42")),
        ("backend_start", Some(BACKEND)),
        ("backend_type", Some("client backend")),
        ("query_start", Some(REQUEST)),
        ("transaction_start", Some(REQUEST)),
        ("database_oid", Some("16384")),
        ("database", Some("shop")),
        ("state", Some("active")),
        ("elapsed_ms", Some("120000")),
        ("current_sql", Some("SELECT id FROM shop_items WHERE tenant=7234")),
    ])
}
pub(crate) async fn request_and_metadata() -> (ActivityRequestFocus, Metadata) {
    let mut reader = fixture::Reader {
        replies: [
            Ok(vec![fixture::metadata_with("t", "2026-11-14T00:00:00Z")]),
            Ok(vec![fixture::member("127.0.0.1")]),
        ]
        .into(),
        ..Default::default()
    };
    let metadata = super::super::metadata::activity(&mut reader, "fixture.invalid", 9).await.unwrap();
    let context = super::super::local_context::Context::observe(&mut reader, &metadata)
        .await
        .unwrap()
        .unwrap()
        .read_context()
        .unwrap();
    let source = CollectionDescription {
        target: context.target,
        partition: context.partition,
        source: "pg_stat_activity".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Activity(ActivitySnapshotDescription {
            epoch: Some(metadata.activity_epoch()),
            database_writable: Some(false),
            observed_from: AT.into(),
            observed_until: AT.into(),
            at: AT.into(),
            scope: culprit_core::MeasurementScope::Database,
            population: ActivityPopulation::ClientBackends,
            native_rows: 1,
            observations: 1,
            coverage: StateSourceCoverage::collected("pg_stat_activity".into(), 1),
        }),
    };
    let actor = super::super::activity::decode_row(actor_row(), "16384").unwrap().facts.unwrap();
    (fixture::focus_from(&source, actor), metadata)
}

pub(crate) fn endpoint_rows(request: &ActivityRequestFocus, startup: bool) -> Vec<Row> {
    let raw = |source: &str, id: &str, values: serde_json::Value| {
        fixture::row(&[
            ("native_source", Some(source)),
            ("native_id", Some(id)),
            ("native_values", Some(&values.to_string())),
        ])
    };
    let mut rows = vec![
        raw(
            "pg_stat_database_conflicts",
            "16384",
            json!({
                "datid":"16384","datname":"shop","confl_tablespace":0,"confl_lock":9007199254740993u64,
                "confl_snapshot":0,"confl_bufferpin":0,"confl_deadlock":0,"confl_active_logicalslot":0,
                "stats_reset":"2026-11-14T01:00:00Z","observed_at":AT,
                "postmaster_started_at":"2026-11-14T00:00:00Z","stats_fetch_consistency":"cache"
            }),
        ),
        raw(
            "pg_recovery",
            "local",
            json!({
                "kind":"standby","name":"this server","server_version_num":"180000",
                "read_database_oid":"16384","read_standby":true,"observed_at":AT,"full_visibility":true,
                "receive_lsn":"0/2000","replay_lsn":"0/1000","behind":"4096",
                "receiver":"streaming","streaming_configured":true,"delay_s":"0"
            }),
        ),
    ];
    if startup {
        rows.push(raw(
            "pg_recovery_process",
            "73",
            json!({
                "pid":"73","started_at":"2026-11-14T00:00:01Z","database_oid":null,
                "wait_type":"Lock","wait_event":"relation","lock_snapshot_rows":2,"edges_limited":false,
                "relation_edges":[{"database_oid":"16384","relation_oid":"42000","requested_mode":"AccessExclusiveLock",
                    "reader_pid":request.original.key.session.id,"reader_started_at":BACKEND,
                    "reader_single_backend":true,"granted_modes":["AccessShareLock"]}]
            }),
        ));
    }
    rows
}

#[tokio::test]
async fn same_native_decoder_retains_exact_conflicts_and_selected_relation_edges_in_one_dispatch() {
    let (request, meta) = request_and_metadata().await;
    let mut reader = fixture::Reader { replies: [Ok(endpoint_rows(&request, true))].into(), ..Default::default() };
    let point = read(&mut reader, &meta, &request).await.unwrap();
    assert_eq!(reader.calls.len(), 1);
    assert_eq!(
        point.conflicts.counters.iter().find(|counter| counter.key == "confl_lock").unwrap().value.integer(),
        Some(9007199254740993)
    );
    let process = &point.processes[0];
    assert!(process.proves_relation_holder(&process.relation_edges[0], &request, "16384"));
    let query = &reader.calls[0];
    assert!(!query.contains("pg_blocking_pids"));
    assert!(query.contains("pg_catalog.pg_locks"));
    assert!(query.contains("backend_type='startup'"));
    assert!(query.contains("c.datid=16384"));
    assert!(!query.contains("pg_replication_slots"));
    assert!(!query.contains("pg_stat_clear_snapshot"));
}

#[tokio::test]
async fn empty_startup_is_retained_context_and_duplicate_or_missing_native_fields_are_refused() {
    let (request, meta) = request_and_metadata().await;
    let rows = endpoint_rows(&request, false);
    let mut reader = fixture::Reader { replies: [Ok(rows.clone())].into(), ..Default::default() };
    let point = read(&mut reader, &meta, &request).await.unwrap();
    assert!(point.processes.is_empty());
    for defect in ["duplicate", "missing", "foreign-database"] {
        let mut rows = rows.clone();
        match defect {
            "duplicate" => rows.push(rows[0].clone()),
            "missing" => {
                rows.remove(0);
            }
            _ => {
                rows[0] = fixture::row(&[
                    ("native_source", Some("pg_stat_database_conflicts")),
                    ("native_id", Some("2")),
                    ("native_values", Some("{}")),
                ])
            }
        }
        let mut reader = fixture::Reader { replies: [Ok(rows)].into(), ..Default::default() };
        assert!(read(&mut reader, &meta, &request).await.is_err(), "{defect}");
        assert_eq!(reader.calls.len(), 1);
    }
}

#[tokio::test]
async fn conflict_database_oid_uses_exact_native_json_string_and_refuses_coercion_or_foreign_identity() {
    let (request, meta) = request_and_metadata().await;
    for oid in [
        json!(16384),
        json!(16384.0),
        json!(null),
        json!(true),
        json!("2"),
        json!("016384"),
        json!("16384 "),
        json!("+16384"),
        json!(""),
    ] {
        let mut rows = endpoint_rows(&request, true);
        let mut values: serde_json::Value = serde_json::from_str(&text(&rows[0], "native_values").unwrap()).unwrap();
        values["datid"] = oid.clone();
        rows[0] = fixture::row(&[
            ("native_source", Some("pg_stat_database_conflicts")),
            ("native_id", Some("16384")),
            ("native_values", Some(&values.to_string())),
        ]);
        let mut reader = fixture::Reader { replies: [Ok(rows)].into(), ..Default::default() };
        assert_eq!(
            CollectionStop::from_error(&read(&mut reader, &meta, &request).await.unwrap_err()),
            CollectionStop::InvalidPage,
            "{oid}"
        );
        assert_eq!(reader.calls.len(), 1, "a rejected native identity cannot dispatch again");
    }
}
