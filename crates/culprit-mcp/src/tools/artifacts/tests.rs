//! Delegated interface/lifetime/presentation controls only; Engine tests own canonical authentication.
use super::super::incidents::tests::{Host, body, report, zero_work};
use super::*;
use culprit_core::{telemetry::*, *};
use culprit_engine::{
    WarehouseArtifactsTask, WarehouseIncidentReader, WarehouseIncidentTask, WarehouseResourcesTask, WarehouseWindowTask,
};
use rmcp::ServerHandler;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, oneshot};
fn query(host: &Host) -> WarehouseArtifactQuery {
    let reference = report(&host.database()).incidents[0].affected_queries[0].evidence[0].clone();
    WarehouseArtifactQuery {
        resource: host.selector(),
        references: vec![reference],
        full_payloads: true,
        expected_sql_sha256: None,
        text: WarehouseArtifactTextRead::Inventory { chunk_chars: MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS },
    }
}
fn args(request: &WarehouseArtifactQuery) -> serde_json::Map<String, Value> {
    json!({"resource":request.resource,"references":request.references,"full_payloads":request.full_payloads,"expected_sql_sha256":request.expected_sql_sha256,
        "continuation":match &request.text{WarehouseArtifactTextRead::Chunk{cursor}=>Some(cursor),_=>None}}).as_object().unwrap().clone()
}
fn value(request: &WarehouseArtifactQuery) -> WarehouseArtifactReport {
    WarehouseArtifactReport {
        query: request.clone(),
        records: vec![],
        gaps: vec![WarehouseArtifactGap::NoDefinitionPayload { reference: Box::new(request.references[0].clone()) }],
        qualifications: vec![t!("artifact.reader.partial")],
        archive_work: zero_work(),
    }
}
struct Reader {
    calls: AtomicUsize,
    value: Mutex<Option<WarehouseArtifactReport>>,
    wait: bool,
    foreign: bool,
    started: Notify,
    release: Notify,
    finished: AtomicUsize,
}
impl WarehouseIncidentReader for Reader {
    fn resources<'a>(
        &'a self,
        _: &'a PublishedResourceRequest,
        _: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }
    fn incident<'a>(
        &'a self,
        _: &'a PublishedResourceSelector,
        _: &'a str,
        _: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }
    fn window<'a>(&'a self, _: &'a WarehouseWindowQuery, _: &'a CollectionCancellation) -> WarehouseWindowTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }
    fn artifacts<'a>(
        &'a self,
        request: &'a WarehouseArtifactQuery,
        _: &'a CollectionCancellation,
    ) -> WarehouseArtifactsTask<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.started.notify_one();
            if self.wait {
                self.release.notified().await;
            }
            let mut value = self.value.lock().unwrap().take().unwrap();
            value.query = request.clone();
            if self.foreign {
                value.query.resource.namespace_id = digest(b"foreign archive");
            }
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(value)
        })
    }
}
fn reader(value: WarehouseArtifactReport, wait: bool, foreign: bool) -> Arc<Reader> {
    Arc::new(Reader {
        calls: AtomicUsize::new(0),
        value: Mutex::new(Some(value)),
        wait,
        foreign,
        started: Notify::new(),
        release: Notify::new(),
        finished: AtomicUsize::new(0),
    })
}
#[tokio::test]
async fn artifact_inventory_requires_delegation_and_never_opens_native_or_secret_owners() {
    let host = Host::new();
    assert!(host.server.get_tool(ARTIFACTS_TOOL).is_none());
    let request = query(&host);
    request.validate().unwrap();
    let reader = reader(value(&request), false, false);
    let (server, _) = host.install(reader.clone());
    assert_eq!(server.get_tool(ARTIFACTS_TOOL).unwrap().annotations.unwrap().read_only_hint, Some(true));
    let result = server.handle("artifact-test", ARTIFACTS_TOOL, args(&request)).await;
    assert_ne!(result.is_error, Some(true));
    assert_eq!(body(&result)["query"], serde_json::to_value(request).unwrap());
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    host.assert_source_free();
}
#[tokio::test]
async fn cancellation_or_revocation_settles_the_started_reader_and_withholds_late_text() {
    for revoke in [false, true] {
        let host = Host::new();
        let request = query(&host);
        let reader = reader(value(&request), true, false);
        let (server, delegation) = host.install(reader.clone());
        let args = args(&request);
        let (cancel, cancelled) = oneshot::channel();
        let work = tokio::spawn(async move {
            server
                .handle_cancellable("artifact-test", ARTIFACTS_TOOL, args, async {
                    let _ = cancelled.await;
                })
                .await
        });
        reader.started.notified().await;
        if revoke {
            delegation.revoke();
        } else {
            cancel.send(()).unwrap();
        }
        tokio::task::yield_now().await;
        assert!(!work.is_finished());
        reader.release.notify_one();
        let result = work.await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(body(&result).get("records").is_none());
        assert_eq!(reader.finished.load(Ordering::SeqCst), 1);
        host.assert_source_free();
    }
}
#[tokio::test]
async fn malformed_private_arguments_and_foreign_output_are_refused_without_echo_or_native_fallback() {
    let host = Host::new();
    let request = query(&host);
    let reader = reader(value(&request), false, true);
    let (server, _) = host.install(reader.clone());
    let mut private = args(&request);
    private.insert("password".into(), json!("private-canary"));
    let result = server.handle("artifact-test", ARTIFACTS_TOOL, private).await;
    assert_eq!(result.is_error, Some(true));
    assert!(!serde_json::to_string(&result).unwrap().contains("private-canary"));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    let result = server.handle("artifact-test", ARTIFACTS_TOOL, args(&request)).await;
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("records").is_none());
    host.assert_source_free();
}
fn record(request: &WarehouseArtifactQuery, payload: WarehouseArtifactText) -> WarehouseArtifactRecord {
    WarehouseArtifactRecord {
        reference: request.references[0].clone(),
        engine: Engine::Mssql,
        scope: WarehouseArtifactScope::Database {
            database: request.resource.database.clone(),
            label: "presentation original".into(),
        },
        source: "presentation-only".into(),
        native_clock: WindowNativeClock::Unknown,
        collected_from: "2020-01-01T12:00:00Z".into(),
        collected_until: "2020-01-01T12:00:01Z".into(),
        linked_from: vec![],
        artifacts: vec![WarehouseDefinitionArtifact {
            location: "/record/data/statement".into(),
            kind: DefinitionContentKind::Sql,
            label: "presentationSQL".into(),
            payload,
            matches_requested_sql: false,
            plan: None,
            limitations: vec![],
        }],
    }
}
#[tokio::test]
async fn chunk_admission_counts_actual_tool_envelope_and_second_json_escaping_and_never_truncates_progress() {
    let host = Host::new();
    let mut request = query(&host);
    let text = "\"\n\u{0}".chars().cycle().take(2048).collect::<String>();
    let sha = digest(text.as_bytes());
    let cursor = WarehouseArtifactTextCursor {
        binding: digest(b"presentation cursor"),
        reference_key: request.references[0].key().unwrap(),
        capture_key: request.references[0].capture_ref().key().unwrap(),
        location: "/record/data/statement".into(),
        payload_sha256: sha.clone(),
        chunk_chars: 2048,
        previous_character: None,
        next_character: 0,
    };
    request.text = WarehouseArtifactTextRead::Chunk { cursor };
    request.validate().unwrap();
    let mut output = value(&request);
    output.gaps.clear();
    output.records.push(record(
        &request,
        WarehouseArtifactText::Chunk {
            sha256: sha,
            redaction_version: 1,
            from_character: 0,
            until_character: 2048,
            total_characters: 2048,
            text: text.clone(),
            next: None,
        },
    ));
    let capability = reader(output, false, false);
    let (server, _) = host.install(capability);
    let result = server.handle("artifact-test", ARTIFACTS_TOOL, args(&request)).await;
    assert_ne!(result.is_error, Some(true));
    assert!(serde_json::to_vec(&result).unwrap().len() <= super::super::response::MAX_OUTPUT_BYTES);
    assert_eq!(body(&result)["records"][0]["artifacts"][0]["payload"]["text"], text);
    let host = Host::new();
    let request = query(&host);
    let mut output = value(&request);
    output.qualifications.push("\u{0}".repeat(100_000));
    let (server, _) = host.install(reader(output, false, false));
    assert_eq!(server.handle("artifact-test", ARTIFACTS_TOOL, args(&request)).await.is_error, Some(true));
    host.assert_source_free();
}
#[tokio::test]
async fn an_injected_inventory_cannot_smuggle_a_complete_body_as_a_chunk_or_plan() {
    let host = Host::new();
    let request = query(&host);
    let mut output = value(&request);
    output.records.push(record(
        &request,
        WarehouseArtifactText::Complete {
            payload: DefinitionPayload::sql("SELECT private_inventory_body", Engine::Mssql),
        },
    ));
    let (server, _) = host.install(reader(output, false, false));
    let result = server.handle("artifact-test", ARTIFACTS_TOOL, args(&request)).await;
    assert_eq!(result.is_error, Some(true));
    assert!(!serde_json::to_string(&result).unwrap().contains("private_inventory_body"));
    host.assert_source_free();
}
