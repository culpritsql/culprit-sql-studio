//! Delegation/cancellation/presentation only. The Engine controls reopen genuine canonical archive originals.
use super::super::incidents::tests::{Host, body, report, zero_work};
use super::*;
use culprit_core::telemetry::digest;
use culprit_core::*;
use culprit_engine::{WarehouseIncidentReader, WarehouseIncidentTask, WarehouseResourcesTask, WarehouseWindowTask};
use rmcp::ServerHandler;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, oneshot};

fn request(host: &Host) -> WarehouseWindowQuery {
    WarehouseWindowQuery {
        resource: host.selector(),
        window: PublishedResourceWindow { from: "2020-01-01T11:00:00Z".into(), until: "2020-01-01T13:00:00Z".into() },
        as_of: "2020-01-01T14:00:00Z".into(),
        view: WarehouseWindowView::PublishedAnswers,
        page_rows: 1,
        after: None,
    }
}
fn arguments(host: &Host) -> serde_json::Map<String, Value> {
    let query = request(host);
    json!({"resource":query.resource,"from":query.window.from,"until":query.window.until,"as_of":query.as_of})
        .as_object()
        .unwrap()
        .clone()
}
fn window_report(host: &Host) -> WarehouseWindowReport {
    WarehouseWindowReport {
        request: request(host),
        discovery_complete: false,
        originals: vec![],
        workload: WindowWorkload::NotRequested,
        next: Some(PublishedResourceCursor {
            selection_key: digest(b"bound continuation"),
            position: PublishedResourcePosition {
                resource_id: digest(b"resource"),
                source_id: digest(b"source"),
                range_id: digest(b"range"),
            },
        }),
        incidents: report(&host.database()),
        incident_limitations: vec![],
        published: vec![],
        gaps: vec![WarehouseWindowGap::MorePublishedAnalyses],
        archive_work: zero_work(),
        qualifications: vec![t!("window.reader.more")],
    }
}
struct Reader {
    calls: AtomicUsize,
    value: Mutex<Option<WarehouseWindowReport>>,
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
    fn artifacts<'a>(
        &'a self,
        _: &'a culprit_core::WarehouseArtifactQuery,
        _: &'a CollectionCancellation,
    ) -> culprit_engine::WarehouseArtifactsTask<'a> {
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
    fn window<'a>(&'a self, query: &'a WarehouseWindowQuery, _: &'a CollectionCancellation) -> WarehouseWindowTask<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.started.notify_one();
            if self.wait {
                self.release.notified().await;
            }
            let mut result = self.value.lock().unwrap().take().unwrap();
            result.request = query.clone();
            if self.foreign {
                result.request.resource.namespace_id = digest(b"foreign namespace");
            }
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(result)
        })
    }
}
fn reader(host: &Host, wait: bool, foreign: bool) -> Arc<Reader> {
    Arc::new(Reader {
        calls: AtomicUsize::new(0),
        value: Mutex::new(Some(window_report(host))),
        wait,
        foreign,
        started: Notify::new(),
        release: Notify::new(),
        finished: AtomicUsize::new(0),
    })
}
#[tokio::test]
async fn delegated_window_is_source_free_and_returns_bound_continuation_and_unknown_workload_without_healthy_empty() {
    let host = Host::new();
    assert!(host.server.get_tool(WINDOW_TOOL).is_none());
    let reader = reader(&host, false, false);
    let (server, _) = host.install(reader.clone());
    let definition = server.get_tool(WINDOW_TOOL).unwrap();
    assert_eq!(definition.annotations.unwrap().read_only_hint, Some(true));
    let result = server.handle("window-test", WINDOW_TOOL, arguments(&host)).await;
    assert_ne!(result.is_error, Some(true));
    let value = body(&result);
    assert_eq!(value["discovery_complete"], false);
    assert_eq!(value["workload_requested"], false);
    assert!(value["next"]["selection_key"].is_string());
    assert_eq!(value["request"]["as_of"], request(&host).as_of);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    host.assert_source_free();
}
#[tokio::test]
async fn window_cancellation_and_revocation_wait_for_the_same_started_owner_and_withhold_late_success() {
    for revoke in [false, true] {
        let host = Host::new();
        let reader = reader(&host, true, false);
        let (server, delegation) = host.install(reader.clone());
        let args = arguments(&host);
        let (cancel, cancelled) = oneshot::channel();
        let work = tokio::spawn(async move {
            server
                .handle_cancellable("window-test", WINDOW_TOOL, args, async {
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
        assert!(body(&result).get("analyses").is_none());
        assert_eq!(reader.finished.load(Ordering::SeqCst), 1);
        host.assert_source_free();
    }
}
#[tokio::test]
async fn malformed_private_arguments_and_foreign_result_binding_are_refused_without_source_work() {
    let host = Host::new();
    let reader = reader(&host, false, true);
    let (server, _) = host.install(reader.clone());
    let mut private = arguments(&host);
    private.insert("password".into(), json!("private-canary"));
    let result = server.handle("window-test", WINDOW_TOOL, private).await;
    assert_eq!(result.is_error, Some(true));
    assert!(!serde_json::to_string(&body(&result)).unwrap().contains("private-canary"));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    let result = server.handle("window-test", WINDOW_TOOL, arguments(&host)).await;
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("analyses").is_none());
    host.assert_source_free();
}
#[test]
fn bounded_window_presentation_retains_next_and_reports_omissions_and_escaped_text() {
    let host = Host::new();
    let mut report = window_report(&host);
    report.incidents.incidents[0].victim_summary = "native \u{1b} label ".repeat(50_000);
    report.incidents.incidents = vec![report.incidents.incidents[0].clone(); 20];
    report.qualifications = vec!["qualification ".repeat(10_000); 30];
    let value = presentation::value(&report).unwrap();
    assert!(serde_json::to_vec(&value).unwrap().len() <= super::super::response::MAX_OUTPUT_BYTES);
    assert_eq!(value["presentation_complete"], false);
    assert!(value["qualifications_omitted"].as_u64().unwrap() >= 14);
    assert_eq!(value["next"], serde_json::to_value(&report.next).unwrap());
    assert!(!serde_json::to_string(&value).unwrap().contains('\u{1b}'));
    assert!(value["incidents"]["incidents_omitted"].as_u64().unwrap() >= 15);
}
