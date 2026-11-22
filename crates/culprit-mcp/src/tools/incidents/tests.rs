use super::*;
use culprit_core::{
    AuthKind, DerivedAttributionStatus, DerivedDisplayContext, DerivedIncident, DerivedIncidentQuery, Engine,
    IncidentHistoryReport, IncidentReadReport, collected_analysis::CollectedEvidenceMetrics, telemetry::*,
};
use culprit_engine::{
    AgentAccess, NewConnection, ProviderConstructionPolicy, ProviderRegistration, WarehouseIncidentReader,
    WarehouseIncidentTask, Workspace,
};
use rmcp::ServerHandler;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, oneshot};

struct Secrets(AtomicUsize);
impl culprit_engine::secrets::SecretStore for Secrets {
    fn get(&self, _: &str) -> Result<Option<String>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some("private-source-password".into()))
    }
    fn set(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
struct Host {
    server: CulpritMcp,
    directory: PathBuf,
    native: Arc<AtomicUsize>,
    secrets: Arc<Secrets>,
}
impl Host {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "culprit-mcp-incident-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let native = Arc::new(AtomicUsize::new(0));
        let called = native.clone();
        let secrets = Arc::new(Secrets(AtomicUsize::new(0)));
        let ws = Workspace::open_in(&directory).with_secrets(secrets.clone()).with_provider(
            Engine::Mssql,
            ProviderRegistration::new(
                Arc::new(move |_, _| {
                    called.fetch_add(1, Ordering::SeqCst);
                    Err(Error::internal("no native provider may run"))
                }),
                ProviderConstructionPolicy::DeferredNativeWork,
                Vec::new(),
            )
            .unwrap(),
        );
        ws.add_connection(NewConnection {
            name: "saved-route".into(),
            input: "Server=fixture.invalid,14333;Database=ExactDB;User ID=source;Password=private-source-password"
                .into(),
            engine: Some(Engine::Mssql),
            auth: Some(AuthKind::Password),
            ..Default::default()
        })
        .unwrap();
        ws.set_agent_access("saved-route", AgentAccess::Stats).unwrap();
        Self { server: CulpritMcp::new(Arc::new(ws), crate::Options::default()), directory, native, secrets }
    }
    fn database(&self) -> culprit_core::DatabaseIdentity {
        saved_incident_database(&self.server.ws.connection("saved-route").unwrap()).unwrap()
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn arguments() -> serde_json::Map<String, Value> {
    json!({"connection":"saved-route", "around":"2020-01-01T12:00:00Z"}).as_object().unwrap().clone()
}
fn body(value: &rmcp::model::CallToolResult) -> Value {
    serde_json::from_str(&value.content[0].as_text().unwrap().text).unwrap()
}
fn report(database: &culprit_core::DatabaseIdentity) -> IncidentReadReport {
    let sample: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/incident-history.sample.json"
    )))
    .unwrap();
    let mut history: IncidentHistoryReport = serde_json::from_value(sample["ongoing"].clone()).unwrap();
    let resolved: IncidentHistoryReport = serde_json::from_value(sample["resolved"].clone()).unwrap();
    history.entries.extend(resolved.entries);
    let database = IncidentDatabaseSelector::from_route(database).unwrap();
    let mut entry = history.entries[0].clone();
    let original =
        entry.original.clone().or_else(|| entry.evidence.iter().find_map(|row| row.reference.clone())).unwrap();
    entry.original = Some(original.clone());
    let query = DerivedIncidentQuery {
        causal_role: None,
        identity: None,
        sql: DefinitionPayload::sql("SELECT name FROM dbo.orders", Engine::Mssql),
        caller: None,
        evidence: vec![original.clone()],
    };
    IncidentReadReport {
        database: database.clone(),
        from: "2020-01-01T11:55:00Z".into(),
        until: "2020-01-01T12:05:00Z".into(),
        recorded_before: "2020-01-01T13:00:00Z".into(),
        discovery_complete: false,
        incidents: vec![DerivedIncident {
            analysis_id: digest(b"formatter analysis"),
            database: Some(database),
            display: DerivedDisplayContext {
                connection_alias: "saved-route".into(),
                environment: None,
                database_name: None,
            },
            entry,
            original_interval: None,
            revision_reference: original,
            victim_summary: "original victim <data>\n\u{1b}trusted?".into(),
            affected_queries: vec![query.clone()],
            culprit_queries: vec![],
            candidate_queries: vec![query],
            attribution_status: DerivedAttributionStatus::CandidatesOnly,
            mechanism: None,
            developer_action: "bounded original action".into(),
            verification: "verify original scope".into(),
            caller: None,
        }],
        analyses: vec![],
        history,
        relationships: vec![],
        gaps: vec![culprit_core::IncidentReadGap::CandidateLimit],
        archive_work: CollectedEvidenceMetrics {
            selected_rows: 0,
            queries: 0,
            pages: 0,
            returned_rows: 0,
            response_bytes: 0,
            proof_bytes: 0,
            recovery_objects: 0,
            recovery_bytes: 0,
        },
        candidate_rows: 1,
    }
}
struct Reader {
    calls: AtomicUsize,
    report: Mutex<Option<IncidentReadReport>>,
}
impl WarehouseIncidentReader for Reader {
    fn incident<'a>(
        &'a self,
        database: &'a culprit_core::DatabaseIdentity,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(around, "2020-01-01T12:00:00Z");
        assert!(!cancellation.is_cancelled());
        assert_eq!(database.database, "ExactDB");
        Box::pin(async move { Ok(self.report.lock().unwrap().take().unwrap()) })
    }
}
fn reader(host: &Host) -> Arc<Reader> {
    Arc::new(Reader { calls: AtomicUsize::new(0), report: Mutex::new(Some(report(&host.database()))) })
}
#[test]
fn the_tool_is_opt_in_read_only_and_accepts_only_saved_connection_and_around_time() {
    let host = Host::new();
    assert!(host.server.get_tool(INCIDENT_TOOL).is_none());
    let server = host.server.clone().with_incident_reader(reader(&host));
    let tool = server.get_tool(INCIDENT_TOOL).unwrap();
    let props = tool.input_schema.get("properties").unwrap().as_object().unwrap();
    assert_eq!(props.len(), 2);
    assert!(props.contains_key("connection") && props.contains_key("around"));
    assert_eq!(tool.input_schema.get("additionalProperties"), Some(&json!(false)));
    assert_eq!(tool.annotations.unwrap().read_only_hint, Some(true));
}
#[tokio::test]
async fn canonical_roles_history_clocks_and_nulls_are_preserved_without_native_or_source_credentials() {
    let host = Host::new();
    let reader = reader(&host);
    let server = host.server.clone().with_incident_reader(reader.clone());
    let result = server.handle("incident-test", INCIDENT_TOOL, arguments()).await;
    assert_ne!(result.is_error, Some(true));
    let output = body(&result);
    assert_eq!(output["discovery_complete"], false);
    assert_eq!(output["gaps_available"], 1);
    let incident = &output["incidents"][0];
    assert_eq!(incident["culprit_queries"], json!([]));
    assert_eq!(incident["attribution_status"], "candidates-only");
    assert_eq!(incident["affected_queries"].as_array().unwrap().len(), 1);
    assert_eq!(incident["candidate_queries"].as_array().unwrap().len(), 1);
    assert!(incident["mechanism"].is_null());
    assert!(incident["caller"].is_null());
    assert_eq!(
        incident["victim_summary"],
        culprit_core::format::escaped_preview("original victim <data>\n\u{1b}trusted?", 1024).text
    );
    assert!(!incident["victim_summary"].as_str().unwrap().contains('\u{1b}'));
    assert!(incident["entry"]["evidence_available"].as_u64().unwrap() > 0);
    assert!(incident["entry"]["evidence"][0]["availability"].is_string());
    assert_eq!(incident["developer_action"], "bounded original action");
    assert_eq!(incident["verification"], "verify original scope");
    assert!(output["history"]["entries"].as_array().unwrap().iter().any(|row| row["lifecycle"] == "resolved"));
    assert_eq!(output["recorded_before"], "2020-01-01T13:00:00Z");
    assert_eq!(host.native.load(Ordering::SeqCst), 0);
    assert_eq!(host.secrets.0.load(Ordering::SeqCst), 0);
    let audit = host.server.ws.audit_log(1).unwrap();
    assert_eq!(audit[0].tool, INCIDENT_TOOL);
    assert!(audit[0].sql.is_none());
    assert!(!result.content[0].as_text().unwrap().text.contains("private-source-password"));
}
#[tokio::test]
async fn missing_reader_denied_routes_and_extra_configuration_never_dispatch_archive_or_native_work() {
    let host = Host::new();
    assert_eq!(host.server.handle("fixture", INCIDENT_TOOL, arguments()).await.is_error, Some(true));
    let reader = reader(&host);
    let mut server = host.server.clone().with_incident_reader(reader.clone());
    host.server.ws.set_agent_access("saved-route", AgentAccess::Off).unwrap();
    assert_eq!(server.handle("fixture", INCIDENT_TOOL, arguments()).await.is_error, Some(true));
    host.server.ws.set_agent_access("saved-route", AgentAccess::Stats).unwrap();
    server.options = Arc::new(crate::Options { connections: vec!["different-route".into()], ..Default::default() });
    assert_eq!(server.handle("fixture", INCIDENT_TOOL, arguments()).await.is_error, Some(true));
    server.options = Arc::new(crate::Options::default());
    for field in ["range_id", "config", "sql", "credential", "budget", "directory"] {
        let mut args = arguments();
        args.insert(field.into(), json!("private input"));
        assert_eq!(server.handle("fixture", INCIDENT_TOOL, args).await.is_error, Some(true));
    }
    let mut args = arguments();
    args.insert("around".into(), json!("not a time"));
    assert_eq!(server.handle("fixture", INCIDENT_TOOL, args).await.is_error, Some(true));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.native.load(Ordering::SeqCst), 0);
    assert_eq!(host.secrets.0.load(Ordering::SeqCst), 0);
}
struct Awaited {
    started: Notify,
    release: Notify,
    finished: AtomicUsize,
    fail: bool,
}
impl WarehouseIncidentReader for Awaited {
    fn incident<'a>(
        &'a self,
        _: &'a culprit_core::DatabaseIdentity,
        _: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            self.finished.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(DeliveryProblem::Integrity.error())
            } else {
                assert!(cancellation.is_cancelled());
                Err(CollectionStop::Cancelled.error())
            }
        })
    }
}
#[tokio::test]
async fn rpc_cancellation_is_signalled_to_the_same_reader_and_awaits_its_return() {
    let host = Host::new();
    let reader = Arc::new(Awaited {
        started: Notify::new(),
        release: Notify::new(),
        finished: AtomicUsize::new(0),
        fail: false,
    });
    let server = host.server.clone().with_incident_reader(reader.clone());
    let (cancel, cancelled) = oneshot::channel();
    let work = tokio::spawn(async move {
        server
            .handle_cancellable("fixture", INCIDENT_TOOL, arguments(), async {
                let _ = cancelled.await;
            })
            .await
    });
    reader.started.notified().await;
    cancel.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!work.is_finished());
    assert_eq!(reader.finished.load(Ordering::SeqCst), 0);
    reader.release.notify_one();
    let result = work.await.unwrap();
    assert_eq!(reader.finished.load(Ordering::SeqCst), 1);
    assert_eq!(body(&result)["error"]["detail"]["collection-stopped"], "cancelled");
}
#[tokio::test]
async fn a_late_archive_failure_never_becomes_a_partial_success_and_is_audited() {
    let host = Host::new();
    let reader =
        Arc::new(Awaited { started: Notify::new(), release: Notify::new(), finished: AtomicUsize::new(0), fail: true });
    let server = host.server.clone().with_incident_reader(reader.clone());
    let work = tokio::spawn(async move { server.handle("fixture", INCIDENT_TOOL, arguments()).await });
    reader.started.notified().await;
    reader.release.notify_one();
    let result = work.await.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(body(&result)["error"]["detail"]["delivery"], "integrity");
    assert_eq!(host.server.ws.audit_log(1).unwrap()[0].outcome, culprit_engine::AuditOutcome::Error);
}
#[test]
fn bounded_presentation_keeps_roles_distinct_reports_omissions_and_does_not_claim_complete_empty() {
    let host = Host::new();
    let mut report = report(&host.database());
    let mut row = report.incidents[0].clone();
    row.victim_summary = "native \u{1b} label ".repeat(50_000);
    row.affected_queries = vec![row.affected_queries[0].clone(); 20];
    report.incidents = vec![row; 20];
    report.gaps = vec![culprit_core::IncidentReadGap::CandidateLimit; 100];
    let value = presentation::value(&report).unwrap();
    assert!(serde_json::to_vec(&value).unwrap().len() <= super::super::response::MAX_OUTPUT_BYTES);
    assert_eq!(value["incidents_available"], 20);
    assert!(value["incidents_omitted"].as_u64().unwrap() >= 15);
    assert_eq!(value["gaps_available"], 100);
    assert!(value["gaps_omitted"].as_u64().unwrap() >= 84);
    assert_eq!(value["presentation_complete"], false);
    assert_eq!(value["discovery_complete"], false);
    if let Some(row) = value["incidents"].as_array().unwrap().first() {
        assert!(row["affected_queries_omitted"].as_u64().unwrap() >= 15);
        assert_eq!(row["culprit_queries"], json!([]));
        assert_eq!(row["text_truncated"], true);
    }
}

struct DelayedResult {
    started: Notify,
    release: Notify,
    report: Mutex<Option<IncidentReadReport>>,
}
impl WarehouseIncidentReader for DelayedResult {
    fn incident<'a>(
        &'a self,
        _: &'a culprit_core::DatabaseIdentity,
        _: &'a str,
        _: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.report.lock().unwrap().take().unwrap())
        })
    }
}
#[tokio::test]
async fn delegation_revoked_while_the_original_reader_runs_prevents_result_publication() {
    let host = Host::new();
    let reader = Arc::new(DelayedResult {
        started: Notify::new(),
        release: Notify::new(),
        report: Mutex::new(Some(report(&host.database()))),
    });
    let server = host.server.clone().with_incident_reader(reader.clone());
    let work = tokio::spawn(async move { server.handle("fixture", INCIDENT_TOOL, arguments()).await });
    reader.started.notified().await;
    host.server.ws.set_agent_access("saved-route", AgentAccess::Off).unwrap();
    reader.release.notify_one();
    let result = work.await.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("incidents").is_none());
    assert_eq!(host.native.load(Ordering::SeqCst), 0);
    assert_eq!(host.secrets.0.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn foreign_route_output_is_refused_and_pre_dispatch_cancellation_never_enters_the_reader() {
    let host = Host::new();
    let mut value = report(&host.database());
    value.database = IncidentDatabaseSelector::from_key(&digest(b"another route")).unwrap();
    let reader = Arc::new(Reader { calls: AtomicUsize::new(0), report: Mutex::new(Some(value)) });
    let server = host.server.clone().with_incident_reader(reader.clone());
    let result = server.handle("fixture", INCIDENT_TOOL, arguments()).await;
    assert_eq!(body(&result)["error"]["detail"]["delivery"], "destination-changed");
    let next_reader = self::reader(&host);
    let server = host.server.clone().with_incident_reader(next_reader.clone());
    let result = server.handle_cancellable("fixture", INCIDENT_TOOL, arguments(), std::future::ready(())).await;
    assert_eq!(body(&result)["error"]["detail"]["collection-stopped"], "cancelled");
    assert_eq!(next_reader.calls.load(Ordering::SeqCst), 0);
    assert_eq!(host.native.load(Ordering::SeqCst), 0);
    assert_eq!(host.secrets.0.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn dropping_an_outer_request_signals_its_existing_reader_cancellation() {
    struct ObserveDrop {
        started: Notify,
        token: Mutex<Option<CollectionCancellation>>,
    }
    impl WarehouseIncidentReader for ObserveDrop {
        fn incident<'a>(
            &'a self,
            _: &'a culprit_core::DatabaseIdentity,
            _: &'a str,
            token: &'a CollectionCancellation,
        ) -> WarehouseIncidentTask<'a> {
            Box::pin(async move {
                *self.token.lock().unwrap() = Some(token.clone());
                self.started.notify_one();
                std::future::pending().await
            })
        }
    }
    let host = Host::new();
    let reader = Arc::new(ObserveDrop { started: Notify::new(), token: Mutex::new(None) });
    let server = host.server.clone().with_incident_reader(reader.clone());
    let mut work = Box::pin(server.handle("fixture", INCIDENT_TOOL, arguments()));
    tokio::select! { biased;
        result = &mut work => panic!("request unexpectedly finished: {result:?}"),
        _ = reader.started.notified() => (),
    }
    assert!(!reader.token.lock().unwrap().as_ref().unwrap().is_cancelled());
    drop(work);
    assert!(reader.token.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(host.native.load(Ordering::SeqCst), 0);
    assert_eq!(host.secrets.0.load(Ordering::SeqCst), 0);
}

#[test]
fn escaped_control_expansion_reports_truncation_even_when_the_original_is_short() {
    let host = Host::new();
    let mut report = report(&host.database());
    let baseline = presentation::value(&report).unwrap();
    assert_eq!(baseline["incidents"][0]["text_truncated"], false);
    report.incidents[0].victim_summary = "\n".repeat(800);
    let output = presentation::value(&report).unwrap();
    assert_eq!(output["incidents"][0]["text_truncated"], true);
    assert_eq!(output["presentation_complete"], false);
    let text = output["incidents"][0]["victim_summary"].as_str().unwrap();
    assert!(text.ends_with('…'));
    assert!(!text.contains('\n'));
    assert!(text.chars().count() <= 1025);
}
