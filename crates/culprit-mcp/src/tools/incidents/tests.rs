//! MCP authorization/lifecycle over the injected interface. Canonical namespace/proof controls live with EvidenceService.
use super::*;
use crate::{ToolMode, WarehouseDelegation};
use culprit_core::{
    AuthKind, DerivedAttributionStatus, DerivedDisplayContext, DerivedIncident, DerivedIncidentQuery, Engine,
    IncidentDatabaseSelector, IncidentHistoryReport, IncidentReadReport, PublishedDatabaseResource,
    PublishedResourceCoverage, PublishedResourceGap, PublishedResourceRequest, PublishedResourceSelector,
    PublishedResourceTime, PublishedResources, collected_analysis::CollectedEvidenceMetrics, telemetry::*,
};
use culprit_engine::{
    AgentAccess, NewConnection, ProviderConstructionPolicy, ProviderRegistration, WarehouseIncidentReader,
    WarehouseIncidentTask, WarehouseResourcesTask, Workspace,
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
pub(in crate::tools) struct Host {
    pub(in crate::tools) server: CulpritMcp,
    directory: PathBuf,
    native: Arc<AtomicUsize>,
    secrets: Arc<Secrets>,
}
impl Host {
    pub(in crate::tools) fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "culprit-mcp-resource-{}-{}",
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
                    Err(Error::internal("native work forbidden"))
                }),
                ProviderConstructionPolicy::DeferredNativeWork,
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(ws.connections().unwrap().is_empty());
        Self {
            server: CulpritMcp::new(
                Arc::new(ws),
                crate::Options { tool_mode: ToolMode::EvidenceOnly, ..Default::default() },
            ),
            directory,
            native,
            secrets,
        }
    }
    pub(in crate::tools) fn database(&self) -> culprit_core::DatabaseIdentity {
        culprit_core::DatabaseIdentity::server(Engine::Mssql, "ExactDB", "fixture.invalid", 14333, None)
    }
    pub(in crate::tools) fn selector(&self) -> PublishedResourceSelector {
        PublishedResourceSelector {
            namespace_id: digest(b"controlled archive namespace"),
            database: IncidentDatabaseSelector::from_route(&self.database()).unwrap(),
        }
    }
    pub(in crate::tools) fn install(
        &self,
        reader: Arc<dyn WarehouseIncidentReader>,
    ) -> (CulpritMcp, Arc<WarehouseDelegation>) {
        let delegation = Arc::new(WarehouseDelegation::read_only(reader));
        (self.server.clone().with_warehouse(delegation.clone()), delegation)
    }
    pub(in crate::tools) fn assert_source_free(&self) {
        assert_eq!(self.native.load(Ordering::SeqCst), 0);
        assert_eq!(self.secrets.0.load(Ordering::SeqCst), 0);
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn arguments(host: &Host) -> serde_json::Map<String, Value> {
    json!({"resource":host.selector(),"around":"2020-01-01T12:00:00Z"}).as_object().unwrap().clone()
}
fn resource_arguments() -> serde_json::Map<String, Value> {
    json!({"as_of":"2020-01-01T13:00:00Z"}).as_object().unwrap().clone()
}
pub(in crate::tools) fn body(value: &rmcp::model::CallToolResult) -> Value {
    serde_json::from_str(&value.content[0].as_text().unwrap().text).unwrap()
}
pub(in crate::tools) fn zero_work() -> CollectedEvidenceMetrics {
    CollectedEvidenceMetrics {
        selected_rows: 0,
        queries: 0,
        pages: 0,
        returned_rows: 0,
        response_bytes: 0,
        proof_bytes: 0,
        recovery_objects: 0,
        recovery_bytes: 0,
    }
}
fn resources(host: &Host) -> PublishedResources {
    PublishedResources {
        as_of: "2020-01-01T13:00:00Z".into(),
        window: None,
        resources: vec![PublishedDatabaseResource {
            position: culprit_core::PublishedResourcePosition {
                resource_id: digest(b"resource"),
                source_id: digest(b"source"),
                range_id: digest(b"range"),
            },
            logical_source_id: digest(b"logical source"),
            selector: host.selector(),
            engine: Engine::Mssql,
            database_name: "ExactDB".into(),
            connection_alias: None,
            source_name: "Original counters".into(),
            collection: TelemetryCollection::Snapshot(SnapshotDataset::Operational),
            continuity: SourceContinuity::Unverified,
            collected_from: "2020-01-01T12:00:00Z".into(),
            collected_until: "2020-01-01T12:00:01Z".into(),
            native_time: PublishedResourceTime::Unknown,
            coverage: PublishedResourceCoverage::Snapshot {
                sources: vec![StateSourceCoverage::observed("original-counter".into(), 1, false)],
            },
        }],
        next: None,
        discovery_complete: false,
        gaps: vec![PublishedResourceGap::MoreCandidates],
        candidate_rows: 1,
        archive_work: zero_work(),
        recovery_metadata_reads: 1,
    }
}
pub(in crate::tools) fn report(database: &culprit_core::DatabaseIdentity) -> IncidentReadReport {
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
    resource_calls: AtomicUsize,
    report: Mutex<Option<IncidentReadReport>>,
    resources: Mutex<Option<PublishedResources>>,
}
impl WarehouseIncidentReader for Reader {
    fn window<'a>(
        &'a self,
        _: &'a culprit_core::WarehouseWindowQuery,
        _: &'a CollectionCancellation,
    ) -> culprit_engine::WarehouseWindowTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }

    fn resources<'a>(
        &'a self,
        request: &'a PublishedResourceRequest,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a> {
        self.resource_calls.fetch_add(1, Ordering::SeqCst);
        assert!(!cancellation.is_cancelled());
        request.validate().unwrap();
        Box::pin(async move {
            let mut result = self.resources.lock().unwrap().take().unwrap();
            result.as_of = request.as_of.clone();
            result.window = request.window.clone();
            Ok(result)
        })
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
        resource: &'a PublishedResourceSelector,
        around: &'a str,
        cancellation: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(around, "2020-01-01T12:00:00Z");
        assert!(!cancellation.is_cancelled());
        resource.validate().unwrap();
        Box::pin(async move { Ok(self.report.lock().unwrap().take().unwrap()) })
    }
}
fn reader(host: &Host) -> Arc<Reader> {
    Arc::new(Reader {
        calls: AtomicUsize::new(0),
        resource_calls: AtomicUsize::new(0),
        report: Mutex::new(Some(report(&host.database()))),
        resources: Mutex::new(Some(resources(host))),
    })
}
fn unsupported_resources<'a>() -> WarehouseResourcesTask<'a> {
    Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
}
#[test]
fn tools_are_explicitly_delegated_and_evidence_only_roster_and_instructions_exclude_native_access() {
    let host = Host::new();
    assert!(host.server.get_tool(INCIDENT_TOOL).is_none());
    assert!(host.server.get_tool("resources").is_none());
    let (server, _) = host.install(reader(&host));
    for name in ["resources", INCIDENT_TOOL] {
        assert_eq!(server.get_tool(name).unwrap().annotations.unwrap().read_only_hint, Some(true));
    }
    for name in ["list_connections", "scan", "run_query", "schema", "query", "explain"] {
        assert!(server.get_tool(name).is_none());
    }
    let tool = server.get_tool(INCIDENT_TOOL).unwrap();
    let properties = tool.input_schema["properties"].as_object().unwrap();
    assert_eq!(properties.len(), 2);
    assert!(properties.contains_key("resource") && properties.contains_key("around"));
    assert!(!properties.contains_key("connection"));
    let instructions = culprit_core::i18n::scope("fr", || {
        let instructions = server.instructions();
        assert_eq!(culprit_core::i18n::current().code, "fr");
        assert!(instructions.starts_with("This server investigates archived database evidence."));
        instructions
    });
    assert!(instructions.contains("resources"));
    assert!(instructions.contains("incident"));
    assert!(!instructions.contains("list_connections"));
    let combined = CulpritMcp::new(host.server.ws.clone(), crate::Options::default())
        .with_warehouse(Arc::new(WarehouseDelegation::read_only(reader(&host))));
    assert!(combined.get_tool("scan").is_some());
    assert!(combined.get_tool("resources").is_some());
}
#[tokio::test]
async fn canonical_roles_history_clocks_and_nulls_are_preserved_without_native_connections_or_source_credentials() {
    let host = Host::new();
    let reader = reader(&host);
    let (server, _) = host.install(reader.clone());
    let result = server.handle("incident-test", INCIDENT_TOOL, arguments(&host)).await;
    assert_ne!(result.is_error, Some(true));
    let output = body(&result);
    assert_eq!(output["discovery_complete"], false);
    assert_eq!(output["gaps_available"], 1);
    let incident = &output["incidents"][0];
    assert_eq!(incident["culprit_queries"], json!([]));
    assert_eq!(incident["attribution_status"], "candidates-only");
    assert_eq!(incident["affected_queries"].as_array().unwrap().len(), 1);
    assert_eq!(incident["candidate_queries"].as_array().unwrap().len(), 1);
    assert!(incident["mechanism"].is_null() && incident["caller"].is_null());
    assert_eq!(
        incident["victim_summary"],
        culprit_core::format::escaped_preview("original victim <data>\n\u{1b}trusted?", 1024).text
    );
    assert!(incident["entry"]["evidence_available"].as_u64().unwrap() > 0);
    assert!(incident["entry"]["evidence"][0]["availability"].is_string());
    assert_eq!(incident["developer_action"], "bounded original action");
    assert_eq!(incident["verification"], "verify original scope");
    assert!(output["history"]["entries"].as_array().unwrap().iter().any(|row| row["lifecycle"] == "resolved"));
    assert_eq!(output["recorded_before"], "2020-01-01T13:00:00Z");
    host.assert_source_free();
    let audit = host.server.ws.audit_log(1).unwrap();
    assert_eq!(audit[0].tool, INCIDENT_TOOL);
    assert!(audit[0].connection.is_none() && audit[0].sql.is_none());
}
#[tokio::test]
async fn resources_returns_copyable_selectors_and_independent_coverage_without_native_allowlist_authority() {
    let host = Host::new();
    let reader = reader(&host);
    let (mut server, _) = host.install(reader.clone());
    server.options = Arc::new(crate::Options {
        tool_mode: ToolMode::EvidenceOnly,
        connections: vec!["unrelated-native-only-selection".into()],
        ..Default::default()
    });
    let listed = server.handle("resource-test", "resources", resource_arguments()).await;
    assert_ne!(listed.is_error, Some(true));
    let listed = body(&listed);
    assert_eq!(listed["resources"][0]["selector"], json!(host.selector()));
    assert!(listed["resources"][0]["connection_alias"].is_null());
    assert_eq!(listed["resources"][0]["coverage"]["sources"][0]["state"], "partial");
    assert_eq!(listed["resources"][0]["coverage"]["sources"][0]["complete_scope"], false);
    assert_eq!(listed["resources"][0]["native_time"]["kind"], "unknown");
    assert_eq!(listed["discovery_complete"], false);
    assert!(listed["resources"][0].get("position").is_none());
    let call = json!({"resource":listed["resources"][0]["selector"],"around":"2020-01-01T12:00:00Z"})
        .as_object()
        .unwrap()
        .clone();
    assert_ne!(server.handle("resource-test", INCIDENT_TOOL, call).await.is_error, Some(true));
    assert_eq!(reader.resource_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    host.assert_source_free();
    let audit = host.server.ws.audit_log(10).unwrap();
    assert_eq!(audit.len(), 2);
    assert!(audit.iter().all(|row| row.connection.is_none() && row.sql.is_none()));
}
#[tokio::test]
async fn missing_or_revoked_delegation_and_extra_native_configuration_never_dispatch_read_or_source_work() {
    let host = Host::new();
    assert_eq!(host.server.handle("fixture", INCIDENT_TOOL, arguments(&host)).await.is_error, Some(true));
    let reader = reader(&host);
    let (server, delegation) = host.install(reader.clone());
    delegation.revoke();
    for tool in [INCIDENT_TOOL, "resources"] {
        let args = if tool == INCIDENT_TOOL { arguments(&host) } else { resource_arguments() };
        assert_eq!(server.handle("fixture", tool, args).await.is_error, Some(true));
    }
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.resource_calls.load(Ordering::SeqCst), 0);
    host.assert_source_free();
    let reader = self::reader(&host);
    let (server, _) = host.install(reader.clone());
    for field in ["connection", "range_id", "config", "sql", "credential", "budget", "directory"] {
        let mut args = arguments(&host);
        args.insert(field.into(), json!("private-input-must-not-echo"));
        let result = server.handle("fixture", INCIDENT_TOOL, args).await;
        assert_eq!(result.is_error, Some(true));
        assert!(!result.content[0].as_text().unwrap().text.contains("private-input-must-not-echo"));
    }
    let mut args = arguments(&host);
    args.insert("around".into(), json!("not a time"));
    assert_eq!(server.handle("fixture", INCIDENT_TOOL, args).await.is_error, Some(true));
    let mut args = arguments(&host);
    args["resource"]["namespace_id"] = json!("not-a-selector");
    assert_eq!(server.handle("fixture", INCIDENT_TOOL, args).await.is_error, Some(true));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    host.assert_source_free();
}
struct Awaited {
    started: Notify,
    release: Notify,
    finished: AtomicUsize,
    fail: bool,
}
impl Awaited {
    async fn outcome<T>(&self, token: &CollectionCancellation) -> Result<T> {
        self.started.notify_one();
        self.release.notified().await;
        self.finished.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(DeliveryProblem::Integrity.error())
        } else {
            assert!(token.is_cancelled());
            Err(CollectionStop::Cancelled.error())
        }
    }
}
impl WarehouseIncidentReader for Awaited {
    fn window<'a>(
        &'a self,
        _: &'a culprit_core::WarehouseWindowQuery,
        _: &'a CollectionCancellation,
    ) -> culprit_engine::WarehouseWindowTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }

    fn resources<'a>(
        &'a self,
        _: &'a PublishedResourceRequest,
        token: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a> {
        Box::pin(self.outcome(token))
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
        token: &'a CollectionCancellation,
    ) -> WarehouseIncidentTask<'a> {
        Box::pin(self.outcome(token))
    }
}
#[tokio::test]
async fn rpc_cancellation_signals_and_awaits_the_same_reader_for_both_resource_and_incident_calls() {
    for tool in [INCIDENT_TOOL, "resources"] {
        let host = Host::new();
        let reader = Arc::new(Awaited {
            started: Notify::new(),
            release: Notify::new(),
            finished: AtomicUsize::new(0),
            fail: false,
        });
        let (server, _) = host.install(reader.clone());
        let (cancel, cancelled) = oneshot::channel();
        let args = if tool == INCIDENT_TOOL { arguments(&host) } else { resource_arguments() };
        let work = tokio::spawn(async move {
            server
                .handle_cancellable("fixture", tool, args, async {
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
        host.assert_source_free();
    }
}
#[tokio::test]
async fn a_late_archive_failure_preserves_first_error_after_cancellation_and_is_audited() {
    let host = Host::new();
    let reader =
        Arc::new(Awaited { started: Notify::new(), release: Notify::new(), finished: AtomicUsize::new(0), fail: true });
    let (server, _) = host.install(reader.clone());
    let args = arguments(&host);
    let (cancel, cancelled) = oneshot::channel();
    let work = tokio::spawn(async move {
        server
            .handle_cancellable("fixture", INCIDENT_TOOL, args, async {
                let _ = cancelled.await;
            })
            .await
    });
    reader.started.notified().await;
    cancel.send(()).unwrap();
    reader.release.notify_one();
    let result = work.await.unwrap();
    assert_eq!(body(&result)["error"]["detail"]["delivery"], "integrity");
    assert_eq!(host.server.ws.audit_log(1).unwrap()[0].outcome, culprit_engine::AuditOutcome::Error);
    host.assert_source_free();
}
struct DelayedResult {
    started: Notify,
    release: Notify,
    report: Mutex<Option<IncidentReadReport>>,
}
impl WarehouseIncidentReader for DelayedResult {
    fn window<'a>(
        &'a self,
        _: &'a culprit_core::WarehouseWindowQuery,
        _: &'a CollectionCancellation,
    ) -> culprit_engine::WarehouseWindowTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }

    fn resources<'a>(
        &'a self,
        _: &'a PublishedResourceRequest,
        _: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a> {
        unsupported_resources()
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
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.report.lock().unwrap().take().unwrap())
        })
    }
}
#[tokio::test]
async fn delegation_revoked_while_original_reader_runs_withholds_its_result_without_relabeling_native_access() {
    let host = Host::new();
    let reader = Arc::new(DelayedResult {
        started: Notify::new(),
        release: Notify::new(),
        report: Mutex::new(Some(report(&host.database()))),
    });
    let (server, delegation) = host.install(reader.clone());
    let args = arguments(&host);
    let work = tokio::spawn(async move { server.handle("fixture", INCIDENT_TOOL, args).await });
    reader.started.notified().await;
    delegation.revoke();
    reader.release.notify_one();
    let result = work.await.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(body(&result).get("incidents").is_none());
    host.assert_source_free();
}
#[tokio::test]
async fn foreign_database_output_and_pre_dispatch_cancellation_are_refused_without_source_work() {
    let host = Host::new();
    let reader = reader(&host);
    reader.report.lock().unwrap().as_mut().unwrap().database =
        IncidentDatabaseSelector::from_key(&digest(b"another route")).unwrap();
    let (server, _) = host.install(reader);
    let result = server.handle("fixture", INCIDENT_TOOL, arguments(&host)).await;
    assert_eq!(body(&result)["error"]["detail"]["delivery"], "destination-changed");
    let reader = self::reader(&host);
    let (server, _) = host.install(reader.clone());
    let result = server.handle_cancellable("fixture", INCIDENT_TOOL, arguments(&host), std::future::ready(())).await;
    assert_eq!(body(&result)["error"]["detail"]["collection-stopped"], "cancelled");
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    host.assert_source_free();
}
#[tokio::test]
async fn forged_native_tools_are_denied_before_provider_observation_even_for_enabled_saved_connections() {
    let host = Host::new();
    host.server
        .ws
        .add_connection(NewConnection {
            name: "native-production".into(),
            input: "Server=fixture.invalid,14333;Database=ExactDB".into(),
            engine: Some(Engine::Mssql),
            auth: Some(AuthKind::None),
            ..Default::default()
        })
        .unwrap();
    host.server.ws.set_agent_access("native-production", AgentAccess::Query).unwrap();
    let (server, _) = host.install(reader(&host));
    for tool in ["scan", "run_query", "explain", "schema", "list_connections"] {
        let result = server
            .handle(
                "forged",
                tool,
                json!({"connection":"native-production","sql":"SELECT private-forged-data"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(body(&result)["error"]["code"], serde_json::to_value(ErrorCode::PermissionDenied).unwrap());
        assert!(body(&result).get("provider_acquisition").is_none());
    }
    assert!(
        host.server.ws.audit_log(10).unwrap().iter().all(|entry| entry.outcome == culprit_engine::AuditOutcome::Denied
            && entry.connection.is_none()
            && entry.sql.is_none())
    );
    host.assert_source_free();
}
#[tokio::test]
async fn resource_output_keeps_coverage_unknowns_text_and_paging_omissions_inside_response_cap() {
    let host = Host::new();
    let reader = reader(&host);
    {
        let mut locked = reader.resources.lock().unwrap();
        let report = locked.as_mut().unwrap();
        let row = &mut report.resources[0];
        row.database_name = "\u{1b}\n🌍".repeat(10000);
        row.connection_alias = Some("native-data".repeat(5000));
        row.coverage = PublishedResourceCoverage::Snapshot {
            sources: vec![
                StateSourceCoverage::unavailable(
                    "native-source".repeat(100),
                    ErrorCode::PermissionDenied,
                    Some(CollectionStop::Cancelled)
                );
                30
            ],
        };
        report.resources = vec![row.clone(); 8];
        report.gaps = vec![PublishedResourceGap::NotPublished; 100];
        report.next = Some(culprit_core::PublishedResourceCursor {
            selection_key: digest(b"selection"),
            position: report.resources[7].position.clone(),
        });
    }
    let (server, _) = host.install(reader);
    let result = server.handle("fixture", "resources", resource_arguments()).await;
    assert_ne!(result.is_error, Some(true));
    let output = body(&result);
    assert!(result.content[0].as_text().unwrap().text.len() <= super::super::response::MAX_OUTPUT_BYTES);
    assert_eq!(output["resources_available"], 8);
    assert_eq!(output["resources_omitted"], 0);
    assert_eq!(output["resources"].as_array().unwrap().len(), 8);
    assert!(!output["next"].is_null());
    assert_eq!(output["resources"][0]["coverage"]["sources_available"], 30);
    assert_eq!(output["resources"][0]["coverage"]["sources_omitted"], 26);
    assert_eq!(output["presentation_complete"], false);
    assert_eq!(output["resources"][0]["coverage"]["sources"][0]["state"], "unavailable");
    assert!(!output["resources"][0]["database_name"].as_str().unwrap().contains('\u{1b}'));
    host.assert_source_free();
}
#[tokio::test]
async fn paging_requires_same_explicit_cutoff_and_rejects_source_paths_or_caller_budget_growth() {
    let host = Host::new();
    let reader = reader(&host);
    let (server, _) = host.install(reader.clone());
    for args in [
        json!({"page_rows":9}),
        json!({"budget":999}),
        json!({"directory":"private-path"}),
        json!({"after":{"selection_key":digest(b"selection"),"position":{"resource_id":digest(b"r"),"source_id":digest(b"s"),"range_id":digest(b"range")}}}),
    ] {
        assert_eq!(server.handle("fixture", "resources", args.as_object().unwrap().clone()).await.is_error, Some(true));
    }
    assert_eq!(reader.resource_calls.load(Ordering::SeqCst), 0);
    host.assert_source_free();
}
#[tokio::test]
async fn dropping_outer_request_signals_its_existing_reader_cancellation() {
    struct ObserveDrop {
        started: Notify,
        token: Mutex<Option<CollectionCancellation>>,
    }
    impl WarehouseIncidentReader for ObserveDrop {
        fn window<'a>(
            &'a self,
            _: &'a culprit_core::WarehouseWindowQuery,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseWindowTask<'a> {
            Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
        }

        fn resources<'a>(
            &'a self,
            _: &'a PublishedResourceRequest,
            _: &'a CollectionCancellation,
        ) -> WarehouseResourcesTask<'a> {
            unsupported_resources()
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
    let (server, _) = host.install(reader.clone());
    let args = arguments(&host);
    let mut work = Box::pin(server.handle("fixture", INCIDENT_TOOL, args));
    tokio::select! {biased;result=&mut work=>panic!("request unexpectedly finished: {result:?}"),_=reader.started.notified()=>()};
    assert!(!reader.token.lock().unwrap().as_ref().unwrap().is_cancelled());
    drop(work);
    assert!(reader.token.lock().unwrap().as_ref().unwrap().is_cancelled());
    host.assert_source_free();
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

struct DelayedResources {
    started: Notify,
    release: Notify,
    report: Mutex<Option<PublishedResources>>,
}
impl WarehouseIncidentReader for DelayedResources {
    fn window<'a>(
        &'a self,
        _: &'a culprit_core::WarehouseWindowQuery,
        _: &'a CollectionCancellation,
    ) -> culprit_engine::WarehouseWindowTask<'a> {
        Box::pin(async { Err(DeliveryProblem::UnsupportedSchema.error()) })
    }

    fn resources<'a>(
        &'a self,
        _: &'a PublishedResourceRequest,
        _: &'a CollectionCancellation,
    ) -> WarehouseResourcesTask<'a> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.report.lock().unwrap().take().unwrap())
        })
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
}
#[tokio::test]
async fn completed_resource_success_is_withheld_after_cancellation_or_revocation_without_retry() {
    for revoked in [false, true] {
        let host = Host::new();
        let reader = Arc::new(DelayedResources {
            started: Notify::new(),
            release: Notify::new(),
            report: Mutex::new(Some(resources(&host))),
        });
        let (server, delegation) = host.install(reader.clone());
        let (cancel, cancelled) = oneshot::channel();
        let work = tokio::spawn(async move {
            server
                .handle_cancellable("fixture", "resources", resource_arguments(), async {
                    let _ = cancelled.await;
                })
                .await
        });
        reader.started.notified().await;
        if revoked {
            delegation.revoke();
        } else {
            cancel.send(()).unwrap();
        }
        reader.release.notify_one();
        let result = work.await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(
            reader.report.lock().unwrap().is_none(),
            "the same already-started reader returned its completed result"
        );
        assert!(body(&result).get("resources").is_none());
        if revoked {
            assert_eq!(body(&result)["error"]["code"], serde_json::to_value(ErrorCode::PermissionDenied).unwrap());
        } else {
            assert_eq!(body(&result)["error"]["detail"]["collection-stopped"], "cancelled");
        }
        host.assert_source_free();
    }
}
