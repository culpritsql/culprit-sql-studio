//! Source-free HTTP handler for shared browser access to telemetry archives.
//! Contains zero database drivers, zero collector schedulers and zero write-store features.
use culprit_core::{
    Error, ErrorCode, Plan, PlanDiff, PublishedResourceRequest, PublishedResourceSelector, Result,
    WarehouseArtifactQuery, WarehouseAwsProfile, WarehouseAzureProfile, WarehouseConnectionInfo,
    WarehouseConnectionRef, WarehouseConnectionSettings, WarehouseEndpoint, WarehouseFleetRequest,
    WarehouseIncidentView, WarehouseRecovery, WarehouseWindowQuery, t, telemetry::digest,
};
use culprit_engine::{
    WarehouseFleetReader, WarehouseIncidentReader,
    evidence::EvidenceConfig,
    export::{ExportFile, ExportFormat, ExportOptions},
};
use culprit_read::cancellation::CollectionCancellation;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HTTP_BODY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONCURRENT_REQUESTS: usize = 32;

pub struct ArchiveWebOptions {
    pub connection_name: String,
    pub bind: SocketAddr,
    pub ui_dist: Option<PathBuf>,
    pub require_auth_header: Option<String>,
    pub allow_unauthenticated_network: bool,
}

impl ArchiveWebOptions {
    pub fn validate(&self) -> Result<()> {
        if self.connection_name.trim().is_empty() || self.connection_name.len() > 128 {
            return Err(Error::invalid(t!("archive_web.cli.invalid_config")));
        }
        if let Some(header) = &self.require_auth_header
            && (header.trim().is_empty()
                || header.len() > 128
                || !header.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
        {
            return Err(Error::invalid(t!("archive_web.cli.invalid_config")));
        }
        if !self.bind.ip().is_loopback() && self.require_auth_header.is_none() && !self.allow_unauthenticated_network {
            return Err(Error::invalid(t!("archive_web.cli.auth_required")));
        }
        Ok(())
    }
}

pub struct HttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

pub struct ArchiveWebApp {
    connection: WarehouseConnectionInfo,
    incidents: Arc<dyn WarehouseIncidentReader>,
    fleet: Arc<dyn WarehouseFleetReader>,
    ui_dist: Option<PathBuf>,
    require_auth_header: Option<String>,
}

impl ArchiveWebApp {
    pub fn new(
        connection: WarehouseConnectionInfo,
        incidents: Arc<dyn WarehouseIncidentReader>,
        fleet: Arc<dyn WarehouseFleetReader>,
        ui_dist: Option<PathBuf>,
        require_auth_header: Option<String>,
    ) -> Self {
        Self {
            connection,
            incidents,
            fleet,
            ui_dist,
            require_auth_header: require_auth_header.map(|h| h.to_ascii_lowercase()),
        }
    }

    pub fn connection_info_from_config(name: &str, config: &EvidenceConfig) -> Result<WarehouseConnectionInfo> {
        let warehouse = match config.warehouse.provider.as_str() {
            "adx" => WarehouseEndpoint::Adx {
                endpoint: config
                    .warehouse
                    .options
                    .get("endpoint")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("adx")
                    .into(),
                database: config
                    .warehouse
                    .options
                    .get("database")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("telemetry")
                    .into(),
                authentication: WarehouseAzureProfile { name: config.warehouse.credential.clone(), tenant: None },
            },
            "clickhouse" => WarehouseEndpoint::Clickhouse {
                endpoint: config
                    .warehouse
                    .options
                    .get("endpoint")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("clickhouse")
                    .into(),
                database: config
                    .warehouse
                    .options
                    .get("database")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("telemetry")
                    .into(),
                username: config
                    .warehouse
                    .options
                    .get("username")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("reader")
                    .into(),
                credential_name: config.warehouse.credential.clone(),
            },
            _ => return Err(Error::invalid(t!("archive_web.cli.invalid_config"))),
        };
        let recovery = match config.recovery.provider.as_str() {
            "azure-blob" => WarehouseRecovery::AzureBlob {
                account: config
                    .recovery
                    .options
                    .get("account")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("recovery")
                    .into(),
                container: config
                    .recovery
                    .options
                    .get("container")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("telemetry")
                    .into(),
                prefix: config.recovery.options.get("prefix").and_then(serde_json::Value::as_str).unwrap_or("").into(),
                authentication: WarehouseAzureProfile { name: config.recovery.credential.clone(), tenant: None },
            },
            "s3" => WarehouseRecovery::S3 {
                bucket: config
                    .recovery
                    .options
                    .get("bucket")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("recovery")
                    .into(),
                region: config
                    .recovery
                    .options
                    .get("region")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("us-east-1")
                    .into(),
                prefix: config.recovery.options.get("prefix").and_then(serde_json::Value::as_str).unwrap_or("").into(),
                expected_owner: config
                    .recovery
                    .options
                    .get("expected_owner")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .into(),
                authentication: WarehouseAwsProfile { name: config.recovery.credential.clone(), profile: None },
            },
            _ => return Err(Error::invalid(t!("archive_web.cli.invalid_config"))),
        };
        let settings = WarehouseConnectionSettings {
            pipeline: config.pipeline.clone(),
            shard: config.shard.clone(),
            warehouse,
            recovery,
        };
        let revision = digest(
            &serde_json::to_vec(&("archive-web-connection-v1", name, &settings))
                .map_err(|_| Error::internal(t!("archive_web.cli.invalid_config")))?,
        );
        Ok(WarehouseConnectionInfo {
            reference: WarehouseConnectionRef { name: name.into(), revision },
            settings,
            password_saved: true,
        })
    }

    pub async fn dispatch(
        &self,
        method: &str,
        raw_path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
        cancellation: &CollectionCancellation,
    ) -> HttpResponse {
        let path = raw_path.split('?').next().unwrap_or(raw_path);
        if path == "/healthz" && method == "GET" {
            return json_ok(&serde_json::json!({ "ok": true, "mode": "archive-web" }));
        }
        if let Some(required) = &self.require_auth_header {
            let authenticated = headers.get(required).is_some_and(|value| !value.trim().is_empty());
            if !authenticated {
                return json_error(401, &Error::new(ErrorCode::PermissionDenied, t!("archive_web.http.unauthorized")));
            }
        }
        if path.starts_with("/api/") {
            return self.dispatch_api(method, path, headers, body, cancellation).await;
        }
        if method != "GET" && method != "HEAD" {
            return HttpResponse {
                status: 405,
                content_type: "text/plain; charset=utf-8",
                body: b"Method Not Allowed".to_vec(),
            };
        }
        self.serve_static(path)
    }

    async fn dispatch_api(
        &self,
        method: &str,
        path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
        cancellation: &CollectionCancellation,
    ) -> HttpResponse {
        let lang = headers
            .get("x-culprit-lang")
            .map(String::as_str)
            .filter(|code| matches!(*code, "en" | "de" | "es" | "fr"))
            .unwrap_or("en");

        match (method, path) {
            ("GET", "/api/archive/mode") => json_ok(&serde_json::json!({ "kind": "archive-web" })),
            ("GET", "/api/archive/connections") => json_ok(&vec![self.connection.clone()]),
            ("POST", "/api/archive/fleet") => {
                let parsed: Result<ConnectionEnvelope<WarehouseFleetRequest>> = parse_json(body);
                match parsed {
                    Ok(envelope) => {
                        if let Err(err) = self.verify_ref(&envelope.connection) {
                            return json_error(409, &err);
                        }
                        respond_result(self.fleet.fleet(&envelope.request, cancellation).await)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/archive/resources") => {
                let parsed: Result<ConnectionEnvelope<PublishedResourceRequest>> = parse_json(body);
                match parsed {
                    Ok(envelope) => {
                        if let Err(err) = self.verify_ref(&envelope.connection) {
                            return json_error(409, &err);
                        }
                        respond_result(self.incidents.resources(&envelope.request, cancellation).await)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/archive/incident") => {
                let parsed: Result<IncidentEnvelope> = parse_json(body);
                match parsed {
                    Ok(envelope) => {
                        if let Err(err) = self.verify_ref(&envelope.connection) {
                            return json_error(409, &err);
                        }
                        let res = self
                            .incidents
                            .incident(&envelope.resource, &envelope.around, cancellation)
                            .await
                            .map(|report| culprit_core::i18n::scope(lang, || WarehouseIncidentView::new(report)));
                        respond_result(res)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/archive/window") => {
                let parsed: Result<ConnectionEnvelope<WarehouseWindowQuery>> = parse_json(body);
                match parsed {
                    Ok(envelope) => {
                        if let Err(err) = self.verify_ref(&envelope.connection) {
                            return json_error(409, &err);
                        }
                        respond_result(self.incidents.window(&envelope.request, cancellation).await)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/archive/artifacts") => {
                let parsed: Result<ConnectionEnvelope<WarehouseArtifactQuery>> = parse_json(body);
                match parsed {
                    Ok(envelope) => {
                        if let Err(err) = self.verify_ref(&envelope.connection) {
                            return json_error(409, &err);
                        }
                        respond_result(self.incidents.artifacts(&envelope.request, cancellation).await)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/plans/parse") => {
                let parsed: Result<ParsePlanBody> = parse_json(body);
                match parsed {
                    Ok(req) => {
                        let lang = lang.to_string();
                        let res = culprit_engine::run_blocking(move || {
                            culprit_core::i18n::scope(&lang, || culprit_plans::parse_plan(&req.text))
                        })
                        .await;
                        respond_result(res)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/plans/diff") => {
                let parsed: Result<DiffPlansBody> = parse_json(body);
                match parsed {
                    Ok(req) => {
                        let lang = lang.to_string();
                        let res = culprit_engine::run_blocking(move || -> Result<PlanDiff> {
                            Ok(culprit_core::i18n::scope(&lang, || {
                                culprit_plans::compare_plans(
                                    &req.a,
                                    &req.name_a,
                                    &req.b,
                                    &req.name_b,
                                    &Default::default(),
                                )
                            }))
                        })
                        .await;
                        respond_result(res)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/plans/export-plan") => {
                let parsed: Result<ExportPlanBody> = parse_json(body);
                match parsed {
                    Ok(req) => {
                        let lang = lang.to_string();
                        let res = culprit_engine::run_blocking(move || -> Result<ExportFile> {
                            culprit_core::i18n::scope(&lang, || {
                                culprit_engine::export_plan(&req.plan, &req.title, req.format, &req.options)
                            })
                        })
                        .await;
                        respond_result(res)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            ("POST", "/api/plans/export-diff") => {
                let parsed: Result<ExportDiffBody> = parse_json(body);
                match parsed {
                    Ok(req) => {
                        let lang = lang.to_string();
                        let res = culprit_engine::run_blocking(move || -> Result<ExportFile> {
                            culprit_core::i18n::scope(&lang, || {
                                let diff = culprit_plans::compare_plans(
                                    &req.a,
                                    &req.name_a,
                                    &req.b,
                                    &req.name_b,
                                    &Default::default(),
                                );
                                culprit_engine::export_diff(
                                    &req.a,
                                    &req.b,
                                    &diff,
                                    &req.name_a,
                                    &req.name_b,
                                    req.format,
                                    &req.options,
                                )
                            })
                        })
                        .await;
                        respond_result(res)
                    }
                    Err(err) => json_error(400, &err),
                }
            }
            _ => json_error(404, &Error::not_found(t!("archive_web.http.read_only"))),
        }
    }

    fn verify_ref(&self, supplied: &WarehouseConnectionRef) -> Result<()> {
        if supplied == &self.connection.reference { Ok(()) } else { Err(Error::invalid(t!("ui.archives.changed"))) }
    }

    fn serve_static(&self, path: &str) -> HttpResponse {
        let Some(root) = &self.ui_dist else {
            return HttpResponse {
                status: 404,
                content_type: "text/plain; charset=utf-8",
                body: b"UI bundle not configured (--ui-dist)".to_vec(),
            };
        };
        let relative = path.trim_start_matches('/');
        let requested = if relative.is_empty() { "index.html" } else { relative };
        let Some(target) = safe_join(root, requested) else {
            return HttpResponse { status: 400, content_type: "text/plain; charset=utf-8", body: b"Bad path".to_vec() };
        };
        let resolved = if target.is_file() { target } else { root.join("index.html") };
        let Ok(bytes) = std::fs::read(&resolved) else {
            return HttpResponse {
                status: 404,
                content_type: "text/plain; charset=utf-8",
                body: b"Not Found".to_vec(),
            };
        };
        if resolved.file_name().and_then(|n| n.to_str()) == Some("index.html")
            && let Ok(html) = String::from_utf8(bytes.clone())
        {
            let marker = "<script>window.__CULPRIT_ARCHIVE_WEB__=true;</script>";
            let injected = if let Some(head_end) = html.find("</head>") {
                format!("{}{marker}{}", &html[..head_end], &html[head_end..])
            } else {
                format!("{marker}{html}")
            };
            return HttpResponse { status: 200, content_type: "text/html; charset=utf-8", body: injected.into_bytes() };
        }
        HttpResponse { status: 200, content_type: mime_for(&resolved), body: bytes }
    }
}

fn safe_join(root: &Path, relative: &str) -> Option<PathBuf> {
    let mut out = root.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            _ => return None,
        }
    }
    Some(out)
}

fn mime_for(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "woff2" => "font/woff2",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionEnvelope<T> {
    connection: WarehouseConnectionRef,
    request: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncidentEnvelope {
    connection: WarehouseConnectionRef,
    resource: PublishedResourceSelector,
    around: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParsePlanBody {
    text: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiffPlansBody {
    a: Plan,
    b: Plan,
    name_a: String,
    name_b: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportPlanBody {
    plan: Plan,
    title: String,
    format: ExportFormat,
    options: ExportOptions,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExportDiffBody {
    a: Plan,
    b: Plan,
    name_a: String,
    name_b: String,
    format: ExportFormat,
    options: ExportOptions,
}

fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T> {
    serde_json::from_slice(body).map_err(|err| Error::invalid(err.to_string()))
}

fn json_ok<T: Serialize>(value: &T) -> HttpResponse {
    match serde_json::to_vec(value) {
        Ok(body) => HttpResponse { status: 200, content_type: "application/json; charset=utf-8", body },
        Err(err) => json_error(500, &Error::internal(err.to_string())),
    }
}

fn json_error(status: u16, error: &Error) -> HttpResponse {
    let body = serde_json::to_vec(error)
        .unwrap_or_else(|_| br#"{"code":"internal","message":"internal error","hint":null}"#.to_vec());
    HttpResponse { status, content_type: "application/json; charset=utf-8", body }
}

fn respond_result<T: Serialize>(result: Result<T>) -> HttpResponse {
    match result {
        Ok(value) => json_ok(&value),
        Err(error) => {
            let status = match error.code {
                ErrorCode::InvalidInput => 400,
                ErrorCode::PermissionDenied | ErrorCode::AuthFailed => 403,
                ErrorCode::NotFound => 404,
                ErrorCode::Unsupported => 422,
                ErrorCode::Timeout => 504,
                _ => 500,
            };
            json_error(status, &error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_core::{
        IncidentReadReport, PublishedResources, WarehouseArtifactReport, WarehouseFleetReport, WarehouseFleetSummary,
        WarehouseWindowReport, collected_analysis::CollectedEvidenceMetrics,
    };

    fn zero_metrics() -> CollectedEvidenceMetrics {
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

    struct StubReader;

    impl WarehouseIncidentReader for StubReader {
        fn resources<'a>(
            &'a self,
            request: &'a PublishedResourceRequest,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseResourcesTask<'a> {
            Box::pin(async move {
                request.validate()?;
                Ok(PublishedResources {
                    as_of: request.as_of.clone(),
                    window: request.window.clone(),
                    resources: vec![],
                    next: None,
                    discovery_complete: true,
                    gaps: vec![],
                    candidate_rows: 0,
                    archive_work: zero_metrics(),
                    recovery_metadata_reads: 0,
                })
            })
        }
        fn window<'a>(
            &'a self,
            _: &'a WarehouseWindowQuery,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseWindowTask<'a> {
            Box::pin(async { Err::<WarehouseWindowReport, _>(Error::not_found("none")) })
        }
        fn artifacts<'a>(
            &'a self,
            _: &'a WarehouseArtifactQuery,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseArtifactsTask<'a> {
            Box::pin(async { Err::<WarehouseArtifactReport, _>(Error::not_found("none")) })
        }
        fn incident<'a>(
            &'a self,
            _: &'a PublishedResourceSelector,
            _: &'a str,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseIncidentTask<'a> {
            Box::pin(async { Err::<IncidentReadReport, _>(Error::not_found("none")) })
        }
    }

    impl WarehouseFleetReader for StubReader {
        fn fleet<'a>(
            &'a self,
            request: &'a WarehouseFleetRequest,
            _: &'a CollectionCancellation,
        ) -> culprit_engine::WarehouseFleetTask<'a> {
            Box::pin(async move {
                request.validate()?;
                Ok(WarehouseFleetReport {
                    request: request.clone(),
                    summary: WarehouseFleetSummary::default(),
                    rows: vec![],
                    next: None,
                    discovery_complete: true,
                    gaps: vec![],
                    candidate_rows: 0,
                    archive_work: zero_metrics(),
                    qualifications: vec![],
                })
            })
        }
    }

    fn test_connection() -> WarehouseConnectionInfo {
        WarehouseConnectionInfo {
            reference: WarehouseConnectionRef { name: "prod-adx".into(), revision: "a".repeat(64) },
            settings: WarehouseConnectionSettings {
                pipeline: "prod".into(),
                shard: "shard-0".into(),
                warehouse: WarehouseEndpoint::Adx {
                    endpoint: "https://cluster.kusto.windows.net".into(),
                    database: "telemetry".into(),
                    authentication: WarehouseAzureProfile { name: "adx".into(), tenant: None },
                },
                recovery: WarehouseRecovery::AzureBlob {
                    account: "acct".into(),
                    container: "telemetry".into(),
                    prefix: "prod".into(),
                    authentication: WarehouseAzureProfile { name: "blob".into(), tenant: None },
                },
            },
            password_saved: true,
        }
    }

    #[test]
    fn non_loopback_bind_requires_explicit_proxy_auth_or_override() {
        let mut opts = ArchiveWebOptions {
            connection_name: "prod-adx".into(),
            bind: "0.0.0.0:8080".parse().unwrap(),
            ui_dist: None,
            require_auth_header: None,
            allow_unauthenticated_network: false,
        };
        assert!(opts.validate().is_err());
        opts.require_auth_header = Some("X-MS-CLIENT-PRINCIPAL-ID".into());
        assert!(opts.validate().is_ok());
    }

    #[tokio::test]
    async fn auth_header_gate_and_fleet_dispatch_work_without_live_sources() {
        let reader = Arc::new(StubReader);
        let conn = test_connection();
        let app =
            ArchiveWebApp::new(conn.clone(), reader.clone(), reader, None, Some("X-MS-CLIENT-PRINCIPAL-ID".into()));
        let cancellation = CollectionCancellation::default();

        let unauth = app.dispatch("GET", "/api/archive/connections", &BTreeMap::new(), b"", &cancellation).await;
        assert_eq!(unauth.status, 401);

        let mut headers = BTreeMap::new();
        headers.insert("x-ms-client-principal-id".into(), "alice@contoso.com".into());
        let ok = app.dispatch("GET", "/api/archive/connections", &headers, b"", &cancellation).await;
        assert_eq!(ok.status, 200);

        let body = serde_json::to_vec(&serde_json::json!({
            "connection": conn.reference,
            "request": WarehouseFleetRequest {
                as_of: "2026-04-01T12:00:00Z".into(),
                ..Default::default()
            }
        }))
        .unwrap();
        let fleet = app.dispatch("POST", "/api/archive/fleet", &headers, &body, &cancellation).await;
        assert_eq!(fleet.status, 200);

        assert!(safe_join(Path::new("/srv/ui"), "../etc/passwd").is_none());
    }
}
