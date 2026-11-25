use super::*;
use culprit_auth::{BearerTokenProvider, DatabaseTokenProvider, PasswordCredential};
use std::collections::BTreeMap;
use zeroize::Zeroizing;

#[derive(Default)]
struct FakeCredentials {
    secrets: BTreeMap<String, String>,
}

impl FakeCredentials {
    fn with(profile: &str, secret: &str) -> Self {
        let mut secrets = BTreeMap::new();
        secrets.insert(profile.to_string(), secret.to_string());
        Self { secrets }
    }
}

impl CredentialResolver for FakeCredentials {
    fn bearer(&self, _profile: &str) -> Result<Arc<dyn BearerTokenProvider>> {
        Err(Error::invalid("unsupported"))
    }
    fn database(&self, _profile: &str) -> Result<Arc<dyn DatabaseTokenProvider>> {
        Err(Error::invalid("unsupported"))
    }
    fn validate_password(&self, profile: &str) -> Result<()> {
        self.secrets.contains_key(profile).then_some(()).ok_or_else(|| Error::invalid("missing profile"))
    }
    fn password(&self, profile: &str) -> Result<PasswordCredential> {
        let value = self.secrets.get(profile).ok_or_else(|| Error::invalid("missing profile"))?;
        Ok(PasswordCredential { username: "bot".into(), password: Zeroizing::new(value.clone()) })
    }
}

struct CustomSink;

#[async_trait]
impl NotificationSink for CustomSink {
    fn kind(&self) -> &'static str {
        "custom-test"
    }
    fn validate_config(&self, config: &serde_json::Value, credentials: &dyn CredentialResolver) -> Result<()> {
        let cred = config.get("credential").and_then(|v| v.as_str()).ok_or_else(|| Error::invalid("missing cred"))?;
        credentials.validate_password(cred)
    }
    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &serde_json::Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt> {
        let cred = config.get("credential").and_then(|v| v.as_str()).unwrap();
        let secret = credentials.password(cred)?;
        let body = serde_json::to_vec(event).unwrap();
        let resp = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Post,
                    url: "https://hooks.example.com/custom",
                    headers: vec![("Authorization", Zeroizing::new(format!("Bearer {}", secret.password.as_str())))],
                    body: Some(&body),
                    max_response_bytes: MAX_ALERT_RESPONSE_BYTES,
                },
                cancellation,
            )
            .await?;
        Ok(SinkDeliveryReceipt {
            sink_kind: self.kind().into(),
            status_code: resp.status,
            external_id: Some("id-1".into()),
        })
    }
}

fn sample_incident(revision: &str, lifecycle: &str, first_obs: &str, obs_until: &str) -> IncidentAlertInput {
    IncidentAlertInput {
        engine: Some(culprit_core::Engine::Mssql),
        target_id: "db:mssql:orders".into(),
        original_scope_id: "scope-1".into(),
        original_source: "mssql.activity".into(),
        episode_id: "ep-42".into(),
        controller_version: 1,
        revision: revision.into(),
        lifecycle: lifecycle.into(),
        first_recorded_observation: first_obs.into(),
        observed_until: obs_until.into(),
        connection_alias: "orders-prod".into(),
        database_name: Some("orders".into()),
        environment: Some("production".into()),
        title: "Blocking chain on dbo.Orders".into(),
        victim_summary: "12 sessions blocked waiting for KEY lock".into(),
        culprit_query: Some("UPDATE dbo.Orders SET Status = 'PAID' WHERE CustomerId = 99812".into()),
        developer_action: "Commit the open transaction promptly.".into(),
        verification: "Verify blocked sessions drop to zero.".into(),
    }
}

#[test]
fn incident_deduplication_ignores_revision_updates_suppresses_backfill_and_emits_matching_resolution() {
    let evaluator = AlertEvaluator::new(AlertEvaluationPolicy {
        novelty_horizon_seconds: 900,
        min_consecutive_windows: 2,
        archive_viewer_origin: Some("https://archives.example.com".into()),
    })
    .unwrap();
    let mut state = DeduplicationState::default();

    // 1. Backfilled episode older than 900s novelty horizon is suppressed without paging.
    let mut backfill = sample_incident("1", "active", "2026-11-25T10:00:00Z", "2026-11-25T10:01:00Z");
    backfill.episode_id = "ep-old".into();
    let out = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:00:00Z".into(),
                incidents: vec![backfill.clone()],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert!(out.is_empty());

    // 2. Fresh active episode triggers exactly once and redacts SQL literals.
    let first = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:00:00Z".into(),
                incidents: vec![sample_incident("1", "active", "2026-11-25T11:58:00Z", "2026-11-25T11:59:00Z")],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].transition, AlertTransition::Triggered);
    assert!(!first[0].culprit_query.as_deref().unwrap().contains("99812"));
    assert!(!first[0].culprit_query.as_deref().unwrap().contains("PAID"));
    assert_eq!(first[0].deep_link.as_deref(), Some("https://archives.example.com/?resource=orders-prod"));

    // 3. Revision 2 enrichment of the same episode does NOT re-alert.
    let second = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:01:00Z".into(),
                incidents: vec![sample_incident("2", "active", "2026-11-25T11:58:00Z", "2026-11-25T12:00:30Z")],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert!(second.is_empty());

    // 4. Lifecycle transition to "resolved" emits Resolved with the exact same dedup_key.
    let resolved = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:05:00Z".into(),
                incidents: vec![sample_incident("3", "resolved", "2026-11-25T11:58:00Z", "2026-11-25T12:04:30Z")],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].transition, AlertTransition::Resolved);
    assert_eq!(resolved[0].dedup_key, first[0].dedup_key);
}

#[test]
fn finding_hysteresis_and_separate_availability_routing_stay_independent() {
    let evaluator = AlertEvaluator::new(AlertEvaluationPolicy::default()).unwrap();
    let mut state = DeduplicationState::default();

    let finding = |until: &str| FindingAlertInput {
        engine: Some(culprit_core::Engine::Mssql),
        resource_id: "res-1".into(),
        finding_id: "missing-index:dbo.Orders(CustomerId)".into(),
        check_id: "missing-index".into(),
        severity: "high".into(),
        actionability: "needs-attention".into(),
        inactive: false,
        observed_from: "2026-11-25T11:00:00Z".into(),
        observed_until: until.into(),
        connection_alias: "orders-prod".into(),
        database_name: Some("orders".into()),
        environment: Some("production".into()),
        title: "Missing index on dbo.Orders(CustomerId)".into(),
        summary: "44% of DB time spent scanning dbo.Orders.".into(),
        query_sql: Some("SELECT * FROM dbo.Orders WHERE CustomerId = 42".into()),
        developer_action: Some("CREATE INDEX ix_orders_customer ON dbo.Orders (CustomerId) WITH (ONLINE = ON);".into()),
        verification: Some("Re-scan after index build.".into()),
    };

    // Window 1: recorded, below min_consecutive_windows=2.
    let w1 = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:00:00Z".into(),
                findings: vec![finding("2026-11-25T11:59:00Z")],
                availability: vec![AvailabilityAlertInput {
                    resource_id: "res-2".into(),
                    connection_alias: "billing-prod".into(),
                    database_name: Some("billing".into()),
                    environment: Some("production".into()),
                    state: "stale".into(),
                    input_read_complete: false,
                    context_issues: vec!["collector-lag".into()],
                    observed_at: "2026-11-25T12:00:00Z".into(),
                }],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert_eq!(w1.len(), 1);
    assert_eq!(w1[0].category, AlertCategory::EvidenceAvailability);
    assert_eq!(w1[0].transition, AlertTransition::AvailabilityDegraded);

    // Window 2: consecutive window triggers high finding alert.
    let w2 = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:05:00Z".into(),
                findings: vec![finding("2026-11-25T12:04:00Z")],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert_eq!(w2.len(), 1);
    assert_eq!(w2[0].category, AlertCategory::HighFinding);
    assert_eq!(w2[0].transition, AlertTransition::Triggered);

    // Complete evaluation of res-1 without the finding resolves it.
    let w3 = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:10:00Z".into(),
                assessed_finding_resources: BTreeSet::from(["res-1".to_string()]),
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    assert_eq!(w3.len(), 1);
    assert_eq!(w3[0].category, AlertCategory::HighFinding);
    assert_eq!(w3[0].transition, AlertTransition::Resolved);
    assert_eq!(w3[0].dedup_key, w2[0].dedup_key);
}

#[tokio::test]
async fn pluggable_notifier_validates_offline_routes_by_category_and_persists_durable_dedup_journal() {
    let dir = tempfile::tempdir().unwrap();
    let store = DurableFileDeduplicationStore::open(dir.path()).unwrap();
    assert!(DurableFileDeduplicationStore::open(dir.path()).is_err(), "exclusive lock prevents dual writers");

    let creds = FakeCredentials::with("custom_token", "super-secret-value");
    let notifier = Notifier::new().with_sink(Arc::new(CustomSink));
    let routes = vec![
        NotificationRoute {
            name: "db_oncall".into(),
            sink: "custom-test".into(),
            categories: BTreeSet::from([AlertCategory::Incident, AlertCategory::Regression]),
            min_severity: AlertSeverity::High,
            send_resolutions: true,
            settings: serde_json::json!({ "credential": "custom_token" }),
        },
        NotificationRoute {
            name: "collector_ops".into(),
            sink: "custom-test".into(),
            categories: BTreeSet::from([AlertCategory::EvidenceAvailability]),
            min_severity: AlertSeverity::Warning,
            send_resolutions: true,
            settings: serde_json::json!({ "credential": "custom_token" }),
        },
    ];
    notifier.validate_routes(&routes, &creds).unwrap();

    let evaluator = AlertEvaluator::new(AlertEvaluationPolicy::default()).unwrap();
    let mut state = store.load().unwrap();
    let events = evaluator
        .evaluate(
            &AlertBatchInput {
                evaluated_at: "2026-11-25T12:00:00Z".into(),
                regressions: vec![RegressionAlertInput {
                    engine: Some(culprit_core::Engine::Mssql),
                    resource_id: "res-1".into(),
                    regression_id: "query-regression:q99".into(),
                    observed_until: "2026-11-25T11:59:00Z".into(),
                    connection_alias: "orders-prod".into(),
                    database_name: Some("orders".into()),
                    environment: Some("production".into()),
                    title: "Query q99 slowed 4.1x after plan switch".into(),
                    summary: "Mean duration rose from 8ms to 33ms.".into(),
                    query_sql: Some("SELECT OrderId FROM dbo.Orders WHERE TenantId = 'acme'".into()),
                    developer_action: Some("Inspect compiled parameter values.".into()),
                    verification: Some("Compare plan 68 vs plan 69.".into()),
                    proximity_marks: vec!["deploy v2.14.0 (2026-11-25T11:50:00Z)".into()],
                }],
                ..Default::default()
            },
            &mut state,
        )
        .unwrap();
    store.save(&state).unwrap();
    assert_eq!(store.load().unwrap(), state);

    let transport = RecordedTransport::default();
    let outcomes =
        notifier.dispatch(&events, &routes, &creds, &transport, &CollectionCancellation::default()).await.unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].route, "db_oncall");
    let calls = transport.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].non_secret_headers.is_empty(), "Authorization header is never logged in non-secret headers");
}

#[test]
fn secret_endpoint_resolution_enforces_https_origin_pinning_and_rejects_host_redirects() {
    assert_eq!(
        resolve_secret_endpoint("https://hooks.slack.com", "/services/T1/B2/XYZ").unwrap(),
        "https://hooks.slack.com/services/T1/B2/XYZ"
    );
    assert_eq!(
        resolve_secret_endpoint("https://hooks.slack.com", "https://hooks.slack.com/services/T1/B2/XYZ").unwrap(),
        "https://hooks.slack.com/services/T1/B2/XYZ"
    );
    assert!(resolve_secret_endpoint("http://hooks.slack.com", "/services/T1").is_err());
    assert!(resolve_secret_endpoint("https://hooks.slack.com", "https://evil.example.com/services/T1").is_err());
    assert!(resolve_secret_endpoint("https://hooks.slack.com", "//evil.example.com/services/T1").is_err());
    assert!(resolve_secret_endpoint("https://hooks.slack.com", "/services/../admin").is_err());
}
