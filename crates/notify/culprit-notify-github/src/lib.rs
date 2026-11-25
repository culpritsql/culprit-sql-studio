//! GitHub Issues adapter creating, updating, and auto-closing deduplicated issues by `dedup_key` marker.

use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, t};
use culprit_notify::{
    HttpMethod, MAX_ALERT_RESPONSE_BYTES, NotificationSink, NotificationTransport, OutboundHttpRequest,
    RedactedAlertEvent, SinkDeliveryReceipt,
};
use culprit_read::cancellation::CollectionCancellation;
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

#[derive(Default)]
pub struct GitHubIssueSink;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GitHubConfig {
    #[serde(default = "default_api_origin")]
    api_origin: String,
    owner: String,
    repo: String,
    token_credential: String,
    #[serde(default)]
    labels: Vec<String>,
}

fn default_api_origin() -> String {
    "https://api.github.com".into()
}

fn valid_repo_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value != "."
        && value != ".."
        && value.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

impl GitHubIssueSink {
    pub fn new() -> Self {
        Self
    }

    fn parse_config(config: &Value) -> Result<(String, GitHubConfig)> {
        let parsed: GitHubConfig =
            serde_json::from_value(config.clone()).map_err(|_| Error::invalid(t!("notify.config.invalid_sink")))?;
        let canonical =
            https_origin(&parsed.api_origin).ok_or_else(|| Error::invalid(t!("notify.config.invalid_origin")))?;
        if !valid_repo_segment(&parsed.owner)
            || !valid_repo_segment(&parsed.repo)
            || parsed.labels.len() > 16
            || parsed.labels.iter().any(|l| l.is_empty() || l.len() > 64 || l.chars().any(char::is_control))
        {
            return Err(Error::invalid(t!("notify.config.invalid_sink")));
        }
        Ok((canonical, parsed))
    }

    pub fn fingerprint_marker(dedup_key: &str) -> String {
        format!("<!-- culprit-alert:{dedup_key} -->")
    }

    fn render_body(event: &RedactedAlertEvent) -> String {
        let mut lines = vec![
            Self::fingerprint_marker(&event.dedup_key),
            format!("### {}", event.headline),
            String::new(),
            event.summary.clone(),
            String::new(),
            format!("- **Resource:** `{}`", event.resource_alias),
            format!("- **Category:** `{}`", event.category.slug()),
            format!("- **Severity:** `{}`", event.severity.label()),
            format!("- **Observed:** `{}`", event.observed_at),
            format!("- **Correlation key:** `{}`", event.dedup_key),
        ];
        if let Some(env) = &event.environment {
            lines.push(format!("- **Environment:** `{env}`"));
        }
        if let Some(sql) = &event.culprit_query {
            lines.push(String::new());
            lines.push("**Redacted SQL:**".into());
            lines.push(format!("```sql\n{sql}\n```"));
        }
        if let Some(fix) = &event.developer_action {
            lines.push(String::new());
            lines.push(format!("**Recommended action:** {fix}"));
        }
        if let Some(verify) = &event.verification {
            lines.push(String::new());
            lines.push(format!("**Verification:** {verify}"));
        }
        if !event.proximity_marks.is_empty() {
            lines.push(String::new());
            lines.push(format!(
                "_Nearby timeline marks (proximity only, not proof):_ {}",
                event.proximity_marks.join(", ")
            ));
        }
        if let Some(link) = &event.deep_link {
            lines.push(String::new());
            lines.push(format!("[Inspect in Culprit]({link})"));
        }
        lines.join("\n")
    }

    fn auth_headers(token: &str) -> Vec<(&'static str, Zeroizing<String>)> {
        vec![
            ("Authorization", Zeroizing::new(format!("Bearer {token}"))),
            ("Accept", Zeroizing::new("application/vnd.github+json".into())),
            ("X-GitHub-Api-Version", Zeroizing::new("2022-11-28".into())),
            ("Content-Type", Zeroizing::new("application/json".into())),
        ]
    }
}

#[async_trait]
impl NotificationSink for GitHubIssueSink {
    fn kind(&self) -> &'static str {
        "github-issues"
    }

    fn validate_config(&self, config: &Value, credentials: &dyn CredentialResolver) -> Result<()> {
        let (_, cfg) = Self::parse_config(config)?;
        credentials.validate_password(&cfg.token_credential)
    }

    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt> {
        let (origin, cfg) = Self::parse_config(config)?;
        let token = credentials.password(&cfg.token_credential)?;
        let marker = Self::fingerprint_marker(&event.dedup_key);

        // 1. Check open issues (up to 30) for an existing matching marker so we deduplicate or auto-close cleanly.
        let list_url = format!("{origin}/repos/{}/{}/issues?state=open&per_page=30", cfg.owner, cfg.repo);
        let list_resp = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Get,
                    url: &list_url,
                    headers: Self::auth_headers(token.password.as_str()),
                    body: None,
                    max_response_bytes: MAX_ALERT_RESPONSE_BYTES,
                },
                cancellation,
            )
            .await?;
        if !(200..300).contains(&list_resp.status) {
            return Err(Error::new(
                ErrorCode::QueryFailed,
                t!("notify.delivery.http_failed", sink = self.kind(), status = list_resp.status.to_string()),
            ));
        }
        let open_issues: Vec<Value> = serde_json::from_slice(&list_resp.body).unwrap_or_default();
        let existing_number = open_issues.iter().find_map(|issue| {
            let body = issue.get("body").and_then(Value::as_str)?;
            body.contains(&marker).then(|| issue.get("number").and_then(Value::as_u64)).flatten()
        });

        if event.transition.is_resolution() {
            let Some(number) = existing_number else {
                return Ok(SinkDeliveryReceipt {
                    sink_kind: self.kind().into(),
                    status_code: list_resp.status,
                    external_id: None,
                });
            };
            let close_url = format!("{origin}/repos/{}/{}/issues/{number}", cfg.owner, cfg.repo);
            let close_body = serde_json::to_vec(&json!({ "state": "closed", "state_reason": "completed" }))
                .map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?;
            let resp = transport
                .send_http(
                    OutboundHttpRequest {
                        method: HttpMethod::Patch,
                        url: &close_url,
                        headers: Self::auth_headers(token.password.as_str()),
                        body: Some(&close_body),
                        max_response_bytes: MAX_ALERT_RESPONSE_BYTES,
                    },
                    cancellation,
                )
                .await?;
            if !(200..300).contains(&resp.status) {
                return Err(Error::new(
                    ErrorCode::QueryFailed,
                    t!("notify.delivery.http_failed", sink = self.kind(), status = resp.status.to_string()),
                ));
            }
            return Ok(SinkDeliveryReceipt {
                sink_kind: self.kind().into(),
                status_code: resp.status,
                external_id: Some(number.to_string()),
            });
        }

        if let Some(number) = existing_number {
            return Ok(SinkDeliveryReceipt {
                sink_kind: self.kind().into(),
                status_code: list_resp.status,
                external_id: Some(number.to_string()),
            });
        }

        let create_url = format!("{origin}/repos/{}/{}/issues", cfg.owner, cfg.repo);
        let mut labels = cfg.labels.clone();
        if !labels.iter().any(|l| l == "culprit") {
            labels.push("culprit".into());
        }
        let create_payload = json!({
            "title": format!("[Culprit] {}: {}", event.resource_alias, event.headline),
            "body": Self::render_body(event),
            "labels": labels,
        });
        let create_bytes =
            serde_json::to_vec(&create_payload).map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?;
        let create_resp = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Post,
                    url: &create_url,
                    headers: Self::auth_headers(token.password.as_str()),
                    body: Some(&create_bytes),
                    max_response_bytes: MAX_ALERT_RESPONSE_BYTES,
                },
                cancellation,
            )
            .await?;
        if !(200..300).contains(&create_resp.status) {
            return Err(Error::new(
                ErrorCode::QueryFailed,
                t!("notify.delivery.http_failed", sink = self.kind(), status = create_resp.status.to_string()),
            ));
        }
        let created: Value = serde_json::from_slice(&create_resp.body).unwrap_or(Value::Null);
        let issue_number = created.get("number").and_then(Value::as_u64).map(|n| n.to_string());
        Ok(SinkDeliveryReceipt {
            sink_kind: self.kind().into(),
            status_code: create_resp.status,
            external_id: issue_number,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_auth::{BearerTokenProvider, DatabaseTokenProvider, PasswordCredential};
    use culprit_notify::{AlertCategory, AlertSeverity, AlertTransition, OutboundHttpResponse, RecordedTransport};
    use std::sync::Arc;

    struct Creds;
    impl CredentialResolver for Creds {
        fn bearer(&self, _: &str) -> Result<Arc<dyn BearerTokenProvider>> {
            Err(Error::invalid("no"))
        }
        fn database(&self, _: &str) -> Result<Arc<dyn DatabaseTokenProvider>> {
            Err(Error::invalid("no"))
        }
        fn validate_password(&self, profile: &str) -> Result<()> {
            (profile == "gh_bot").then_some(()).ok_or_else(|| Error::invalid("missing"))
        }
        fn password(&self, profile: &str) -> Result<PasswordCredential> {
            self.validate_password(profile)?;
            Ok(PasswordCredential { username: "bot".into(), password: Zeroizing::new("ghp_testtoken".into()) })
        }
    }

    #[tokio::test]
    async fn github_sink_creates_issue_with_marker_and_closes_matching_issue_on_resolution() {
        let sink = GitHubIssueSink::new();
        let cfg = json!({
            "owner": "acme",
            "repo": "orders-service",
            "token_credential": "gh_bot",
            "labels": ["database"]
        });
        sink.validate_config(&cfg, &Creds).unwrap();
        assert!(
            sink.validate_config(
                &json!({
                    "owner": "../evil",
                    "repo": "orders-service",
                    "token_credential": "gh_bot"
                }),
                &Creds
            )
            .is_err()
        );

        let mut event = RedactedAlertEvent {
            dedup_key: "culprit:high-finding:idx1".into(),
            category: AlertCategory::HighFinding,
            transition: AlertTransition::Triggered,
            severity: AlertSeverity::High,
            engine: Some(culprit_core::Engine::Postgres),
            resource_alias: "orders-prod".into(),
            database_name: Some("orders".into()),
            environment: Some("production".into()),
            headline: "Missing index on orders(customer_id)".into(),
            summary: "Full scan consumes 41% DB time".into(),
            culprit_query: Some("SELECT * FROM orders WHERE customer_id = ?".into()),
            victim_summary: None,
            developer_action: Some("CREATE INDEX CONCURRENTLY ...".into()),
            verification: None,
            proximity_marks: Vec::new(),
            observed_at: "2026-11-25T12:00:00Z".into(),
            deep_link: None,
        };

        let marker = GitHubIssueSink::fingerprint_marker(&event.dedup_key);
        let transport = RecordedTransport::with_replies([
            Ok(OutboundHttpResponse { status: 200, body: b"[]".to_vec() }),
            Ok(OutboundHttpResponse { status: 201, body: serde_json::to_vec(&json!({ "number": 77 })).unwrap() }),
            Ok(OutboundHttpResponse {
                status: 200,
                body: serde_json::to_vec(&json!([{ "number": 77, "body": format!("{marker}\nexisting") }])).unwrap(),
            }),
            Ok(OutboundHttpResponse {
                status: 200,
                body: serde_json::to_vec(&json!({ "number": 77, "state": "closed" })).unwrap(),
            }),
        ]);

        let created = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        assert_eq!(created.external_id.as_deref(), Some("77"));

        event.transition = AlertTransition::Resolved;
        let closed = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        assert_eq!(closed.external_id.as_deref(), Some("77"));
        assert_eq!(transport.calls().len(), 4);
        assert_eq!(transport.calls()[3].method, HttpMethod::Patch);
    }
}
