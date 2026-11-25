//! Pure Slack Block Kit adapter implementing `NotificationSink` over an injected `NotificationTransport`.

use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, t};
use culprit_notify::{
    HttpMethod, MAX_ALERT_RESPONSE_BYTES, NotificationSink, NotificationTransport, OutboundHttpRequest,
    RedactedAlertEvent, SinkDeliveryReceipt, resolve_secret_endpoint,
};
use culprit_read::cancellation::CollectionCancellation;
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

#[derive(Default)]
pub struct SlackSink;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SlackConfig {
    #[serde(default = "default_origin")]
    origin: String,
    credential: String,
    #[serde(default)]
    channel: Option<String>,
}

fn default_origin() -> String {
    "https://hooks.slack.com".into()
}

impl SlackSink {
    pub fn new() -> Self {
        Self
    }

    fn parse_config(config: &Value) -> Result<SlackConfig> {
        let parsed: SlackConfig =
            serde_json::from_value(config.clone()).map_err(|_| Error::invalid(t!("notify.config.invalid_sink")))?;
        if https_origin(&parsed.origin).is_none() {
            return Err(Error::invalid(t!("notify.config.invalid_origin")));
        }
        if let Some(ch) = &parsed.channel
            && (ch.is_empty() || ch.len() > 128 || ch.chars().any(char::is_control))
        {
            return Err(Error::invalid(t!("notify.config.invalid_sink")));
        }
        Ok(parsed)
    }

    pub fn format_payload(event: &RedactedAlertEvent, channel: Option<&str>) -> Value {
        let prefix = if event.transition.is_resolution() { "RESOLVED" } else { event.severity.label() };
        let header_text = format!("[{}] {}: {}", prefix.to_ascii_uppercase(), event.resource_alias, event.headline);
        let mut fields = vec![
            json!({ "type": "mrkdwn", "text": format!("*Resource:*\n`{}`", event.resource_alias) }),
            json!({ "type": "mrkdwn", "text": format!("*Category:*\n`{}`", event.category.slug()) }),
            json!({ "type": "mrkdwn", "text": format!("*Observed:*\n`{}`", event.observed_at) }),
            json!({ "type": "mrkdwn", "text": format!("*Correlation:*\n`{}`", event.dedup_key) }),
        ];
        if let Some(env) = &event.environment {
            fields.push(json!({ "type": "mrkdwn", "text": format!("*Environment:*\n`{env}`") }));
        }
        let mut blocks = vec![
            json!({
                "type": "header",
                "text": { "type": "plain_text", "text": truncate_chars(&header_text, 150) }
            }),
            json!({
                "type": "section",
                "text": { "type": "mrkdwn", "text": event.summary }
            }),
            json!({
                "type": "section",
                "fields": fields
            }),
        ];
        if let Some(query) = &event.culprit_query {
            blocks.push(json!({
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("*Redacted culprit SQL:*\n```{}```", truncate_chars(query, 1500)) }
            }));
        }
        if let Some(action) = &event.developer_action {
            blocks.push(json!({
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("*Fix:*\n{action}") }
            }));
        }
        if !event.proximity_marks.is_empty() {
            blocks.push(json!({
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": format!("Nearby timeline marks (proximity, not proof): {}", event.proximity_marks.join(", "))
                }]
            }));
        }
        if let Some(link) = &event.deep_link {
            blocks.push(json!({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": { "type": "plain_text", "text": "Inspect in Culprit" },
                    "url": link
                }]
            }));
        }
        let mut root = json!({
            "text": header_text,
            "blocks": blocks,
        });
        if let Some(ch) = channel {
            root["channel"] = Value::String(ch.to_string());
        }
        root
    }
}

fn truncate_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

#[async_trait]
impl NotificationSink for SlackSink {
    fn kind(&self) -> &'static str {
        "slack"
    }

    fn validate_config(&self, config: &Value, credentials: &dyn CredentialResolver) -> Result<()> {
        let cfg = Self::parse_config(config)?;
        credentials.validate_password(&cfg.credential)
    }

    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt> {
        let cfg = Self::parse_config(config)?;
        let secret = credentials.password(&cfg.credential)?;
        let url = resolve_secret_endpoint(&cfg.origin, secret.password.as_str())?;
        let payload = Self::format_payload(event, cfg.channel.as_deref());
        let body = serde_json::to_vec(&payload).map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?;
        let response = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Post,
                    url: &url,
                    headers: vec![("Content-Type", Zeroizing::new("application/json".into()))],
                    body: Some(&body),
                    max_response_bytes: MAX_ALERT_RESPONSE_BYTES,
                },
                cancellation,
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(Error::new(
                ErrorCode::QueryFailed,
                t!("notify.delivery.http_failed", sink = self.kind(), status = response.status.to_string()),
            ));
        }
        Ok(SinkDeliveryReceipt { sink_kind: self.kind().into(), status_code: response.status, external_id: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_auth::{BearerTokenProvider, DatabaseTokenProvider, PasswordCredential};
    use culprit_notify::{AlertCategory, AlertSeverity, AlertTransition, RecordedTransport};
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
            (profile == "slack_ops").then_some(()).ok_or_else(|| Error::invalid("missing"))
        }
        fn password(&self, profile: &str) -> Result<PasswordCredential> {
            self.validate_password(profile)?;
            Ok(PasswordCredential { username: "slack".into(), password: Zeroizing::new("/services/T1/B2/SEC".into()) })
        }
    }

    #[tokio::test]
    async fn slack_sink_validates_origin_formats_block_kit_and_dispatches_via_injected_transport() {
        let sink = SlackSink::new();
        let cfg = json!({ "origin": "https://hooks.slack.com", "credential": "slack_ops", "channel": "#db-alerts" });
        sink.validate_config(&cfg, &Creds).unwrap();
        assert!(
            sink.validate_config(&json!({ "origin": "http://hooks.slack.com", "credential": "slack_ops" }), &Creds)
                .is_err()
        );

        let event = RedactedAlertEvent {
            dedup_key: "culprit:incident:abcd".into(),
            category: AlertCategory::Incident,
            transition: AlertTransition::Triggered,
            severity: AlertSeverity::Critical,
            engine: Some(culprit_core::Engine::Mssql),
            resource_alias: "orders-prod".into(),
            database_name: Some("orders".into()),
            environment: Some("production".into()),
            headline: "Blocking chain on dbo.Orders".into(),
            summary: "8 victim sessions blocked".into(),
            culprit_query: Some("UPDATE dbo.Orders SET Status = ? WHERE Id = ?".into()),
            victim_summary: Some("8 victim sessions blocked".into()),
            developer_action: Some("Commit open transaction.".into()),
            verification: Some("Check blocked sessions.".into()),
            proximity_marks: vec!["deploy v1.2".into()],
            observed_at: "2026-11-25T12:00:00Z".into(),
            deep_link: Some("https://archives.example.com/?resource=orders-prod".into()),
        };
        let transport = RecordedTransport::default();
        let receipt = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        assert_eq!(receipt.status_code, 200);
        let calls = transport.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].url, "https://hooks.slack.com/services/T1/B2/SEC");
        let sent: Value = serde_json::from_slice(calls[0].body.as_deref().unwrap()).unwrap();
        assert_eq!(sent["channel"], "#db-alerts");
        assert!(sent["blocks"].as_array().unwrap().len() >= 5);
    }
}
