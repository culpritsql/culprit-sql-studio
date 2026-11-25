//! Pure Microsoft Teams Adaptive Card v1.5 adapter implementing `NotificationSink`.

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
pub struct TeamsSink;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TeamsConfig {
    origin: String,
    credential: String,
}

impl TeamsSink {
    pub fn new() -> Self {
        Self
    }

    fn parse_config(config: &Value) -> Result<TeamsConfig> {
        let parsed: TeamsConfig =
            serde_json::from_value(config.clone()).map_err(|_| Error::invalid(t!("notify.config.invalid_sink")))?;
        if https_origin(&parsed.origin).is_none() {
            return Err(Error::invalid(t!("notify.config.invalid_origin")));
        }
        Ok(parsed)
    }

    pub fn format_adaptive_card(event: &RedactedAlertEvent) -> Value {
        let badge = if event.transition.is_resolution() { "RESOLVED" } else { event.severity.label() };
        let mut facts = vec![
            json!({ "title": "Resource", "value": event.resource_alias }),
            json!({ "title": "Category", "value": event.category.slug() }),
            json!({ "title": "Observed", "value": event.observed_at }),
            json!({ "title": "Correlation", "value": event.dedup_key }),
        ];
        if let Some(env) = &event.environment {
            facts.push(json!({ "title": "Environment", "value": env }));
        }
        if !event.proximity_marks.is_empty() {
            facts.push(json!({
                "title": "Nearby marks (proximity only)",
                "value": event.proximity_marks.join(", ")
            }));
        }
        let mut body = vec![
            json!({
                "type": "TextBlock",
                "size": "Large",
                "weight": "Bolder",
                "wrap": true,
                "text": format!("[{}] {}: {}", badge.to_ascii_uppercase(), event.resource_alias, event.headline)
            }),
            json!({
                "type": "TextBlock",
                "wrap": true,
                "text": event.summary
            }),
            json!({
                "type": "FactSet",
                "facts": facts
            }),
        ];
        if let Some(query) = &event.culprit_query {
            body.push(json!({
                "type": "TextBlock",
                "fontType": "Monospace",
                "wrap": true,
                "text": query
            }));
        }
        if let Some(action) = &event.developer_action {
            body.push(json!({
                "type": "TextBlock",
                "wrap": true,
                "text": format!("**Fix:** {action}")
            }));
        }
        let actions: Vec<Value> = event
            .deep_link
            .iter()
            .map(|url| json!({ "type": "Action.OpenUrl", "title": "Inspect in Culprit", "url": url }))
            .collect();

        json!({
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": Value::Null,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.5",
                    "body": body,
                    "actions": actions
                }
            }]
        })
    }
}

#[async_trait]
impl NotificationSink for TeamsSink {
    fn kind(&self) -> &'static str {
        "teams"
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
        let payload = Self::format_adaptive_card(event);
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
            (profile == "teams_ops").then_some(()).ok_or_else(|| Error::invalid("missing"))
        }
        fn password(&self, profile: &str) -> Result<PasswordCredential> {
            self.validate_password(profile)?;
            Ok(PasswordCredential {
                username: "teams".into(),
                password: Zeroizing::new("/webhookb2/guid/IncomingWebhook/secret".into()),
            })
        }
    }

    #[tokio::test]
    async fn teams_sink_formats_adaptive_card_v1_5_and_pins_https_origin() {
        let sink = TeamsSink::new();
        let cfg = json!({ "origin": "https://contoso.webhook.office.com", "credential": "teams_ops" });
        sink.validate_config(&cfg, &Creds).unwrap();

        let event = RedactedAlertEvent {
            dedup_key: "culprit:regression:1234".into(),
            category: AlertCategory::Regression,
            transition: AlertTransition::Triggered,
            severity: AlertSeverity::High,
            engine: Some(culprit_core::Engine::Mssql),
            resource_alias: "shop-prod".into(),
            database_name: Some("shop".into()),
            environment: Some("production".into()),
            headline: "Query q12 regressed 3.8x".into(),
            summary: "Plan switched from Index Seek to Clustered Index Scan".into(),
            culprit_query: Some("SELECT * FROM Orders WHERE CustomerId = ?".into()),
            victim_summary: None,
            developer_action: Some("Compare plans 68 and 69.".into()),
            verification: None,
            proximity_marks: vec!["deploy v4.1".into()],
            observed_at: "2026-11-25T12:00:00Z".into(),
            deep_link: Some("https://archives.example.com/?resource=shop-prod".into()),
        };
        let transport = RecordedTransport::default();
        let receipt = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        assert_eq!(receipt.status_code, 200);
        let calls = transport.calls();
        assert_eq!(calls[0].url, "https://contoso.webhook.office.com/webhookb2/guid/IncomingWebhook/secret");
        let card: Value = serde_json::from_slice(calls[0].body.as_deref().unwrap()).unwrap();
        assert_eq!(card["attachments"][0]["content"]["version"], "1.5");
        assert_eq!(
            card["attachments"][0]["content"]["actions"][0]["url"],
            "https://archives.example.com/?resource=shop-prod"
        );
    }
}
