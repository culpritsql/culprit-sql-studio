//! PagerDuty Events API v2 adapter implementing `NotificationSink` (`trigger` + auto-`resolve` via `dedup_key`).

use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, t};
use culprit_notify::{
    AlertSeverity, HttpMethod, MAX_ALERT_RESPONSE_BYTES, NotificationSink, NotificationTransport, OutboundHttpRequest,
    RedactedAlertEvent, SinkDeliveryReceipt,
};
use culprit_read::cancellation::CollectionCancellation;
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

#[derive(Default)]
pub struct PagerDutySink;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PagerDutyConfig {
    #[serde(default = "default_origin")]
    origin: String,
    routing_key_credential: String,
}

fn default_origin() -> String {
    "https://events.pagerduty.com".into()
}

impl PagerDutySink {
    pub fn new() -> Self {
        Self
    }

    fn parse_config(config: &Value) -> Result<(String, String)> {
        let parsed: PagerDutyConfig =
            serde_json::from_value(config.clone()).map_err(|_| Error::invalid(t!("notify.config.invalid_sink")))?;
        let canonical =
            https_origin(&parsed.origin).ok_or_else(|| Error::invalid(t!("notify.config.invalid_origin")))?;
        Ok((canonical, parsed.routing_key_credential))
    }

    fn pd_severity(severity: AlertSeverity) -> &'static str {
        match severity {
            AlertSeverity::Critical => "critical",
            AlertSeverity::High => "error",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Info => "info",
        }
    }
}

#[async_trait]
impl NotificationSink for PagerDutySink {
    fn kind(&self) -> &'static str {
        "pagerduty"
    }

    fn validate_config(&self, config: &Value, credentials: &dyn CredentialResolver) -> Result<()> {
        let (_, routing_cred) = Self::parse_config(config)?;
        credentials.validate_password(&routing_cred)
    }

    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt> {
        let (origin, routing_cred) = Self::parse_config(config)?;
        let routing_key = credentials.password(&routing_cred)?;
        let action = if event.transition.is_resolution() { "resolve" } else { "trigger" };
        let links: Vec<Value> =
            event.deep_link.iter().map(|href| json!({ "href": href, "text": "Inspect in Culprit" })).collect();

        let body_value = json!({
            "routing_key": routing_key.password.as_str(),
            "event_action": action,
            "dedup_key": event.dedup_key,
            "links": links,
            "payload": {
                "summary": format!("{}: {}", event.resource_alias, event.headline),
                "timestamp": event.observed_at,
                "source": event.resource_alias,
                "severity": Self::pd_severity(event.severity),
                "component": event.database_name.as_deref().unwrap_or(&event.resource_alias),
                "class": event.category.slug(),
                "custom_details": {
                    "summary": event.summary,
                    "environment": event.environment,
                    "culprit_query": event.culprit_query,
                    "victim_summary": event.victim_summary,
                    "developer_action": event.developer_action,
                    "verification": event.verification,
                    "proximity_marks": event.proximity_marks,
                }
            }
        });
        let body = Zeroizing::new(
            serde_json::to_vec(&body_value).map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?,
        );
        let endpoint = format!("{origin}/v2/enqueue");
        let response = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Post,
                    url: &endpoint,
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
        Ok(SinkDeliveryReceipt {
            sink_kind: self.kind().into(),
            status_code: response.status,
            external_id: Some(event.dedup_key.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_auth::{BearerTokenProvider, DatabaseTokenProvider, PasswordCredential};
    use culprit_notify::{AlertCategory, AlertTransition, RecordedTransport};
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
            (profile == "pd_routing").then_some(()).ok_or_else(|| Error::invalid("missing"))
        }
        fn password(&self, profile: &str) -> Result<PasswordCredential> {
            self.validate_password(profile)?;
            Ok(PasswordCredential { username: "pd".into(), password: Zeroizing::new("rk-secret-123".into()) })
        }
    }

    #[tokio::test]
    async fn pagerduty_sink_triggers_and_auto_resolves_using_the_same_dedup_key() {
        let sink = PagerDutySink::new();
        let cfg = json!({ "routing_key_credential": "pd_routing" });
        sink.validate_config(&cfg, &Creds).unwrap();

        let mut event = RedactedAlertEvent {
            dedup_key: "culprit:incident:ep99".into(),
            category: AlertCategory::Incident,
            transition: AlertTransition::Triggered,
            severity: AlertSeverity::Critical,
            engine: Some(culprit_core::Engine::Mssql),
            resource_alias: "payments-prod".into(),
            database_name: Some("payments".into()),
            environment: Some("production".into()),
            headline: "THREADPOOL worker starvation".into(),
            summary: "24 requests waiting on worker threads".into(),
            culprit_query: None,
            victim_summary: Some("24 requests waiting".into()),
            developer_action: Some("Resolve head blocker.".into()),
            verification: None,
            proximity_marks: Vec::new(),
            observed_at: "2026-11-25T12:00:00Z".into(),
            deep_link: None,
        };
        let transport =
            RecordedTransport::ok_json(202, &json!({ "status": "success", "dedup_key": "culprit:incident:ep99" }));
        let r1 = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        assert_eq!(r1.status_code, 202);
        assert_eq!(r1.external_id.as_deref(), Some("culprit:incident:ep99"));

        event.transition = AlertTransition::Resolved;
        event.severity = AlertSeverity::Info;
        let _ = sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();

        let calls = transport.calls();
        assert_eq!(calls.len(), 2);
        let trigger_json: Value = serde_json::from_slice(calls[0].body.as_deref().unwrap()).unwrap();
        let resolve_json: Value = serde_json::from_slice(calls[1].body.as_deref().unwrap()).unwrap();
        assert_eq!(trigger_json["event_action"], "trigger");
        assert_eq!(resolve_json["event_action"], "resolve");
        assert_eq!(trigger_json["dedup_key"], resolve_json["dedup_key"]);
    }
}
