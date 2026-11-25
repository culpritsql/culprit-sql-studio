//! Generic JSON webhook adapter signed with `X-Culprit-Signature-256: sha256=<hex>` (`ring::hmac`).

use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, t};
use culprit_notify::{
    HttpMethod, MAX_ALERT_RESPONSE_BYTES, NotificationSink, NotificationTransport, OutboundHttpRequest,
    RedactedAlertEvent, SinkDeliveryReceipt,
};
use culprit_read::cancellation::CollectionCancellation;
use ring::hmac;
use serde::Deserialize;
use serde_json::{Value, json};
use zeroize::Zeroizing;

pub const SIGNATURE_HEADER: &str = "X-Culprit-Signature-256";

#[derive(Default)]
pub struct WebhookSink;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookConfig {
    origin: String,
    #[serde(default = "default_path")]
    path: String,
    signing_credential: String,
}

fn default_path() -> String {
    "/".into()
}

impl WebhookSink {
    pub fn new() -> Self {
        Self
    }

    fn parse_config(config: &Value) -> Result<(String, String)> {
        let parsed: WebhookConfig =
            serde_json::from_value(config.clone()).map_err(|_| Error::invalid(t!("notify.config.invalid_sink")))?;
        let canonical =
            https_origin(&parsed.origin).ok_or_else(|| Error::invalid(t!("notify.config.invalid_origin")))?;
        if !parsed.path.starts_with('/')
            || parsed.path.starts_with("//")
            || parsed.path.contains("..")
            || parsed.path.len() > 512
            || parsed.path.chars().any(char::is_control)
        {
            return Err(Error::invalid(t!("notify.config.invalid_sink")));
        }
        Ok((format!("{canonical}{}", parsed.path), parsed.signing_credential))
    }

    pub fn sign_sha256_hex(secret: &[u8], body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let tag = hmac::sign(&key, body);
        let hex: String = tag.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        format!("sha256={hex}")
    }
}

#[async_trait]
impl NotificationSink for WebhookSink {
    fn kind(&self) -> &'static str {
        "webhook"
    }

    fn validate_config(&self, config: &Value, credentials: &dyn CredentialResolver) -> Result<()> {
        let (_, signing_cred) = Self::parse_config(config)?;
        credentials.validate_password(&signing_cred)
    }

    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt> {
        let (endpoint, signing_cred) = Self::parse_config(config)?;
        let secret = credentials.password(&signing_cred)?;
        let envelope = json!({
            "schema": "culprit.alert.v1",
            "event": event,
        });
        let body = serde_json::to_vec(&envelope).map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?;
        let signature = Self::sign_sha256_hex(secret.password.as_bytes(), &body);
        let response = transport
            .send_http(
                OutboundHttpRequest {
                    method: HttpMethod::Post,
                    url: &endpoint,
                    headers: vec![
                        ("Content-Type", Zeroizing::new("application/json".into())),
                        (SIGNATURE_HEADER, Zeroizing::new(signature)),
                    ],
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
            (profile == "wh_hmac").then_some(()).ok_or_else(|| Error::invalid("missing"))
        }
        fn password(&self, profile: &str) -> Result<PasswordCredential> {
            self.validate_password(profile)?;
            Ok(PasswordCredential { username: "hmac".into(), password: Zeroizing::new("signing-key-xyz".into()) })
        }
    }

    #[tokio::test]
    async fn webhook_sink_signs_exact_body_bytes_with_hmac_sha256() {
        let sink = WebhookSink::new();
        let cfg = json!({
            "origin": "https://automation.example.com",
            "path": "/hooks/culprit",
            "signing_credential": "wh_hmac"
        });
        sink.validate_config(&cfg, &Creds).unwrap();
        assert!(
            sink.validate_config(
                &json!({
                    "origin": "https://automation.example.com",
                    "path": "/hooks/../admin",
                    "signing_credential": "wh_hmac"
                }),
                &Creds
            )
            .is_err()
        );

        let event = RedactedAlertEvent {
            dedup_key: "culprit:high-finding:f1".into(),
            category: AlertCategory::HighFinding,
            transition: AlertTransition::Triggered,
            severity: AlertSeverity::High,
            engine: Some(culprit_core::Engine::Postgres),
            resource_alias: "analytics-prod".into(),
            database_name: Some("analytics".into()),
            environment: Some("production".into()),
            headline: "Missing index on events(tenant_id)".into(),
            summary: "Sequential scan accounts for 52% of DB time".into(),
            culprit_query: None,
            victim_summary: None,
            developer_action: Some("CREATE INDEX CONCURRENTLY ...".into()),
            verification: None,
            proximity_marks: Vec::new(),
            observed_at: "2026-11-25T12:00:00Z".into(),
            deep_link: None,
        };
        let transport = RecordedTransport::default();
        sink.deliver(&event, &cfg, &Creds, &transport, &CollectionCancellation::default()).await.unwrap();
        let call = &transport.calls()[0];
        let body = call.body.as_deref().unwrap();
        let expected_sig = WebhookSink::sign_sha256_hex(b"signing-key-xyz", body);
        assert!(call.non_secret_headers.contains(&(SIGNATURE_HEADER.to_string(), expected_sig)));
    }
}
