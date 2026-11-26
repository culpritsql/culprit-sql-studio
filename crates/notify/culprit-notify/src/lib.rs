//! Pure alert deduplication state machine and pluggable outbound notification sink contracts.
//! This crate owns zero network clients, cloud SDKs, database drivers, or warehouse write adapters.

pub mod evaluator;
pub mod store;
pub mod transport;

use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, format, redact, t};
use culprit_read::cancellation::CollectionCancellation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub use evaluator::{
    AlertBatchInput, AlertEvaluationPolicy, AlertEvaluator, AvailabilityAlertInput, FindingAlertInput,
    IncidentAlertInput, RegressionAlertInput,
};
pub use store::{
    DeduplicationState, DeduplicationStore, DurableFileDeduplicationStore, MemoryDeduplicationStore,
    PendingRouteDelivery,
};
pub use transport::{
    HttpMethod, NotificationTransport, OutboundHttpRequest, OutboundHttpResponse, RecordedCall, RecordedTransport,
};

pub const MAX_ALERT_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_FIELD_CHARS: usize = 1024;
const MAX_QUERY_CHARS: usize = 2048;
const MAX_ROUTE_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AlertCategory {
    Incident,
    HighFinding,
    Regression,
    Deadlock,
    EvidenceAvailability,
}

impl AlertCategory {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Incident => "incident",
            Self::HighFinding => "high-finding",
            Self::Regression => "regression",
            Self::Deadlock => "deadlock",
            Self::EvidenceAvailability => "evidence-availability",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AlertTransition {
    Triggered,
    Resolved,
    AvailabilityDegraded,
    AvailabilityRecovered,
}

impl AlertTransition {
    pub fn is_resolution(self) -> bool {
        matches!(self, Self::Resolved | Self::AvailabilityRecovered)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    High,
    Critical,
}

impl AlertSeverity {
    pub fn label(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

/// Redacted Gold-projected alert event. Never carries raw SQL literals, connection strings, or credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedactedAlertEvent {
    pub dedup_key: String,
    pub category: AlertCategory,
    pub transition: AlertTransition,
    pub severity: AlertSeverity,
    #[serde(default)]
    pub engine: Option<culprit_core::Engine>,
    pub resource_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub headline: String,
    pub summary: String,
    pub culprit_query: Option<String>,
    pub victim_summary: Option<String>,
    pub developer_action: Option<String>,
    pub verification: Option<String>,
    /// Nearby timeline changes (such as `culprit mark` deploy markers). Proximity only, never causal proof.
    #[serde(default)]
    pub proximity_marks: Vec<String>,
    pub observed_at: String,
    pub deep_link: Option<String>,
}

impl RedactedAlertEvent {
    /// Enforce redaction and bounded control-character-free text on every outbound field.
    /// Never guesses a SQL dialect when `engine` is absent: SQL without a known `Engine` is withheld.
    pub fn sanitized(mut self) -> Self {
        self.resource_alias = sanitize_text(&self.resource_alias, 256);
        self.database_name = self.database_name.as_deref().map(|v| sanitize_text(v, 256));
        self.environment = self.environment.as_deref().map(|v| sanitize_text(v, 128));
        self.headline = sanitize_text(&self.headline, MAX_FIELD_CHARS);
        self.summary = sanitize_text(&self.summary, MAX_FIELD_CHARS);
        self.culprit_query = match (self.culprit_query.as_deref(), self.engine) {
            (Some(sql), Some(engine)) => {
                let redacted = redact::redact_sql(sql, engine);
                Some(sanitize_text(&redacted, MAX_QUERY_CHARS))
            }
            _ => None,
        };
        self.victim_summary = self.victim_summary.as_deref().map(|v| sanitize_text(v, MAX_FIELD_CHARS));
        self.developer_action = self.developer_action.as_deref().map(|v| sanitize_text(v, MAX_FIELD_CHARS));
        self.verification = self.verification.as_deref().map(|v| sanitize_text(v, MAX_FIELD_CHARS));
        self.proximity_marks = self.proximity_marks.into_iter().take(8).map(|mark| sanitize_text(&mark, 256)).collect();
        self.observed_at = sanitize_text(&self.observed_at, 64);
        self.deep_link = self.deep_link.as_deref().and_then(validate_deep_link);
        self
    }
}

pub fn sanitize_text(value: &str, max_chars: usize) -> String {
    format::escaped_preview(value.trim(), max_chars).text
}

pub fn validate_deep_link(url: &str) -> Option<String> {
    let trimmed = url.trim();
    let rest = trimmed.strip_prefix("https://")?;
    let host_end = rest.find('/').unwrap_or(rest.len());
    let origin_candidate = &trimmed[..("https://".len() + host_end)];
    https_origin(origin_candidate)?;
    if trimmed.len() <= 512 && !trimmed.chars().any(char::is_control) { Some(trimmed.to_string()) } else { None }
}

/// Deterministic correlation key across PagerDuty, GitHub Issues, Slack, Teams, and webhooks.
pub fn deterministic_dedup_key(category: AlertCategory, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(category.slug().as_bytes());
    for part in parts {
        hasher.update([0x1f]);
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(16).map(|byte| format!("{byte:02x}")).collect();
    format!("culprit:{}:{hex}", category.slug())
}

/// Validate a canonical HTTPS origin paired with a secret webhook path or full HTTPS URL from `SecretStore` /
/// `CredentialResolver`, ensuring the secret target never redirects requests to a different host.
pub fn resolve_secret_endpoint(configured_origin: &str, secret_target: &str) -> Result<String> {
    let canonical =
        https_origin(configured_origin).ok_or_else(|| Error::invalid(t!("notify.config.invalid_origin")))?;
    let target = secret_target.trim();
    if target.is_empty() || target.len() > 2048 || target.chars().any(char::is_control) {
        return Err(Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")));
    }
    if target.starts_with('/') {
        if target.starts_with("//") || target.contains("..") {
            return Err(Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")));
        }
        return Ok(format!("{canonical}{target}"));
    }
    let Some(rest) = target.strip_prefix("https://") else {
        return Err(Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")));
    };
    let slash =
        rest.find('/').ok_or_else(|| Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")))?;
    let target_origin = https_origin(&target[..("https://".len() + slash)])
        .ok_or_else(|| Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")))?;
    let path = &rest[slash..];
    if target_origin != canonical || path.starts_with("//") || path.contains("..") {
        return Err(Error::new(ErrorCode::AuthFailed, t!("notify.delivery.invalid_secret_target")));
    }
    Ok(format!("{canonical}{path}"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkDeliveryReceipt {
    pub sink_kind: String,
    pub status_code: u16,
    pub external_id: Option<String>,
}

#[async_trait]
pub trait NotificationSink: Send + Sync {
    /// Stable adapter identifier, e.g. `"slack"`, `"teams"`, `"pagerduty"`, `"webhook"`, `"github-issues"`.
    fn kind(&self) -> &'static str;

    /// Offline configuration validation (`culprit collect notify validate`).
    /// Verifies HTTPS origins and schema fields without reading secret values or opening sockets.
    fn validate_config(&self, config: &serde_json::Value, credentials: &dyn CredentialResolver) -> Result<()>;

    /// Format `RedactedAlertEvent` and dispatch through the host-injected transport.
    async fn deliver(
        &self,
        event: &RedactedAlertEvent,
        config: &serde_json::Value,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<SinkDeliveryReceipt>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRoute {
    pub name: String,
    pub sink: String,
    pub categories: BTreeSet<AlertCategory>,
    #[serde(default = "default_min_severity")]
    pub min_severity: AlertSeverity,
    #[serde(default = "default_send_resolutions")]
    pub send_resolutions: bool,
    pub settings: serde_json::Value,
}

fn default_min_severity() -> AlertSeverity {
    AlertSeverity::Warning
}

fn default_send_resolutions() -> bool {
    true
}

impl NotificationRoute {
    pub fn matches(&self, event: &RedactedAlertEvent) -> bool {
        if !self.categories.contains(&event.category) {
            return false;
        }
        if event.transition.is_resolution() {
            return self.send_resolutions;
        }
        event.severity >= self.min_severity
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDeliveryOutcome {
    pub route: String,
    pub sink: String,
    pub dedup_key: String,
    pub transition: AlertTransition,
    pub status_code: u16,
    pub external_id: Option<String>,
}

/// Pluggable sink registry (`Notifier::new().with_sink(...)`). External embedders register custom channels
/// without modifying the evaluator or built-in adapters.
#[derive(Default, Clone)]
pub struct Notifier {
    sinks: BTreeMap<String, Arc<dyn NotificationSink>>,
}

#[derive(Debug)]
pub struct DispatchBatchResult {
    pub deliveries: Vec<RouteDeliveryOutcome>,
    pub failures: Vec<(String, Error)>,
}

impl Notifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_sink(mut self, sink: Arc<dyn NotificationSink>) -> Self {
        self.sinks.insert(sink.kind().to_string(), sink);
        self
    }

    pub fn sink(&self, kind: &str) -> Result<&dyn NotificationSink> {
        self.sinks
            .get(kind)
            .map(Arc::as_ref)
            .ok_or_else(|| Error::new(ErrorCode::Unsupported, t!("notify.sink.unsupported", kind = kind)))
    }

    pub fn validate_routes(&self, routes: &[NotificationRoute], credentials: &dyn CredentialResolver) -> Result<()> {
        if routes.is_empty() || routes.len() > 64 {
            return Err(Error::invalid(t!("notify.config.invalid_routes")));
        }
        let mut names = BTreeSet::new();
        for route in routes {
            if route.name.is_empty()
                || route.name.len() > MAX_ROUTE_NAME_BYTES
                || !route.name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                || !names.insert(route.name.as_str())
                || route.categories.is_empty()
            {
                return Err(Error::invalid(t!("notify.config.invalid_routes")));
            }
            let sink = self.sink(&route.sink)?;
            sink.validate_config(&route.settings, credentials)?;
        }
        Ok(())
    }

    /// Queue sanitized events per matching route inside `state.pending_deliveries` so independent routes can retry
    /// failed deliveries without re-alerting routes that already succeeded.
    pub fn enqueue_events(
        &self,
        events: &[RedactedAlertEvent],
        routes: &[NotificationRoute],
        queued_at: &str,
        state: &mut DeduplicationState,
    ) {
        let active_routes: BTreeSet<&str> = routes.iter().map(|r| r.name.as_str()).collect();
        state.pending_deliveries.retain(|p| active_routes.contains(p.route.as_str()));
        let stamp = sanitize_text(queued_at, 64);

        for raw_event in events {
            let event = raw_event.clone().sanitized();
            for route in routes.iter().filter(|route| route.matches(&event)) {
                if let Some(existing) = state.pending_deliveries.iter_mut().find(|p| {
                    p.route == route.name
                        && p.event.dedup_key == event.dedup_key
                        && p.event.transition == event.transition
                }) {
                    existing.queued_at = stamp.clone();
                    existing.event = event.clone();
                } else {
                    state.pending_deliveries.push(PendingRouteDelivery {
                        route: route.name.clone(),
                        queued_at: stamp.clone(),
                        event: event.clone(),
                    });
                }
            }
        }
    }

    /// Attempt every pending route delivery independently while preserving per-route FIFO ordering.
    /// Succeeded deliveries are removed from `state.pending_deliveries`; failed deliveries remain queued for retry.
    pub async fn dispatch_pending(
        &self,
        state: &mut DeduplicationState,
        routes: &[NotificationRoute],
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> DispatchBatchResult {
        let route_by_name: BTreeMap<&str, &NotificationRoute> =
            routes.iter().map(|route| (route.name.as_str(), route)).collect();
        let queued = std::mem::take(&mut state.pending_deliveries);
        let mut remaining = Vec::new();
        let mut deliveries = Vec::new();
        let mut failures = Vec::new();
        let mut failed_routes = BTreeSet::new();

        for mut item in queued.into_iter() {
            let Some(route) = route_by_name.get(item.route.as_str()).copied() else {
                continue;
            };
            if cancellation.is_cancelled() {
                if failures.is_empty() {
                    failures.push((route.name.clone(), culprit_core::telemetry::CollectionStop::Cancelled.error()));
                }
                remaining.push(item);
                continue;
            }
            if failed_routes.contains(route.name.as_str()) {
                // Keep later transitions for an already-failing route behind the earlier failed transition.
                remaining.push(item);
                continue;
            }
            item.event = item.event.sanitized();
            let result = match self.sink(&route.sink) {
                Ok(sink) => sink.deliver(&item.event, &route.settings, credentials, transport, cancellation).await,
                Err(err) => Err(err),
            };
            match result {
                Ok(receipt) => {
                    deliveries.push(RouteDeliveryOutcome {
                        route: route.name.clone(),
                        sink: receipt.sink_kind,
                        dedup_key: item.event.dedup_key,
                        transition: item.event.transition,
                        status_code: receipt.status_code,
                        external_id: receipt.external_id,
                    });
                }
                Err(err) => {
                    failed_routes.insert(route.name.clone());
                    failures.push((route.name.clone(), err));
                    remaining.push(item);
                }
            }
        }
        state.pending_deliveries = remaining;
        DispatchBatchResult { deliveries, failures }
    }

    pub async fn dispatch(
        &self,
        events: &[RedactedAlertEvent],
        routes: &[NotificationRoute],
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<Vec<RouteDeliveryOutcome>> {
        let mut state = DeduplicationState::default();
        self.enqueue_events(events, routes, "", &mut state);
        let batch = self.dispatch_pending(&mut state, routes, credentials, transport, cancellation).await;
        if let Some((_, first_err)) = batch.failures.into_iter().next() {
            return Err(first_err);
        }
        Ok(batch.deliveries)
    }
}

#[cfg(test)]
mod tests;
