//! Host notification composition: bounded HTTPS transport, named credential profiles, and pluggable sink registration.

use crate::credentials::{ConfiguredCredentials, CredentialProfile};
use async_trait::async_trait;
use culprit_auth::{CredentialResolver, https_origin};
use culprit_core::{Error, ErrorCode, Result, t, telemetry::CollectionStop};
use culprit_notify::{
    AlertBatchInput, AlertEvaluationPolicy, AlertEvaluator, DeduplicationStore, DurableFileDeduplicationStore,
    HttpMethod, NotificationRoute, NotificationSink, NotificationTransport, Notifier, OutboundHttpRequest,
    OutboundHttpResponse, RedactedAlertEvent, RouteDeliveryOutcome,
};
use culprit_read::cancellation::CollectionCancellation;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
use zeroize::Zeroizing;

const NOTIFY_CONFIG_MAX_BYTES: u64 = 4 * 1024 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
static NOTIFY_HTTP_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

fn http_agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        ureq::Agent::config_builder()
            .https_only(true)
            .max_redirects(0)
            .http_status_as_error(false)
            .timeout_global(Some(HTTP_TIMEOUT))
            .user_agent(concat!("culprit-notify/", env!("CARGO_PKG_VERSION")))
            .build()
            .into()
    })
}

/// Bounded, redirect-disabled HTTPS-only transport injected into pluggable `NotificationSink` adapters.
#[derive(Default, Clone)]
pub struct EngineNotificationTransport;

#[async_trait]
impl NotificationTransport for EngineNotificationTransport {
    async fn send_http(
        &self,
        request: OutboundHttpRequest<'_>,
        cancellation: &CollectionCancellation,
    ) -> Result<OutboundHttpResponse> {
        if cancellation.is_cancelled() {
            return Err(CollectionStop::Cancelled.error());
        }
        let Some(rest) = request.url.strip_prefix("https://") else {
            return Err(Error::invalid(t!("notify.config.invalid_origin")));
        };
        let host_end = rest.find('/').unwrap_or(rest.len());
        if https_origin(&request.url[..("https://".len() + host_end)]).is_none() {
            return Err(Error::invalid(t!("notify.config.invalid_origin")));
        }

        let method = request.method;
        let url = request.url.to_string();
        let headers: Vec<(String, Zeroizing<String>)> =
            request.headers.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        let body_bytes: Option<Zeroizing<Vec<u8>>> = request.body.map(|slice| Zeroizing::new(slice.to_vec()));
        let max_bytes = request.max_response_bytes;

        let permit =
            NOTIFY_HTTP_PERMITS.acquire().await.map_err(|_| Error::internal(t!("engine.http.readers_unavailable")))?;
        let cancel_clone = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if cancel_clone.is_cancelled() {
                return Err(CollectionStop::Cancelled.error());
            }
            let mut response = match method {
                HttpMethod::Get => {
                    let mut req = http_agent().get(&url);
                    for (k, v) in &headers {
                        req = req.header(k, v.as_str());
                    }
                    req.call()
                }
                HttpMethod::Post => {
                    let mut req = http_agent().post(&url);
                    for (k, v) in &headers {
                        req = req.header(k, v.as_str());
                    }
                    req.send(body_bytes.as_ref().map(|b| b.as_slice()).unwrap_or(&[]))
                }
                HttpMethod::Patch => {
                    let mut req = http_agent().patch(&url);
                    for (k, v) in &headers {
                        req = req.header(k, v.as_str());
                    }
                    req.send(body_bytes.as_ref().map(|b| b.as_slice()).unwrap_or(&[]))
                }
            }
            .map_err(|_| Error::new(ErrorCode::ConnectionFailed, t!("notify.delivery.transport_failed")))?;

            let status = response.status().as_u16();
            let mut out = Vec::new();
            response
                .body_mut()
                .as_reader()
                .take((max_bytes + 1) as u64)
                .read_to_end(&mut out)
                .map_err(|_| Error::internal(t!("notify.delivery.response_too_large")))?;
            if out.len() > max_bytes {
                return Err(Error::internal(t!("notify.delivery.response_too_large")));
            }
            Ok(OutboundHttpResponse { status, body: out })
        })
        .await
        .map_err(|_| Error::internal(t!("engine.http.reader_stopped")))?
    }
}

/// Construct a `Notifier` pre-populated with all compiled built-in sink adapters.
/// Embedders can register additional custom sinks via `.with_sink(Arc::new(MySink))`.
pub fn builtin_notifier() -> Notifier {
    #[allow(unused_mut)]
    let mut notifier = Notifier::new();
    #[cfg(feature = "notify-slack")]
    {
        notifier = notifier.with_sink(Arc::new(culprit_notify_slack::SlackSink::new()));
    }
    #[cfg(feature = "notify-teams")]
    {
        notifier = notifier.with_sink(Arc::new(culprit_notify_teams::TeamsSink::new()));
    }
    #[cfg(feature = "notify-pagerduty")]
    {
        notifier = notifier.with_sink(Arc::new(culprit_notify_pagerduty::PagerDutySink::new()));
    }
    #[cfg(feature = "notify-webhook")]
    {
        notifier = notifier.with_sink(Arc::new(culprit_notify_webhook::WebhookSink::new()));
    }
    #[cfg(feature = "notify-github")]
    {
        notifier = notifier.with_sink(Arc::new(culprit_notify_github::GitHubIssueSink::new()));
    }
    notifier
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifierConfig {
    pub state: PathBuf,
    #[serde(default)]
    pub policy: AlertEvaluationPolicy,
    pub credentials: BTreeMap<String, CredentialProfile>,
    pub routes: Vec<NotificationRoute>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyRunReport {
    pub events_emitted: usize,
    pub deliveries: Vec<RouteDeliveryOutcome>,
    pub events: Vec<RedactedAlertEvent>,
}

impl NotifierConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| Error::invalid(t!("notify.config.invalid_file")))?
            .take(NOTIFY_CONFIG_MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::invalid(t!("notify.config.invalid_file")))?;
        if bytes.len() as u64 > NOTIFY_CONFIG_MAX_BYTES {
            return Err(Error::invalid(t!("notify.config.invalid_file")));
        }
        let cfg: Self = serde_json::from_slice(&bytes).map_err(|_| Error::invalid(t!("notify.config.invalid_file")))?;
        cfg.validate_with(&builtin_notifier())?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_with(&builtin_notifier())
    }

    pub fn validate_with(&self, notifier: &Notifier) -> Result<()> {
        if !self.state.is_absolute()
            || self.state.components().any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(Error::invalid(t!("notify.config.invalid_state_path")));
        }
        self.policy.validate()?;
        let resolver = ConfiguredCredentials::new(&self.credentials)?;
        notifier.validate_routes(&self.routes, &resolver)
    }

    pub async fn evaluate_and_deliver(
        &self,
        batch: &AlertBatchInput,
        dry_run: bool,
        cancellation: &CollectionCancellation,
    ) -> Result<NotifyRunReport> {
        self.evaluate_with(batch, dry_run, &builtin_notifier(), &EngineNotificationTransport, cancellation).await
    }

    pub async fn evaluate_with(
        &self,
        batch: &AlertBatchInput,
        dry_run: bool,
        notifier: &Notifier,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<NotifyRunReport> {
        self.validate_with(notifier)?;
        let resolver = ConfiguredCredentials::new(&self.credentials)?;
        self.evaluate_with_credentials(batch, dry_run, notifier, &resolver, transport, cancellation).await
    }

    pub async fn evaluate_with_credentials(
        &self,
        batch: &AlertBatchInput,
        dry_run: bool,
        notifier: &Notifier,
        credentials: &dyn CredentialResolver,
        transport: &dyn NotificationTransport,
        cancellation: &CollectionCancellation,
    ) -> Result<NotifyRunReport> {
        notifier.validate_routes(&self.routes, credentials)?;
        let store = DurableFileDeduplicationStore::open(&self.state)?;
        let mut state = store.load()?;
        let evaluator = AlertEvaluator::new(self.policy.clone())?;
        let events = evaluator.evaluate(batch, &mut state)?;
        if dry_run {
            return Ok(NotifyRunReport { events_emitted: events.len(), deliveries: Vec::new(), events });
        }
        let deliveries = notifier.dispatch(&events, &self.routes, credentials, transport, cancellation).await?;
        // Persist deduplication transitions only after all deliveries succeed so transient HTTP failures retry cleanly.
        store.save(&state)?;
        Ok(NotifyRunReport { events_emitted: events.len(), deliveries, events })
    }
}

pub fn register_sink(notifier: Notifier, sink: Arc<dyn NotificationSink>) -> Notifier {
    notifier.with_sink(sink)
}
