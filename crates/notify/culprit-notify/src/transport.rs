use async_trait::async_trait;
use culprit_core::{Error, Result, t, telemetry::CollectionStop};
use culprit_read::cancellation::CollectionCancellation;
use std::{collections::VecDeque, sync::Mutex};
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Patch,
}

pub struct OutboundHttpRequest<'a> {
    pub method: HttpMethod,
    pub url: &'a str,
    pub headers: Vec<(&'a str, Zeroizing<String>)>,
    pub body: Option<&'a [u8]>,
    pub max_response_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait NotificationTransport: Send + Sync {
    async fn send_http(
        &self,
        request: OutboundHttpRequest<'_>,
        cancellation: &CollectionCancellation,
    ) -> Result<OutboundHttpResponse>;
}

/// Recorded HTTP request with header names only (header values stay zeroized and are never retained in plain logs),
/// plus an optional non-secret header map for signature/content-type assertions in unit tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedCall {
    pub method: HttpMethod,
    pub url: String,
    pub header_names: Vec<String>,
    pub non_secret_headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// Deterministic in-memory transport double for 100% offline sink unit testing.
#[derive(Default)]
pub struct RecordedTransport {
    replies: Mutex<VecDeque<Result<OutboundHttpResponse>>>,
    calls: Mutex<Vec<RecordedCall>>,
}

impl RecordedTransport {
    pub fn with_replies(replies: impl IntoIterator<Item = Result<OutboundHttpResponse>>) -> Self {
        Self { replies: Mutex::new(replies.into_iter().collect()), calls: Mutex::new(Vec::new()) }
    }

    pub fn ok_json(status: u16, json: &serde_json::Value) -> Self {
        Self::with_replies([Ok(OutboundHttpResponse { status, body: serde_json::to_vec(json).unwrap_or_default() })])
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl NotificationTransport for RecordedTransport {
    async fn send_http(
        &self,
        request: OutboundHttpRequest<'_>,
        cancellation: &CollectionCancellation,
    ) -> Result<OutboundHttpResponse> {
        if cancellation.is_cancelled() {
            return Err(CollectionStop::Cancelled.error());
        }
        let header_names = request.headers.iter().map(|(name, _)| (*name).to_string()).collect();
        let non_secret_headers = request
            .headers
            .iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case("authorization"))
            .map(|(name, value)| ((*name).to_string(), value.as_str().to_string()))
            .collect();
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(RecordedCall {
            method: request.method,
            url: request.url.to_string(),
            header_names,
            non_secret_headers,
            body: request.body.map(<[u8]>::to_vec),
        });
        let reply = self
            .replies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
            .unwrap_or_else(|| Ok(OutboundHttpResponse { status: 200, body: b"{}".to_vec() }))?;
        if reply.body.len() > request.max_response_bytes {
            return Err(Error::internal(t!("notify.delivery.response_too_large")));
        }
        Ok(reply)
    }
}
