//! Bounded native Kusto HTTP and authentication, shared by query and ingestion clients.
use crate::authentication::Audience;
use crate::authentication::TokenProvider;
use crate::authentication::kusto_origin;
use crate::response;
use crate::response::Column;
use crate::response::QueryTable;
use culprit_core::{Result, telemetry::DeliveryProblem};
use ring::rand::SecureRandom;
use serde_json::Value;
use std::sync::Arc;
use std::{sync::OnceLock, time::Duration};
use tokio::sync::{OnceCell, Semaphore};
use zeroize::Zeroizing;

#[cfg(test)]
mod emulator_diagnostics;

const METADATA_BYTES: u64 = 64 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_PARALLEL_REQUESTS: usize = 2;
static METADATA_REQUESTS: Semaphore = Semaphore::const_new(2);

pub(crate) struct Service {
    origin: String,
    credential: Arc<dyn TokenProvider>,
    audience: OnceCell<Audience>,
    requests: Arc<Semaphore>,
}
impl Service {
    #[cfg(test)]
    pub(crate) fn fixture(origin: String, credential: Arc<dyn TokenProvider>) -> Self {
        Self {
            origin,
            credential,
            audience: OnceCell::new_with(Some(
                Audience::kusto("https://fixture.kusto.windows.net", "https://kusto.kusto.windows.net").unwrap(),
            )),
            requests: Arc::new(Semaphore::new(MAX_PARALLEL_REQUESTS)),
        }
    }
    pub fn new(endpoint: &str, credential: Arc<dyn TokenProvider>) -> Result<Self> {
        Ok(Self {
            origin: kusto_origin(endpoint)?,
            credential,
            audience: OnceCell::new(),
            requests: Arc::new(Semaphore::new(MAX_PARALLEL_REQUESTS)),
        })
    }
    async fn audience(&self) -> Result<&Audience> {
        self.audience
            .get_or_try_init(|| async {
                let origin = self.origin.clone();
                let permit = METADATA_REQUESTS.acquire().await.map_err(|_| DeliveryProblem::Transport.error())?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    discover(&origin)
                })
                .await
                .map_err(|_| DeliveryProblem::Transport.error())?
            })
            .await
    }
    pub async fn execute<T: Send + 'static>(
        &self,
        work: impl FnOnce(&str, &str) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self.requests.clone().acquire_owned().await.map_err(|_| DeliveryProblem::Transport.error())?;
        let token = self.credential.bearer_token(self.audience().await?.resource()).await?;
        let origin = self.origin.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&origin, &token)
        })
        .await
        .map_err(|_| DeliveryProblem::Transport.error())?
    }
}

pub(crate) fn request_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    ring::rand::SystemRandom::new().fill(&mut bytes).map_err(|_| DeliveryProblem::Transport.error())?;
    Ok(format!("Culprit.Delivery;{}", bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>()))
}

#[cfg(feature = "write")]
pub(crate) fn command(origin: &str, token: &str, command: &str) -> Result<Zeroizing<Vec<u8>>> {
    let authorization = Zeroizing::new(format!("Bearer {token}"));
    let body = serde_json::json!({"csl":command,
        "properties":serde_json::json!({"Options":{"servertimeout":"00:00:30"}}).to_string()});
    let mut response = agent(origin)
        .post(format!("{origin}/v1/rest/mgmt"))
        .header("Authorization", authorization.as_str())
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("Accept-Encoding", "identity")
        .header("x-ms-app", "Culprit")
        .header("x-ms-client-request-id", request_id()?)
        .send(body.to_string())
        .map_err(|_| DeliveryProblem::Transport.error())?;
    if response.status().as_u16() != 200 {
        return Err(DeliveryProblem::Transport.error());
    }
    response
        .body_mut()
        .with_config()
        .limit(METADATA_BYTES)
        .read_to_vec()
        .map(Zeroizing::new)
        .map_err(|_| DeliveryProblem::Transport.error())
}

pub(crate) fn discover(origin: &str) -> Result<Audience> {
    let mut response = agent(origin)
        .get(format!("{origin}/v1/rest/auth/metadata"))
        .header("Accept", "application/json")
        .header("Accept-Encoding", "identity")
        .call()
        .map_err(|_| DeliveryProblem::Transport.error())?;
    if response.status().as_u16() != 200 {
        return Err(DeliveryProblem::Transport.error());
    }
    let bytes = response
        .body_mut()
        .with_config()
        .limit(METADATA_BYTES)
        .read_to_vec()
        .map_err(|_| DeliveryProblem::Transport.error())?;
    audience(origin, &bytes)
}
pub(crate) fn audience(origin: &str, bytes: &[u8]) -> Result<Audience> {
    if bytes.len() as u64 > METADATA_BYTES {
        return Err(DeliveryProblem::Capacity.error());
    }
    let metadata: Value = serde_json::from_slice(bytes).map_err(|_| DeliveryProblem::Integrity.error())?;
    let advertised = metadata
        .get("AzureAD")
        .and_then(|azure| azure.get("KustoServiceResourceId"))
        .and_then(Value::as_str)
        .ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
    Audience::kusto(origin, advertised)
}
pub(crate) fn query(
    origin: &str,
    token: &str,
    request_id: &str,
    body: Vec<u8>,
    columns: &[Column],
    row_limit: usize,
    byte_limit: usize,
) -> Result<QueryTable> {
    let auth = Zeroizing::new(format!("Bearer {token}"));
    let mut response = agent(origin)
        .post(format!("{origin}/v2/rest/query"))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Accept-Encoding", "identity")
        .header("Authorization", auth.as_str())
        .header("x-ms-client-request-id", request_id)
        .header("x-ms-app", "Culprit")
        .header("x-ms-readonly", "true")
        .send(body)
        .map_err(|_| DeliveryProblem::Transport.error())?;
    if response.status().as_u16() != 200 {
        #[cfg(test)]
        if origin.starts_with("http://127.0.0.1:") {
            // Synthetic owned-emulator replies only; production/cloud errors remain credential-free.
            const DIAGNOSTIC_CHARS: usize = 4096;
            let status = response.status();
            match response.body_mut().with_config().limit(METADATA_BYTES).read_to_vec() {
                Ok(bytes) => eprintln!(
                    "Owned emulator query HTTP {status}: {}",
                    String::from_utf8_lossy(&bytes).chars().take(DIAGNOSTIC_CHARS).collect::<String>()
                ),
                Err(_) => eprintln!("Owned emulator query HTTP {status}: bounded diagnostic reply unavailable"),
            }
        }
        return Err(DeliveryProblem::Transport.error());
    }
    let bytes = response
        .body_mut()
        .with_config()
        .limit(byte_limit as u64)
        .read_to_vec()
        .map_err(|_| DeliveryProblem::Transport.error())?;
    let result = response::parse(&bytes, columns, row_limit, byte_limit);
    #[cfg(test)]
    if origin.starts_with("http://127.0.0.1:") && token == "owned-emulator-only" {
        emulator_diagnostics::report(request_id, &bytes, result.is_ok());
    }
    #[cfg(test)]
    if result.is_err() && origin.starts_with("http://127.0.0.1:") {
        // Only this test-only loopback transport can receive owned synthetic fixture data.
        // Reuse the already bounded reply; never make a second diagnostic request.
        const DIAGNOSTIC_CHARS: usize = 4096;
        eprintln!(
            "Owned emulator query HTTP 200 failed response validation: {}",
            String::from_utf8_lossy(&bytes).chars().take(DIAGNOSTIC_CHARS).collect::<String>()
        );
    }
    result
}
pub(crate) fn agent(origin: &str) -> &'static ureq::Agent {
    static CLOUD: OnceLock<ureq::Agent> = OnceLock::new();
    #[cfg(test)]
    static LOOPBACK: OnceLock<ureq::Agent> = OnceLock::new();
    #[cfg(test)]
    let secure = !origin.starts_with("http://127.0.0.1:");
    #[cfg(not(test))]
    let secure = {
        let _ = origin;
        true
    };
    #[cfg(test)]
    let cell = if secure { &CLOUD } else { &LOOPBACK };
    #[cfg(not(test))]
    let cell = &CLOUD;
    cell.get_or_init(|| {
        ureq::Agent::config_builder()
            .https_only(secure)
            .proxy(None)
            .max_redirects(0)
            .http_status_as_error(false)
            .timeout_global(Some(HTTP_TIMEOUT))
            .build()
            .into()
    })
}
