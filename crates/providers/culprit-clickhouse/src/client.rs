//! One outstanding native request per source. Losing its HTTP response leaves settlement unconfirmed.
use culprit_core::{
    AuthKind, ConnectionSpec, Engine, Error, ErrorCode, ProviderReadContext, Result, limits, message,
    telemetry::{CollectionStop, SourceShutdown},
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::de::DeserializeOwned;
use std::{
    net::{IpAddr, Ipv6Addr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

const STATEMENT_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_ROWS_READ: &str = "2000000";
const MAX_BYTES_READ: &str = "134217728";
const MAX_MEMORY_BYTES: &str = "134217728";
const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Client {
    endpoint: Arc<Endpoint>,
    timeout: Duration,
    deadline: Option<std::time::Instant>,
    session: Option<Arc<HttpSession>>,
    admission: Option<culprit_core::telemetry::SourceProfileAdmission>,
}
struct HttpSession {
    id: String,
    established: AtomicBool,
}
fn request_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    SystemRandom::new().fill(&mut bytes).map_err(|_| invalid_response())?;
    Ok(format!("culprit-{:032x}", u128::from_be_bytes(bytes)))
}
struct Endpoint {
    url: String,
    user: String,
    password: String,
    database: String,
    agent: ureq::Agent,
    outstanding: AtomicBool,
    context: ProviderReadContext,
}

pub(crate) type Parameters = Vec<(&'static str, String)>;

impl Client {
    pub(crate) fn new(spec: &ConnectionSpec, context: ProviderReadContext) -> Result<Self> {
        if !matches!(spec.auth, AuthKind::Password | AuthKind::None) {
            return Err(Error::invalid(message!("provider.clickhouse.auth")));
        }
        let invalid = || Error::invalid(message!("provider.clickhouse.endpoint"));
        if spec.transport.is_some() || spec.ssh.is_some() || spec.engine != Some(Engine::Clickhouse) {
            return Err(invalid());
        }
        let host = authority(&spec.host).ok_or_else(invalid)?;
        let security = culprit_core::connection_security(spec).ok_or_else(invalid)?;
        let scheme = match security.effective_mode.as_str() {
            "verify-full" => "https",
            "disable" => "http",
            _ => return Err(invalid()),
        };
        let user = spec.user.as_deref().unwrap_or("default");
        let password = spec.password.as_deref().unwrap_or("");
        if [user, password].iter().any(|v| ureq::http::HeaderValue::from_str(v).is_err()) {
            return Err(invalid());
        }
        let port = Engine::Clickhouse.network_port(spec.port)?;
        let agent = ureq::Agent::config_builder()
            .max_redirects(0)
            .proxy(None)
            .http_status_as_error(false)
            .timeout_global(Some(limits::OBSERVE_TIMEOUT + Duration::from_secs(2)))
            .build()
            .into();
        context.complete_reader_coverage();
        Ok(Self {
            endpoint: Arc::new(Endpoint {
                url: format!("{scheme}://{host}:{port}/"),
                user: user.into(),
                password: password.into(),
                database: spec.database.clone().unwrap_or_else(|| "default".into()),
                agent,
                outstanding: AtomicBool::new(false),
                context,
            }),
            timeout: STATEMENT_TIMEOUT,
            deadline: None,
            session: None,
            admission: None,
        })
    }

    pub(crate) fn session(&self) -> Result<Self> {
        Ok(Self {
            session: Some(Arc::new(HttpSession { id: request_id()?, established: AtomicBool::new(false) })),
            ..self.clone()
        })
    }
    pub(crate) fn with_budget(
        &self,
        budget: &culprit_core::telemetry::CollectionBudget,
        admission: Option<&culprit_core::telemetry::SourceProfileAdmission>,
    ) -> Result<Self> {
        Ok(Self {
            timeout: self.timeout.min(Duration::from_millis(u64::from(budget.statement_timeout_ms))),
            admission: admission.cloned(),
            deadline: Some(std::time::Instant::now() + Duration::from_millis(u64::from(budget.total_timeout_ms))),
            ..self.session()?
        })
    }
    pub(crate) fn profiled(&self) -> bool {
        self.admission.is_some()
    }
    pub(crate) fn check_admission(&self) -> Result<()> {
        if self.deadline.is_some_and(|end| std::time::Instant::now() >= end) {
            return Err(CollectionStop::TimedOut.error());
        }
        if self.admission.as_ref().is_some_and(|a| !a.approval_is_active()) {
            Err(CollectionStop::SourceChanged.error())
        } else {
            Ok(())
        }
    }
    pub(crate) fn shutdown(&self) -> SourceShutdown {
        if self.endpoint.outstanding.load(Ordering::Acquire) {
            SourceShutdown::Unconfirmed
        } else {
            SourceShutdown::Quiescent
        }
    }

    pub(crate) async fn rows<T: DeserializeOwned>(&self, sql: &'static str, params: Parameters) -> Result<Vec<T>> {
        let body = self.dispatch(format!("{sql}\nFORMAT JSON"), params, None).await?;
        let reply: Rows<T> = serde_json::from_slice(&body).map_err(|_| invalid_response())?;
        Ok(reply.data)
    }
    pub(crate) async fn dispatch(
        &self,
        sql: String,
        params: Parameters,
        display_rows: Option<usize>,
    ) -> Result<Vec<u8>> {
        self.check_admission()?;
        let id = request_id()?;
        let client = self.clone();
        let guard = limits::OperationGuard::new(self.timeout)?;
        let operation = guard.handle();
        // Reserve synchronously. Dropping the future between spawning and first dispatch cannot hide native work.
        if self.endpoint.outstanding.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        let body = tokio::task::spawn_blocking(move || {
            if let Err(error) = operation.check().and_then(|()| client.check_admission()) {
                client.endpoint.outstanding.store(false, Ordering::Release);
                return Err(error);
            }
            let reader = client.endpoint.context.reader(Engine::Clickhouse);
            reader.setup_not_applicable();
            reader.checkpoint(1, None);
            let (settled, reply) = client.request(&sql, params, &id, display_rows);
            if settled {
                client.endpoint.outstanding.store(false, Ordering::Release);
            }
            reader.finish(if settled { SourceShutdown::Quiescent } else { SourceShutdown::Unconfirmed });
            reply
        })
        .await
        .map_err(|_| invalid_response())??;
        self.check_admission()?;
        Ok(body)
    }
}

#[derive(serde::Deserialize)]
struct Rows<T> {
    data: Vec<T>,
}

fn authority(host: &str) -> Option<String> {
    let unwrapped = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')).unwrap_or(host);
    if let Ok(ip) = unwrapped.parse::<Ipv6Addr>() {
        return Some(format!("[{ip}]"));
    }
    if host.parse::<IpAddr>().is_ok()
        || (host.len() <= 253
            && host.trim_end_matches('.').split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }))
    {
        return Some(host.into());
    }
    None
}
pub(crate) fn invalid_response() -> Error {
    Error::new(ErrorCode::QueryFailed, message!("provider.clickhouse.response"))
}

impl Client {
    fn request(&self, sql: &str, params: Parameters, id: &str, display_rows: Option<usize>) -> (bool, Result<Vec<u8>>) {
        let mut request = self
            .endpoint
            .agent
            .post(&self.endpoint.url)
            .header("X-ClickHouse-User", &self.endpoint.user)
            .header("X-ClickHouse-Key", &self.endpoint.password)
            .query("database", &self.endpoint.database)
            .query("query_id", id)
            .query("wait_end_of_query", "1")
            .query("max_execution_time", self.timeout.as_secs_f64().to_string())
            .query("max_threads", "1")
            .query("max_rows_to_read", MAX_ROWS_READ)
            .query("max_bytes_to_read", MAX_BYTES_READ)
            .query("max_memory_usage", MAX_MEMORY_BYTES)
            .query("max_result_bytes", "7340032")
            .query("result_overflow_mode", "throw")
            .query("read_overflow_mode", "throw")
            .query("output_format_json_quote_64bit_integers", "0")
            .query("readonly", "1");
        if let Some(session) = &self.session {
            request = request.query("session_id", &session.id).query("session_timeout", "60");
            if session.established.load(Ordering::Acquire) {
                request = request.query("session_check", "1");
            }
        }
        if let Some(rows) = display_rows {
            request = request.query("limit", rows.to_string());
        }
        for (key, value) in params {
            request = request.query(format!("param_{key}"), value);
        }
        let mut reply = match request.send(sql) {
            Ok(reply) => reply,
            Err(error) => {
                let before_dispatch = matches!(
                    error,
                    ureq::Error::HostNotFound | ureq::Error::ConnectionFailed | ureq::Error::Tls(_)
                ) || matches!(error, ureq::Error::Io(ref io) if io.kind() == std::io::ErrorKind::ConnectionRefused);
                return (
                    before_dispatch,
                    Err(Error::new(ErrorCode::ConnectionFailed, message!("provider.clickhouse.connect"))),
                );
            }
        };
        let status = reply.status();
        let code = reply
            .headers()
            .get("X-ClickHouse-Exception-Code")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u32>().ok());
        if !status.is_success() || code.is_some() {
            let failure = match code {
                Some(516) => Error::new(ErrorCode::AuthFailed, message!("provider.clickhouse.credentials")),
                Some(164 | 452) => Error::new(ErrorCode::PermissionDenied, message!("provider.clickhouse.profile")),
                Some(497) => Error::new(ErrorCode::PermissionDenied, message!("provider.clickhouse.permission")),
                Some(159) => Error::new(ErrorCode::Timeout, message!("provider.clickhouse.timeout")),
                Some(code) => Error::new(
                    ErrorCode::QueryFailed,
                    message!("provider.clickhouse.native", status = status.as_u16(), code = code),
                ),
                None => Error::new(
                    ErrorCode::ConnectionFailed,
                    message!("provider.clickhouse.gateway", status = status.as_u16()),
                ),
            };
            let consistent = reply.headers().get("X-ClickHouse-Query-Id").is_none_or(|v| v.as_bytes() == id.as_bytes());
            return (code.is_some() && consistent, Err(failure));
        }
        let matching_id = reply.headers().get("X-ClickHouse-Query-Id").is_some_and(|v| v.as_bytes() == id.as_bytes());
        let bytes =
            reply.body_mut().with_config().limit(MAX_RESPONSE_BYTES).read_to_vec().map_err(|_| invalid_response());
        if matching_id
            && bytes.is_ok()
            && let Some(session) = &self.session
        {
            session.established.store(true, Ordering::Release);
        }
        (matching_id && bytes.is_ok(), if matching_id { bytes } else { Err(invalid_response()) })
    }
}
