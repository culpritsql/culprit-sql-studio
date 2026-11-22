//! Azure CLI bearer credentials for one explicit audience. ARM is the default; archive scopes are host-validated.
use culprit_auth::{BearerTokenProvider, CommandRunner};
use culprit_core::{Error, ErrorCode, Result, t};
use std::sync::Arc;
use zeroize::Zeroizing;

pub struct AzureCliCredential {
    runner: Arc<dyn CommandRunner>,
    resource: String,
    tenant: Option<String>,
}
impl AzureCliCredential {
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        Self { runner, resource: "https://management.azure.com/".into(), tenant: None }
    }
    /// The host selects a validated service audience. This credential admits exactly that audience; it never
    /// discovers an identity/authority or falls back to device login. Normal ARM construction stays unchanged.
    pub fn for_audience(runner: Arc<dyn CommandRunner>, resource: String, tenant: Option<String>) -> Result<Self> {
        let canonical = culprit_auth::https_origin(&resource)
            .ok_or_else(|| Error::invalid(t!("engine.credentials.invalid_profile")))?;
        if resource.trim_end_matches('/') != canonical
            || tenant.as_deref().is_some_and(|tenant| !crate::is_guid(tenant))
        {
            return Err(Error::invalid(t!("engine.credentials.invalid_profile")));
        }
        Ok(Self { runner, resource, tenant })
    }
}
#[async_trait::async_trait]
impl BearerTokenProvider for AzureCliCredential {
    async fn bearer_token(&self, resource: &str) -> Result<Zeroizing<String>> {
        if resource != self.resource {
            return Err(Error::invalid(t!("engine.credentials.invalid_profile")));
        }
        let mut arguments =
            vec!["account", "get-access-token", "--resource", resource, "--query", "accessToken", "--output", "tsv"];
        if let Some(tenant) = &self.tenant {
            arguments.extend(["--tenant", tenant]);
        }
        let output = self.runner.output(&arguments, &[]).await?;
        if !output.status.success() || output.stdout.is_empty() {
            return Err(if self.resource == "https://management.azure.com/" {
                Error::new(ErrorCode::AuthFailed, t!("engine.discover.no_arm_token"))
                    .with_hint(t!("engine.discover.no_arm_token_hint"))
            } else {
                Error::new(ErrorCode::AuthFailed, t!("engine.credentials.archive_token_failed"))
                    .with_hint(t!("engine.credentials.archive_token_hint"))
            });
        }
        let token = Zeroizing::new(
            String::from_utf8(output.stdout)
                .map_err(|_| Error::new(ErrorCode::AuthFailed, t!("engine.discover.unreadable_token")))?,
        );
        if token.trim().is_empty() || token.len() > 32768 || token.trim().bytes().any(|b| !b.is_ascii_graphic()) {
            return Err(Error::new(ErrorCode::AuthFailed, t!("engine.discover.empty_token")));
        }
        Ok(Zeroizing::new(token.trim().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Runner {
        token: &'static [u8],
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CommandRunner for Runner {
        async fn output(&self, arguments: &[&str], _: &[(&str, &str)]) -> Result<std::process::Output> {
            assert_eq!(arguments[3], "https://management.azure.com/");
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            #[cfg(unix)]
            let status = {
                use std::os::unix::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(0)
            };
            #[cfg(windows)]
            let status = {
                use std::os::windows::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(0)
            };
            Ok(std::process::Output { status, stdout: self.token.to_vec(), stderr: vec![] })
        }
    }
    struct ScopedRunner {
        arguments: std::sync::Mutex<Vec<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl CommandRunner for ScopedRunner {
        async fn output(&self, arguments: &[&str], _: &[(&str, &str)]) -> Result<std::process::Output> {
            self.arguments.lock().unwrap().push(arguments.iter().map(|argument| (*argument).into()).collect());
            #[cfg(unix)]
            let status = {
                use std::os::unix::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(0)
            };
            #[cfg(windows)]
            let status = {
                use std::os::windows::process::ExitStatusExt;
                std::process::ExitStatus::from_raw(0)
            };
            Ok(std::process::Output { status, stdout: b"opaque-read-token\n".to_vec(), stderr: vec![] })
        }
    }
    #[tokio::test]
    async fn explicit_archive_audiences_and_tenants_never_widen_default_arm_or_cross_request_scopes() {
        let runner = Arc::new(ScopedRunner { arguments: Default::default() });
        let storage = AzureCliCredential::for_audience(
            runner.clone(),
            "https://storage.azure.com/".into(),
            Some("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb".into()),
        )
        .unwrap();
        let kusto = AzureCliCredential::for_audience(
            runner.clone(),
            "https://cluster.westeurope.kusto.windows.net".into(),
            Some("cccccccc-1111-2222-3333-dddddddddddd".into()),
        )
        .unwrap();
        assert!(storage.bearer_token("https://cluster.westeurope.kusto.windows.net").await.is_err());
        assert!(kusto.bearer_token("https://storage.azure.com/").await.is_err());
        assert!(AzureCliCredential::new(runner.clone()).bearer_token("https://storage.azure.com/").await.is_err());
        assert!(runner.arguments.lock().unwrap().is_empty());
        assert_eq!(storage.bearer_token("https://storage.azure.com/").await.unwrap().as_str(), "opaque-read-token");
        assert_eq!(
            kusto.bearer_token("https://cluster.westeurope.kusto.windows.net").await.unwrap().as_str(),
            "opaque-read-token"
        );
        let arguments = runner.arguments.lock().unwrap();
        assert_eq!(arguments[0][8..], ["--tenant", "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"]);
        assert_eq!(arguments[1][8..], ["--tenant", "cccccccc-1111-2222-3333-dddddddddddd"]);
        for invalid in [
            "http://storage.azure.com/",
            "https://storage.azure.com/?token=private",
            "https://user:private@storage.azure.com/",
            "https://storage.azure.com/extra",
        ] {
            assert!(AzureCliCredential::for_audience(runner.clone(), invalid.into(), None).is_err());
        }
        assert!(
            AzureCliCredential::for_audience(
                runner.clone(),
                "https://storage.azure.com/".into(),
                Some("https://foreign.invalid/authority".into())
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn control_plane_tokens_validate_the_native_audience_and_sanitize_invalid_output() {
        for token in [b"owned-token\n".as_slice(), b"\n", b"private-canary token", &[0xff]] {
            let runner = Arc::new(Runner { token, calls: std::sync::atomic::AtomicUsize::new(0) });
            let credential = AzureCliCredential::new(runner.clone());
            assert!(credential.bearer_token("https://foreign.invalid/").await.is_err());
            assert_eq!(runner.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let result = credential.bearer_token("https://management.azure.com/").await;
            if token == b"owned-token\n" {
                assert_eq!(result.unwrap().as_str(), "owned-token");
            } else {
                assert!(!format!("{:?}", result.err().unwrap()).contains("private-canary"));
            }
        }
    }
}
