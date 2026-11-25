//! Pluggable warehouse alert evaluation and outbound delivery (`culprit collect notify`).

use crate::output::{Format, Out};
use clap::Subcommand;
use culprit_core::{Error, Result, format, t};
use culprit_engine::notify::NotifierConfig;
use culprit_notify::AlertBatchInput;
use culprit_read::cancellation::CollectionCancellation;
use std::{
    io::Read,
    path::{Path, PathBuf},
};

const MAX_BATCH_INPUT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Subcommand)]
pub(crate) enum NotifyCommand {
    #[command(about = culprit_core::t!("notify.cli.validate"))]
    Validate {
        #[arg(long, value_name = "FILE", help = culprit_core::t!("notify.cli.config"))]
        config: PathBuf,
    },
    #[command(about = culprit_core::t!("notify.cli.evaluate"))]
    Evaluate {
        #[arg(long, value_name = "FILE", help = culprit_core::t!("notify.cli.config"))]
        config: PathBuf,
        #[arg(long, value_name = "FILE", help = culprit_core::t!("notify.cli.input"))]
        input: PathBuf,
        #[arg(long, help = culprit_core::t!("notify.cli.dry_run"))]
        dry_run: bool,
    },
}

pub(crate) async fn run(command: NotifyCommand, out: &Out) -> Result<u8> {
    match command {
        NotifyCommand::Validate { config } => {
            let _ = NotifierConfig::from_file(&config)?;
            out.done(&t!("notify.cli.valid"));
            Ok(0)
        }
        NotifyCommand::Evaluate { config, input, dry_run } => {
            let cfg = NotifierConfig::from_file(&config)?;
            let batch = read_batch(&input)?;
            let cancellation = CollectionCancellation::default();
            let report = crate::collection_shutdown::run(
                &cancellation,
                cfg.evaluate_and_deliver(&batch, dry_run, &cancellation),
            )
            .await?;
            if out.format() == Format::Json {
                out.value(
                    &serde_json::to_value(&report).map_err(|_| Error::internal(t!("notify.delivery.encode_failed")))?,
                );
            } else {
                out.done(&t!(
                    "notify.cli.evaluated",
                    events = format::count(report.events_emitted as f64),
                    deliveries = format::count(report.deliveries.len() as f64)
                ));
            }
            Ok(0)
        }
    }
}

fn read_batch(path: &Path) -> Result<AlertBatchInput> {
    let mut bytes = Vec::new();
    if path.as_os_str() == "-" {
        std::io::stdin()
            .lock()
            .take(MAX_BATCH_INPUT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::invalid(t!("notify.cli.invalid_input")))?;
    } else {
        std::fs::File::open(path)
            .map_err(|_| Error::invalid(t!("notify.cli.invalid_input")))?
            .take(MAX_BATCH_INPUT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::invalid(t!("notify.cli.invalid_input")))?;
    }
    if bytes.len() as u64 > MAX_BATCH_INPUT_BYTES {
        return Err(Error::invalid(t!("notify.cli.invalid_input")));
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::invalid(t!("notify.cli.invalid_input")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use culprit_notify::{IncidentAlertInput, RecordedTransport};

    #[tokio::test]
    async fn notify_cli_parses_validates_offline_and_delivers_signed_webhook_end_to_end() {
        let cli =
            crate::Cli::try_parse_from(["culprit", "collect", "notify", "validate", "--config", "/tmp/notify.json"])
                .unwrap();
        assert!(matches!(cli.command, crate::Command::Collect(crate::collect_cmd::CollectCommand::Notify(_))));

        let temp = std::env::temp_dir().join(format!("culprit-notify-cli-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp);
        std::fs::create_dir_all(&temp).unwrap();
        let state_dir = temp.join("state");
        let cfg_path = temp.join("notify.json");
        std::fs::write(
            &cfg_path,
            serde_json::to_vec(&serde_json::json!({
                "state": state_dir,
                "policy": { "novelty_horizon_seconds": 3600, "min_consecutive_windows": 1 },
                "credentials": {
                    "wh_secret": {
                        "kind": "password-environment",
                        "username": "culprit",
                        "variable": "CULPRIT_TEST_NOTIFY_HMAC"
                    }
                },
                "routes": [{
                    "name": "ops_webhook",
                    "sink": "webhook",
                    "categories": ["incident"],
                    "min_severity": "high",
                    "settings": {
                        "origin": "https://hooks.example.com",
                        "path": "/culprit",
                        "signing_credential": "wh_secret"
                    }
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        // Offline validation succeeds without CULPRIT_TEST_NOTIFY_HMAC being set!
        let cfg = NotifierConfig::from_file(&cfg_path).unwrap();
        let batch = AlertBatchInput {
            evaluated_at: "2026-11-25T12:00:00Z".into(),
            incidents: vec![IncidentAlertInput {
                engine: Some(culprit_core::Engine::Postgres),
                target_id: "db:postgres:shop".into(),
                original_scope_id: "s1".into(),
                original_source: "postgres.activity".into(),
                episode_id: "ep1".into(),
                controller_version: 1,
                revision: "1".into(),
                lifecycle: "active".into(),
                first_recorded_observation: "2026-11-25T11:59:00Z".into(),
                observed_until: "2026-11-25T11:59:30Z".into(),
                connection_alias: "shop-prod".into(),
                database_name: Some("shop".into()),
                environment: Some("production".into()),
                title: "Blocking chain on orders".into(),
                victim_summary: "4 sessions blocked".into(),
                culprit_query: Some("UPDATE orders SET total = 99 WHERE id = 7".into()),
                developer_action: "Commit open transaction.".into(),
                verification: "Check blocked sessions.".into(),
            }],
            ..Default::default()
        };

        struct FixedCred;
        impl culprit_auth::CredentialResolver for FixedCred {
            fn bearer(&self, _: &str) -> Result<std::sync::Arc<dyn culprit_auth::BearerTokenProvider>> {
                Err(Error::invalid("no"))
            }
            fn database(&self, _: &str) -> Result<std::sync::Arc<dyn culprit_auth::DatabaseTokenProvider>> {
                Err(Error::invalid("no"))
            }
            fn validate_password(&self, profile: &str) -> Result<()> {
                (profile == "wh_secret").then_some(()).ok_or_else(|| Error::invalid("missing"))
            }
            fn password(&self, profile: &str) -> Result<culprit_auth::PasswordCredential> {
                self.validate_password(profile)?;
                Ok(culprit_auth::PasswordCredential {
                    username: "culprit".into(),
                    password: zeroize::Zeroizing::new("hmac-key-test".into()),
                })
            }
        }

        let transport = RecordedTransport::default();
        let notifier = culprit_engine::notify::builtin_notifier();
        let r1 = cfg
            .evaluate_with_credentials(
                &batch,
                false,
                &notifier,
                &FixedCred,
                &transport,
                &CollectionCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(r1.events_emitted, 1);
        assert_eq!(r1.deliveries.len(), 1);
        assert_eq!(transport.calls().len(), 1);

        // Second evaluation of the same episode emits 0 new alerts thanks to persisted durable dedup journal!
        let r2 = cfg
            .evaluate_with_credentials(
                &batch,
                false,
                &notifier,
                &FixedCred,
                &transport,
                &CollectionCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(r2.events_emitted, 0);
        assert_eq!(transport.calls().len(), 1);
        let _ = std::fs::remove_dir_all(&temp);
    }
}
