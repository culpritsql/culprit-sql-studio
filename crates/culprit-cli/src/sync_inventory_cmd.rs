//! Cloud database discovery to sharded collector configuration (`culprit collect sync-inventory`).

use crate::{
    conn_cmd::CloudArg,
    output::{Format, Out},
};
use clap::{Args, ValueEnum};
use culprit_collector::fleet::onboarding::OnboardingOptions;
use culprit_core::{Error, Result, format, t};
use culprit_engine::{
    DiscoveryResult, Workspace,
    inventory_onboarding::{
        HelmValuesRenderer, InventoryRenderer, JsonConfigRenderer, OnboardingTargetSpec, plan_discovered_inventory,
        write_rendered_artifacts,
    },
};
use serde::Serialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
};

const MAX_DISCOVERY_JSON_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum RenderFormatChoice {
    Helm,
    CollectorJson,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum TargetKindChoice {
    Adx,
    Clickhouse,
}

#[derive(Args)]
pub(crate) struct SyncInventoryArgs {
    #[arg(long, value_enum, conflicts_with = "discovered_json", required_unless_present = "discovered_json", help = culprit_core::t!("onboarding.cli.cloud"))]
    pub cloud: Option<CloudArg>,
    #[arg(long, value_name = "FILE", help = culprit_core::t!("onboarding.cli.discovered_json"))]
    pub discovered_json: Option<PathBuf>,
    /// AWS region(s) to scan when `--cloud aws` is selected.
    #[arg(long = "region", value_name = "REGION")]
    pub regions: Vec<String>,
    /// Optional search terms restricting discovery to matching database/server names.
    pub terms: Vec<String>,
    #[arg(long, default_value_t = 1, help = culprit_core::t!("onboarding.cli.shards"))]
    pub shards: u32,
    #[arg(long, default_value = "shard", help = culprit_core::t!("onboarding.cli.shard_prefix"))]
    pub shard_prefix: String,
    #[arg(long, value_enum, default_value = "helm", help = culprit_core::t!("onboarding.cli.format"))]
    pub render_format: RenderFormatChoice,
    #[arg(long, value_name = "DIR", help = culprit_core::t!("onboarding.cli.output_dir"))]
    pub output_dir: PathBuf,
    #[arg(long, help = culprit_core::t!("onboarding.cli.exclude_production"))]
    pub exclude_production: bool,
    #[arg(long, default_value = "database-telemetry", help = culprit_core::t!("onboarding.cli.pipeline"))]
    pub pipeline: String,
    #[arg(long, value_enum, help = culprit_core::t!("onboarding.cli.target_kind"))]
    pub target_kind: Option<TargetKindChoice>,
    #[arg(long)]
    pub storage_account: Option<String>,
    #[arg(long, default_value = "culprit-native")]
    pub storage_container: String,
    #[arg(long)]
    pub adx_endpoint: Option<String>,
    #[arg(long, default_value = "culprit")]
    pub adx_database: String,
    #[arg(long)]
    pub tenant_id: Option<String>,
    #[arg(long)]
    pub client_id: Option<String>,
    #[arg(long)]
    pub s3_bucket: Option<String>,
    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,
    #[arg(long, value_name = "AWS_ACCOUNT_ID", help = culprit_core::t!("onboarding.cli.s3_expected_owner"))]
    pub s3_expected_owner: Option<String>,
    #[arg(long)]
    pub clickhouse_endpoint: Option<String>,
    #[arg(long, default_value = "culprit")]
    pub clickhouse_database: String,
    #[arg(long)]
    pub aws_profile: Option<String>,
    #[arg(long, default_value = "culprit_writer")]
    pub clickhouse_user: String,
    #[arg(long, default_value = "CULPRIT_CLICKHOUSE_PASSWORD")]
    pub clickhouse_password_env: String,
    #[arg(long, default_value = "/var/lib/culprit")]
    pub runtime_root: PathBuf,
}

#[derive(Serialize)]
struct SyncInventorySummary {
    format: &'static str,
    shards: usize,
    total_targets: u32,
    skipped_production: u32,
    output_dir: PathBuf,
    files: Vec<PathBuf>,
}

pub(crate) async fn run(args: SyncInventoryArgs, out: &Out) -> Result<u8> {
    let discovery = match (&args.discovered_json, args.cloud) {
        (Some(path), _) => read_discovery_json(path)?,
        (None, Some(CloudArg::Azure)) => {
            if !args.regions.is_empty() {
                return Err(Error::invalid(t!("cli.discover.region_aws")));
            }
            Workspace::open()?.discover_azure(&args.terms).await?
        }
        (None, Some(CloudArg::Aws)) => Workspace::open()?.discover_aws(&args.regions, &args.terms).await?,
        (None, None) => return Err(Error::invalid(t!("onboarding.invalid_options"))),
    };

    let options = OnboardingOptions {
        shard_count: args.shards,
        shard_prefix: args.shard_prefix.clone(),
        include_production: !args.exclude_production,
        explicit_query_history: false,
    };
    let plan = plan_discovered_inventory(&discovery, &options)?;

    let effective_target = args.target_kind.unwrap_or(match args.cloud {
        Some(CloudArg::Aws) => TargetKindChoice::Clickhouse,
        _ => TargetKindChoice::Adx,
    });
    let target_spec = match effective_target {
        TargetKindChoice::Adx => OnboardingTargetSpec::AzureAdx {
            pipeline: args.pipeline.clone(),
            storage_account: args.storage_account.unwrap_or_else(|| "replacewithstorageacct".into()),
            storage_container: args.storage_container,
            adx_endpoint: args
                .adx_endpoint
                .unwrap_or_else(|| "https://replace-cluster.westeurope.kusto.windows.net".into()),
            adx_database: args.adx_database,
            tenant_id: args.tenant_id,
            client_id: args.client_id,
        },
        TargetKindChoice::Clickhouse => {
            let inferred_owner = {
                let mut owners = discovery
                    .items
                    .iter()
                    .filter(|d| d.cloud == culprit_engine::Cloud::Aws)
                    .map(|d| d.subscription.as_str())
                    .filter(|acct| acct.len() == 12 && acct.bytes().all(|b| b.is_ascii_digit()))
                    .collect::<Vec<_>>();
                owners.sort_unstable();
                owners.dedup();
                (owners.len() == 1).then(|| owners[0].to_string())
            };
            OnboardingTargetSpec::AwsClickHouse {
                pipeline: args.pipeline.clone(),
                s3_bucket: args.s3_bucket.unwrap_or_else(|| "replace-culprit-archive".into()),
                s3_region: args.s3_region,
                s3_expected_owner: args.s3_expected_owner.or(inferred_owner).unwrap_or_else(|| "000000000000".into()),
                clickhouse_endpoint: args
                    .clickhouse_endpoint
                    .unwrap_or_else(|| "https://clickhouse.internal:8443".into()),
                clickhouse_database: args.clickhouse_database,
                aws_profile: args.aws_profile,
                clickhouse_user: args.clickhouse_user,
                clickhouse_password_env: args.clickhouse_password_env,
            }
        }
    };

    let json_renderer = JsonConfigRenderer { runtime_root: args.runtime_root };
    let renderer: &dyn InventoryRenderer = match args.render_format {
        RenderFormatChoice::Helm => &HelmValuesRenderer,
        RenderFormatChoice::CollectorJson => &json_renderer,
    };

    let artifacts = renderer.render(&plan, &target_spec)?;
    let files = write_rendered_artifacts(&args.output_dir, &artifacts)?;

    if out.format() == Format::Json {
        let summary = SyncInventorySummary {
            format: renderer.format_name(),
            shards: plan.shards.len(),
            total_targets: plan.total_targets,
            skipped_production: plan.skipped_production,
            output_dir: args.output_dir,
            files,
        };
        out.value(&serde_json::to_value(&summary).map_err(|e| Error::internal(e.to_string()))?);
    } else {
        out.done(&t!(
            "onboarding.cli.written",
            files = format::count(files.len() as f64),
            shards = format::count(plan.shards.len() as f64),
            targets = format::count(f64::from(plan.total_targets)),
            dir = args.output_dir.display().to_string()
        ));
    }
    Ok(0)
}

fn read_discovery_json(path: &Path) -> Result<DiscoveryResult> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| Error::invalid(t!("onboarding.invalid_discovered_json")))?
        .take(MAX_DISCOVERY_JSON_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::invalid(t!("onboarding.invalid_discovered_json")))?;
    if bytes.len() as u64 > MAX_DISCOVERY_JSON_BYTES {
        return Err(Error::invalid(t!("onboarding.invalid_discovered_json")));
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::invalid(t!("onboarding.invalid_discovered_json")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cli_parses_sync_inventory_offline_and_cloud_modes() {
        let parsed = crate::Cli::try_parse_from([
            "culprit",
            "collect",
            "sync-inventory",
            "--discovered-json",
            "estate.json",
            "--shards",
            "4",
            "--render-format",
            "helm",
            "--output-dir",
            "/tmp/out",
        ])
        .unwrap();
        let crate::Command::Collect(crate::collect_cmd::CollectCommand::SyncInventory(args)) = parsed.command else {
            panic!("expected SyncInventory")
        };
        assert_eq!(args.shards, 4);
        assert_eq!(args.render_format, RenderFormatChoice::Helm);

        assert!(
            crate::Cli::try_parse_from([
                "culprit",
                "collect",
                "sync-inventory",
                "--cloud",
                "azure",
                "--discovered-json",
                "estate.json",
                "--output-dir",
                "/tmp/out",
            ])
            .is_err(),
            "--cloud and --discovered-json must conflict"
        );
    }
}
