//! Generate namespace-bound query helpers; deployment is explicit and no source database is opened.
use crate::{
    bounded_output,
    output::{Format, Out},
};
use clap::Args;
use culprit_core::{ComponentVersion, Result, t, telemetry::DeliveryProblem};
use culprit_engine::evidence::{EvidenceConfig, EvidenceRuntime};
use culprit_evidence::analytical::{
    evidence::EvidenceReadBudget,
    query_helpers::{QueryHelperAssets, QueryHelperPolicy},
};
use culprit_read::cancellation::CollectionCancellation;
use std::{io::Write, path::PathBuf};
#[derive(Args)]
pub(crate) struct QueryAssetsArgs {
    #[arg(long,value_name="FILE",help=t!("evidence.cli.config"))]
    config: PathBuf,
    #[arg(long,help=t!("query_helpers.cli.name"))]
    name: String,
    #[arg(long,value_name="UTC",help=t!("query_helpers.cli.history_from"))]
    history_from: String,
    #[arg(long,help=t!("query_helpers.cli.rules"))]
    rules_version: String,
    #[arg(long,default_value="culprit-checks",help=t!("query_helpers.cli.analyzer_component"))]
    analyzer_component: String,
    #[arg(long,default_value=env!("CARGO_PKG_VERSION"),help=t!("query_helpers.cli.analyzer_version"))]
    analyzer_version: String,
    #[arg(long,help=t!("query_helpers.cli.analyzer_revision"))]
    analyzer_revision: Option<String>,
    #[arg(long,default_value_t=bounded_output::DEFAULT_JSON_BYTES,help=t!("evidence.cli.max_output_bytes"))]
    max_output_bytes: usize,
}
impl QueryAssetsArgs {
    fn policy(&self) -> Result<QueryHelperPolicy> {
        let policy = QueryHelperPolicy {
            name: self.name.clone(),
            analyzer: ComponentVersion::package(
                &self.analyzer_component,
                &self.analyzer_version,
                self.analyzer_revision
                    .as_deref()
                    .or(option_env!("CULPRIT_BUILD_REVISION"))
                    .or(option_env!("GITHUB_SHA")),
            ),
            rules_version: self.rules_version.clone(),
            history_from: self.history_from.clone(),
        };
        policy.validate()?;
        Ok(policy)
    }
}
pub(crate) async fn run(args: QueryAssetsArgs, out: &Out) -> Result<u8> {
    if !matches!(out.format(), Format::Text | Format::Json) {
        return Err(culprit_core::Error::invalid(t!("evidence.cli.format")));
    }
    bounded_output::validate_json_limit(args.max_output_bytes)?;
    let policy = args.policy()?;
    let config = EvidenceConfig::from_file(&args.config)?;
    let service = EvidenceRuntime::default().evidence(&config)?;
    let cancellation = CollectionCancellation::default();
    let binding = crate::collection_shutdown::run(
        &cancellation,
        service.query_helper_binding(policy, EvidenceReadBudget::default(), &cancellation),
    )
    .await?;
    let assets = match config.warehouse.provider.as_str() {
        "adx" => culprit_store_adx::query_assets::configured(binding)?,
        "clickhouse" => culprit_store_clickhouse::query_assets::configured(binding)?,
        _ => return Err(DeliveryProblem::UnsupportedSchema.error()),
    };
    emit(assets, out, args.max_output_bytes)
}
fn emit(assets: QueryHelperAssets, out: &Out, limit: usize) -> Result<u8> {
    let bytes = encoded(assets, out.format(), limit)?;
    std::io::stdout().lock().write_all(&bytes).map_err(|_| culprit_core::Error::internal(t!("cli.output_failed")))?;
    Ok(0)
}
fn encoded(assets: QueryHelperAssets, format: Format, limit: usize) -> Result<Vec<u8>> {
    if format == Format::Json {
        bounded_output::json(&assets, limit)
    } else if assets.script.len() > limit {
        Err(DeliveryProblem::Capacity.error())
    } else {
        Ok(assets.script.into_bytes())
    }
}

#[cfg(test)]
mod tests;
