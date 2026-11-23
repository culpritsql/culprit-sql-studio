//! One observed resource-name/time request reads warehouse originals without constructing a source provider.
use crate::{
    bounded_output,
    output::{Format, Out, observations},
};
use clap::Args;
use culprit_core::{Result, t, telemetry::DeliveryProblem};
use culprit_engine::evidence::EvidenceConfig;
use culprit_engine::evidence::EvidenceRuntime;
use culprit_engine::evidence::IncidentRequest;
use culprit_read::cancellation::CollectionCancellation;
use std::{io::Write, path::PathBuf};

#[derive(Args)]
pub(crate) struct IncidentArgs {
    #[arg(value_name = "RESOURCE", help=culprit_core::t!("incident.cli.resource_name"))]
    pub(crate) resource: String,
    #[arg(long, value_name="RFC3339", help=culprit_core::t!("incident.cli.around"))]
    pub(crate) around: String,
    #[arg(long, value_name="FILE", help=culprit_core::t!("evidence.cli.config"))]
    pub(crate) config: PathBuf,
    #[arg(long, default_value_t=bounded_output::DEFAULT_JSON_BYTES, help=culprit_core::t!("evidence.cli.max_output_bytes"))]
    pub(crate) max_output_bytes: usize,
}
pub(crate) async fn run(args: IncidentArgs, out: &Out) -> Result<u8> {
    if !matches!(out.format(), Format::Json | Format::Text) {
        return Err(culprit_core::Error::invalid(t!("evidence.cli.format")));
    }
    bounded_output::validate_json_limit(args.max_output_bytes)?;
    // Only independently named warehouse/recovery profiles are resolved. No provider/factory/source login exists.
    let service = EvidenceRuntime::default().evidence(&EvidenceConfig::from_file(&args.config)?)?;
    let cancellation = CollectionCancellation::default();
    let result = crate::collection_shutdown::run(
        &cancellation,
        service.incident_named(&args.resource, &args.around, IncidentRequest::default_budget(), &cancellation),
    )
    .await?;
    let bytes = if out.format() == Format::Json {
        bounded_output::json(&result, args.max_output_bytes)?
    } else {
        let text = observations::incidents(&result);
        if text.len() > args.max_output_bytes {
            return Err(DeliveryProblem::Capacity.error());
        }
        text.into_bytes()
    };
    std::io::stdout().lock().write_all(&bytes).map_err(|_| culprit_core::Error::internal(t!("cli.output_failed")))?;
    Ok(if result.discovery_complete { 0 } else { 2 })
}
#[cfg(test)]
pub(crate) mod tests;
