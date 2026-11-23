//! One resource/window request reads published answers or explicitly requested compatible workload, source-free.
use crate::{
    bounded_output,
    output::{Format, Out, observations},
};
use clap::Args;
use culprit_core::{
    PublishedResourceCursor, PublishedResourceRequest, PublishedResourceWindow, Result, WarehouseWindowView, t,
    telemetry::DeliveryProblem,
};
use culprit_engine::evidence::{EvidenceConfig, EvidenceRuntime, IncidentRequest, NamedWindowQuery};
use culprit_read::cancellation::CollectionCancellation;
use std::{io::Write, path::PathBuf};

#[derive(Args)]
pub(crate) struct WindowArgs {
    #[arg(value_name="RESOURCE",help=culprit_core::t!("incident.cli.resource_name"))]
    resource: String,
    #[arg(long,value_name="UTC",help=culprit_core::t!("window.cli.from"))]
    from: String,
    #[arg(long,value_name="UTC",help=culprit_core::t!("window.cli.until"))]
    until: String,
    #[arg(long,value_name="FILE",help=culprit_core::t!("evidence.cli.config"))]
    config: PathBuf,
    #[arg(long,help=culprit_core::t!("window.cli.workload"),conflicts_with="cursor")]
    workload: bool,
    #[arg(long,value_name="UTC",help=culprit_core::t!("resources.cli.as_of"))]
    as_of: Option<String>,
    #[arg(long,default_value_t=culprit_core::DEFAULT_WAREHOUSE_WINDOW_PAGE_ROWS,
        value_parser=clap::value_parser!(u32).range(1..=i64::from(culprit_core::MAX_WAREHOUSE_WINDOW_PAGE_ROWS)),
        help=culprit_core::t!("window.cli.page_rows"))]
    page_rows: u32,
    #[arg(long,value_name="JSON",requires="as_of",value_parser=parse_cursor,help=culprit_core::t!("window.cli.cursor"))]
    cursor: Option<PublishedResourceCursor>,
    #[arg(long,default_value_t=bounded_output::DEFAULT_JSON_BYTES,help=culprit_core::t!("evidence.cli.max_output_bytes"))]
    max_output_bytes: usize,
    #[arg(long,value_name="NUMBER",value_parser=clap::value_parser!(usize),requires="full_payloads",help=culprit_core::t!("artifact.cli.detail"))]
    detail: Option<usize>,
    #[arg(long,requires="detail",help=culprit_core::t!("artifact.cli.full_payloads"))]
    full_payloads: bool,
}
fn parse_cursor(value: &str) -> std::result::Result<PublishedResourceCursor, String> {
    super::resources_cmd::parse_cursor(value)
}
pub(crate) async fn run(args: WindowArgs, out: &Out) -> Result<u8> {
    if !matches!(out.format(), Format::Text | Format::Json) {
        return Err(culprit_core::Error::invalid(t!("evidence.cli.format")));
    }
    bounded_output::validate_json_limit(args.max_output_bytes)?;
    let window = PublishedResourceWindow { from: args.from, until: args.until };
    if !window.is_valid() {
        return Err(DeliveryProblem::InvalidContract.error());
    }
    let as_of = args.as_of.unwrap_or_else(|| PublishedResourceRequest::default().as_of);
    let request = NamedWindowQuery {
        resource_name: &args.resource,
        window: &window,
        as_of: &as_of,
        view: if args.workload {
            WarehouseWindowView::CompatibleWorkload
        } else {
            WarehouseWindowView::PublishedAnswers
        },
        page_rows: args.page_rows,
        after: args.cursor.as_ref(),
    };
    let service = EvidenceRuntime::default().evidence(&EvidenceConfig::from_file(&args.config)?)?;
    let cancellation = CollectionCancellation::default();
    if args.detail.is_some() && !args.full_payloads {
        return Err(DeliveryProblem::InvalidContract.error());
    }
    if let Some(detail) = args.detail {
        let report = crate::collection_shutdown::run(
            &cancellation,
            service.window_artifact_named(&request, detail, IncidentRequest::default_budget(), &cancellation),
        )
        .await?;
        let complete = report.gaps.is_empty();
        let bytes = if out.format() == Format::Json {
            bounded_output::json(&report, args.max_output_bytes)?
        } else {
            let text = observations::artifacts(&report);
            if text.len() > args.max_output_bytes {
                return Err(DeliveryProblem::Capacity.error());
            }
            text.into_bytes()
        };
        std::io::stdout()
            .lock()
            .write_all(&bytes)
            .map_err(|_| culprit_core::Error::internal(t!("cli.output_failed")))?;
        return Ok(if complete { 0 } else { 2 });
    }
    let result = crate::collection_shutdown::run(
        &cancellation,
        service.window_named(&request, IncidentRequest::default_budget(), &cancellation),
    )
    .await?;
    let bytes = if out.format() == Format::Json {
        bounded_output::json(&result, args.max_output_bytes)?
    } else {
        let text = observations::window(&result)?;
        if text.len() > args.max_output_bytes {
            return Err(DeliveryProblem::Capacity.error());
        }
        text.into_bytes()
    };
    std::io::stdout().lock().write_all(&bytes).map_err(|_| culprit_core::Error::internal(t!("cli.output_failed")))?;
    Ok(if result.discovery_complete { 0 } else { 2 })
}
#[cfg(test)]
mod tests;
