//! Source-free archive discovery is dispatched before opening any workspace or source provider.
use crate::{
    bounded_output,
    output::{Format, Out},
};
use clap::Args;
use culprit_core::{
    PublishedResourceCoverage, PublishedResourceCursor, PublishedResourceRequest, PublishedResourceTime,
    PublishedResourceWindow, PublishedResources, Result, format, t, telemetry::DeliveryProblem,
};
use culprit_engine::evidence::{EvidenceConfig, EvidenceRuntime};
use culprit_evidence::analytical::evidence::EvidenceReadBudget;
use culprit_read::cancellation::CollectionCancellation;
use std::{io::Write, path::PathBuf};

#[derive(Args)]
pub(crate) struct ResourcesArgs {
    #[arg(long,value_name="FILE",help=culprit_core::t!("evidence.cli.config"))]
    config: PathBuf,
    #[arg(long,value_name="UTC",help=culprit_core::t!("resources.cli.as_of"))]
    as_of: Option<String>,
    #[arg(long,value_name="UTC",requires="until",help=culprit_core::t!("resources.cli.from"))]
    from: Option<String>,
    #[arg(long,value_name="UTC",requires="from",help=culprit_core::t!("resources.cli.until"))]
    until: Option<String>,
    #[arg(long,default_value_t=culprit_core::MAX_PUBLISHED_RESOURCE_PAGE_ROWS,value_parser=clap::value_parser!(u32).range(1..=i64::from(culprit_core::MAX_PUBLISHED_RESOURCE_PAGE_ROWS)),help=culprit_core::t!("resources.cli.page_rows"))]
    page_rows: u32,
    #[arg(long,value_name="JSON",requires="as_of",value_parser=parse_cursor,help=culprit_core::t!("resources.cli.cursor"))]
    cursor: Option<PublishedResourceCursor>,
    #[arg(long,default_value_t=bounded_output::DEFAULT_JSON_BYTES,help=culprit_core::t!("evidence.cli.max_output_bytes"))]
    max_output_bytes: usize,
}
impl ResourcesArgs {
    fn request(&self) -> Result<PublishedResourceRequest> {
        let window = match (&self.from, &self.until) {
            (Some(from), Some(until)) => Some(PublishedResourceWindow { from: from.clone(), until: until.clone() }),
            (None, None) => None,
            _ => return Err(DeliveryProblem::InvalidContract.error()),
        };
        let request = PublishedResourceRequest {
            as_of: self.as_of.clone().unwrap_or_else(|| PublishedResourceRequest::default().as_of),
            window,
            after: self.cursor.clone(),
            page_rows: self.page_rows,
        };
        request.validate()?;
        Ok(request)
    }
}
pub(crate) async fn run(args: ResourcesArgs, out: &Out) -> Result<u8> {
    if !matches!(out.format(), Format::Text | Format::Json) {
        return Err(culprit_core::Error::invalid(t!("evidence.cli.format")));
    }
    bounded_output::validate_json_limit(args.max_output_bytes)?;
    let request = args.request()?;
    let service = EvidenceRuntime::default().evidence(&EvidenceConfig::from_file(&args.config)?)?;
    let cancellation = CollectionCancellation::default();
    let report = crate::collection_shutdown::run(
        &cancellation,
        service.resources(&request, EvidenceReadBudget::default(), &cancellation),
    )
    .await?;
    let bytes = if out.format() == Format::Json {
        bounded_output::json(&report, args.max_output_bytes)?
    } else {
        let text = human(&report);
        if text.len() > args.max_output_bytes {
            return Err(DeliveryProblem::Capacity.error());
        }
        text.into_bytes()
    };
    std::io::stdout().lock().write_all(&bytes).map_err(|_| culprit_core::Error::internal(t!("cli.output_failed")))?;
    Ok(if report.discovery_complete { 0 } else { 2 })
}
pub(crate) fn parse_cursor(value: &str) -> std::result::Result<PublishedResourceCursor, String> {
    if value.len() > 4096 {
        return Err(t!("resources.cli.invalid_cursor"));
    }
    let cursor: PublishedResourceCursor =
        serde_json::from_str(value).map_err(|_| t!("resources.cli.invalid_cursor"))?;
    if !culprit_core::telemetry::valid_digest(&cursor.selection_key) || !cursor.position.is_valid() {
        return Err(t!("resources.cli.invalid_cursor"));
    }
    Ok(cursor)
}
fn human(report: &PublishedResources) -> String {
    let mut lines = vec![
        t!("resources.cli.scope", as_of = preview(&report.as_of)),
        t!(
            "resources.cli.summary",
            n = report.resources.len() as f64,
            count = format::count(report.resources.len() as f64)
        ),
    ];
    for resource in &report.resources {
        lines.push(t!(
            "resources.cli.entry",
            database = preview(&resource.database_name),
            engine = resource.engine.label(),
            source = preview(&resource.source_name),
            from = preview(&resource.collected_from),
            until = preview(&resource.collected_until)
        ));
        match &resource.coverage {
            PublishedResourceCoverage::Snapshot { sources } if sources.iter().any(|source| !source.complete_scope) => {
                lines.push(t!("resources.cli.native_partial"))
            }
            PublishedResourceCoverage::Activity { coverage } if !coverage.complete_scope => {
                lines.push(t!("resources.cli.native_partial"))
            }
            PublishedResourceCoverage::Unknown => lines.push(t!("resources.cli.native_unknown")),
            _ => {}
        }
        if matches!(resource.native_time, PublishedResourceTime::Unknown) {
            lines.push(t!("resources.cli.clock_unknown"));
        }
    }
    if report.next.is_some() {
        lines.push(t!("resources.cli.more"));
    }
    if report.gaps.iter().any(|gap| !matches!(gap, culprit_core::PublishedResourceGap::MoreCandidates)) {
        lines.push(t!("resources.cli.unavailable"));
    }
    lines.join("\n") + "\n"
}
fn preview(value: &str) -> String {
    format::escaped_preview(value, 256).text
}
#[cfg(test)]
mod tests;
