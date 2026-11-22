//! Presentation preserves the canonical resource selector and independent clocks; it derives no health or identity.
use culprit_core::{telemetry::*, *};
use serde::Serialize;
use serde_json::Value;

const LABEL_CHARS: usize = 128;
const SOURCE_CHARS: usize = 64;
const COVERAGE_SOURCES: usize = 4;
const GAPS: usize = 16;

pub(super) fn value(report: &PublishedResources) -> Result<Value> {
    let resources = report.resources.iter().map(Resource::new).collect::<Result<Vec<_>>>()?;
    let gaps = report.gaps.iter().take(GAPS).collect::<Vec<_>>();
    let presentation_complete = resources.iter().all(|row| !row.text_truncated && row.coverage.is_complete())
        && gaps.len() == report.gaps.len();
    let value = serde_json::json!({
        "as_of":report.as_of, "window":report.window,
        "source_context":t!("mcp.resources.source_context"),
        "discovery_complete":report.discovery_complete,"presentation_complete":presentation_complete,
        "resources_available":report.resources.len(),"resources_omitted":0,"resources":resources,
        "next":report.next,"gaps_available":report.gaps.len(),"gaps_omitted":report.gaps.len()-gaps.len(),"gaps":gaps,
        "candidate_rows":report.candidate_rows,"archive_work":report.archive_work,
        "recovery_metadata_reads":report.recovery_metadata_reads,
    });
    super::super::response::bounded(value)
}
#[derive(Serialize)]
struct Resource<'a> {
    selector: &'a PublishedResourceSelector,
    engine: Engine,
    database_name: String,
    connection_alias: Option<String>,
    source_name: String,
    collection: TelemetryCollection,
    continuity: &'a SourceContinuity,
    collected_from: &'a str,
    collected_until: &'a str,
    native_time: &'a PublishedResourceTime,
    coverage: Coverage,
    text_truncated: bool,
}
impl<'a> Resource<'a> {
    fn new(resource: &'a PublishedDatabaseResource) -> Result<Self> {
        resource.selector.validate()?;
        if ObservationBounds::new(&resource.collected_from, &resource.collected_until).is_none() {
            return Err(DeliveryProblem::Integrity.error());
        }
        let name = format::escaped_preview(&resource.database_name, LABEL_CHARS);
        let alias = resource.connection_alias.as_deref().map(|text| format::escaped_preview(text, LABEL_CHARS));
        let source = format::escaped_preview(&resource.source_name, SOURCE_CHARS);
        let text_truncated = name.truncated || alias.as_ref().is_some_and(|text| text.truncated) || source.truncated;
        Ok(Self {
            selector: &resource.selector,
            engine: resource.engine,
            database_name: name.text,
            connection_alias: alias.map(|text| text.text),
            source_name: source.text,
            collection: resource.collection,
            continuity: &resource.continuity,
            collected_from: &resource.collected_from,
            collected_until: &resource.collected_until,
            native_time: &resource.native_time,
            coverage: Coverage::new(&resource.coverage),
            text_truncated,
        })
    }
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum Coverage {
    Snapshot { sources_available: usize, sources_omitted: usize, sources: Vec<Source> },
    Activity { source: Source },
    Unknown,
}
impl Coverage {
    fn new(coverage: &PublishedResourceCoverage) -> Self {
        match coverage {
            PublishedResourceCoverage::Snapshot { sources } => Self::Snapshot {
                sources_available: sources.len(),
                sources_omitted: sources.len().saturating_sub(COVERAGE_SOURCES),
                sources: sources.iter().take(COVERAGE_SOURCES).map(Source::new).collect(),
            },
            PublishedResourceCoverage::Activity { coverage } => Self::Activity { source: Source::new(coverage) },
            PublishedResourceCoverage::Unknown => Self::Unknown,
        }
    }
    fn is_complete(&self) -> bool {
        match self {
            Self::Snapshot { sources_omitted, sources, .. } => {
                *sources_omitted == 0 && sources.iter().all(|source| !source.text_truncated)
            }
            Self::Activity { source } => !source.text_truncated,
            Self::Unknown => true,
        }
    }
}
#[derive(Serialize)]
struct Source {
    source: String,
    state: SourceState,
    records: u32,
    complete_scope: bool,
    error_code: Option<ErrorCode>,
    stopped: Option<CollectionStop>,
    text_truncated: bool,
}
impl Source {
    fn new(source: &StateSourceCoverage) -> Self {
        let name = format::escaped_preview(&source.source, SOURCE_CHARS);
        Self {
            source: name.text,
            state: source.state,
            records: source.records,
            complete_scope: source.complete_scope,
            error_code: source.error_code,
            stopped: source.stopped,
            text_truncated: name.truncated,
        }
    }
}
