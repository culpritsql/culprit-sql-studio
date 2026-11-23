//! Nearby native point context is selected by original identity and UTC, never by display name or current lookup.
use super::*;
use rusqlite::{Connection, Transaction};
use std::collections::BTreeMap;
mod selection;

// This bounds retained enrichment freshness, not native sampling/causal duration or source cost.
const MAX_POINT_DISTANCE_SECONDS: i64 = 300;
// Same-owner statement definitions and prior counter rosters run on slower hourly/baseline cadences.
const MAX_STATEMENT_CONTEXT_SECONDS: i64 = 86_400;

fn dataset(source: &CollectionDescription) -> Option<SnapshotDataset> {
    match &source.observation {
        SourceObservation::SourceRead { snapshot, .. } if snapshot.dataset == SnapshotDataset::TransactionMetadata => {
            return Some(snapshot.dataset);
        }
        SourceObservation::SourceRead { .. } | SourceObservation::PagedSourceRead { .. } => return None,
        _ => (),
    }
    let dataset = source.observation.snapshot()?.dataset;
    matches!(
        dataset,
        SnapshotDataset::Operational
            | SnapshotDataset::DatabasePerformanceCounters
            | SnapshotDataset::Cleanup
            | SnapshotDataset::Replication
            | SnapshotDataset::HyperscaleLogComponents
            | SnapshotDataset::MaintenanceProgress
            | SnapshotDataset::StatementCounters
            | SnapshotDataset::StatementDefinitions
    )
    .then_some(dataset)
}
fn family(source: &CollectionDescription) -> Result<String> {
    let dataset = dataset(source).ok_or_else(integrity)?;
    let mut sources = source
        .observation
        .snapshot()
        .ok_or_else(integrity)?
        .sources
        .iter()
        .map(|coverage| coverage.source.as_str())
        .collect::<Vec<_>>();
    sources.sort_unstable();
    crate::synthesis::synthesis_metadata_key(&("synthesis-native-point-family-v1", dataset, &source.source, sources))
}
fn native(source: &CollectionDescription) -> Option<NativeReadContext> {
    let context = match source.observation.snapshot_acquisition_context() {
        Some(context) => context.clone(),
        None => SourceReadExecutionScope::context_from_source(source).ok()?,
    };
    context.has_qualified_local_owner().then_some(context)
}
fn scope(context: &NativeReadContext) -> Result<Option<String>> {
    let kind = match &context.target {
        CollectionTarget::Database(_) => "database",
        CollectionTarget::ConfiguredConnection(_) => return Ok(None),
        CollectionTarget::Shared(_) => "shared",
    };
    if !context.has_qualified_local_owner() {
        return Ok(None);
    }
    crate::synthesis::synthesis_metadata_key(&(
        "synthesis-native-point-scope-v1",
        kind,
        &context.target,
        &context.partition,
    ))
    .map(Some)
}
pub(super) fn retain(
    tx: &Transaction<'_>,
    original: &DiagnosticCaptureRef,
    source: &CollectionDescription,
    from: &str,
    until: &str,
) -> Result<()> {
    if dataset(source).is_none() {
        return Ok(());
    }
    let Some(context) = native(source) else {
        return Ok(());
    };
    let Some(scope) = scope(&context)? else {
        return Ok(());
    };
    heads::retain(tx, original, source, &scope, &family(source)?, from, until)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    CommitPipeline,
    TransactionMetadata,
    CommitAndTransaction,
    StatementWorkload,
}
impl Purpose {
    fn including(self, other: Self) -> Self {
        match (self, other) {
            (Self::CommitPipeline, Self::CommitPipeline) => Self::CommitPipeline,
            (Self::TransactionMetadata, Self::TransactionMetadata) => Self::TransactionMetadata,
            (Self::StatementWorkload, Self::StatementWorkload) => Self::StatementWorkload,
            _ => Self::CommitAndTransaction,
        }
    }
}
struct Anchor {
    context: NativeReadContext,
    at: chrono::DateTime<chrono::FixedOffset>,
    purpose: Purpose,
    capture: DiagnosticCaptureRef,
}
impl Anchor {
    fn accepts(&self, dataset: SnapshotDataset) -> bool {
        match self.purpose {
            Purpose::TransactionMetadata => dataset == SnapshotDataset::TransactionMetadata,
            Purpose::StatementWorkload => {
                matches!(dataset, SnapshotDataset::StatementCounters | SnapshotDataset::StatementDefinitions)
            }
            Purpose::CommitPipeline => !matches!(
                dataset,
                SnapshotDataset::TransactionMetadata
                    | SnapshotDataset::StatementCounters
                    | SnapshotDataset::StatementDefinitions
            ),
            Purpose::CommitAndTransaction => {
                !matches!(dataset, SnapshotDataset::StatementCounters | SnapshotDataset::StatementDefinitions)
            }
        }
    }
}
enum AnchorSelection {
    NotRequired,
    Unavailable(SynthesisContextIssue),
    Ready(Box<Anchor>),
}
fn activity_purpose(source: &CollectionDescription, row: &TelemetryRecord) -> Option<Purpose> {
    let TelemetryRecord::Activity(actor) = row else { return None };
    if actor.idle
        || actor.key.request.is_none()
        || matches!(actor.scope, ActivityScopeBasis::OtherDatabase | ActivityScopeBasis::BlockingReference)
    {
        return None;
    }
    let Some(wait) = &actor.wait else { return None };
    if wait.class.and_then(TransactionMetadataWait::from_class).is_some() {
        return Some(Purpose::TransactionMetadata);
    }
    (wait.class == Some(ActivityWaitClass::LogRateGovernance)
        || wait
            .native_type
            .as_deref()
            .is_some_and(|native| source.target.engine().spec().commit_wait_stage(native).is_some()))
    .then_some(Purpose::CommitPipeline)
}
fn anchor(capture: &SynthesisCapture) -> Result<AnchorSelection> {
    if dataset(capture.source()) == Some(SnapshotDataset::TransactionMetadata) {
        let Some(context) = native(capture.source()) else {
            return Ok(AnchorSelection::Unavailable(SynthesisContextIssue::UnqualifiedPointScope));
        };
        let at = instant(&context.observed_from).ok_or_else(integrity)?;
        return Ok(AnchorSelection::Ready(Box::new(Anchor {
            context,
            at,
            purpose: Purpose::TransactionMetadata,
            capture: capture.original().clone(),
        })));
    }
    if dataset(capture.source()) == Some(SnapshotDataset::StatementCounters) {
        let Some(context) = native(capture.source()) else {
            return Ok(AnchorSelection::Unavailable(SynthesisContextIssue::UnqualifiedPointScope));
        };
        let at = instant(&context.observed_until).ok_or_else(integrity)?;
        return Ok(AnchorSelection::Ready(Box::new(Anchor {
            context,
            at,
            purpose: Purpose::StatementWorkload,
            capture: capture.original().clone(),
        })));
    }
    let purpose = capture
        .records()
        .iter()
        .filter_map(|record| activity_purpose(capture.source(), &record.record))
        .reduce(Purpose::including);
    let Some(purpose) = purpose else { return Ok(AnchorSelection::NotRequired) };
    let Some(header) = capture.source().observation.activity() else { return Ok(AnchorSelection::NotRequired) };
    if ObservationBounds::new(&header.at, &header.at).is_none() {
        return Ok(AnchorSelection::Unavailable(SynthesisContextIssue::UnknownPointClock));
    }
    let context = NativeReadContext::from_activity(capture.source())?;
    Ok(AnchorSelection::Ready(Box::new(Anchor {
        context,
        at: instant(&header.at).ok_or_else(integrity)?,
        purpose,
        capture: capture.original().clone(),
    })))
}

pub(super) struct PointSelection {
    pub hints: Vec<heads::Hint>,
    pub issues: Vec<SynthesisContextIssue>,
}
pub(super) fn select(conn: &Connection, capture: &SynthesisCapture) -> Result<PointSelection> {
    match anchor(capture)? {
        AnchorSelection::NotRequired => Ok(PointSelection { hints: vec![], issues: vec![] }),
        AnchorSelection::Unavailable(issue) => Ok(PointSelection { hints: vec![], issues: vec![issue] }),
        AnchorSelection::Ready(anchor) => selection::select(conn, &anchor),
    }
}
fn instant(value: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    ObservationBounds::new(value, value)?;
    chrono::DateTime::parse_from_rfc3339(value).ok()
}
fn owner_matches(point: &NativeReadContext, anchor: &NativeReadContext) -> bool {
    if !point.has_qualified_local_owner() || !anchor.has_qualified_local_owner() || point.partition != anchor.partition
    {
        return false;
    }
    match (&point.target, &anchor.target) {
        (CollectionTarget::Database(_), CollectionTarget::Database(_)) => point.same_local_database_owner(anchor),
        (CollectionTarget::Shared(owner), _) => {
            matches!(&anchor.partition,SourcePartition::InstanceLocal {member:Some(member),..} if member==owner)
        }
        _ => false,
    }
}
fn role_matches(dataset: SnapshotDataset, point: &NativeReadContext, anchor: &NativeReadContext) -> bool {
    // The bounded transaction statistic can support producer subtraction only with known unchanged role.
    // Statement definitions and counter deltas also stay within the same observed primary/standby role.
    !matches!(
        dataset,
        SnapshotDataset::Cleanup
            | SnapshotDataset::TransactionMetadata
            | SnapshotDataset::StatementCounters
            | SnapshotDataset::StatementDefinitions
    ) || point.database_writable.is_some() && point.database_writable == anchor.database_writable
}
fn near(point: &NativeReadContext, anchor: &Anchor) -> bool {
    let (Some(from), Some(until)) = (instant(&point.observed_from), instant(&point.observed_until)) else {
        return false;
    };
    if anchor.purpose == Purpose::StatementWorkload {
        return until <= anchor.at
            && anchor.at.signed_duration_since(until) <= chrono::Duration::seconds(MAX_STATEMENT_CONTEXT_SECONDS);
    }
    let distance = if anchor.at < from {
        from.signed_duration_since(anchor.at)
    } else if anchor.at > until {
        anchor.at.signed_duration_since(until)
    } else {
        chrono::Duration::zero()
    };
    distance <= chrono::Duration::seconds(MAX_POINT_DISTANCE_SECONDS)
}
pub(super) fn matches_anchor(point: &SynthesisCapture, original: &SynthesisCapture) -> bool {
    let (Some(dataset), Some(context), Ok(AnchorSelection::Ready(anchor))) =
        (dataset(point.source()), native(point.source()), anchor(original))
    else {
        return false;
    };
    anchor.accepts(dataset)
        && owner_matches(&context, &anchor.context)
        && role_matches(dataset, &context, &anchor.context)
        && near(&context, &anchor)
}
