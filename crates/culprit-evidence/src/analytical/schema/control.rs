//! One nonrecursive completion row per verified capture, including explicit successful-empty coverage.
use super::*;
use ScalarType::*;
pub fn published_ranges() -> Result<LogicalSchema> {
    LogicalSchema::new(
        "CulpritPublishedRangesV1",
        [
            Field::required("ContractId", "/_culprit/contract_id", String),
            Field::required("ManifestSha256", "/_culprit/manifest_sha256", String),
            Field::required("SchemaDigest", "/_culprit/schema_digest", String),
            Field::required("RecoveryStoreId", "/recovery_store_id", String),
            Field::required("ReceiptsSha256", "/receipts_sha256", String),
            Field::required("PublicationId", "/publication_id", String),
            Field::required("ResourceId", "/_culprit/resource_id", String),
            Field::required("ResourceKind", "/_culprit/resource_kind", String),
            Field::optional("DatabaseSelector", "/_culprit/database_selector", String),
            Field::required("SourceId", "/_culprit/source_id", String),
            Field::required("LogicalSourceId", "/_culprit/logical_source_id", String),
            Field::required("SourceName", "/_culprit/source_name", String),
            Field::required("Engine", "/_culprit/engine", String),
            Field::required("RangeId", "/_culprit/range_id", String),
            Field::required("RangeKind", "/_culprit/range_kind", String),
            Field::optional("Dataset", "/_culprit/dataset", String),
            Field::required("SourceContinuity", "/_culprit/continuity", String),
            Field::required("ReferencesComplete", "/_culprit/references_complete", Bool),
            Field::required("ReferenceCoverage", "/reference_coverage", JsonContainer),
            Field::required("ObservationId", "/capture/observation_id", String),
            Field::required("OutboxId", "/capture/revision/outbox_id", String),
            Field::required("OwnerEpoch", "/capture/revision/owner_epoch", Long),
            Field::required("CaptureSequence", "/capture/revision/sequence", Long),
            Field::required("CollectedFrom", "/_culprit/collected_from", Timestamp),
            Field::required("CollectedUntil", "/_culprit/collected_until", Timestamp),
            Field::optional("IntervalId", "/_culprit/interval_id", String),
            Field::optional("IntervalFrom", "/_culprit/interval_from", Timestamp),
            Field::optional("IntervalUntil", "/_culprit/interval_until", Timestamp),
            Field::optional("ObservedAt", "/_culprit/observed_at", Timestamp),
            Field::optional("ObservedFrom", "/_culprit/observed_from", Timestamp),
            Field::optional("ObservedUntil", "/_culprit/observed_until", Timestamp),
            Field::required("SourceDescription", "/source", JsonContainer),
            super::super::investigations::CONTEXT_FIELD,
            Field::required("SourceQueries", "/source_queries", Long),
            Field::optional("ReadMetrics", "/read_metrics", JsonContainer),
            Field::required("Batches", "/batches", JsonContainer),
        ],
    )
}

/// Collector observations of configured-route attempts are not native database-health samples.
pub fn connections(common: &[Field]) -> Result<LogicalSchema> {
    LogicalSchema::new(
        "CulpritCollectorConnectionsV1",
        common.iter().copied().chain([
            Field::required("ConnectionId", "/_culprit/configured_connection/id", String),
            Field::required("RouteRevision", "/_culprit/configured_connection/route_revision", String),
            Field::required("ConnectionName", "/_culprit/configured_connection/name", String),
            Field::required("OriginalAttempt", "/record/data/original", JsonContainer),
            Field::required("OriginalState", "/record/data/original_state", String),
            Field::optional("OriginalStatus", "/_projection/original_status", String),
            Field::optional("OriginalStop", "/_projection/original_stop", String),
            Field::required("OriginalEvidence", "/record/data/original_evidence", String),
            Field::required("SourceWork", "/record/data/source_work", String),
            Field::required("CollectorOutcome", "/record/data/collector_outcome", JsonContainer),
            Field::required("CollectorStartedAt", "/record/data/collector_started_at", Timestamp),
            Field::required("CollectorFinishedAt", "/record/data/collector_finished_at", Timestamp),
            Field::optional("CollectorElapsedMs", "/record/data/collector_elapsed_ms", Real),
            Field::required("ConnectionOutcome", "/_projection/connection_outcome", String),
            Field::required("OrderedCollectorClocks", "/_projection/ordered_collector_clocks", Bool),
            Field::optional("ConnectionPhases", "/record/data/connections", JsonContainer),
            Field::optional("OriginalSourceQueries", "/record/data/original_source_queries", Long),
            Field::optional("NativeTargetLink", "/record/data/native", JsonContainer),
            Field::required("Details", "/record/data", JsonContainer),
        ]),
    )
}
