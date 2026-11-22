//! Shared immutable native-definition content bindings; row receipts remain the authority.
use super::super::*;

pub(in crate::telemetry) fn definition_scope(source: &CollectionDescription, domain: &str) -> Result<String> {
    let partition = if let Some(execution) = source.observation.execution_context() {
        if source.observation.collection() != TelemetryCollection::Snapshot(SnapshotDataset::RetainedPlans)
            || !source.partition_is_consistent()
            || SourceReadExecutionScope::from_context(SourceReadQuestion::RelationalInputContext, execution).is_err()
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        &execution.partition
    } else {
        &source.partition
    };
    if source.target.engine() != crate::Engine::Mssql
        || source.target.database().is_none()
        || !partition.is_qualified()
        || !matches!(partition, SourcePartition::InstanceLocal { .. })
    {
        return Err(CollectionStop::InvalidPage.error());
    }
    source.scoped_id(domain, &())
}

pub(in crate::telemetry) fn definition_dataset(source: &CollectionDescription) -> Option<SnapshotDataset> {
    match &source.observation {
        SourceObservation::Snapshot(snapshot) => Some(snapshot.dataset),
        SourceObservation::PagedSnapshot(snapshot) => Some(snapshot.dataset),
        SourceObservation::SourceRead { snapshot, .. } => Some(snapshot.dataset),
        SourceObservation::PagedSourceRead { snapshot, .. } => Some(snapshot.dataset),
        _ => None,
    }
}

#[derive(Serialize)]
pub(in crate::telemetry) struct PayloadBinding<'a> {
    source_utf16_bytes: Option<&'a str>,
    native_sha256: Option<&'a str>,
    representation: Representation<'a>,
}
#[derive(Serialize)]
enum Representation<'a> {
    Available { sha256: &'a str, redaction_version: u32 },
    Unavailable(PayloadUnavailable),
}
impl<'a> From<&'a MssqlCachedPayload> for PayloadBinding<'a> {
    fn from(value: &'a MssqlCachedPayload) -> Self {
        Self {
            source_utf16_bytes: value.source_utf16_bytes.as_deref(),
            native_sha256: value.native_sha256.as_deref(),
            representation: match &value.payload {
                DefinitionPayload::Available { sha256, redaction_version, .. } => {
                    Representation::Available { sha256, redaction_version: *redaction_version }
                }
                DefinitionPayload::Unavailable { reason } => Representation::Unavailable(*reason),
            },
        }
    }
}
