//! Native event type matters: IPC SyncRep is remote acknowledgement; LWLock SyncRep is not.
use crate::{CommitPipelineCounterKind as Kind, CommitPipelineStage as Stage, telemetry::*};
pub(super) fn wait_stage(native: &str) -> Option<Stage> {
    Some(match native {
        "IO: WALWrite" | "IO: WalWrite" => Stage::LocalLogWrite,
        "IO: WALSync" | "IO: WalSync" => Stage::LocalLogSync,
        "IO: WALInitWrite" | "IO: WalInitWrite" => Stage::SegmentInitializationWrite,
        "IO: WALInitSync" | "IO: WalInitSync" => Stage::SegmentInitializationSync,
        "IPC: SyncRep" => Stage::SynchronousAcknowledgement,
        _ => return None,
    })
}
pub(super) fn counter_kind(source: &str, set: &NativeCounterSet, counter: &NativeCounterSample) -> Option<Kind> {
    if counter.native_object != source
        || counter.native_name != counter.key
        || counter.nature != CounterNature::Cumulative
    {
        return None;
    }
    let (kind, unit) = match (source, counter.key.as_str()) {
        ("pg_stat_wal", "wal_bytes") => (Kind::GeneratedBytes, CounterUnit::Bytes),
        ("pg_stat_wal", "wal_records") => (Kind::LogRecords, CounterUnit::Count),
        ("pg_stat_wal", "wal_fpi") => (Kind::FullPageImages, CounterUnit::Count),
        ("pg_stat_wal", "wal_buffers_full") => (Kind::BufferFullWrites, CounterUnit::Count),
        ("pg_stat_wal", "wal_write") => (Kind::WriteOperations, CounterUnit::Count),
        ("pg_stat_wal", "wal_sync") => (Kind::SyncOperations, CounterUnit::Count),
        ("pg_stat_wal", "wal_write_time") => (Kind::WriteTime, CounterUnit::Milliseconds),
        ("pg_stat_wal", "wal_sync_time") => (Kind::SyncTime, CounterUnit::Milliseconds),
        ("pg_stat_io", "write_bytes") if set.properties.get("object").map(String::as_str) == Some("wal") => {
            (Kind::WrittenBytes, CounterUnit::Bytes)
        }
        ("pg_stat_io", "writes") if set.properties.get("object").map(String::as_str) == Some("wal") => {
            (Kind::WriteOperations, CounterUnit::Count)
        }
        ("pg_stat_io", "fsyncs") if set.properties.get("object").map(String::as_str) == Some("wal") => {
            (Kind::SyncOperations, CounterUnit::Count)
        }
        ("pg_stat_io", "write_time") if set.properties.get("object").map(String::as_str) == Some("wal") => {
            (Kind::WriteTime, CounterUnit::Milliseconds)
        }
        ("pg_stat_io", "fsync_time") if set.properties.get("object").map(String::as_str) == Some("wal") => {
            (Kind::SyncTime, CounterUnit::Milliseconds)
        }
        _ => return None,
    };
    (counter.unit == unit).then_some(kind)
}
