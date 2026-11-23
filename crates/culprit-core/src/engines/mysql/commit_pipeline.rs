//! Global redo totals do not identify a database, current request, transaction, commit count or cause.
use crate::{CommitPipelineCounterKind as Kind, telemetry::*};
pub(super) fn counter_kind(source: &str, _set: &NativeCounterSet, counter: &NativeCounterSample) -> Option<Kind> {
    if counter.native_object != source
        || counter.native_name != counter.key
        || counter.nature != CounterNature::Cumulative
    {
        return None;
    }
    match (source, counter.key.as_str(), counter.unit) {
        ("global-status", "Innodb_os_log_written", CounterUnit::Bytes) => Some(Kind::WrittenBytes),
        ("global-status", "Innodb_log_waits", CounterUnit::Count)
        | ("information_schema.innodb_metrics", "log_waits", CounterUnit::Count) => Some(Kind::LogBufferWaits),
        _ => None,
    }
}
