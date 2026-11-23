//! Documented native wait and existing performance-spec semantics; unsupported internal names stay unknown.
use crate::{CommitPipelineCounterKind as Kind, CommitPipelineStage as Stage, telemetry::*};
pub(super) fn wait_stage(native: &str) -> Option<Stage> {
    Some(match native {
        "WRITELOG" => Stage::LocalLogFlush,
        "LOGBUFFER" => Stage::LogBufferSpace,
        "LOGMGR_RESERVE_APPEND" => Stage::LogSpaceReuse,
        "HADR_SYNC_COMMIT" => Stage::SynchronousHardeningAcknowledgement,
        _ => return None,
    })
}
pub(super) fn counter_kind(source: &str, set: &NativeCounterSet, counter: &NativeCounterSample) -> Option<Kind> {
    if source != crate::source_id::PERFORMANCE_COUNTERS {
        return None;
    }
    let profile = crate::Engine::Mssql.spec().performance_counters()?;
    let spec = profile.field_in_sample(
        set,
        &counter.native_object,
        &counter.native_name,
        counter.native_instance.as_deref(),
    )?;
    let native_type = counter.native_type.as_deref()?.parse::<i32>().ok()?;
    if spec.meaning(native_type) != (counter.nature, counter.unit) || counter.nature != CounterNature::Cumulative {
        return None;
    }
    Some(match spec.id {
        "log_bytes_flushed" => Kind::WrittenBytes,
        "log_flushes" => Kind::FlushOperations,
        "log_flush_wait" => Kind::FlushWaitTime,
        _ => return None,
    })
}
