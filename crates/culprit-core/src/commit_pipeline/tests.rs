use super::*;
use crate::{Engine, MeasurementScope, ResetMarker};
fn set(source: &str, key: &str, unit: CounterUnit) -> NativeCounterSet {
    let mut counter = NativeCounterSample::gauge(key, 9007199254740993, unit, MeasurementScope::Server, source, key);
    counter.nature = CounterNature::Cumulative;
    NativeCounterSet {
        identity: NativeIdentity::new(source),
        scope: MeasurementScope::Server,
        counters: vec![counter],
        ratios: vec![],
        ratio_gaps: vec![],
        properties: Default::default(),
    }
}
#[test]
fn exact_native_event_type_and_spelling_distinguish_commit_stages_across_every_engine() {
    for engine in Engine::ALL {
        for (native, kind) in [
            ("WRITELOG", CommitPipelineStage::LocalLogFlush),
            ("LOGBUFFER", CommitPipelineStage::LogBufferSpace),
            ("HADR_SYNC_COMMIT", CommitPipelineStage::SynchronousHardeningAcknowledgement),
        ] {
            assert_eq!(
                engine.spec().commit_wait_stage(native),
                (engine == Engine::Mssql).then_some(kind),
                "{engine:?}: {native}"
            );
        }
        for (native, kind) in [
            ("IO: WALWrite", CommitPipelineStage::LocalLogWrite),
            ("IO: WalWrite", CommitPipelineStage::LocalLogWrite),
            ("IO: WALSync", CommitPipelineStage::LocalLogSync),
            ("IO: WalSync", CommitPipelineStage::LocalLogSync),
            ("IPC: SyncRep", CommitPipelineStage::SynchronousAcknowledgement),
        ] {
            assert_eq!(
                engine.spec().commit_wait_stage(native),
                (engine == Engine::Postgres).then_some(kind),
                "{engine:?}: {native}"
            );
        }
        for native in [
            "LWLock: SyncRep",
            "LWLock: WALWrite",
            "writeLog",
            "LOGMGR_QUEUE",
            "LOGMGR_FLUSH",
            "IO: DataFileWrite",
            "Client: ClientWrite",
            "wait/io/file/innodb/innodb_log_file",
        ] {
            assert!(engine.spec().commit_wait_stage(native).is_none(), "{engine:?}: {native}");
        }
    }
}
#[test]
fn pg18_wal_io_uses_the_explicit_wal_object_and_preserves_exact_native_units_nature_and_unknown_reset() {
    let mut source = set("pg_stat_io", "write_bytes", CounterUnit::Bytes);
    source.properties.insert("object".into(), "relation".into());
    assert!(Engine::Postgres.spec().commit_counter_kind("pg_stat_io", &source, &source.counters[0]).is_none());
    source.properties.insert("object".into(), "wal".into());
    assert_eq!(
        Engine::Postgres.spec().commit_counter_kind("pg_stat_io", &source, &source.counters[0]),
        Some(CommitPipelineCounterKind::WrittenBytes)
    );
    assert_eq!(source.counters[0].value.integer(), Some(9007199254740993));
    assert_eq!(source.counters[0].reset, ResetMarker::Unknown);
    for fault in ["nature", "unit", "source"] {
        let mut changed = source.clone();
        match fault {
            "nature" => changed.counters[0].nature = CounterNature::Gauge,
            "unit" => changed.counters[0].unit = CounterUnit::Count,
            _ => changed.counters[0].native_object = "other".into(),
        };
        assert!(
            Engine::Postgres.spec().commit_counter_kind("pg_stat_io", &changed, &changed.counters[0]).is_none(),
            "{fault}"
        );
    }
}
#[test]
fn shared_redo_bytes_and_buffer_wait_counts_are_distinct_and_other_provider_names_grant_no_meaning() {
    let bytes = set("global-status", "Innodb_os_log_written", CounterUnit::Bytes);
    let waits = set("global-status", "Innodb_log_waits", CounterUnit::Count);
    for engine in Engine::ALL {
        assert_eq!(
            engine.spec().commit_counter_kind("global-status", &bytes, &bytes.counters[0]),
            (engine == Engine::Mysql).then_some(CommitPipelineCounterKind::WrittenBytes)
        );
        assert_eq!(
            engine.spec().commit_counter_kind("global-status", &waits, &waits.counters[0]),
            (engine == Engine::Mysql).then_some(CommitPipelineCounterKind::LogBufferWaits)
        );
    }
    assert_eq!(bytes.scope, MeasurementScope::Server);
    assert_eq!(waits.counters[0].reset, ResetMarker::Unknown);
}

#[test]
fn log_counter_names_cannot_override_actual_windows_type_or_claim_a_rate_or_database_owner() {
    let mut source = set("sys.dm_os_performance_counters", "log-bytes", CounterUnit::Bytes);
    let counter = &mut source.counters[0];
    counter.native_object = "SQLServer:Databases".into();
    counter.native_name = "Log Bytes Flushed/sec".into();
    counter.native_instance = Some("_Total".into());
    counter.native_type = Some("272696576".into());
    assert_eq!(
        Engine::Mssql.spec().commit_counter_kind(crate::source_id::PERFORMANCE_COUNTERS, &source, &source.counters[0]),
        Some(CommitPipelineCounterKind::WrittenBytes)
    );
    assert_eq!(source.counters[0].nature, CounterNature::Cumulative);
    assert_eq!(source.counters[0].scope, MeasurementScope::Server);
    for native_type in ["65792", "-1", "not-a-native-code"] {
        let mut wrong = source.clone();
        wrong.counters[0].native_type = Some(native_type.into());
        assert!(
            Engine::Mssql
                .spec()
                .commit_counter_kind(crate::source_id::PERFORMANCE_COUNTERS, &wrong, &wrong.counters[0])
                .is_none()
        );
    }
    assert!(
        Engine::Mssql
            .spec()
            .commit_counter_kind("same-sounding-unowned-source", &source, &source.counters[0])
            .is_none()
    );
}
