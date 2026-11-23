//! One sealed-input corpus for pure Gold and both physical proof readers. These are controlled originals, not native acceptance.
use super::*;
mod replacements;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitPipelineIntervalCase {
    Postgres17,
    Postgres18,
    LargeInteger,
    TimingZero,
    TimingMissing,
    NoWork,
    UnknownReset,
    ChangedReset,
    FutureReset,
    Decreased,
    Partial,
    ForeignMember,
    Restart,
    DuplicateBefore,
    OverlappingWindow,
    Unbracketed,
    CloserUnknownReset,
    ChangedPrimaryRole,
    NativeWrite,
    RemoteAcknowledgement,
    ChangedPopulation,
    CloserUnavailable,
    MssqlUnknownReset,
    MssqlUnknownRole,
    MssqlChangedRole,
    CloserRestartBefore,
    CloserRestartAfter,
    CloserRestartPair,
    CloserMemberBefore,
    CloserMemberAfter,
    CloserUnknownOwner,
    CloserEmptyUnknownOwner,
    UnrelatedSharedResource,
    ManyUnrelatedSharedFrames,
}
impl CommitPipelineIntervalCase {
    pub const ALL: [Self; 34] = [
        Self::Postgres17,
        Self::Postgres18,
        Self::LargeInteger,
        Self::TimingZero,
        Self::TimingMissing,
        Self::NoWork,
        Self::UnknownReset,
        Self::ChangedReset,
        Self::FutureReset,
        Self::Decreased,
        Self::Partial,
        Self::ForeignMember,
        Self::Restart,
        Self::DuplicateBefore,
        Self::OverlappingWindow,
        Self::Unbracketed,
        Self::CloserUnknownReset,
        Self::ChangedPrimaryRole,
        Self::NativeWrite,
        Self::RemoteAcknowledgement,
        Self::ChangedPopulation,
        Self::CloserUnavailable,
        Self::MssqlUnknownReset,
        Self::MssqlUnknownRole,
        Self::MssqlChangedRole,
        Self::CloserRestartBefore,
        Self::CloserRestartAfter,
        Self::CloserRestartPair,
        Self::CloserMemberBefore,
        Self::CloserMemberAfter,
        Self::CloserUnknownOwner,
        Self::CloserEmptyUnknownOwner,
        Self::UnrelatedSharedResource,
        Self::ManyUnrelatedSharedFrames,
    ];
}
pub fn commit_pipeline_interval_fixture(case: CommitPipelineIntervalCase) -> CommitPipelineFixture {
    if let Some(fixture) = replacements::fixture(case) {
        return fixture;
    }
    use CommitPipelineIntervalCase as Case;
    if matches!(case, Case::MssqlUnknownReset | Case::MssqlUnknownRole | Case::MssqlChangedRole) {
        return mssql(case);
    }
    let pg18 = matches!(case, Case::Postgres18 | Case::ChangedPopulation);
    let mut fixture =
        other_provider(if pg18 { CommitPipelineCase::Postgres18 } else { CommitPipelineCase::Postgres17 });
    let TelemetryRecord::Activity(actor) = &mut fixture.originals[0].records[0] else { unreachable!() };
    actor.elapsed_ms = Some("100000".into());
    if let Some(wait) = &mut actor.wait {
        wait.class = None;
        wait.resources = None;
        wait.identified_resources.clear();
    }
    if let Some(wait) = &mut actor.wait {
        if case == Case::NativeWrite {
            wait.native_type = Some("IO: WALWrite".into());
        }
        if case == Case::RemoteAcknowledgement {
            wait.native_type = Some("IPC: SyncRep".into());
        }
    }
    let context = fixture.originals.pop().unwrap();
    let mut before = duplicate(&context);
    let mut after = duplicate(&context);
    bounds(&mut before, "2020-01-01T12:00:00Z", "2020-01-01T12:00:00.2Z");
    bounds(&mut after, "2020-01-01T12:00:01.8Z", "2020-01-01T12:00:02Z");
    replace_counters(&mut before, pg18, false, case);
    replace_counters(&mut after, pg18, true, case);
    match case {
        Case::Partial => {
            let header = after.source.observation.snapshot().unwrap().clone();
            let mut header = header;
            header.sources[0].state = culprit_core::SourceState::Partial;
            header.sources[0].complete_scope = false;
            after.source.observation = SourceObservation::Snapshot(header);
        }
        Case::ForeignMember => {
            if let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut after.source.partition {
                member.native.id = "foreign-server".into();
                after.source.target = CollectionTarget::Shared(member.clone());
            }
        }
        Case::Restart => {
            if let SourcePartition::InstanceLocal { epoch, .. } = &mut after.source.partition {
                *epoch = Some("changed-native-process".into());
            }
        }
        Case::OverlappingWindow => bounds(&mut before, "2020-01-01T12:00:00Z", "2020-01-01T12:00:01.2Z"),
        Case::Unbracketed => bounds(&mut after, "2020-01-01T12:00:00.3Z", "2020-01-01T12:00:00.5Z"),
        Case::ChangedPopulation => {
            let native = set(&mut after);
            native.identity = native.identity.clone().qualified("backend_type", "walwriter");
            native.properties.insert("backend_type".into(), "walwriter".into());
            let key = native.key().unwrap();
            let TelemetryRecord::Operational(row) = &mut after.records[0] else { unreachable!() };
            row.key = key;
        }
        Case::ChangedPrimaryRole => {
            let SourceObservation::Activity(header) = &mut fixture.originals[0].source.observation else {
                unreachable!()
            };
            header.database_writable = Some(false);
        }
        _ => (),
    }
    if case == Case::DuplicateBefore {
        fixture.originals.push(duplicate(&before));
    }
    if case == Case::CloserUnknownReset {
        let mut older = duplicate(&before);
        bounds(&mut older, "2019-12-31T23:59:59Z", "2019-12-31T23:59:59.2Z");
        for sample in &mut set(&mut older).counters {
            sample.reset = ResetMarker::At("2019-01-01T00:00:00Z".into());
        }
        fixture.originals.push(older);
    }
    if case == Case::CloserUnavailable {
        let mut unavailable = duplicate(&after);
        bounds(&mut unavailable, "2020-01-01T12:00:01.5Z", "2020-01-01T12:00:01.6Z");
        unavailable.records.clear();
        let SourceObservation::Snapshot(header) = &mut unavailable.source.observation else { unreachable!() };
        header.records = 0;
        header.sources[0] = StateSourceCoverage::unavailable(
            header.sources[0].source.clone(),
            culprit_core::ErrorCode::PermissionDenied,
            None,
        );
        fixture.originals.push(unavailable);
    }
    fixture.originals.extend([before, after]);
    fixture
}
fn duplicate(original: &CommitPipelineOriginalFixture) -> CommitPipelineOriginalFixture {
    CommitPipelineOriginalFixture { source: original.source.clone(), records: original.records.clone() }
}
fn bounds(original: &mut CommitPipelineOriginalFixture, from: &str, until: &str) {
    let SourceObservation::Snapshot(header) = &mut original.source.observation else { unreachable!() };
    header.observed_from = from.into();
    header.observed_until = until.into();
}
fn set(original: &mut CommitPipelineOriginalFixture) -> &mut NativeCounterSet {
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::CounterSet(set), .. }) =
        &mut original.records[0]
    else {
        unreachable!()
    };
    set
}
fn replace_counters(
    original: &mut CommitPipelineOriginalFixture,
    pg18: bool,
    after: bool,
    case: CommitPipelineIntervalCase,
) {
    use CommitPipelineIntervalCase as Case;
    let set = set(original);
    let native_source = if pg18 { "pg_stat_io" } else { "pg_stat_wal" };
    set.identity = NativeIdentity::new(native_source).qualified("postmaster_started_at", "2019-01-01T00:00:00Z");
    if pg18 {
        for (key, value) in [("object", "wal"), ("backend_type", "client backend"), ("context", "normal")] {
            set.identity = set.identity.clone().qualified(key, value);
            set.properties.insert(key.into(), value.into());
        }
    }
    let fields = if pg18 {
        vec![
            ("write_bytes", CounterUnit::Bytes, "1000", "1040"),
            ("writes", CounterUnit::Count, "10", "12"),
            ("fsyncs", CounterUnit::Count, "5", "7"),
            ("write_time", CounterUnit::Milliseconds, "30", "42.5"),
            ("fsync_time", CounterUnit::Milliseconds, "15", "35.5"),
        ]
    } else {
        vec![
            ("wal_bytes", CounterUnit::Bytes, "1000", "1040"),
            ("wal_records", CounterUnit::Count, "100", "105"),
            ("wal_fpi", CounterUnit::Count, "10", "11"),
            ("wal_buffers_full", CounterUnit::Count, "1", "3"),
            ("wal_write", CounterUnit::Count, "10", "12"),
            ("wal_sync", CounterUnit::Count, "5", "7"),
            ("wal_write_time", CounterUnit::Milliseconds, "30", "42.5"),
            ("wal_sync_time", CounterUnit::Milliseconds, "15", "35.5"),
        ]
    };
    set.counters = fields
        .into_iter()
        .filter_map(|(name, unit, a, b)| {
            if case == Case::TimingMissing && after && unit == CounterUnit::Milliseconds {
                return None;
            }
            let mut value = if after && case != Case::NoWork { b } else { a }.to_owned();
            if case == Case::LargeInteger && unit == CounterUnit::Bytes {
                value = if after { "9007199254741010" } else { "9007199254740993" }.into();
            }
            if case == Case::TimingZero && unit == CounterUnit::Milliseconds || case == Case::Decreased && after {
                value = "0".into();
            }
            let value = if value.contains('.') {
                MeasurementValue::Real(value.parse().unwrap())
            } else {
                MeasurementValue::Integer(value)
            };
            Some(NativeCounterSample {
                key: name.into(),
                value,
                unit,
                nature: CounterNature::Cumulative,
                scope: MeasurementScope::Server,
                reset: match case {
                    Case::UnknownReset => ResetMarker::Unknown,
                    Case::CloserUnknownReset if !after => ResetMarker::Unknown,
                    Case::ChangedReset if after => ResetMarker::At("2019-12-31T23:00:00Z".into()),
                    Case::FutureReset => ResetMarker::At("2020-01-01T12:00:01Z".into()),
                    _ => ResetMarker::At("2019-01-01T00:00:00Z".into()),
                },
                native_object: native_source.into(),
                native_name: name.into(),
                native_instance: None,
                native_type: None,
            })
        })
        .collect();
    let key = set.key().unwrap();
    let TelemetryRecord::Operational(row) = &mut original.records[0] else { unreachable!() };
    row.key = key;
}
fn mssql(case: CommitPipelineIntervalCase) -> CommitPipelineFixture {
    use CommitPipelineIntervalCase as Case;
    let (source, records) = database_performance_counter_evidence(DatabasePerformanceCounterCase::Exact);
    let mut primary = commit_pipeline_fixture(CommitPipelineCase::Exact).originals.remove(0);
    primary.records.truncate(1);
    primary.source.target = source.target.clone();
    primary.source.partition = source.partition.clone();
    let SourceObservation::Activity(header) = &mut primary.source.observation else { unreachable!() };
    header.epoch = source.partition.native_epoch().map(str::to_owned);
    header.native_rows = 1;
    header.observations = 1;
    header.coverage = StateSourceCoverage::collected(primary.source.source.clone(), 1);
    match case {
        Case::MssqlUnknownRole => header.database_writable = None,
        // This native counter source admits writable endpoints only. A real intervening Activity
        // role change must refuse the comparison without inventing an inadmissible counter capture.
        Case::MssqlChangedRole => header.database_writable = Some(false),
        _ => (),
    }
    let TelemetryRecord::Activity(actor) = &mut primary.records[0] else { unreachable!() };
    actor.database = Some(source.target.database().unwrap().database.clone());
    let before = CommitPipelineOriginalFixture { source: source.clone(), records: records.clone() };
    let mut after = CommitPipelineOriginalFixture { source, records };
    let SourceObservation::DatabasePerformanceCounters { snapshot, .. } = &mut after.source.observation else {
        unreachable!()
    };
    snapshot.observed_from = "2020-01-01T12:00:02Z".into();
    snapshot.observed_until = "2020-01-01T12:00:03Z".into();
    let set = set(&mut after);
    set.properties.insert(PERFORMANCE_COUNTER_CLOCK_PROPERTY.into(), "2020-01-01T12:00:02.500000000Z".into());
    for sample in &mut set.counters {
        if let MeasurementValue::Integer(value) = &mut sample.value {
            *value = (value.parse::<u64>().unwrap() + 2).to_string();
        }
    }
    CommitPipelineFixture { originals: vec![primary, before, after] }
}
