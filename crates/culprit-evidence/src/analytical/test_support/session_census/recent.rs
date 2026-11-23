//! Shared controlled original corpus; every consumer still commits/reads canonical receipts and EOF itself.
use super::*;

#[derive(Clone, Copy, Debug)]
pub enum RecentClientCase {
    IdlePool,
    Mars,
    Boundary,
    ExcludedAndUnknown,
    Subset,
    Partial,
    ConflictingMars,
    UnknownCounters,
    Empty,
    EmptySubset,
}
impl RecentClientCase {
    pub const ALL: [Self; 10] = [
        Self::IdlePool,
        Self::Mars,
        Self::Boundary,
        Self::ExcludedAndUnknown,
        Self::Subset,
        Self::Partial,
        Self::ConflictingMars,
        Self::UnknownCounters,
        Self::Empty,
        Self::EmptySubset,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::IdlePool => "recent-idle",
            Self::Mars => "recent-mars",
            Self::Boundary => "recent-boundary",
            Self::ExcludedAndUnknown => "recent-excluded-unknown",
            Self::Subset => "recent-subset",
            Self::Partial => "recent-partial",
            Self::ConflictingMars => "recent-conflict",
            Self::UnknownCounters => "recent-unknown-totals",
            Self::Empty => "recent-empty",
            Self::EmptySubset => "recent-empty-subset",
        }
    }
}
pub fn recent_client_evidence(case: RecentClientCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let (mut source, rows) = session_census_evidence(SessionCensusCase::IdleConnection);
    let epoch = digest(b"controlled-recent-client-native-process-and-database-epoch");
    source.partition = SourcePartition::InstanceLocal {
        member: Some(SharedResourceIdentity {
            engine: culprit_core::Engine::Mssql,
            kind: SharedResourceKind::Instance,
            namespace: NativeIdentity::new("controlled-recent-client-native-machine"),
            native: NativeIdentity::new("controlled-recent-client-native-instance"),
        }),
        epoch: Some(epoch.clone()),
    };
    let SourceObservation::Activity(native) = &mut source.observation else { unreachable!() };
    native.epoch = Some(epoch);
    native.database_writable = Some(true);
    let TelemetryRecord::Activity(first) = rows.into_iter().next().unwrap() else { unreachable!() };
    let mut first = *first;
    first.client.as_mut().unwrap().application = Some("fixture-application".into());
    first.client.as_mut().unwrap().user = Some("shared-login".into());
    first.session_counters.physical_reads = Some("7".into());
    first.session_counters.writes = Some("2".into());
    context(&mut first).tds_protocol_version = Some("1946157060".into());
    let observations = match case {
        RecentClientCase::IdlePool | RecentClientCase::Subset | RecentClientCase::Partial => vec![first],
        RecentClientCase::Mars | RecentClientCase::ConflictingMars => {
            let mut one = current_request(first, "0", "11111111-1111-1111-1111-111111111111");
            context(&mut one).session_database_id = Some("7".into());
            context(&mut one).session_database_name = Some("db".into());
            one.request_counters.as_mut().unwrap().logical_reads = Some("9999999".into());
            let mut two = current_request(one.clone(), "1", "22222222-2222-2222-2222-222222222222");
            context(&mut two).session_database_id = Some("7".into());
            context(&mut two).session_database_name = Some("db".into());
            if matches!(case, RecentClientCase::ConflictingMars) {
                two.session_counters.physical_reads = Some("8".into());
            }
            vec![one, two]
        }
        RecentClientCase::Boundary => [3_599_999, 3_600_000, 3_600_001]
            .into_iter()
            .enumerate()
            .map(|(i, age)| {
                let mut row = session(&first, i as u32);
                row.key.session.generation = "2020-01-01T10:00:00.0000000".into();
                row.last_request_age_ms = Some("3601000".into());
                let session_context = row.session_context.as_deref_mut().unwrap();
                session_context.login_age_ms = Some("7200000".into());
                session_context.last_request_end_age_ms = Some(age.to_string());
                let native = context(&mut row);
                native.times.login_without_offset = "2020-01-01T10:00:00.0000000".into();
                native.elapsed_ms = Some("7200000".into());
                native.times.last_request_start_without_offset = Some("2020-01-01T10:59:59.0000000".into());
                context(&mut row).times.last_request_end_without_offset = Some(
                    match i {
                        0 => "2020-01-01T11:00:00.0010000",
                        1 => "2020-01-01T11:00:00.0000000",
                        _ => "2020-01-01T10:59:59.9990000",
                    }
                    .into(),
                );
                row
            })
            .collect(),
        RecentClientCase::ExcludedAndUnknown => {
            let mut rows = [
                "Microsoft SQL Server Management Studio - Query",
                "SqlServerExtensionCollector",
                "azdata-Query",
                "customer-azdata-workload",
            ]
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                let mut row = session(&first, i as u32);
                row.client.as_mut().unwrap().application = Some(name.into());
                row
            })
            .collect::<Vec<_>>();
            let mut missing_program = session(&first, 4);
            missing_program.client.as_mut().unwrap().application = None;
            let mut missing_end = session(&first, 5);
            missing_end.session_context.as_deref_mut().unwrap().last_request_end_age_ms = None;
            context(&mut missing_end).times.last_request_end_without_offset = None;
            let mut future_end = session(&first, 6);
            future_end.session_context.as_deref_mut().unwrap().last_request_end_age_ms = None;
            context(&mut future_end).times.last_request_end_without_offset = Some("2020-01-01T12:00:01.0000000".into());
            rows.extend([missing_program, missing_end, future_end]);
            rows
        }
        RecentClientCase::UnknownCounters => {
            let mut other = session(&first, 1);
            other.session_counters.physical_reads = None;
            other.session_counters.logical_reads = None;
            other.session_counters.writes = Some("0".into());
            other.client.as_mut().unwrap().user = None;
            context(&mut other).tds_protocol_version = None;
            context(&mut other).elapsed_ms = Some("-1".into());
            vec![first, other]
        }
        RecentClientCase::Empty | RecentClientCase::EmptySubset => vec![],
    };
    let SourceObservation::Activity(native) = &mut source.observation else { unreachable!() };
    native.observations = observations.len() as u32;
    native.native_rows = native.observations;
    native.coverage = StateSourceCoverage::collected(source.source.clone(), native.observations);
    if matches!(case, RecentClientCase::Subset | RecentClientCase::EmptySubset) {
        native.population = ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers;
    }
    if matches!(case, RecentClientCase::Partial) {
        native.native_rows += 1;
        native.coverage = StateSourceCoverage::observed(source.source.clone(), native.observations, false);
        native.coverage.stopped = Some(CollectionStop::RowLimit);
    }
    let read = ActivityCensusRead::new(source, observations, true).unwrap();
    (read.source, read.observations.into_iter().map(TelemetryRecord::from).collect())
}
fn context(row: &mut ActivityObservation) -> &mut MssqlSessionContext {
    let Some(SessionNativeContext::Mssql(native)) = row.session_context.as_deref_mut().unwrap().native.as_deref_mut()
    else {
        unreachable!()
    };
    native
}
fn session(original: &ActivityObservation, index: u32) -> ActivityObservation {
    let mut row = original.clone();
    row.key.session.id = (61 + index).to_string();
    if let Some(MssqlIdleSessionConnection::Observed { connection_id, .. }) = &mut context(&mut row).idle_connection {
        *connection_id = format!("37dd5a75-5bb6-4487-89a7-{index:012}");
    }
    row
}
