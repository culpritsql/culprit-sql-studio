//! Canonical native-shaped ticket cohorts, not actual engine execution or native-work qualification.
use super::*;
use culprit_core::{Engine, LockWait, MeasurementScope, blocking::*};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataLockContextCase {
    Mysql,
    Mariadb,
    MixedTickets,
    MissingSql,
    UnknownEvent,
    UnknownTransaction,
    Prefix,
    ChangedTicket,
    EndedRequest,
    OtherObject,
    UnknownMode,
    Convert,
}
impl MetadataLockContextCase {
    pub const ALL: &'static [Self] = &[
        Self::Mysql,
        Self::Mariadb,
        Self::MixedTickets,
        Self::MissingSql,
        Self::UnknownEvent,
        Self::UnknownTransaction,
        Self::Prefix,
        Self::ChangedTicket,
        Self::EndedRequest,
        Self::OtherObject,
        Self::UnknownMode,
        Self::Convert,
    ];
    pub fn has_candidates(self) -> bool {
        matches!(self, Self::Mysql | Self::Mariadb | Self::MixedTickets | Self::MissingSql | Self::UnknownTransaction)
    }
}
pub struct MetadataLockOriginal {
    pub source: CollectionDescription,
    pub original: ExportedRecord,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub execution: SourceReadExecutionScope,
}
pub fn metadata_lock_original() -> MetadataLockOriginal {
    let (mut source, _) = session_census_evidence(SessionCensusCase::MysqlShared);
    let actor = actor(701, false);
    let SourceObservation::Activity(native) = &mut source.observation else { unreachable!() };
    native.population = ActivityPopulation::DefaultDatabaseConnections;
    native.native_rows = 1;
    native.observations = 1;
    native.coverage = StateSourceCoverage::collected(source.source.clone(), 1);
    let recovery = Recovery::capture(
        "fixture:metadata-lock-parent",
        &digest(b"metadata-lock-parent-schema"),
        source.clone(),
        vec![actor.clone().into()],
        980_000,
    );
    let original = ExportedRecord::new(&source, &recovery.manifest.capture, actor.clone().into()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &recovery.manifest.capture_manifest_sha256, &original, 0).unwrap();
    let update = observe_diagnostic_activity(
        &source,
        &reference.capture_ref(),
        &[DiagnosticActivityRow { reference: &reference, observation: &actor }],
        None,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:01Z",
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::LockWait { .. }))
        .unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::MetadataLockContext,
        &episode,
        &source,
        &reference,
        &original,
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&source).unwrap()).unwrap();
    MetadataLockOriginal { source, original, episode, focus, execution }
}
pub fn metadata_lock_context_evidence(case: MetadataLockContextCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let original = metadata_lock_original();
    metadata_lock_context_result(&original.focus, &original.execution, case)
}
pub fn metadata_lock_context_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: MetadataLockContextCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    use MetadataLockContextCase::*;
    let original = focus.metadata_lock_request().unwrap();
    let mut root = original.original.clone();
    let mut gate = actor(702, false);
    let holder = actor(703, true);
    gate.statement = DefinitionPayload::sql(
        "ALTER TABLE db.items ADD COLUMN note varchar(20) DEFAULT 'mdl-private-canary'",
        Engine::Mysql,
    );
    if case == MissingSql {
        gate.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Restricted };
    }
    let mut point = MysqlMetadataLockPoint {
        origin: if case == Mariadb {
            MysqlMetadataLockOrigin::MariadbPerformanceSchema
        } else {
            MysqlMetadataLockOrigin::MysqlPerformanceSchema
        },
        observed_at: "2020-01-01T12:00:02.2Z".into(),
        settled_at: "2020-01-01T12:00:03.8Z".into(),
        subject: root.key.clone(),
        subject_matches: true,
        subject_settled: true,
        availability: MysqlMetadataLockAvailability::Observed,
        snapshot_rows: 3,
        snapshot_limit: 63,
        snapshot_complete: true,
        tickets: vec![
            ticket(&root, "1", "SHARED_READ", "PENDING"),
            ticket(&gate, "2", "EXCLUSIVE", "PENDING"),
            ticket(&holder, "3", "SHARED_READ", "GRANTED"),
        ],
    };
    match case {
        MixedTickets => {
            point.tickets.push(ticket(&gate, "4", "SHARED_UPGRADABLE", "GRANTED"));
            point.snapshot_rows = 4;
        }
        UnknownEvent => point.tickets[1].owner_event_id = None,
        UnknownTransaction => point.tickets[2].transaction = None,
        Prefix => {
            point.availability = MysqlMetadataLockAvailability::Limited;
            point.snapshot_rows = 64;
            point.snapshot_complete = false;
        }
        ChangedTicket => point.tickets[1].settled = false,
        OtherObject => point.tickets[1].resource.name = "other_items".into(),
        UnknownMode => point.tickets[1].mode = "FUTURE_MODE".into(),
        Convert => point.tickets[1].status = "CONVERT".into(),
        _ => {}
    }
    root.wait.as_mut().unwrap().resources = Some(vec![LockWait {
        session: root.key.session.id.clone(),
        mode: None,
        resource: Some("METADATA".into()),
        object: None,
        index: None,
        native: Some(Box::new(NativeLockEvidence::MysqlMetadata(Box::new(point)))),
    }]);
    let mut rows = vec![root, gate, holder];
    if case == EndedRequest {
        rows.clear();
    }
    let name = "mysql-metadata-lock-context";
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        database_writable: execution.database_writable(),
        observed_from: "2020-01-01T12:00:02Z".into(),
        observed_until: "2020-01-01T12:00:04Z".into(),
    };
    let source = CollectionDescription {
        target: context.target.clone(),
        partition: context.partition.clone(),
        source: name.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::MetadataLockContext {
            snapshot: ActivitySnapshotDescription {
                epoch: context.partition.native_epoch().map(str::to_owned),
                database_writable: context.database_writable,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                at: "2020-01-01T12:00:03Z".into(),
                scope: MeasurementScope::Database,
                population: ActivityPopulation::ActiveRequestsOpenTransactionsAndBlockers,
                native_rows: rows.len() as u32,
                observations: rows.len() as u32,
                coverage: StateSourceCoverage::observed(name.into(), rows.len() as u32, false),
            },
            focus_key: focus.key().unwrap(),
            execution: Box::new(context),
            result: Box::new(MetadataLockContextDescription {
                request: ActivityBlockingFocusResult {
                    focus: original.clone(),
                    binding: if case == EndedRequest {
                        FocusedRequestBinding::ParticipantMissing
                    } else {
                        FocusedRequestBinding::Matched
                    },
                    membership: if case == EndedRequest {
                        FocusedRequestMembership::NotObserved
                    } else {
                        FocusedRequestMembership::Observed
                    },
                    request_end: if case == EndedRequest {
                        BlockingRequestState::NotRead
                    } else {
                        BlockingRequestState::SameStatement
                    },
                    closure: BlockingClosureDescription {
                        status: if case == EndedRequest {
                            BlockingClosureStatus::NotRead
                        } else {
                            BlockingClosureStatus::LockEvidenceLimit
                        },
                        max_lookups: MAX_BLOCKING_LOOKUPS,
                        max_depth: 1,
                        max_participants: MAX_BLOCKING_PARTICIPANTS,
                    },
                },
                cohort: rows.iter().map(|row| row.key.clone()).collect(),
            }),
        },
    };
    assert!(source.partition_is_consistent(), "{case:?}");
    let borrowed = rows.iter().map(|observation| ActivityFrameRow { observation, reference: None }).collect::<Vec<_>>();
    let frame = ActivityFrameEvidence::new(&source, &borrowed, true).unwrap();
    let SourceObservation::MetadataLockContext { result, .. } = &source.observation else { unreachable!() };
    assert!(result.accepts_frame(&frame), "{case:?}");
    (source, rows.into_iter().map(TelemetryRecord::from).collect())
}
fn actor(id: u32, idle: bool) -> ActivityObservation {
    let mut actor = activity_observation(DefinitionPayload::sql(
        "SELECT id FROM db.items WHERE token='mdl-private-canary'",
        Engine::Mysql,
    ));
    actor.key = ActivityKey {
        session: NativeActivityIdentity { id: id.to_string(), generation: (id + 300).to_string() },
        request: (!idle).then(|| NativeActivityIdentity { id: "3".into(), generation: "10000".into() }),
        transaction: None,
    };
    actor.statement_identity = actor
        .key
        .request
        .as_ref()
        .and_then(|request| culprit_core::engines::mysql_activity_statement_identity(&actor.key.session, request));
    actor.idle = idle;
    actor.scope = if idle { ActivityScopeBasis::SessionDatabase } else { ActivityScopeBasis::RequestDatabase };
    actor.database = Some("db".into());
    actor.state = if idle { "Sleep" } else { "Waiting for table metadata lock" }.into();
    actor.command = Some(if idle { "Sleep" } else { "Query" }.into());
    actor.waiting_on_lock = (!idle).then_some(true);
    actor.elapsed_ms = (!idle).then(|| "40000".into());
    actor.session_counters = ActivityCounters::default();
    actor.request_counters = (!idle).then(ActivityCounters::default);
    actor.session_context = Some(Box::new(ActivitySessionContext {
        lifecycle: MysqlSessionContext::lifecycle(idle, !idle),
        login_age_ms: None,
        last_request_end_age_ms: None,
        client_library: None,
        client_process_id: None,
        native: Some(Box::new(SessionNativeContext::Mysql(MysqlSessionContext {
            default_database: Some("db".into()),
            request_schema: (!idle).then(|| "db".into()),
            thread_type: Some("FOREGROUND".into()),
            thread_name: Some("thread/sql/one_connection".into()),
        }))),
    }));
    actor.client = Some(ActivityClient {
        application: None,
        user: Some(format!("native-login-{id}")),
        host: Some("native-client".into()),
    });
    actor.wait = (!idle).then(|| ActivityWait {
        class: None,
        native_type: Some("Waiting for table metadata lock".into()),
        elapsed_ms: Some("40000".into()),
        owners: vec![],
        resources: None,
        identified_resources: vec![],
    });
    if idle {
        actor.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
    }
    assert!(actor.is_valid());
    actor
}
fn ticket(actor: &ActivityObservation, address: &str, mode: &str, status: &str) -> MysqlMetadataLockTicket {
    MysqlMetadataLockTicket {
        address: address.into(),
        resource: MysqlMetadataLockResource { object_type: "TABLE".into(), schema: "db".into(), name: "items".into() },
        mode: mode.into(),
        duration: "TRANSACTION".into(),
        status: status.into(),
        owner_thread_id: Some(actor.key.session.generation.clone()),
        owner_process_id: Some(actor.key.session.id.clone()),
        owner_event_id: Some("8".into()),
        actor: Some(actor.key.clone()),
        transaction: Some(MysqlMetadataTransactionEvent {
            event_id: Some("2".into()),
            timer_start: Some("9000".into()),
            state: Some("ACTIVE".into()),
            end_event_id: None,
        }),
        matched: true,
        settled: true,
    }
}

pub fn assert_metadata_lock_context_capture(
    capture: &crate::analytical::evidence::EvidenceCapture,
    source: &CollectionDescription,
    records: &[TelemetryRecord],
) {
    assert_eq!(capture.source(), source);
    assert!(capture.is_read_complete());
    let actual = capture.records().iter().map(|row| row.row().record.clone()).collect::<Vec<_>>();
    assert_eq!(&actual, records);
    let SourceObservation::MetadataLockContext { result, .. } = &capture.source().observation else { panic!() };
    let rows = actual
        .iter()
        .map(|record| match record {
            TelemetryRecord::Activity(actor) => ActivityFrameRow { observation: actor, reference: None },
            _ => panic!(),
        })
        .collect::<Vec<_>>();
    assert!(result.accepts_frame(&ActivityFrameEvidence::new(capture.source(), &rows, true).unwrap()));
}

#[cfg(test)]
mod original_statement_controls {
    use super::*;
    #[test]
    fn native_lock_signal_uses_its_exact_original_focus_and_receipt_without_a_second_statement_token() {
        let original = metadata_lock_original();
        assert!(
            matches!(&original.episode.signal,DiagnosticSignal::LockWait {key} if key==&original.focus.metadata_lock_request().unwrap().original.key)
        );
        let mut focus = original.focus.clone();
        let SourceReadPrerequisite::ObservedMetadataLockWait { request } = &mut focus.prerequisite else { panic!() };
        request.statement_identity = NativeIdentity::new("unrelated-query-hash");
        assert!(
            !focus.is_valid(),
            "the original native statement token is independently pinned by ActivityRequestFocus"
        );
        let mut row = original.original.clone();
        let TelemetryRecord::Activity(actor) = &mut row.record else { panic!() };
        actor.statement = DefinitionPayload::sql("SELECT id FROM db.other_items WHERE id=99", Engine::Mysql);
        assert!(
            SourceReadFocus::from_followup_input(
                SourceReadQuestion::MetadataLockContext,
                &original.episode,
                &original.source,
                &original.focus.basis,
                &row
            )
            .is_err(),
            "changed original SQL cannot reuse the sealed first receipt"
        );
    }
}
