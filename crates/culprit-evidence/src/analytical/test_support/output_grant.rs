//! Controlled canonical shapes reuse native memory fixtures; these originals do not qualify production work.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputGrantCase {
    Held,
    Peer,
    OtherWait,
    Zero,
    Null,
    Missing,
    Partial,
    RestrictedSql,
    UnknownDuration,
}
impl OutputGrantCase {
    pub const ALL: &'static [Self] = &[
        Self::Held,
        Self::Peer,
        Self::OtherWait,
        Self::Zero,
        Self::Null,
        Self::Missing,
        Self::Partial,
        Self::RestrictedSql,
        Self::UnknownDuration,
    ];
}
pub struct OutputGrantOriginal {
    pub source: CollectionDescription,
    pub original: ExportedRecord,
    pub episode: DiagnosticEpisode,
    pub focus: SourceReadFocus,
    pub execution: SourceReadExecutionScope,
}
pub fn output_grant_original() -> OutputGrantOriginal {
    let old = request_memory_investigation_fixture();
    let mut source = old.baseline_source;
    source.source = culprit_core::source_id::SESSIONS.into();
    let SourceObservation::Activity(native) = &mut source.observation else { unreachable!() };
    native.coverage.source = source.source.clone();
    let TelemetryRecord::Activity(mut actor) = old.baseline_record.record else { unreachable!() };
    actor.elapsed_ms = Some("1".into());
    actor.waiting_on_lock = Some(false);
    let wait = actor.wait.as_mut().unwrap();
    wait.class = None;
    wait.native_type = Some("ASYNC_NETWORK_IO".into());
    wait.elapsed_ms = Some("27".into());
    wait.resources = None;
    let original =
        ExportedRecord::new(&source, &old.baseline_record.capture, TelemetryRecord::Activity(actor.clone())).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"output-original-manifest"), &original, 0).unwrap();
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
        .iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::ClientResultOutput { .. }))
        .unwrap()
        .clone();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::OutputGrantRetention,
        &episode,
        &source,
        &reference,
        &original,
    )
    .unwrap()
    .unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&source).unwrap()).unwrap();
    OutputGrantOriginal { source, original, episode, focus, execution }
}
pub fn output_grant_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: OutputGrantCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let request = focus.output_request().unwrap();
    let (_, mut rows) = request_memory_evidence(
        DefinitionPayload::sql("SELECT payload FROM orders WHERE token='output-private-canary'", focus.target.engine()),
        true,
    );
    let TelemetryRecord::Operational(row) = &mut rows[0] else { unreachable!() };
    let OperationalFact::ExecutionGrant(grant) = &mut row.fact else { unreachable!() };
    grant.observed_at = "2020-01-01T12:00:03Z".into();
    let mut actor = request.original.clone();
    actor.statement =
        DefinitionPayload::sql("SELECT payload FROM orders WHERE token='output-private-canary'", focus.target.engine());
    actor.wait.as_mut().unwrap().elapsed_ms = Some("31".into());
    if case == OutputGrantCase::OtherWait {
        actor.wait.as_mut().unwrap().native_type = Some("SOS_SCHEDULER_YIELD".into());
    }
    if case == OutputGrantCase::UnknownDuration {
        actor.wait.as_mut().unwrap().elapsed_ms = None;
    }
    if case == OutputGrantCase::RestrictedSql {
        actor.statement = DefinitionPayload::Unavailable { reason: PayloadUnavailable::Restricted };
        grant.statement_relationship = GrantStatementRelationship::Unresolved;
    }
    grant.request = Some(actor);
    grant.resource = Some(NativeIdentity::new("2").qualified("resource_semaphore_id", "0"));
    grant.native_null_columns.retain(|column| column != "pool_id");
    grant.native_properties.insert("pool_id".into(), "2".into());
    if case == OutputGrantCase::Zero {
        grant.granted_kibibytes = Some("0".into());
    }
    if case == OutputGrantCase::Null {
        grant.granted_kibibytes = None;
        grant.native_null_columns.push("granted_memory_kb".into());
    }
    row.key = grant.key().unwrap();
    if case == OutputGrantCase::Peer {
        let mut peer = grant.as_ref().clone();
        peer.identity = NativeIdentity::new("other-output-resource-reservation");
        let actor = peer.request.as_mut().unwrap();
        actor.key.session.id = "77".into();
        actor.key.request.as_mut().unwrap().id = "1".into();
        actor.statement_identity = Some(NativeIdentity::new("other-current-statement"));
        actor.wait = None;
        let key = peer.key().unwrap();
        rows.push(TelemetryRecord::Operational(OperationalObservation {
            source: peer.source.clone(),
            key,
            fact: OperationalFact::ExecutionGrant(Box::new(peer)),
        }));
    }
    if case == OutputGrantCase::Missing {
        rows.clear();
    }
    let name = culprit_core::source_id::CURRENT_REQUEST_MEMORY;
    let context = NativeReadContext {
        target: focus.target.clone(),
        partition: execution.partition().clone(),
        database_writable: execution.database_writable(),
        observed_from: "2020-01-01T12:00:02Z".into(),
        observed_until: "2020-01-01T12:00:04Z".into(),
    };
    let source = CollectionDescription {
        target: focus.target.clone(),
        partition: SourcePartition::Target,
        source: name.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::SourceRead {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::CurrentRequestMemory,
                observed_from: context.observed_from.clone(),
                observed_until: context.observed_until.clone(),
                records: rows.len() as u32,
                sources: vec![StateSourceCoverage::observed(
                    name.into(),
                    rows.len() as u32,
                    case != OutputGrantCase::Partial,
                )],
            },
            focus_key: focus.key().unwrap(),
            execution: Box::new(context),
        },
    };
    assert!(source.partition_is_consistent());
    assert!(rows.iter().all(TelemetryRecord::is_valid));
    (source, rows)
}
pub fn output_grant_evidence(case: OutputGrantCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let original = output_grant_original();
    output_grant_result(&original.focus, &original.execution, case)
}
