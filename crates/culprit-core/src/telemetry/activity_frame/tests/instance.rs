use super::*;
fn instance_source(rows: usize) -> CollectionDescription {
    let instance = SharedResourceIdentity {
        engine: Engine::Mssql,
        kind: SharedResourceKind::Instance,
        namespace: NativeIdentity::new("controlled-native-server"),
        native: NativeIdentity::new("controlled-native-member"),
    };
    let mut source = source(rows);
    source.target = CollectionTarget::Shared(instance.clone());
    source.partition =
        SourcePartition::InstanceLocal { member: Some(instance), epoch: Some(digest(b"native-instance-process")) };
    let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
    frame.epoch = source.partition.native_epoch().map(str::to_owned);
    frame.scope = MeasurementScope::Server;
    frame.database_writable = None;
    frame.population = ActivityPopulation::UserSessionsCurrentRequestsAndBlockers;
    source
}
#[test]
fn instance_member_conditions_require_the_same_exact_shared_native_frame_and_cannot_mint_database_followups() {
    let mut actor = request("42");
    actor.scope = ActivityScopeBasis::InstanceMember;
    actor.database = Some("observed-other-database".into());
    actor.elapsed_ms = Some("120000".into());
    actor.statement_identity = Some(
        NativeIdentity::new("AB".repeat(64))
            .qualified("native_object", "sys.dm_exec_requests.sql_handle")
            .qualified("database_id", "7")
            .qualified("statement_start_offset", "0")
            .qualified("statement_end_offset", "-1"),
    );
    let source = instance_source(1);
    let refs = references(&source, std::slice::from_ref(&actor));
    let rows = [actor.clone()];
    let borrowed = borrowed(&rows, &refs);
    let frame = ActivityFrameEvidence::new(&source, &borrowed, true).unwrap();
    assert!(actor.conditions(&ActivityPolicy::default()).is_empty());
    assert!(ActivityEvidence::select(&rows, &ActivityPolicy::default()).unwrap().conditions.is_empty());
    let selected = ActivityEvidence::select_frame(&frame, &ActivityPolicy::default()).unwrap();
    assert!(selected.conditions[&actor.key].contains(&ActivityCondition::LongRunning));
    let update = observe_diagnostic_activity(
        &source,
        &refs[0].capture_ref(),
        &[DiagnosticActivityRow { reference: &refs[0], observation: &actor }],
        None,
        &ActivityPolicy::default(),
        "2026-11-13T10:00:01Z",
    )
    .unwrap();
    assert!(!update.revisions.is_empty());
    let episode = &update.revisions[0].episode;
    assert_eq!(episode.scope.target, source.target);
    let focus = ActivityRequestFocus {
        parent: refs[0].clone(),
        target: source.target.clone(),
        partition: source.partition.clone(),
        database_writable: None,
        source: source.source.clone(),
        epoch: source.partition.native_epoch().unwrap().into(),
        original_observed_at: source.observation.activity().unwrap().at.clone(),
        original: actor.clone(),
        statement_identity: actor.statement_identity.clone().unwrap(),
    };
    assert!(focus.statement_identity.is_valid());
    assert_eq!(focus.original.statement_identity.as_ref(), Some(&focus.statement_identity));
    assert!(!focus.is_valid(), "a reported database label cannot supply original native DB ownership");
}
#[test]
fn member_rows_and_server_headers_refuse_cross_target_member_role_epoch_population_and_ordinary_database_promotion() {
    for case in ["database", "member", "role", "epoch", "population", "node", "ordinary-other", "ordinary-request"] {
        let mut source = instance_source(1);
        let mut actor = request("42");
        actor.scope = ActivityScopeBasis::InstanceMember;
        match case {
            "database" => {
                source.target = DatabaseIdentity::server(Engine::Mssql, "sample", "fixture.invalid", 1433, None).into()
            }
            "member" => {
                let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut source.partition else {
                    panic!()
                };
                member.native.id = "another-member".into();
            }
            "role" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.database_writable = Some(true);
            }
            "epoch" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.epoch = Some(digest(b"another-process"));
            }
            "population" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.population = ActivityPopulation::DefaultDatabaseConnections;
            }
            "node" => {
                let SourceObservation::Activity(frame) = &mut source.observation else { panic!() };
                frame.scope = MeasurementScope::Node;
            }
            "ordinary-other" => actor.scope = ActivityScopeBasis::OtherDatabase,
            "ordinary-request" => actor.scope = ActivityScopeBasis::RequestDatabase,
            _ => unreachable!(),
        }
        let row = ActivityFrameRow { observation: &actor, reference: None };
        assert!(ActivityFrameEvidence::new(&source, &[row], true).is_err(), "{case}");
        let capture = CaptureIdentity { observation_id: "controlled-instance-original".into(), revision: None };
        assert!(ExportedRecord::new(&source, &capture, TelemetryRecord::from(actor)).is_err(), "{case}");
    }
}
