use super::*;

fn instance() -> SharedResourceIdentity {
    SharedResourceIdentity {
        engine: Engine::Mssql,
        kind: SharedResourceKind::Instance,
        namespace: NativeIdentity::new("private-diagnostic-instance-namespace"),
        native: NativeIdentity::new("private-diagnostic-instance-member"),
    }
}
fn native_original() -> (CollectionDescription, ActivityObservation) {
    let member = instance();
    activity_original(
        CollectionTarget::Shared(member.clone()),
        SourcePartition::InstanceLocal {
            member: Some(member),
            epoch: Some("private-diagnostic-instance-process".into()),
        },
    )
}

#[test]
fn shared_diagnostic_parent_declares_one_exact_instance_frame_without_database_ownership_or_full_coverage() {
    let (source, actor) = native_original();
    let frame = source.observation.activity().unwrap();
    assert!(source.has_qualified_instance_activity());
    assert_eq!(frame.scope, MeasurementScope::Server);
    assert_eq!(frame.database_writable, None);
    assert_eq!(frame.population, ActivityPopulation::UserSessionsCurrentRequestsAndBlockers);
    assert!(!frame.coverage.complete_scope);
    assert_eq!(actor.scope, ActivityScopeBasis::InstanceMember);
    assert_eq!(actor.database, None);
    ExportedRecord::new(&source, &fixture().0.capture, TelemetryRecord::from(actor)).unwrap();
}

#[test]
fn missing_foreign_changed_epoch_role_and_database_row_cannot_borrow_the_instance_header() {
    let (source, actor) = native_original();
    let mut variants = Vec::new();
    let mut missing = source.clone();
    missing.partition = SourcePartition::Target;
    variants.push(missing);
    let mut unknown = source.clone();
    unknown.partition =
        SourcePartition::InstanceLocal { member: None, epoch: Some("private-diagnostic-instance-process".into()) };
    variants.push(unknown);
    let mut foreign = source.clone();
    let SourcePartition::InstanceLocal { member: Some(member), .. } = &mut foreign.partition else { unreachable!() };
    member.native.id = "different-instance-member".into();
    variants.push(foreign);
    let mut epoch = source.clone();
    let SourcePartition::InstanceLocal { epoch: original, .. } = &mut epoch.partition else { unreachable!() };
    *original = Some("different-instance-process".into());
    variants.push(epoch);
    let mut role = source.clone();
    let SourceObservation::Activity(frame) = &mut role.observation else { unreachable!() };
    frame.database_writable = Some(true);
    variants.push(role);
    for source in variants {
        assert!(!source.has_qualified_instance_activity());
        assert!(ExportedRecord::new(&source, &fixture().0.capture, TelemetryRecord::from(actor.clone())).is_err());
    }
    let mut row = actor;
    row.scope = ActivityScopeBasis::RequestDatabase;
    assert!(ExportedRecord::new(&source, &fixture().0.capture, TelemetryRecord::from(row)).is_err());
}

#[test]
fn database_file_shared_and_independent_epoch_privacy_captures_stay_canonical() {
    let cases = diagnostic_evidence_cases();
    assert_eq!(cases.len(), 4);
    for (source, records) in cases {
        assert!(source.partition_is_consistent());
        let SourceObservation::Diagnostics(header) = &source.observation else { unreachable!() };
        assert!(!header.scope_bindings.is_empty());
        assert!(header.scope_bindings.values().all(|binding| binding.target == source.target));
        for record in records {
            ExportedRecord::new(&source, &fixture().0.capture, record).unwrap();
        }
    }
}
