use super::*;
use culprit_evidence::analytical::test_support::{self, TransactionMetadataSlruCase as Case};
struct Original {
    source: CollectionDescription,
    row: ExportedRecord,
    reference: DiagnosticEvidenceRef,
}
fn original(source: CollectionDescription, records: Vec<TelemetryRecord>, id: &str) -> Original {
    let capture = CaptureIdentity {
        observation_id: id.into(),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"slru-comparison-outbox"),
            owner_epoch: "1".into(),
            sequence: if id == "before" { "1" } else { "2" }.into(),
        }),
    };
    let row = ExportedRecord::new(&source, &capture, records[0].clone()).unwrap();
    let reference = DiagnosticEvidenceRef::from_export(&source, &digest(id.as_bytes()), &row, 0).unwrap();
    Original { source, row, reference }
}
fn pair(case: Case) -> (TransactionMetadataAnswer, Original, Original) {
    let input = test_support::transaction_metadata_original(TransactionMetadataWait::Subtransaction);
    let [(source, rows), (after_source, after_rows)] =
        test_support::transaction_metadata_slru_pair(&input.focus, &input.execution, case);
    let before = original(source, rows, "before");
    let after = original(after_source, after_rows, "after");
    let answer = TransactionMetadataAnswer::from_originals(
        &input.focus,
        &input.source,
        &input.original,
        &after.source,
        &after.reference.capture_ref(),
        &[(&after.reference, &after.row)],
    )
    .unwrap();
    (answer, before, after)
}
fn inputs<'a>(before: &'a Original, after: &'a Original) -> [SlruOriginal<'a>; 2] {
    [
        SlruOriginal {
            source: &before.source,
            reference: before.reference.clone(),
            record: &before.row.record,
            read_complete: true,
        },
        SlruOriginal {
            source: &after.source,
            reference: after.reference.clone(),
            record: &after.row.record,
            read_complete: true,
        },
    ]
}
fn field<'a>(change: &'a PostgresSlruChange, name: &str) -> &'a PostgresSlruCounterChange {
    let PostgresSlruChange::Observed { counters, .. } = change else { panic!("known interval") };
    counters.iter().find(|counter| counter.native_name == name).unwrap()
}
#[test]
fn same_reset_native_bigints_keep_three_shared_families_exact_clocks_zero_and_partial_source_separate() {
    for case in [Case::Continued, Case::Partial, Case::Zero, Case::LatePoint] {
        let (answer, before, after) = pair(case);
        let report = compare(&answer, &inputs(&before, &after)).unwrap();
        assert_eq!(report.len(), 3);
        for row in &report {
            let PostgresSlruChange::Observed {
                scope,
                before: reference,
                after: current,
                observed_from,
                observed_until,
                before_context,
                after_context,
                ..
            } = row
            else {
                panic!("{case:?}")
            };
            assert_eq!(*scope, MeasurementScope::Server);
            assert_ne!(reference.capture, current.capture);
            assert!(before_context.same_local_database_owner(after_context));
            assert!(ObservationBounds::new(observed_from, observed_until).unwrap().has_positive_duration());
            assert_eq!(
                field(row, "blks_read").change.as_deref(),
                Some(if case == Case::Zero {
                    "0"
                } else if case == Case::LatePoint {
                    "3"
                } else {
                    "2"
                })
            );
            if case == Case::Continued {
                assert_eq!(field(row, "blks_hit").change.as_deref(), Some("7"));
            }
        }
        let PostgresSlruChange::Observed { before_state, .. } = &report[0] else { panic!() };
        assert_eq!(*before_state, if case == Case::Partial { SourceState::Partial } else { SourceState::Collected });
    }
}
#[test]
fn unknown_changed_submicrosecond_and_future_resets_never_borrow_other_slru_family_continuity() {
    for (case, gap) in [
        (Case::UnknownReset, PostgresSlruChangeGap::UnknownReset),
        (Case::ChangedReset, PostgresSlruChangeGap::ResetChanged),
        (Case::ResetAfterPoint, PostgresSlruChangeGap::ResetClock),
    ] {
        let (answer, before, after) = pair(case);
        let changes = compare(&answer, &inputs(&before, &after)).unwrap();
        assert_eq!(changes[0], PostgresSlruChange::Unavailable { kind: PostgresSlruKind::Subtransaction, reason: gap });
        assert!(matches!(changes[1], PostgresSlruChange::Observed { kind: PostgresSlruKind::MultiXactMember, .. }));
        assert!(matches!(changes[2], PostgresSlruChange::Observed { kind: PostgresSlruKind::MultiXactOffset, .. }));
    }
}
#[test]
fn missing_or_decreased_counter_is_not_zero_or_an_inferred_reset_and_other_fields_stay_observed() {
    for case in [Case::MissingCounter, Case::Decreased] {
        let (answer, before, after) = pair(case);
        let changes = compare(&answer, &inputs(&before, &after)).unwrap();
        assert_eq!(field(&changes[0], "blks_read").change, None);
        assert_eq!(field(&changes[0], "blks_hit").change.as_deref(), Some("7"));
        assert_eq!(field(&changes[1], "blks_read").change.as_deref(), Some("2"));
    }
}
#[test]
fn foreign_member_restart_role_and_overlapping_clocks_withhold_comparison_without_relabeling_a_request() {
    for (case, gap) in [
        (Case::ForeignMember, PostgresSlruChangeGap::IncompatibleOwner),
        (Case::Restart, PostgresSlruChangeGap::IncompatibleOwner),
        (Case::ChangedRole, PostgresSlruChangeGap::RoleChanged),
        (Case::Overlapping, PostgresSlruChangeGap::UnorderedClocks),
    ] {
        let (answer, before, after) = pair(case);
        let changes = compare(&answer, &inputs(&before, &after)).unwrap();
        assert!(
            changes.iter().all(|change| matches!(change,PostgresSlruChange::Unavailable{reason,..} if *reason==gap))
        );
    }
}
#[test]
fn one_point_ambiguous_clock_and_unread_body_do_not_invent_an_interval_or_choose_a_convenient_reset() {
    let (answer, before, after) = pair(Case::Continued);
    let mut selected = inputs(&before, &after);
    assert!(compare(&answer, &selected[1..]).unwrap().iter().all(|change| matches!(
        change,
        PostgresSlruChange::Unavailable { reason: PostgresSlruChangeGap::MissingPoint, .. }
    )));
    selected[0].read_complete = false;
    assert!(compare(&answer, &selected).unwrap().iter().all(|change| matches!(
        change,
        PostgresSlruChange::Unavailable { reason: PostgresSlruChangeGap::UnreadBody, .. }
    )));
    let clone = original(before.source.clone(), vec![before.row.record.clone()], "ambiguous");
    let mut selected = inputs(&before, &after).into_iter().collect::<Vec<_>>();
    selected.push(SlruOriginal {
        source: &clone.source,
        reference: clone.reference.clone(),
        record: &clone.row.record,
        read_complete: true,
    });
    assert!(compare(&answer, &selected).unwrap().iter().all(|change| matches!(
        change,
        PostgresSlruChange::Unavailable { reason: PostgresSlruChangeGap::AmbiguousPoint, .. }
    )));
}
#[test]
fn edited_row_or_source_cannot_borrow_a_canonical_endpoint_reference() {
    let (answer, mut before, after) = pair(Case::Continued);
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::PostgresTransactionMetadata(point),
        ..
    }) = &mut before.row.record
    else {
        panic!()
    };
    point.slrus[0].blocks_read = Some("39".into());
    assert!(compare(&answer, &inputs(&before, &after)).is_err());
}

#[test]
fn serialized_answer_cannot_relabel_its_actual_native_result_context() {
    let (mut answer, before, after) = pair(Case::Continued);
    for context in [&mut answer.basis_context, &mut answer.result_context] {
        let SourcePartition::InstanceLocal { epoch, .. } = &mut context.partition else { panic!() };
        *epoch = Some("forged-native-process".into());
    }
    assert!(answer.is_valid(), "structural summary alone is not actual original evidence");
    assert!(compare(&answer, &inputs(&before, &after)).is_err());
}
