use super::*;
use crate::outbox::artifacts;
use crate::outbox::investigations::tests::{activity, baseline};
use culprit_evidence::analytical::test_support::{DiagnosticOriginalCase, diagnostic_original_evidence};

fn original(files: &mut Files, case: DiagnosticOriginalCase) -> DiagnosticInput {
    let (source, records) = diagnostic_original_evidence(case);
    let request = CollectionRequest {
        subject: match &source.target {
            CollectionTarget::Database(_) => CollectionSubject::Database,
            CollectionTarget::Shared(resource) => CollectionSubject::Shared { resource: resource.clone() },
            CollectionTarget::ConfiguredConnection(_) => panic!("an original corpus has a captured native target"),
        },
        selection: CollectionSelection::Snapshot { dataset: source.observation.snapshot().unwrap().dataset },
        budget: CollectionBudget { page_bytes: 512 * 1024, total_bytes: 4 * 1024 * 1024, ..Default::default() },
    };
    assert!(source.matches_request(&request), "the capture subject must retain the original native scope");
    let reservation = files.reserve("fixture", &request).unwrap();
    let identity = reservation.entry.identity.clone();
    files.journal.source_work(&reservation.entry, SourceWorkState::Active).unwrap();
    let mut writer = ObservationFiles::create(&reservation.directory, request, identity.clone(), None).unwrap();
    writer.describe(source).unwrap();
    writer.write_stream(0, &records).unwrap();
    writer.confirm_source(SourceCompletion::Verified).unwrap();
    assert_eq!(writer.finish(1).unwrap().status, CollectionStatus::Complete);
    files.journal.source_work(&reservation.entry, SourceWorkState::Quiescent).unwrap();
    files.finish(reservation).unwrap();
    files.diagnostic_input(&identity.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap()
}

fn observation(input: &DiagnosticInput) -> DiagnosticUpdate {
    let rows = input
        .rows
        .iter()
        .map(|row| DiagnosticOriginalRow { reference: &row.reference, record: &row.row.record })
        .collect::<Vec<_>>();
    let analyzed_at = "2020-01-01T12:01:00Z";
    let update = observe_diagnostic_original(
        &input.source,
        &input.parent,
        &rows,
        input.checkpoint.as_ref(),
        &input.historical_episodes,
        analyzed_at,
    )
    .unwrap();
    DiagnosticUpdate {
        step_id: digest(b"canonical original reuse"),
        analyzed_at: analyzed_at.into(),
        revisions: update.revisions,
        checkpoint: update.checkpoint,
        historical_episodes: input.historical_episodes.clone(),
    }
}

#[test]
fn every_original_report_reuses_one_fresh_canonical_input_and_preserves_the_published_revisions() {
    for (case, reports) in [(DiagnosticOriginalCase::Memory, 2), (DiagnosticOriginalCase::MysqlError, 4)] {
        let root = tempfile::tempdir().unwrap();
        let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
        let input = original(&mut files, case);
        let update = observation(&input);
        assert_eq!(update.revisions.len(), reports);
        assert!(
            update
                .revisions
                .iter()
                .all(|revision| matches!(revision.episode.report, Some(DiagnosticReport::OriginalObservation { .. })))
        );
        for _ in 0..2 {
            let before = artifacts::validation_count();
            files.validate_diagnostic_update(&input, &update).unwrap();
            assert_eq!(
                artifacts::validation_count() - before,
                1,
                "each call verifies EOF once; report count cannot multiply the canonical body pass"
            );
        }
        let expected = update.revisions.clone();
        let capture = files.commit_diagnostics(input.clone(), update.clone()).unwrap();
        assert_eq!(files.validated(&capture.observation_id).unwrap().diagnostic_revisions, expected);
        let mut stale = update;
        stale.step_id = digest(b"another step with the old controller head");
        let error = files.commit_diagnostics(input, stale).unwrap_err();
        assert_eq!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Conflict)));
    }
}

#[test]
fn other_report_references_and_their_input_share_one_canonical_body_with_exact_receipts() {
    let root = tempfile::tempdir().unwrap();
    let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
    let current = baseline(&mut files, vec![activity("42")], true, 0);
    let other = baseline(&mut files, vec![activity("43"), activity("44")], true, 2);
    let current = files.diagnostic_input(&current.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let other = files.diagnostic_input(&other.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let mut originals = CommitOriginals::new(current).unwrap();
    let before = artifacts::validation_count();
    for _ in 0..3 {
        for expected in &other.rows {
            let (source, row) = originals.reference(&mut files, &expected.reference).unwrap();
            assert_eq!(source, other.source);
            assert_eq!(row, expected.row);
        }
        let input = originals.input(&mut files, &other.parent, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
        assert_eq!(input.parent, other.parent);
        assert_eq!(input.rows.len(), other.rows.len());
        assert!(
            input
                .rows
                .iter()
                .zip(&other.rows)
                .all(|(actual, expected)| actual.reference == expected.reference && actual.row == expected.row)
        );
    }
    assert_eq!(artifacts::validation_count() - before, 1);
    assert!(originals.input(&mut files, &other.parent, DIAGNOSTIC_CONTROLLER_VERSION + 1).is_err());
    for change in ["ordinal", "payload", "manifest", "scope", "capture-owner-epoch"] {
        let mut wrong = other.rows[0].reference.clone();
        match change {
            "ordinal" => wrong.row_ordinal += 20,
            "payload" => wrong.payload_sha256 = digest(b"different immutable payload"),
            "manifest" => wrong.manifest_sha256 = digest(b"different manifest"),
            "scope" => wrong.source_scope_id = digest(b"different scope"),
            "capture-owner-epoch" => wrong.capture.revision.as_mut().unwrap().owner_epoch = "2".into(),
            _ => unreachable!(),
        }
        assert!(originals.reference(&mut files, &wrong).is_err(), "{change}");
    }
    assert_eq!(
        artifacts::validation_count() - before,
        1,
        "a known capture receipt cannot be replaced under the same observation id"
    );
}

#[test]
fn a_verified_view_preserves_original_bytes_but_a_new_commit_rejects_same_length_tampering() {
    let root = tempfile::tempdir().unwrap();
    let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
    let capture = baseline(&mut files, vec![activity("42")], true, 0);
    let current = files.diagnostic_input(&capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let expected = current.rows[0].clone();
    let path = files.directory(&capture.observation_id).join("activity.ndjson");
    let mut originals = CommitOriginals::new(current).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    let changed = text.replace("90000", "90001");
    assert_ne!(text, changed);
    assert_eq!(text.len(), changed.len());
    std::fs::write(&path, changed).unwrap();
    let (_, row) = originals.reference(&mut files, &expected.reference).unwrap();
    assert_eq!(row, expected.row, "already verified bytes cannot be replaced after their hash");
    drop(originals);
    assert!(files.diagnostic_input(&capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).is_err());
    assert_eq!(files.journal.capture(&capture.observation_id).unwrap().state, OutboxCaptureState::Quarantined);
}

#[test]
fn uncached_partial_and_missing_original_bodies_refuse_before_any_view_is_admitted() {
    for missing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
        let current = baseline(&mut files, vec![activity("42")], true, 0);
        let other = baseline(&mut files, vec![activity("43")], true, 2);
        let current = files.diagnostic_input(&current.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
        let other = files.diagnostic_input(&other.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
        let path = files.directory(&other.parent.capture.observation_id).join("activity.ndjson");
        if missing {
            std::fs::remove_file(path).unwrap();
        } else {
            std::fs::write(path, b"{\n").unwrap();
        }
        let mut originals = CommitOriginals::new(current).unwrap();
        assert!(originals.reference(&mut files, &other.rows[0].reference).is_err());
        assert_eq!(
            files.journal.capture(&other.parent.capture.observation_id).unwrap().state,
            OutboxCaptureState::Quarantined
        );
    }
}

#[test]
fn a_reused_body_cannot_replace_the_inputs_recorded_scope_or_role() {
    for change in ["database-incarnation", "epoch", "role"] {
        let root = tempfile::tempdir().unwrap();
        let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
        let current = baseline(&mut files, vec![activity("42")], true, 0);
        let other = baseline(&mut files, vec![activity("43")], true, 2);
        let current = files.diagnostic_input(&current.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
        let other = files.diagnostic_input(&other.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
        let mut originals = CommitOriginals::new(current).unwrap();
        originals.reference(&mut files, &other.rows[0].reference).unwrap();
        let mut changed = other.source.clone();
        match change {
            "database-incarnation" => {
                let CollectionTarget::Database(database) = &mut changed.target else { panic!() };
                database.incarnation = Some("a-different-native-database".into());
            }
            "epoch" => {
                let SourceObservation::Activity(activity) = &mut changed.observation else { panic!() };
                activity.epoch = Some("a-different-native-process".into());
            }
            "role" => {
                let SourceObservation::Activity(activity) = &mut changed.observation else { panic!() };
                activity.database_writable = Some(false);
            }
            _ => unreachable!(),
        }
        files
            .journal
            .conn
            .execute(
                "UPDATE diagnostic_inputs SET source_json=?1 WHERE observation_id=?2",
                params![encode(&changed).unwrap(), other.parent.capture.observation_id],
            )
            .unwrap();
        assert!(originals.input(&mut files, &other.parent, DIAGNOSTIC_CONTROLLER_VERSION).is_err(), "{change}");
    }
}

#[test]
fn changing_foreign_captures_releases_the_previous_input_and_returning_revalidates_it() {
    let root = tempfile::tempdir().unwrap();
    let mut files = Files::create(&root.path().join("outbox"), Default::default()).unwrap();
    let current = baseline(&mut files, vec![activity("42")], true, 0);
    let first = baseline(&mut files, vec![activity("43")], true, 2);
    let second = baseline(&mut files, vec![activity("44")], true, 4);
    let current = files.diagnostic_input(&current.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let first = files.diagnostic_input(&first.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let second = files.diagnostic_input(&second.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let mut originals = CommitOriginals::new(current).unwrap();
    let before = artifacts::validation_count();
    let input = originals.input(&mut files, &first.parent, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let previous = Rc::downgrade(&input);
    drop(input);
    assert!(previous.upgrade().is_some(), "the currently selected foreign input is shared");
    originals.reference(&mut files, &second.rows[0].reference).unwrap();
    assert!(previous.upgrade().is_none(), "foreign input bodies cannot accumulate across captures");
    let (_, restored) = originals.reference(&mut files, &first.rows[0].reference).unwrap();
    assert_eq!(restored, first.rows[0].row);
    assert_eq!(artifacts::validation_count() - before, 3, "an evicted foreign body is verified again");
}

#[test]
fn restored_foreign_rows_reuse_their_verified_checksum_and_a_new_scope_refuses_tampering() {
    let root = tempfile::tempdir().unwrap();
    let mut local = Files::create(&root.path().join("local"), Default::default()).unwrap();
    let current = baseline(&mut local, vec![activity("42")], true, 0);
    let other = baseline(&mut local, vec![activity("43")], true, 2);
    let current = local.diagnostic_input(&current.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let other = local.diagnostic_input(&other.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let store = digest(b"controlled-commit-reuse-recovery");
    let mut cold = Files::create_remote(&root.path().join("cold"), Default::default(), &store).unwrap();
    for input in [&current, &other] {
        cold.restore_diagnostic_input(RestoredInput {
            store_id: store.clone(),
            parent: input.parent.clone(),
            source: input.source.clone(),
            investigation: input.investigation.clone(),
            connection: input.connection.clone(),
            rows: input.rows.iter().map(|row| (row.reference.row_ordinal, row.row.clone())).collect(),
        })
        .unwrap();
    }
    let current = cold.diagnostic_input(&current.parent.capture.observation_id, DIAGNOSTIC_CONTROLLER_VERSION).unwrap();
    let mut originals = CommitOriginals::new(current.clone()).unwrap();
    let ((), reads) = super::super::super::input::count_reference_body_reads(|| {
        originals.reference(&mut cold, &other.rows[0].reference)?;
        let input = originals.input(&mut cold, &other.parent, DIAGNOSTIC_CONTROLLER_VERSION)?;
        assert_eq!(input.rows[0].reference, other.rows[0].reference);
        assert_eq!(input.rows[0].row, other.rows[0].row);
        originals.reference(&mut cold, &other.rows[0].reference)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(reads, 1);
    assert!(!cold.directory(&other.parent.capture.observation_id).exists());
    drop(originals);
    let stored: String = cold
        .journal
        .conn
        .query_row(
            "SELECT restored_json FROM diagnostic_inputs WHERE observation_id=?1",
            [&other.parent.capture.observation_id],
            |row| row.get(0),
        )
        .unwrap();
    let changed = stored.replace("90000", "90001");
    assert_ne!(stored, changed);
    assert_eq!(stored.len(), changed.len());
    cold.journal
        .conn
        .execute(
            "UPDATE diagnostic_inputs SET restored_json=?1 WHERE observation_id=?2",
            params![changed, other.parent.capture.observation_id],
        )
        .unwrap();
    let mut fresh = CommitOriginals::new(current).unwrap();
    assert!(fresh.reference(&mut cold, &other.rows[0].reference).is_err());
}
