use super::*;
use crate::delivery::diagnostic_recovery::authenticate_original_history;
use crate::delivery::file_store::FileOwnership;
use crate::delivery::file_store::FileRecoveryStore;
use crate::delivery::recovery::RecoveryStore;
use culprit_evidence::delivery::contract::Contract;
use culprit_evidence::delivery::originals::original_revision;
use culprit_evidence::delivery::recovery::CommittedRange;
use culprit_evidence::delivery::recovery::RecoveryRead;
use culprit_read::cancellation::CollectionCancellation;
use std::collections::BTreeSet;

const GROUPS: u32 = 64;
const WINDOWS: u32 = 65;
fn frame(window: u32, groups: u32) -> Frame {
    let (mut source, rows) = diagnostic_original_evidence(Case::HistoryException);
    let from = chrono::DateTime::parse_from_rfc3339("2020-01-01T06:00:00Z").unwrap()
        + chrono::Duration::hours(i64::from(window));
    let until = from + chrono::Duration::hours(1);
    let SourceObservation::QueryHistory(history) = &mut source.observation else { panic!() };
    history.interval.native_id = (window + 1).to_string();
    history.interval.from = from.to_rfc3339();
    history.interval.until = until.to_rfc3339();
    let owner = history.acquisition_owner.as_mut().unwrap();
    owner.observed_from = (until + chrono::Duration::seconds(1)).to_rfc3339();
    owner.observed_until = (until + chrono::Duration::seconds(2)).to_rfc3339();
    let mut query = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(row) => Some(row.clone()),
            _ => None,
        })
        .unwrap();
    query.id = query.identity.definition_id(&source).unwrap();
    let plan = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::PlanDefinition(row) => Some(row.clone()),
            _ => None,
        })
        .unwrap();
    let runtime = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::QueryRuntime(row) => Some(row.clone()),
            _ => None,
        })
        .unwrap();
    let mut records = vec![TelemetryRecord::QueryDefinition(query.clone())];
    for number in 0..groups {
        let mut plan = plan.clone();
        plan.identity.plan_id = (number + 500).to_string();
        plan.id = plan.identity.definition_id(&source).unwrap();
        plan.query_definition_id = Some(query.id.clone());
        let mut runtime = runtime.clone();
        runtime.key.native.id = (number + 1000).to_string();
        runtime.key.native.qualifiers.insert("plan_id".into(), plan.identity.plan_id.clone());
        runtime.key.references.query.as_mut().unwrap().definition_id = Some(query.id.clone());
        let reference = runtime.key.references.plan.as_mut().unwrap();
        reference.native = plan.identity.native();
        reference.definition_id = Some(plan.id.clone());
        runtime.first_execution_at = Some((from + chrono::Duration::minutes(1)).to_rfc3339());
        runtime.last_execution_at = Some((until - chrono::Duration::minutes(1)).to_rfc3339());
        records.extend([TelemetryRecord::PlanDefinition(plan), TelemetryRecord::QueryRuntime(runtime)]);
    }
    assert!(source.partition_is_consistent());
    assert!(records.iter().all(TelemetryRecord::is_valid));
    Frame { source, records }
}
fn budget() -> CollectionBudget {
    CollectionBudget {
        max_queries: 2,
        max_pages: 8,
        max_snapshot_rows: 256,
        page_rows: 256,
        page_bytes: 512 * 1024,
        total_bytes: 4 * 1024 * 1024,
        ..Default::default()
    }
}
struct History {
    archive: tempfile::TempDir,
    store: FileRecoveryStore,
    owner: FileOwnership,
    contract: Contract,
    harness: Harness,
}
impl History {
    async fn new() -> Self {
        let archive = tempfile::tempdir().unwrap();
        let contract = Contract::new(
            "pipeline".into(),
            "shard".into(),
            "file:original-history".into(),
            digest(b"original-history"),
        )
        .unwrap();
        let store = FileRecoveryStore::create(&archive.path().join("recovery"), contract.clone()).unwrap();
        let owner = store.acquire().await.unwrap();
        let mut harness = Harness::with_recovery_store(Some(store.id())).await;
        harness.outbox.begin_remote_epoch(owner.fence(), 0).await.unwrap();
        // Recovered/observed history without a current saved route gets no new native question. This control
        // retires terminal informational groups; genuinely owed/admitted questions remain bounded working data.
        harness.policies = DiagnosticPolicies::for_activity(
            "another-current-route",
            CollectionSubject::Database,
            ActivityPolicy::default(),
        )
        .unwrap();
        Self { archive, store, owner, contract, harness }
    }
    async fn archive(&self, capture: &CaptureIdentity) -> CommittedRange {
        let range = super::super::recovery::archive_capture(
            &self.harness.outbox,
            &self.store,
            self.owner.fence(),
            &self.contract,
            capture,
        )
        .await;
        self.harness.outbox.delivery().mark_archived(&range).await.unwrap();
        range
    }
    async fn process(&mut self, frame: Frame) -> (CommittedRange, CommittedRange, Vec<DiagnosticRevision>) {
        let native = self.harness.capture_with_budget(frame, budget()).await;
        let source = self.archive(&native).await;
        let before = self.harness.calls.counts();
        assert_eq!(
            analyze_pending(
                &mut self.harness.outbox,
                &self.harness.policies,
                &CollectionCancellation::default(),
                1,
                Some(&self.store)
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(before, self.harness.calls.counts());
        let revisions = self.harness.revisions(&native).await;
        assert!(self.harness.checkpoint().await.episodes.is_empty());
        let capture = self.harness.outbox.status(20,None).await.unwrap().entries.into_iter().find(|entry|
            matches!(&entry.request.selection,CollectionSelection::Diagnostics{parent,..} if parent.capture==native)
        ).unwrap().capture;
        let derived = self.archive(&capture).await;
        (source, derived, revisions)
    }
}
#[tokio::test]
async fn more_than_4096_groups_across_intervals_archive_restart_and_reconcile_the_old_native_group() {
    assert!(GROUPS as usize * WINDOWS as usize > MAX_DIAGNOSTIC_EPISODES);
    let mut fixture = History::new().await;
    let mut prior = None;
    let mut all = BTreeSet::new();
    let mut original = None;
    for interval in 0..WINDOWS {
        let (source, derived, revisions) = fixture.process(frame(interval, GROUPS)).await;
        assert_eq!(revisions.len(), GROUPS as usize);
        for revision in revisions {
            assert!(all.insert(revision.episode.original_group_key().unwrap().unwrap()));
            assert_eq!(revision.episode.lifecycle, DiagnosticLifecycle::Uncertain);
            assert!(revision.episode.recovery.is_none());
            if original.is_none() {
                original = Some(revision.episode)
            }
        }
        if let Some((source, derived)) = prior.take() {
            assert!(fixture.harness.outbox.delivery().evict(&source).await.unwrap());
            assert!(fixture.harness.outbox.delivery().evict(&derived).await.unwrap());
        }
        prior = Some((source, derived));
    }
    assert_eq!(all.len(), GROUPS as usize * WINDOWS as usize);
    let original = original.unwrap();
    let key = original.original_group_key().unwrap().unwrap();
    let pointer = fixture.store.diagnostic_original_group(&key).await.unwrap().unwrap();
    let verified = authenticate_original_history(&fixture.store, &key, &pointer).await.unwrap();
    assert_eq!(original_revision(&verified.record().record).unwrap().episode.first, original.first);
    let History { archive, store, owner, contract, mut harness } = fixture;
    drop(harness.outbox);
    drop(owner);
    let owner = store.acquire().await.unwrap();
    harness.outbox = Outbox::open(&harness.path).unwrap();
    assert_eq!(harness.outbox.recover().await.unwrap().source_debts, 0);
    let high = store.recover(None, 1).await.unwrap().high_sequence;
    harness.outbox.begin_remote_epoch(owner.fence(), high).await.unwrap();
    let mut fixture = History { archive, store, owner, contract, harness };
    let mut repeated = frame(0, GROUPS);
    let SourceObservation::QueryHistory(history) = &mut repeated.source.observation else { panic!() };
    let owner = history.acquisition_owner.as_mut().unwrap();
    owner.observed_from = "2020-01-05T12:00:00Z".into();
    owner.observed_until = "2020-01-05T12:00:01Z".into();
    let (_, _, replayed) = fixture.process(repeated).await;
    let replayed = replayed.iter().find(|revision| revision.episode.id == original.id).unwrap();
    assert_eq!(replayed.episode.first, original.first);
    assert_eq!(replayed.episode.revision, "2");
    assert_eq!(replayed.episode.analysis, original.analysis);
    assert_eq!(replayed.episode.issue, original.issue);
    assert_eq!(replayed.episode.source_read, original.source_read);
    assert!(replayed.episode.recovery.is_none());
    assert_eq!(fixture.harness.calls.counts(), [66, 66, 66, 66]);
}
#[tokio::test]
async fn unavailable_missing_tampered_or_partial_history_defers_without_new_episode_or_native_reopen() {
    for fault in ["unavailable", "missing-hint", "tampered-hint", "partial-body"] {
        let mut fixture = History::new().await;
        let (_, original, revisions) = fixture.process(frame(0, 1)).await;
        let key = revisions[0].episode.original_group_key().unwrap().unwrap();
        let pointer = fixture.store.diagnostic_original_group(&key).await.unwrap().unwrap();
        let hash = digest(&pointer.bytes().unwrap());
        let path = fixture.archive.path().join("recovery/diagnostic-originals").join(format!("{hash}.json"));
        match fault {
            "missing-hint" => std::fs::remove_file(path).unwrap(),
            "tampered-hint" => std::fs::write(path, b"{}").unwrap(),
            "partial-body" => {
                let body = original
                    .manifest()
                    .batches
                    .iter()
                    .find(|batch| batch.stream == TelemetryStream::OperationalSamples)
                    .unwrap();
                std::fs::write(fixture.archive.path().join("recovery").join(&body.object), b"{\n").unwrap();
            }
            _ => (),
        }
        let mut repeated = frame(0, 1);
        source_at(&mut repeated.source, 6);
        let input = fixture.harness.capture_with_budget(repeated, budget()).await;
        let before = fixture.harness.calls.counts();
        let status = fixture.harness.outbox.status(20, None).await.unwrap();
        let recovery = if fault == "unavailable" { None } else { Some(&fixture.store as &dyn RecoveryStore) };
        assert!(
            analyze_pending(
                &mut fixture.harness.outbox,
                &fixture.harness.policies,
                &CollectionCancellation::default(),
                1,
                recovery
            )
            .await
            .is_err(),
            "{fault}"
        );
        assert_eq!(fixture.harness.calls.counts(), before);
        let after = fixture.harness.outbox.status(20, None).await.unwrap();
        assert_eq!(after.captures, status.captures);
        assert!(!after.entries.iter().any(|entry| matches!(&entry.request.selection,
            CollectionSelection::Diagnostics{parent,..} if parent.capture==input)));
        assert_eq!(
            fixture
                .harness
                .outbox
                .next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION)
                .await
                .unwrap()
                .unwrap()
                .parent
                .capture,
            input
        );
    }
}
#[test]
fn canonical_answered_question_is_unchanged_after_reobserving_its_original_without_new_question_or_recovery() {
    use culprit_evidence::analytical::test_support::{
        QueryCompilationFollowupCase, query_compilation_followup_evidence, query_compilation_followup_original,
    };
    let basis = query_compilation_followup_original();
    let (_, rows) = query_compilation_followup_evidence(QueryCompilationFollowupCase::Exact);
    let terminal = rows.iter().find_map(original_revision).unwrap().episode.clone();
    assert!(matches!(&terminal.source_read.as_ref().unwrap().state, SourceReadStageState::Answered { .. }));
    assert!(!terminal.retained_in_checkpoint());
    let mut source = basis.source;
    source_at(&mut source, 6);
    let capture = CaptureIdentity {
        observation_id: "later-original-answer-check".into(),
        revision: terminal.first.capture.revision.clone(),
    };
    let row = ExportedRecord::new(&source, &capture, basis.original.record).unwrap();
    let reference = DiagnosticEvidenceRef::from_export(&source, &digest(b"later-original"), &row, 0).unwrap();
    let result = observe_diagnostic_original(
        &source,
        &reference.capture_ref(),
        &[DiagnosticOriginalRow { reference: &reference, record: &row.record }],
        None,
        &[terminal.clone()],
        "2020-01-01T12:00:08Z",
    )
    .unwrap();
    assert!(result.checkpoint.episodes.is_empty());
    let [revision] = result.revisions.as_slice() else { panic!() };
    assert_eq!(revision.episode.id, terminal.id);
    assert_eq!(revision.episode.first, terminal.first);
    assert_eq!(revision.episode.source_read, terminal.source_read);
    assert_eq!(revision.episode.analysis, terminal.analysis);
    assert_eq!(revision.episode.issue, terminal.issue);
    assert!(revision.episode.recovery.is_none());
    assert!(result.focuses.is_empty());
}
#[test]
fn an_owed_original_question_stays_working_and_cannot_be_installed_as_retired_history() {
    use culprit_evidence::analytical::test_support::query_compilation_followup_original;
    let basis = query_compilation_followup_original();
    let owed = basis
        .episode
        .enroll_source_read(basis.focus)
        .unwrap()
        .transition_source_read(SourceReadStageState::Collecting)
        .unwrap();
    assert!(owed.retained_in_checkpoint());
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: basis.source.target.clone(),
        input: owed.first.capture_ref(),
        episodes: vec![owed.clone()],
    };
    assert!(checkpoint.is_valid());
    let mut source = basis.source;
    source_at(&mut source, 6);
    let capture = CaptureIdentity {
        observation_id: "owed-original-next-point".into(),
        revision: owed.first.capture.revision.clone(),
    };
    let row = ExportedRecord::new(&source, &capture, basis.original.record).unwrap();
    let reference = DiagnosticEvidenceRef::from_export(&source, &digest(b"owed-original-next"), &row, 0).unwrap();
    let rows = [DiagnosticOriginalRow { reference: &reference, record: &row.record }];
    let update = observe_diagnostic_original(
        &source,
        &reference.capture_ref(),
        &rows,
        Some(&checkpoint),
        &[],
        "2020-01-01T12:00:08Z",
    )
    .unwrap();
    assert_eq!(update.checkpoint.episodes.len(), 1);
    assert_eq!(update.checkpoint.episodes[0].source_read, owed.source_read);
    assert!(update.checkpoint.episodes[0].retained_in_checkpoint());
    assert!(
        observe_diagnostic_original(&source, &reference.capture_ref(), &rows, None, &[owed], "2020-01-01T12:00:08Z")
            .is_err()
    );
}
#[tokio::test]
async fn an_index_pointer_to_another_actual_native_group_cannot_supply_first_identity_or_answers() {
    let mut fixture = History::new().await;
    let (_, _, revisions) = fixture.process(frame(0, 2)).await;
    let first = revisions[0].episode.original_group_key().unwrap().unwrap();
    let other = revisions[1].episode.original_group_key().unwrap().unwrap();
    assert_ne!(first, other);
    let mut pointer = fixture.store.diagnostic_original_group(&first).await.unwrap().unwrap();
    pointer.group_key = other.clone();
    assert!(authenticate_original_history(&fixture.store, &other, &pointer).await.is_err());
    assert!(fixture.harness.checkpoint().await.episodes.is_empty());
}
#[tokio::test]
async fn archive_acknowledgement_between_absence_snapshot_and_commit_defers_then_reuses_original_identity() {
    let mut fixture = History::new().await;
    let native = fixture.harness.capture_with_budget(frame(0, 1), budget()).await;
    fixture.archive(&native).await;
    assert_eq!(
        analyze_pending(
            &mut fixture.harness.outbox,
            &fixture.harness.policies,
            &CollectionCancellation::default(),
            1,
            Some(&fixture.store)
        )
        .await
        .unwrap(),
        1
    );
    let original = fixture.harness.revisions(&native).await.remove(0).episode;
    let key = original.original_group_key().unwrap().unwrap();
    let derived = fixture
        .harness
        .outbox
        .status(20, None)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| {
            matches!(&entry.request.selection,
            CollectionSelection::Diagnostics { parent, .. } if parent.capture == native)
        })
        .unwrap()
        .capture;
    let mut repeated = frame(0, 1);
    source_at(&mut repeated.source, 6);
    let capture = fixture.harness.capture_with_budget(repeated, budget()).await;
    let before = fixture.harness.calls.counts();
    let witness = fixture.harness.outbox.original_history_witness().await.unwrap();
    assert!(witness.is_complete());

    // These are separate serialized owner operations in precisely the formerly unsafe order:
    // complete roster observed, archived local hint removed, then the selected key appears absent.
    fixture.archive(&derived).await;
    assert!(fixture.harness.outbox.local_original_group(&key).await.unwrap().is_none());
    let absent = [(key, None)].into_iter().collect();
    let error =
        fixture.harness.outbox.complete_original_history(&capture.observation_id, absent, witness).await.unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Conflict))));
    let pending = fixture.harness.outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    assert_eq!(pending.parent.capture, capture);
    assert!(pending.original_history.is_none());
    assert!(!fixture.harness.outbox.status(20, None).await.unwrap().entries.iter().any(|entry|
        matches!(&entry.request.selection, CollectionSelection::Diagnostics { parent, .. } if parent.capture == capture)));

    // A fresh bounded reconciliation authenticates the archived body; it neither invents a first
    // occurrence nor repeats a native source call or an already terminal question.
    assert_eq!(
        analyze_pending(
            &mut fixture.harness.outbox,
            &fixture.harness.policies,
            &CollectionCancellation::default(),
            1,
            Some(&fixture.store)
        )
        .await
        .unwrap(),
        1
    );
    let replayed = fixture.harness.revisions(&capture).await.remove(0).episode;
    assert_eq!(replayed.id, original.id);
    assert_eq!(replayed.first, original.first);
    assert_eq!(replayed.source_read, original.source_read);
    assert_eq!(replayed.analysis, original.analysis);
    assert_eq!(replayed.issue, original.issue);
    assert!(replayed.recovery.is_none());
    assert_eq!(fixture.harness.calls.counts(), before);
}

#[tokio::test]
async fn a_new_committed_group_invalidates_older_absence_even_while_local_history_is_complete() {
    let mut fixture = History::new().await;
    let native = fixture.harness.capture_with_budget(frame(0, 1), budget()).await;
    let witness = fixture.harness.outbox.original_history_witness().await.unwrap();
    assert!(witness.is_complete());
    assert_eq!(
        analyze_pending(
            &mut fixture.harness.outbox,
            &fixture.harness.policies,
            &CollectionCancellation::default(),
            1,
            Some(&fixture.store)
        )
        .await
        .unwrap(),
        1
    );
    let current = fixture.harness.outbox.original_history_witness().await.unwrap();
    assert!(current.is_complete());
    assert_ne!(current, witness, "the actual new group commit must invalidate prior absence");
    let original = fixture.harness.revisions(&native).await.remove(0).episode;
    let key = original.original_group_key().unwrap().unwrap();
    let mut repeated = frame(0, 1);
    source_at(&mut repeated.source, 6);
    let capture = fixture.harness.capture_with_budget(repeated, budget()).await;
    let before = fixture.harness.calls.counts();
    let absent = [(key, None)].into_iter().collect();
    let error =
        fixture.harness.outbox.complete_original_history(&capture.observation_id, absent, witness).await.unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Conflict))));
    let pending = fixture.harness.outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap();
    assert_eq!(pending.parent.capture, capture);
    assert!(pending.original_history.is_none());
    assert_eq!(fixture.harness.calls.counts(), before);
}

#[tokio::test]
async fn actual_reconciliation_caller_has_bounded_inline_storage_and_starts_no_native_work() {
    let mut harness = Harness::new().await;
    harness.capture_with_budget(frame(0, 1), budget()).await;
    let input = Box::new(harness.outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap());
    let cancellation = CollectionCancellation::default();
    let before = harness.calls.counts();
    let future = crate::diagnostics::original_history::reconcile(&mut harness.outbox, input, None, &cancellation);
    assert!(
        std::mem::size_of_val(&future) <= 4096,
        "the actual owned reconciliation future must fit the standard stack"
    );
    assert!(future.await.unwrap().is_some());
    assert_eq!(harness.calls.counts(), before);
}

#[tokio::test]
async fn only_authenticated_retired_priors_supply_old_first_identity_to_a_new_revision() {
    let mut fixture = History::new().await;
    let (_, _, revisions) = fixture.process(frame(0, 1)).await;
    let original = revisions[0].episode.clone();
    let mut repeated = frame(0, 1);
    source_at(&mut repeated.source, 6);
    let capture = fixture.harness.capture_with_budget(repeated, budget()).await;
    let before = fixture.harness.calls.counts();
    let queued =
        Box::new(fixture.harness.outbox.next_diagnostic_input(DIAGNOSTIC_CONTROLLER_VERSION).await.unwrap().unwrap());
    let input = crate::diagnostics::original_history::reconcile(
        &mut fixture.harness.outbox,
        queued,
        Some(&fixture.store),
        &CollectionCancellation::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(input.checkpoint.as_ref().unwrap().episodes.is_empty());
    assert_eq!(input.historical_episodes.len(), 1);
    assert_eq!(input.historical_episodes[0].first, original.first);
    assert!(
        input.rows.iter().all(|row| row.reference != original.first),
        "the old first reference is admitted by its authenticated prior, not a later capture row"
    );
    let mut update =
        crate::diagnostics::analyze(&input, &ActivityPolicy::default(), &chrono::Utc::now().to_rfc3339()).unwrap();
    crate::diagnostics::planning::qualify_questions(
        &input,
        fixture.harness.policies.assignment(&input).unwrap(),
        &mut update,
    )
    .unwrap();
    crate::diagnostics::planning::settle_original_observations(&input, &mut update).unwrap();

    // A caller cannot manufacture the historical inventory that admission now reuses.
    let mut edited = (*input).clone();
    edited.historical_episodes[0].first = edited.rows[0].reference.clone();
    let mut edited_update = update.clone();
    edited_update.historical_episodes = edited.historical_episodes.clone();
    let captures = fixture.harness.outbox.status(20, None).await.unwrap().captures;
    let error = fixture.harness.outbox.commit_diagnostics(edited, edited_update).await.unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Conflict))));
    assert_eq!(fixture.harness.outbox.status(20, None).await.unwrap().captures, captures);
    fixture.harness.outbox.commit_diagnostics(*input, update).await.unwrap();
    let next = fixture.harness.revisions(&capture).await.remove(0).episode;
    assert_eq!(next.first, original.first);
    assert_eq!(next.id, original.id);
    assert_eq!(next.revision, "2");
    assert_eq!(next.source_read, original.source_read);
    assert_eq!(next.analysis, original.analysis);
    assert_eq!(next.issue, original.issue);
    assert!(next.recovery.is_none());
    assert_eq!(fixture.harness.calls.counts(), before);
}

mod selected_batches;
