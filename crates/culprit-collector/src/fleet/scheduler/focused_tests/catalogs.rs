use super::*;

fn catalog_inventory() -> FleetInventory {
    let mut inventory = single_target(900);
    let mut settings = inventory.targets[0].schedules[0].clone();
    settings.id = "settings".into();
    settings.every_seconds = 600;
    settings.collection = ScheduledCollection::Snapshot { dataset: SnapshotDataset::Settings };
    inventory.targets[0].schedules.push(settings);
    inventory
}

fn settings(scheduler: &Scheduler) -> &ScheduledJob {
    scheduler.jobs.iter().find(|pending| pending.job.schedule.id == "settings").unwrap()
}

fn is_settings(work: &Work) -> bool {
    matches!(work, Work::Scheduled(job) if job.schedule.id == "settings")
}

fn restored(inventory: &FleetInventory, state: ScheduleState, elapsed: i64) -> Scheduler {
    let mut scheduler = Scheduler::new(inventory, "one").unwrap();
    scheduler.restore(state, Duration::ZERO, wall(elapsed)).unwrap();
    scheduler
}

#[test]
fn admitted_focus_runs_after_activity_before_settings_without_renewing_its_deadline_or_turn() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    let original = serde_json::to_value(&state.jobs[&settings(&scheduler).job.key]).unwrap();
    let catalog_turn = settings(&scheduler).turn;
    let focus = job(&inventory, "adverse-activity", "page-target");
    enqueue(&mut scheduler, &mut state, focus.clone(), 0);
    assert!(scheduler.take(seconds(59)).unwrap().is_none());
    assert_eq!(scheduler.wait(seconds(59)), seconds(1));
    assert!(matches!(reserve(&mut scheduler, &mut state, 60), Work::Focused(actual) if actual == focus));
    assert_eq!(settings(&scheduler).turn, catalog_turn);
    assert_eq!(settings(&scheduler).protected_deadline, seconds(300));
    assert_eq!(serde_json::to_value(&state.jobs[&settings(&scheduler).job.key]).unwrap(), original);
    finish(&mut scheduler, &mut state, 60, Outcome::FocusedComplete { result: result("focus-answer") });
    assert!(scheduler.take(seconds(119)).unwrap().is_none());
    assert!(is_settings(&reserve(&mut scheduler, &mut state, 120)));
    finish(&mut scheduler, &mut state, 120, Outcome::Complete);
    assert_eq!(settings(&scheduler).due, seconds(720));
    assert_eq!(settings(&scheduler).protected_deadline, seconds(1_020));
}

#[test]
fn successive_focused_episodes_cannot_starve_the_same_due_catalog() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    for index in 0..4 {
        enqueue(&mut scheduler, &mut state, job(&inventory, &format!("episode-{index}"), "page-target"), 0);
    }
    for at in [60, 120, 180] {
        assert!(matches!(reserve(&mut scheduler, &mut state, at), Work::Focused(_)));
        finish(&mut scheduler, &mut state, at, Outcome::FocusedComplete { result: result(&format!("answer-{at}")) });
        assert_eq!(settings(&scheduler).protected_deadline, seconds(300));
    }
    assert!(is_settings(&reserve(&mut scheduler, &mut state, 240)));
    assert!(seconds(240) <= settings(&scheduler).protected_deadline);
    finish(&mut scheduler, &mut state, 240, Outcome::Complete);
    assert!(matches!(reserve(&mut scheduler, &mut state, 300), Work::Focused(_)));
}

#[test]
fn only_the_named_stable_catalogs_receive_half_period_grace_capped_at_five_minutes() {
    let mut schedule = catalog_inventory().targets.remove(0).schedules.remove(1);
    for dataset in [
        SnapshotDataset::Settings,
        SnapshotDataset::Tables,
        SnapshotDataset::Columns,
        SnapshotDataset::Indexes,
        SnapshotDataset::ForeignKeys,
        SnapshotDataset::CheckConstraints,
        SnapshotDataset::Objects,
        SnapshotDataset::Triggers,
        SnapshotDataset::Partitions,
    ] {
        schedule.collection = ScheduledCollection::Snapshot { dataset };
        for (period, millis) in [(60, 30_000), (599, 299_500), (600, 300_000), (1_200, 300_000)] {
            schedule.every_seconds = period;
            assert_eq!(catalog::grace(&schedule), Duration::from_millis(millis));
        }
    }
    for dataset in [
        SnapshotDataset::Operational,
        SnapshotDataset::WaitCounters,
        SnapshotDataset::Statistics,
        SnapshotDataset::MissingIndexes,
        SnapshotDataset::CurrentRequestWork,
        SnapshotDataset::RetainedPlans,
    ] {
        schedule.collection = ScheduledCollection::Snapshot { dataset };
        assert_eq!(catalog::grace(&schedule), Duration::ZERO);
    }
}

#[test]
fn due_activity_history_and_live_snapshots_remain_hard_even_beside_a_stable_catalog() {
    for collection in [
        ScheduledCollection::Activity { policy: ActivityPolicy::default() },
        ScheduledCollection::QueryHistory { lookback_seconds: 3_600, reconcile_seconds: 3_600 },
        ScheduledCollection::ResourceHistory { window_seconds: 300, overlap_seconds: 30 },
        ScheduledCollection::DeadlockHistory { window_seconds: 300, overlap_seconds: 30 },
        ScheduledCollection::Snapshot { dataset: SnapshotDataset::Operational },
        ScheduledCollection::Snapshot { dataset: SnapshotDataset::WaitCounters },
    ] {
        let mut inventory = catalog_inventory();
        let hard = &mut inventory.targets[0].schedules[0];
        hard.every_seconds = 60;
        hard.collection = collection;
        assert_eq!(catalog::grace(hard), Duration::ZERO);
        let (mut scheduler, mut state) = initialize(&inventory);
        baseline(&mut scheduler, &mut state, 0);
        enqueue(&mut scheduler, &mut state, job(&inventory, "must-wait", "page-target"), 0);
        assert!(matches!(reserve(&mut scheduler, &mut state, 60), Work::Scheduled(_)));
        assert!(state.focused.values().all(|pending| pending.status == FocusStatus::Pending && pending.attempts == 0));
    }
}

#[test]
fn catalog_grace_keeps_exact_independent_resource_serial_fit_without_adding_its_cooldown() {
    for other_period in [71, 80] {
        let mut inventory = catalog_inventory();
        let mut other = inventory.targets[0].clone();
        other.id = "independent".into();
        other.connection = "independent".into();
        other.admission_group = "other".into();
        other.schedules.truncate(1);
        other.schedules[0].every_seconds = other_period;
        inventory.targets.push(other);
        let (mut scheduler, mut state) = initialize(&inventory);
        baseline(&mut scheduler, &mut state, 0);
        baseline(&mut scheduler, &mut state, 0);
        enqueue(&mut scheduler, &mut state, job(&inventory, "serial-fit", "page-target"), 0);
        let work = reserve(&mut scheduler, &mut state, 60);
        assert_eq!(matches!(work, Work::Focused(_)), other_period == 80);
        if other_period == 71 {
            assert!(is_settings(&work));
        }
    }
}

#[test]
fn a_denied_focused_route_does_not_reserve_native_capacity_or_spin_on_catalog_grace() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    let focus = job(&inventory, "denied", "page-target");
    enqueue(&mut scheduler, &mut state, focus.clone(), 0);
    let before = serde_json::to_value(&state.focused[&focus.key]).unwrap();
    assert_eq!(scheduler.wait_allowed(seconds(60), |_| false), Duration::ZERO);
    assert!(is_settings(&scheduler.take_allowed(seconds(60), |_| false).unwrap().unwrap()));
    let update = scheduler.admission_checkpoint(seconds(60), wall(60)).unwrap();
    persist(&mut scheduler, &mut state, update);
    finish(&mut scheduler, &mut state, 60, Outcome::Complete);
    let sequence = scheduler.sequence;
    assert!(scheduler.take_allowed(seconds(120), |_| false).unwrap().is_none());
    assert_eq!(scheduler.wait_allowed(seconds(120), |_| false), seconds(540));
    assert_eq!(scheduler.sequence, sequence);
    assert_eq!(serde_json::to_value(&state.focused[&focus.key]).unwrap(), before);
    assert_eq!(scheduler.focused[&focus.key].source_work, SourceWorkState::NotStarted);
}

#[test]
fn restart_preserves_near_and_expired_catalog_deadlines_instead_of_resetting_grace() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    let focus = job(&inventory, "restart", "page-target");
    enqueue(&mut scheduler, &mut state, focus.clone(), 0);
    let boundary = seconds(300) - minimum_collection_slot(&focus.budget);
    for (elapsed, fits) in [
        (boundary.as_secs() - 1, true),
        (boundary.as_secs(), true),
        (boundary.as_secs() + 1, false),
        (300, false),
        (3_600, false),
    ] {
        let mut resumed = restored(&inventory, state.clone(), elapsed as i64);
        assert_eq!(settings(&resumed).protected_deadline, seconds(300).saturating_sub(seconds(elapsed)));
        let work = resumed.take(Duration::ZERO).unwrap().unwrap();
        assert_eq!(matches!(work, Work::Focused(_)), fits);
        if !fits {
            assert!(is_settings(&work));
        }
    }
}

#[test]
fn wall_rollback_cannot_replenish_catalog_grace_or_earn_native_resource_cooldown() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    enqueue(&mut scheduler, &mut state, job(&inventory, "initial", "page-target"), 0);
    let mut rollback = restored(&inventory, state.clone(), -1);
    assert_eq!(settings(&rollback).protected_deadline, Duration::ZERO);
    assert!(rollback.take(seconds(59)).unwrap().is_none());
    assert_eq!(rollback.wait(seconds(59)), seconds(1));
    assert!(is_settings(&rollback.take(seconds(60)).unwrap().unwrap()));
    enqueue(&mut scheduler, &mut state, job(&inventory, "later", "another-recipe"), 280);
    let mut near = restored(&inventory, state, 279);
    assert_eq!(settings(&near).protected_deadline, settings(&near).due);
    assert!(is_settings(&near.take(Duration::ZERO).unwrap().unwrap()));
}

#[test]
fn required_deadline_rejects_legacy_missing_or_forged_grace_and_clock_anchors() {
    let inventory = catalog_inventory();
    let (mut scheduler, mut state) = initialize(&inventory);
    baseline(&mut scheduler, &mut state, 0);
    let catalog_key = settings(&scheduler).job.key.clone();
    let saved = serde_json::to_value(&state.jobs[&catalog_key]).unwrap();
    let mut missing = saved.clone();
    missing.as_object_mut().unwrap().remove("protected_deadline");
    assert!(serde_json::from_value::<JobState>(missing).is_err());
    for defect in 0..3 {
        let mut bad = state.clone();
        let mut row = saved.clone();
        match defect {
            0 => row["protected_deadline"]["remaining_ms"] = serde_json::json!(300_001),
            1 => row["protected_deadline"]["observed_at"] = serde_json::json!(wall(1)),
            _ => {
                let key = bad.jobs.keys().find(|key| **key != catalog_key).unwrap().clone();
                let mut hard = serde_json::to_value(&bad.jobs[&key]).unwrap();
                hard["protected_deadline"]["remaining_ms"] = serde_json::json!(1);
                bad.jobs.insert(key, serde_json::from_value(hard).unwrap());
            }
        }
        bad.jobs.insert(catalog_key.clone(), serde_json::from_value(row).unwrap());
        let mut resumed = Scheduler::new(&inventory, "one").unwrap();
        assert!(resumed.restore(bad, Duration::ZERO, wall(0)).is_err());
    }
}

#[tokio::test]
async fn the_existing_fleet_journal_roundtrip_keeps_the_original_catalog_deadline() {
    use crate::fleet::journal::FleetJournal;
    let directory = tempfile::tempdir().unwrap();
    let inventory = catalog_inventory();
    let path = directory.path().join("fleet");
    let journal = FleetJournal::open(&path, &inventory, "one").await.unwrap();
    let mut scheduler = Scheduler::new(&inventory, "one").unwrap();
    let state = scheduler.initial_state(Duration::ZERO, wall(0)).unwrap();
    scheduler.persisted(journal.initialize_scheduler(state).await.unwrap()).unwrap();
    assert!(matches!(scheduler.take(Duration::ZERO).unwrap(), Some(Work::Scheduled(_))));
    let update = scheduler.admission_checkpoint(Duration::ZERO, wall(0)).unwrap();
    scheduler.persisted(journal.update_scheduler(update).await.unwrap()).unwrap();
    scheduler.finish(Duration::ZERO, Outcome::Complete).unwrap();
    let update = scheduler.completion_checkpoint(Duration::ZERO, wall(0)).unwrap();
    scheduler.persisted(journal.update_scheduler(update).await.unwrap()).unwrap();
    let key = settings(&scheduler).job.key.clone();
    let initial = serde_json::to_value(&journal.scheduler().await.unwrap().unwrap().jobs[&key]).unwrap();
    drop(scheduler);
    drop(journal);
    let reopened = FleetJournal::open(&path, &inventory, "one").await.unwrap();
    let saved = reopened.scheduler().await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(&saved.jobs[&key]).unwrap(), initial);
    assert_eq!(initial["protected_deadline"]["remaining_ms"], serde_json::json!(300_000));
    let focus = job(&inventory, "fit-only-no-native-admission", "page-target");
    let boundary = seconds(300) - minimum_collection_slot(&focus.budget);
    for (elapsed, fits) in [(boundary.as_secs(), true), (boundary.as_secs() + 1, false), (301, false)] {
        let resumed = restored(&inventory, saved.clone(), elapsed as i64);
        assert_eq!(resumed.periodic_deadlines().permits(&focus, Duration::ZERO), fits);
    }
}
