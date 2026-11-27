//! A real twenty-second wait sits wholly between two ordinary observations. One intervening Activity
//! observation and its bounded native follow-up can establish the owner while the relationship still exists.
use super::owned_fixture::{Captured, capture_reserved};
use crate::connect::Connector;
use crate::native_test_support::{actor::Actor, owner, specification};
use culprit_collector::{
    diagnostics::{DiagnosticPolicies, analyze_pending},
    outbox::Outbox,
};
use culprit_core::{ConnectionSpec, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{Instant, sleep, sleep_until, timeout};

const BLOCK_DURATION: Duration = Duration::from_secs(20);
const SAMPLE_DELAY: Duration = Duration::from_secs(10);

fn activity() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Activity,
        budget: CollectionBudget::default(),
    }
}

fn observe(capture: &Captured, previous: Option<&DiagnosticCheckpoint>) -> DiagnosticObservationUpdate {
    let references: Vec<_> = (0..capture.records.len()).map(|index| capture.reference(index)).collect();
    let rows: Vec<_> = capture
        .records
        .iter()
        .zip(&references)
        .map(|(record, reference)| {
            let TelemetryRecord::Activity(observation) = &record.record else { panic!("Activity capture") };
            DiagnosticActivityRow { observation, reference }
        })
        .collect();
    let source = capture.source();
    observe_diagnostic_activity(
        source,
        &references[0].capture_ref(),
        &rows,
        previous,
        &ActivityPolicy::default(),
        &source.observation.activity().unwrap().at,
    )
    .unwrap()
}

async fn wait_until_blocked(control: &mut Actor, waiter: i32, holder: i32) -> Instant {
    timeout(Duration::from_secs(4), async {
        loop {
            let row = control
                .one("SELECT $2::integer = ANY(pg_catalog.pg_blocking_pids($1))", &[&waiter, &holder])
                .await
                .unwrap();
            if row.get::<_, bool>(0) {
                return Instant::now();
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the waiter was blocked by the fixture holder")
}

async fn sample_while_blocked(
    source: &ConnectionSpec,
    outbox: &mut Outbox,
    cancellation: &CollectionCancellation,
    before: &DiagnosticCheckpoint,
    waiter: &NativeActivityIdentity,
    holder: &NativeActivityIdentity,
) -> (Captured, Captured) {
    let sample = capture_reserved(outbox, source, activity(), cancellation).await;
    let detected = observe(&sample, Some(before));
    let focus = detected
        .focuses
        .into_iter()
        .find_map(|(_, focus)| match focus {
            DiagnosticFocus::Blocking { focus } if focus.original.key.session == *waiter => Some(focus),
            _ => None,
        })
        .expect("one native wait under the default diagnostic policy");
    assert_eq!(focus.original.waiting_on_lock, Some(true));
    assert!(!focus.original.conditions(&ActivityPolicy::default()).contains(&ActivityCondition::Blocked));
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::FocusedBlocking { focus },
        budget: crate::telemetry::blocking::bounded_budget(&CollectionBudget::default()),
    };
    let focused = capture_reserved(outbox, source, request, cancellation).await;
    let observed_owner = focused.records.iter().any(|record| {
        let TelemetryRecord::Activity(row) = &record.record else { return false };
        row.key.session == *waiter
            && row.wait.iter().flat_map(|wait| &wait.owners).any(|owner| owner.session().as_ref() == Some(holder))
    });
    assert!(observed_owner, "the focused native edge names the original holder and its backend generation");
    (sample, focused)
}

async fn lock_messages(control: &mut Actor, waiter: i32) -> Vec<Value> {
    // The supervisor reads PostgreSQL's own measurement independently of the provider captures.
    let row = control
        .one("SELECT pg_catalog.pg_read_file(pg_catalog.pg_current_logfile('jsonlog'),0,1048576)", &[])
        .await
        .unwrap();
    let log: String = row.get(0);
    let (complete, _) = log.rsplit_once('\n').unwrap_or(("", ""));
    complete
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|row| row["pid"].as_i64() == Some(i64::from(waiter)))
        .collect()
}

async fn verify_native_wait_log(control: &mut Actor, waiter: i32, holder: &str) {
    let messages = timeout(Duration::from_secs(4), async {
        loop {
            let messages = lock_messages(control, waiter).await;
            if messages.iter().any(|row| row["message"].as_str().is_some_and(|text| text.contains(" acquired "))) {
                return messages;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the logging collector persisted completion");
    let waiting = messages
        .iter()
        .find(|row| row["message"].as_str().is_some_and(|s| s.contains(" still waiting for ")))
        .expect("native waiting record");
    assert!(waiting["detail"].as_str().unwrap().contains(&format!("Process holding the lock: {holder}.")));
    let acquired = messages
        .iter()
        .find_map(|row| {
            let text = row["message"].as_str()?;
            text.contains(" acquired ")
                .then_some(text)?
                .rsplit_once(" after ")?
                .1
                .strip_suffix(" ms")?
                .parse::<f64>()
                .ok()
        })
        .expect("native completed duration");
    assert!((20_000.0..30_000.0).contains(&acquired), "native measured wait {acquired} ms");
}

#[tokio::test]
#[ignore = "requires the owned PostgreSQL short-blocker jsonlog fixture"]
async fn a_twenty_second_wait_between_ordinary_polls_retains_the_sampled_owner_and_native_clock() {
    let cancellation = CollectionCancellation::default();
    let spec = specification("culprit_smoke", CollectionBudget::default()).await;
    let connector = Connector::new(&spec).unwrap();
    let marker = format!("culprit_owned_{}", owner());
    let actor = |label| Actor::open(&connector, "culprit_smoke", &marker, label, &cancellation);
    let mut holder = actor("fixture-short-holder").await.unwrap();
    let mut waiter = actor("fixture-short-waiter").await.unwrap();
    let mut control = actor("fixture-short-control").await.unwrap();
    waiter.execute("SET log_lock_waits=on; SET deadlock_timeout='250ms'").await.unwrap();
    let holder_pid: i32 = holder.identity.id.parse().unwrap();
    let gate = i64::from(holder_pid);
    let waiter_pid = waiter.identity.id.parse().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("outbox");
    let mut outbox = Outbox::create(&path, OutboxLimits::default()).unwrap();
    let before = capture_reserved(&mut outbox, &spec, activity(), &cancellation).await;
    let baseline = observe(&before, None).checkpoint;
    holder.hold_gate(gate).await.unwrap();
    waiter.wait_on_gate(gate).unwrap();
    let began = wait_until_blocked(&mut control, waiter_pid, holder_pid).await;
    sleep_until(began + SAMPLE_DELAY).await;
    let (sample, focused) =
        sample_while_blocked(&spec, &mut outbox, &cancellation, &baseline, &waiter.identity, &holder.identity).await;
    assert!(began.elapsed() < BLOCK_DURATION, "sample and bounded follow-up must both fit inside the wait");
    sleep_until(began + BLOCK_DURATION).await;
    holder.execute("SELECT pg_catalog.pg_advisory_unlock_all()").await.unwrap();
    waiter.settle().await.unwrap();
    let after = capture_reserved(&mut outbox, &spec, activity(), &cancellation).await;
    let is_wait = |episode: &DiagnosticEpisode| matches!(&episode.signal,DiagnosticSignal::LockWait { key } if key.session == waiter.identity);
    assert!(
        !observe(&after, Some(&baseline)).checkpoint.episodes.iter().any(is_wait),
        "the two ordinary samples contain no positive wait"
    );

    verify_native_wait_log(&mut control, waiter_pid, &holder.identity.id).await;

    let policy =
        DiagnosticPolicies::for_activity("owned-native-parent", CollectionSubject::Database, ActivityPolicy::default())
            .unwrap();
    assert_eq!(analyze_pending(&mut outbox, &policy, &cancellation, 3, None).await.unwrap(), 3);
    drop(outbox);
    let mut restored = Outbox::open(&path).unwrap();
    let checkpoints = restored.diagnostic_checkpoints(DIAGNOSTIC_CONTROLLER_VERSION, None, 16).await.unwrap();
    let episode = checkpoints
        .checkpoints
        .iter()
        .flat_map(|checkpoint| &checkpoint.episodes)
        .find(|episode| is_wait(episode))
        .expect("durable diagnostic episode");
    assert_eq!(episode.first.manifest_sha256, sample.manifest_sha256);
    assert_eq!(analyze_pending(&mut restored, &policy, &cancellation, 16, None).await.unwrap(), 1);
    for original in [&before, &sample, &focused, &after] {
        restored.verify(&original.manifest.capture.observation_id).await.unwrap();
    }
    let retired = [holder.retire().await, waiter.retire().await, control.retire().await];
    assert!(retired.into_iter().all(|settled| settled));
}
