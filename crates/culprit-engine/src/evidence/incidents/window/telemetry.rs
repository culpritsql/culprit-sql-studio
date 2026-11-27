//! Pure projection of non-query window telemetry captures (resources, waits, activity, deadlocks, replication).
use super::*;
use culprit_core::{
    Deadlock, MeasurementScope, ResetMarker, ResourceSample, WindowActivityPoint, WindowReplicationPoint,
    WindowResourceCapture, WindowTelemetry, WindowWaitDelta, WindowWaitInterval,
    telemetry::{ActivityWaitOwner, SnapshotDataset, SourceObservation, TelemetryCollection, WaitCounterObservation},
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_WAIT_DELTAS_PER_INTERVAL: usize = 32;

pub(super) fn project(
    captures: &[&EvidenceCapture],
    check_cancelled: &dyn Fn() -> Result<()>,
) -> Result<Option<WindowTelemetry>> {
    let mut telemetry = WindowTelemetry::default();
    let mut seen_deadlocks = BTreeSet::new();

    type WaitPoint<'a> =
        (DiagnosticCaptureRef, String, String, bool, Engine, MeasurementScope, Vec<&'a WaitCounterObservation>);
    let mut wait_groups: BTreeMap<String, Vec<WaitPoint<'_>>> = BTreeMap::new();

    for capture in captures {
        check_cancelled()?;
        let source = capture.source();
        let native_clock = WindowNativeClock::from_original(source);

        if capture.families().contains(&EvidenceFamily::ResourceSamples)
            && source.observation.collection() == TelemetryCollection::ResourceHistory
        {
            let mut samples = Vec::new();
            for record in capture.records() {
                if let TelemetryRecord::ResourceSample(measurement) = &record.row().record {
                    samples.push(ResourceSample::from_measurement(measurement.clone()));
                }
            }
            if !samples.is_empty() {
                samples.sort_by(|a, b| a.at.cmp(&b.at));
                telemetry.resources.push(WindowResourceCapture {
                    original: super::reference(capture)?,
                    native_clock: native_clock.clone(),
                    read_complete: capture.is_read_complete(),
                    samples,
                });
            }
        }

        if capture.families().contains(&EvidenceFamily::ActivityObservations) && source.observation.activity().is_some()
        {
            let mut active_sessions = 0u32;
            let mut blocked_sessions = 0u32;
            let mut idle_in_transaction_sessions = 0u32;
            let mut waiting_on_lock_sessions = 0u32;
            let mut max_elapsed_ms: Option<u64> = None;
            let mut max_wait_ms: Option<u64> = None;
            let mut max_transaction_age_ms: Option<u64> = None;

            for record in capture.records() {
                let TelemetryRecord::Activity(obs) = &record.row().record else { continue };
                if !obs.idle {
                    active_sessions = active_sessions.saturating_add(1);
                }
                let has_blocker =
                    obs.wait.as_ref().is_some_and(|wait| wait.owners.iter().any(ActivityWaitOwner::is_blocker));
                if has_blocker || obs.waiting_on_lock == Some(true) {
                    blocked_sessions = blocked_sessions.saturating_add(1);
                }
                if obs.idle
                    && (obs.open_transactions.is_some_and(|count| count > 0) || obs.transaction_age_ms.is_some())
                {
                    idle_in_transaction_sessions = idle_in_transaction_sessions.saturating_add(1);
                }
                if obs.waiting_on_lock == Some(true) {
                    waiting_on_lock_sessions = waiting_on_lock_sessions.saturating_add(1);
                }
                if let Some(ms) = obs.elapsed_ms.as_deref().and_then(|value| value.parse::<u64>().ok()) {
                    max_elapsed_ms = Some(max_elapsed_ms.map_or(ms, |prev| prev.max(ms)));
                }
                if let Some(ms) = obs
                    .wait
                    .as_ref()
                    .and_then(|wait| wait.elapsed_ms.as_deref())
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    max_wait_ms = Some(max_wait_ms.map_or(ms, |prev| prev.max(ms)));
                }
                if let Some(ms) = obs.transaction_age_ms.as_deref().and_then(|value| value.parse::<u64>().ok()) {
                    max_transaction_age_ms = Some(max_transaction_age_ms.map_or(ms, |prev| prev.max(ms)));
                }
            }

            let blocking = if matches!(
                source.observation,
                SourceObservation::FocusedBlocking { .. } | SourceObservation::MetadataLockContext { .. }
            ) {
                crate::collected_resources::blocking(capture).ok().map(|analysis| *analysis.report)
            } else {
                None
            };

            telemetry.activity.push(WindowActivityPoint {
                original: super::reference(capture)?,
                native_clock: native_clock.clone(),
                read_complete: capture.is_read_complete(),
                active_sessions,
                blocked_sessions,
                idle_in_transaction_sessions,
                waiting_on_lock_sessions,
                max_elapsed_ms,
                max_wait_ms,
                max_transaction_age_ms,
                blocking,
            });
        }

        if source.observation.collection() == TelemetryCollection::DeadlockHistory {
            let engine = source.target.engine();
            for record in capture.records() {
                if let TelemetryRecord::Deadlock(event) = &record.row().record {
                    let mut deadlock = Deadlock::new(
                        &source.source,
                        Some(event.at.clone()),
                        event.processes.clone(),
                        event.resources.clone(),
                    );
                    deadlock.pattern = event.pattern;
                    culprit_core::deadlock::classify(engine, &mut deadlock);
                    if seen_deadlocks.insert(deadlock.id.clone()) {
                        telemetry.deadlocks.push(deadlock);
                    }
                }
            }
        }

        if capture.families().contains(&EvidenceFamily::ReplicationSamples)
            && source.observation.snapshot().is_some_and(|snap| snap.dataset == SnapshotDataset::Replication)
            && let Ok(analysis) = crate::collected_resources::replication(capture)
        {
            telemetry.replication.push(WindowReplicationPoint {
                original: super::reference(capture)?,
                native_clock: native_clock.clone(),
                read_complete: capture.is_read_complete(),
                report: analysis.report,
            });
        }

        if capture.families().contains(&EvidenceFamily::WaitCounterSamples)
            && source.observation.snapshot().is_some_and(|snap| snap.dataset == SnapshotDataset::WaitCounters)
            && let WindowNativeClock::Observation { from, until } | WindowNativeClock::Interval { from, until } =
                &native_clock
        {
            let mut rows = Vec::new();
            for record in capture.records() {
                if let TelemetryRecord::WaitCounter(row) = &record.row().record {
                    rows.push(row);
                }
            }
            if !rows.is_empty() {
                let original = super::reference(capture)?;
                let group_key = original.source_scope_id.clone();
                let scope = rows.first().map_or(source.target.measurement_scope(), |row| row.scope);
                wait_groups.entry(group_key).or_default().push((
                    original,
                    from.clone(),
                    until.clone(),
                    capture.is_read_complete(),
                    source.target.engine(),
                    scope,
                    rows,
                ));
            }
        }
    }

    for points in wait_groups.values_mut() {
        check_cancelled()?;
        points.sort_by(|a, b| (&a.1, &a.2, &a.0.manifest_sha256).cmp(&(&b.1, &b.2, &b.0.manifest_sha256)));
        for pair in points.windows(2) {
            let [prev, curr] = pair else { continue };
            if prev.2 > curr.1 || (prev.1 == curr.1 && prev.2 == curr.2) {
                continue;
            }
            let mut baseline_map = BTreeMap::new();
            for row in &prev.6 {
                if row.reset != ResetMarker::Unknown {
                    baseline_map.insert((row.source.as_str(), row.wait_type.as_str()), *row);
                }
            }
            let mut waits = Vec::new();
            let spec = curr.4.spec();
            for row in &curr.6 {
                if row.reset == ResetMarker::Unknown {
                    continue;
                }
                let Some(before) = baseline_map
                    .get(&(row.source.as_str(), row.wait_type.as_str()))
                    .filter(|before| before.scope == row.scope && before.reset == row.reset)
                else {
                    continue;
                };
                let (Some(curr_wait), Some(prev_wait)) =
                    (row.wait_time_ms.parse::<u64>().ok(), before.wait_time_ms.parse::<u64>().ok())
                else {
                    continue;
                };
                let (Some(curr_sig), Some(prev_sig)) =
                    (row.signal_wait_time_ms.parse::<u64>().ok(), before.signal_wait_time_ms.parse::<u64>().ok())
                else {
                    continue;
                };
                let (Some(curr_tasks), Some(prev_tasks)) =
                    (row.waiting_tasks_count.parse::<u64>().ok(), before.waiting_tasks_count.parse::<u64>().ok())
                else {
                    continue;
                };
                if curr_wait < prev_wait || curr_sig < prev_sig || curr_tasks < prev_tasks {
                    continue;
                }
                let wait_ms = (curr_wait - prev_wait) as f64;
                let waiting_tasks = (curr_tasks - prev_tasks) as f64;
                if wait_ms <= 0.0 && waiting_tasks <= 0.0 {
                    continue;
                }
                let category = row
                    .mechanism
                    .map(|mechanism| mechanism.class())
                    .or_else(|| spec.native_wait_class(&row.wait_type))
                    .map_or_else(|| "Other".to_owned(), |class| class.category().to_owned());
                waits.push(WindowWaitDelta {
                    category,
                    wait_type: Some(row.wait_type.clone()),
                    wait_ms,
                    signal_wait_ms: (curr_sig - prev_sig) as f64,
                    waiting_tasks,
                    max_wait_ms: row.max_wait_time_ms.parse::<u64>().ok(),
                });
            }
            if !waits.is_empty() {
                waits.sort_by(|a, b| b.wait_ms.total_cmp(&a.wait_ms));
                waits.truncate(MAX_WAIT_DELTAS_PER_INTERVAL);
                telemetry.wait_intervals.push(WindowWaitInterval {
                    baseline: prev.0.clone(),
                    current: curr.0.clone(),
                    from: prev.2.clone(),
                    until: curr.2.clone(),
                    scope: curr.5,
                    read_complete: prev.3 && curr.3,
                    waits,
                });
            }
        }
    }

    telemetry.resources.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.activity.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.replication.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.deadlocks.sort_by(|a, b| b.at.cmp(&a.at));
    telemetry.wait_intervals.sort_by(|a, b| (&a.from, &a.until).cmp(&(&b.from, &b.until)));

    if telemetry.is_empty() { Ok(None) } else { Ok(Some(telemetry)) }
}

fn clock_sort_key(clock: &WindowNativeClock) -> (&str, &str) {
    match clock {
        WindowNativeClock::Interval { from, until } | WindowNativeClock::Observation { from, until } => (from, until),
        WindowNativeClock::Unknown => ("", ""),
    }
}
