//! Pure projection of non-query window telemetry captures (resources, waits, activity, deadlocks, replication,
//! storage, memory clerks, memory grants, spills, scheduled jobs, and schema catalogs).
use super::*;
use culprit_core::{
    Deadlock, MeasurementScope, ResetMarker, ResourceSample, WindowActivityPoint, WindowJobsPoint,
    WindowMemoryClerkPoint, WindowMemoryGrantKind, WindowMemoryGrantPoint, WindowReplicationPoint,
    WindowResourceCapture, WindowSchemaCatalog, WindowSpillPoint, WindowStoragePoint, WindowTelemetry, WindowWaitDelta,
    WindowWaitInterval,
    telemetry::{
        ActivityWaitOwner, CatalogExpression, DefinitionPayload, MssqlIndexParentKind, MssqlIndexPresentation,
        ObjectDefinitionFact, OperationalFact, SnapshotDataset, SourceObservation, TelemetryCollection,
        WaitCounterObservation,
    },
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
    let mut schema_heads: Vec<(SnapshotDataset, (String, String), &EvidenceCapture)> = Vec::new();

    for capture in captures {
        check_cancelled()?;
        let source = capture.source();
        let native_clock = WindowNativeClock::from_original(source);

        if capture.is_read_complete()
            && let Some(snap) = source.observation.snapshot()
            && matches!(
                snap.dataset,
                SnapshotDataset::Tables
                    | SnapshotDataset::Indexes
                    | SnapshotDataset::Columns
                    | SnapshotDataset::ForeignKeys
            )
        {
            let (f, u) = clock_sort_key(&native_clock);
            let key = (f.to_owned(), u.to_owned());
            if let Some(existing) = schema_heads.iter_mut().find(|(ds, _, _)| *ds == snap.dataset) {
                if key >= existing.1 {
                    *existing = (snap.dataset, key, *capture);
                }
            } else {
                schema_heads.push((snap.dataset, key, *capture));
            }
        }

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

        if capture.families().contains(&EvidenceFamily::OperationalSamples)
            && let Some(snap) = source.observation.snapshot()
        {
            match snap.dataset {
                SnapshotDataset::Operational => {
                    if let Ok(analysis) = crate::collected_resources::storage(capture) {
                        telemetry.storage.push(WindowStoragePoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            report: *analysis.report,
                        });
                    }
                    if let Ok(analysis) = crate::collected_resources::memory_clerks(capture) {
                        telemetry.memory_clerks.push(WindowMemoryClerkPoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            report: *analysis.report,
                        });
                    }
                }
                SnapshotDataset::CurrentRequestMemory => {
                    if let Ok(analysis) = crate::collected_resources::request_memory(capture) {
                        telemetry.memory_grants.push(WindowMemoryGrantPoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            kind: WindowMemoryGrantKind::RequestGrants,
                            report: analysis.report,
                        });
                    }
                }
                SnapshotDataset::SessionMemory => {
                    if let Ok(analysis) = crate::collected_resources::session_memory(capture) {
                        telemetry.memory_grants.push(WindowMemoryGrantPoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            kind: WindowMemoryGrantKind::SessionMemory,
                            report: analysis.report,
                        });
                    }
                }
                SnapshotDataset::CachedRuntime => {
                    let def_capture = captures.iter().copied().find(|candidate| {
                        candidate.range_id() != capture.range_id()
                            && candidate.families().contains(&EvidenceFamily::ObjectDefinitions)
                            && candidate.source().observation.snapshot().is_some_and(|s| {
                                matches!(s.dataset, SnapshotDataset::CachedDefinitions | SnapshotDataset::RetainedPlans)
                            })
                    });
                    let spill_analysis = def_capture
                        .and_then(|defs| crate::collected_resources::completed_spills(capture, Some(defs)).ok())
                        .or_else(|| crate::collected_resources::completed_spills(capture, None).ok());
                    if let Some(analysis) = spill_analysis {
                        telemetry.spills.push(WindowSpillPoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            report: analysis.report,
                        });
                    }
                }
                SnapshotDataset::Jobs => {
                    if let Ok(analysis) = crate::collected_resources::jobs(capture) {
                        telemetry.jobs.push(WindowJobsPoint {
                            original: super::reference(capture)?,
                            native_clock: native_clock.clone(),
                            read_complete: capture.is_read_complete(),
                            report: analysis.report,
                        });
                    }
                }
                _ => {}
            }
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

    if !schema_heads.is_empty() {
        let mut catalog = WindowSchemaCatalog::default();
        for (dataset, (_, until), cap) in schema_heads {
            if !until.is_empty() && catalog.observed_at.as_deref().is_none_or(|prev| until.as_str() > prev) {
                catalog.observed_at = Some(until);
            }
            for record in cap.records() {
                match (dataset, &record.row().record) {
                    (SnapshotDataset::Tables, TelemetryRecord::Operational(obs)) => {
                        if let OperationalFact::MssqlTable(table) = &obs.fact
                            && let Some(projected) = table.as_ref().clone().into_table_stat()
                        {
                            catalog.tables.push(projected);
                        }
                    }
                    (SnapshotDataset::Indexes, TelemetryRecord::Operational(obs)) => {
                        if let OperationalFact::MssqlIndex(index) = &obs.fact
                            && index.parent_kind != MssqlIndexParentKind::IndexedView
                        {
                            let filter = match &index.filter {
                                Some(CatalogExpression {
                                    payload: DefinitionPayload::Available { text, .. }, ..
                                }) => Some(text.clone()),
                                _ => None,
                            };
                            let presentation = MssqlIndexPresentation { filter, filter_facts: None, last_read: None };
                            if let Some(projected) = index.as_ref().clone().into_index_stat(presentation) {
                                catalog.indexes.push(projected);
                            }
                        }
                    }
                    (SnapshotDataset::Columns, TelemetryRecord::ObjectDefinition(row)) => {
                        if let ObjectDefinitionFact::MssqlColumn(column) = &row.fact {
                            let default = match &column.default {
                                Some(CatalogExpression {
                                    payload: DefinitionPayload::Available { text, .. }, ..
                                }) => Some(text.clone()),
                                _ => None,
                            };
                            catalog.columns.push(column.as_ref().clone().into_column_info(default));
                        }
                    }
                    (SnapshotDataset::ForeignKeys, TelemetryRecord::ObjectDefinition(row)) => {
                        if let ObjectDefinitionFact::MssqlForeignKey(fk) = &row.fact {
                            catalog.foreign_keys.push(fk.as_ref().clone().into_foreign_key());
                        }
                    }
                    _ => {}
                }
            }
        }
        catalog.tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        catalog.indexes.sort_by(|a, b| (&a.schema, &a.table, &a.name).cmp(&(&b.schema, &b.table, &b.name)));
        catalog.columns.sort_by(|a, b| (&a.schema, &a.table, a.ordinal).cmp(&(&b.schema, &b.table, b.ordinal)));
        catalog.foreign_keys.sort_by(|a, b| (&a.schema, &a.table, &a.name).cmp(&(&b.schema, &b.table, &b.name)));
        if !catalog.is_empty() {
            telemetry.schema = Some(catalog);
        }
    }

    telemetry.resources.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.activity.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.replication.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.storage.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.memory_clerks.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.memory_grants.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.spills.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
    telemetry.jobs.sort_by(|a, b| clock_sort_key(&a.native_clock).cmp(&clock_sort_key(&b.native_clock)));
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

#[cfg(all(test, feature = "collector"))]
mod tests {
    use super::*;
    use crate::collected_evidence::RetainedEvidenceFixture;
    use culprit_evidence::analytical::{evidence::RecoveryPayloadBudget, test_support};

    #[tokio::test]
    async fn operational_window_telemetry_projects_storage_clerks_grants_spills_and_jobs() {
        let (storage_src, storage_rows) = test_support::storage_evidence(test_support::StorageCase::FileCensus);
        let storage_cap = RetainedEvidenceFixture::with_sequence(storage_src, storage_rows, 970_001)
            .await
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;

        let (clerk_src, clerk_rows) =
            test_support::instance_memory_clerk_evidence(test_support::MemoryClerkCase::Exact);
        let clerk_cap = RetainedEvidenceFixture::with_sequence(clerk_src, clerk_rows, 970_002)
            .await
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;

        let (grant_src, grant_rows) = test_support::request_memory_evidence_cases().remove(0);
        let grant_cap = RetainedEvidenceFixture::with_sequence(grant_src, grant_rows, 970_003)
            .await
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;

        let spill_case = test_support::completed_spill_evidence_cases()
            .into_iter()
            .find(|c| c.name == "recorded_spills")
            .expect("recorded_spills fixture");
        let spill_cap = RetainedEvidenceFixture::with_sequence(spill_case.runtime.0, spill_case.runtime.1, 970_004)
            .await
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;
        let defs = spill_case.definitions.expect("definitions fixture");
        let defs_cap = RetainedEvidenceFixture::with_sequence(defs.0, defs.1, 970_005)
            .await
            .read_families_with_recovery(&[EvidenceFamily::ObjectDefinitions], RecoveryPayloadBudget::default())
            .await;

        let (jobs_src, jobs_rows) = test_support::jobs_evidence_cases().remove(0);
        let jobs_cap = RetainedEvidenceFixture::with_sequence(jobs_src, jobs_rows, 970_006)
            .await
            .read_families_with_recovery(&[EvidenceFamily::OperationalSamples], RecoveryPayloadBudget::default())
            .await;

        let refs = [&storage_cap, &clerk_cap, &grant_cap, &spill_cap, &defs_cap, &jobs_cap];
        let projected = project(&refs, &|| Ok(())).expect("projection ok").expect("non-empty telemetry");

        assert_eq!(projected.storage.len(), 1);
        assert!(projected.storage[0].report.file_summary.is_some());
        assert_eq!(projected.memory_clerks.len(), 1);
        assert!(!projected.memory_clerks[0].report.entries.is_empty());
        assert_eq!(projected.memory_grants.len(), 1);
        assert_eq!(projected.memory_grants[0].kind, WindowMemoryGrantKind::RequestGrants);
        assert_eq!(projected.spills.len(), 1);
        assert!(!projected.spills[0].report.entries.is_empty());
        assert_eq!(projected.jobs.len(), 1);
    }
}
