use super::*;
use culprit_core::telemetry::StorageRead;
use culprit_core::{
    CommitPipelineCounterKind, CommitPipelineFact, CommitPipelineIncrease, CommitPipelineReport, CommitPipelineStage,
    ResetMarker,
};
use test_support::{CommitPipelineCase as Case, commit_pipeline_fixture};

async fn captures(case: Case, sequence: u64) -> Vec<EvidenceCapture> {
    let mut captures = Vec::new();
    for (index, original) in commit_pipeline_fixture(case).originals.into_iter().enumerate() {
        captures.push(Box::pin(retained(original.source, original.records, sequence + index as u64)).await);
    }
    captures
}
fn explain(captures: &[EvidenceCapture]) -> CommitPipelineReport {
    let originals = captures.iter().map(|capture| capture.verified_original().unwrap()).collect::<Vec<_>>();
    let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
    let inputs = super::super::commit_pipeline::inputs(&borrowed, originals[0].original()).unwrap();
    culprit_checks::commit_pipeline::explain(&inputs, &originals[0].reference(0).unwrap()).unwrap()
}
fn observe(capture: &EvidenceCapture, previous: Option<&DiagnosticCheckpoint>) -> DiagnosticObservationUpdate {
    let original = capture.verified_original().unwrap();
    let references =
        (0..original.records().len()).map(|position| original.reference(position).unwrap()).collect::<Vec<_>>();
    let rows = original
        .records()
        .iter()
        .zip(&references)
        .map(|(row, reference)| {
            let TelemetryRecord::Activity(observation) = &row.row().record else { panic!() };
            DiagnosticActivityRow { reference, observation }
        })
        .collect::<Vec<_>>();
    observe_diagnostic_activity(
        original.source(),
        original.original(),
        &rows,
        previous,
        &ActivityPolicy::default(),
        "2020-01-01T12:00:10Z",
    )
    .unwrap()
}
fn log_episode(update: &DiagnosticObservationUpdate) -> DiagnosticEpisode {
    update
        .checkpoint
        .episodes
        .iter()
        .find(|episode| {
            matches!(episode.signal, DiagnosticSignal::Activity { condition: ActivityCondition::LongRunning, .. })
        })
        .unwrap()
        .clone()
}
fn transaction_count(report: &CommitPipelineReport) -> usize {
    report
        .increases
        .iter()
        .filter(|increase| matches!(increase, CommitPipelineIncrease::TransactionLogBytes { .. }))
        .count()
}
fn allocation_count(report: &CommitPipelineReport) -> usize {
    report
        .increases
        .iter()
        .filter(|increase| matches!(increase, CommitPipelineIncrease::CatalogLogAllocationBytes { .. }))
        .count()
}

#[tokio::test]
async fn exact_log_wait_keeps_independent_totals_and_producer_candidate_without_acquiring_sql_or_cause() {
    let captures = captures(Case::Exact, 1_180_000).await;
    for (index, capture) in captures[3..].iter().enumerate() {
        let original = capture.verified_original().unwrap();
        let raw = original.operational_read().unwrap();
        let read = StorageRead::from_source(original.source(), raw.observations, true).unwrap();
        let summary = culprit_checks::storage::explain(&read).unwrap().file_summary.unwrap();
        assert_eq!(summary.source_state, SourceState::Collected, "each original census is coherent before comparison");
        assert_eq!(summary.log_file_count, Some(1));
        let at = if index == 0 { "2020-01-01T12:00:00.5Z" } else { "2020-01-01T12:00:02.5Z" };
        assert!(
            summary
                .observed_at
                .as_deref()
                .is_some_and(|clock| ObservationBounds::new(clock, clock).is_some_and(|bounds| bounds.contains(at)))
        );
        let log = read
            .observations
            .iter()
            .find_map(|row| match &row.fact {
                OperationalFact::Space(space) if row.source == source_id::LOG_CAPACITY => Some(space),
                _ => None,
            })
            .unwrap();
        assert_eq!(log.observed_at, if index == 0 { "2020-01-01T12:00:00.75Z" } else { "2020-01-01T12:00:02.75Z" });
    }
    let report = explain(&captures);
    assert_eq!(report.stage, Some(CommitPipelineStage::LocalLogFlush));
    assert_eq!(report.wait_ms.as_deref(), Some("37"));
    assert_eq!(report.increases.len(), 2);
    assert!(report.increases.iter().any(|increase| matches!(increase,
        CommitPipelineIncrease::TransactionLogBytes { bytes, actor: Some(_), .. } if bytes=="16")));
    assert!(report.increases.iter().any(|increase| matches!(increase,
        CommitPipelineIncrease::CatalogLogAllocationBytes { bytes, .. } if bytes=="131072")));
    assert!(
        report
            .inputs
            .iter()
            .any(|input| input.context.as_ref().is_some_and(|context| context.database_writable.is_none()))
    );
    let revision = history(log_episode(&observe(&captures[0], None)), 1_180_020).await;
    let mut selected = vec![&revision];
    selected.extend(captures.iter());
    let output = project(&selected, vec![]).unwrap();
    output.validate().unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(incident.culprit_queries.is_empty());
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::CandidatesOnly);
    assert_eq!(incident.candidate_queries.len(), 1);
    let candidate = &incident.candidate_queries[0];
    assert!(
        matches!(&candidate.identity, Some(DerivedQueryIdentity::Activity { actor, statement: None }) if actor.request.is_none() && actor.transaction.as_deref()==Some("900"))
    );
    assert!(matches!(candidate.sql, DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected }));
    assert_eq!(candidate.evidence.len(), 3);
    assert_eq!(
        candidate.caller.as_ref().unwrap().client.as_ref().unwrap().application.as_deref(),
        Some("native-log-producer")
    );
    assert!(!serde_json::to_string(incident).unwrap().contains("commit-private-canary"));
    assert!(incident.mechanism.as_deref().unwrap().contains("flush"));
}

#[tokio::test]
async fn partial_denied_foreign_restart_and_single_points_withhold_increases_without_losing_original_gaps() {
    for (index, case) in
        [Case::SingleLifetime, Case::Partial, Case::Denied, Case::ForeignMember, Case::Restart].into_iter().enumerate()
    {
        let originals = captures(case, 1_181_000 + index as u64 * 10).await;
        let report = explain(&originals);
        assert_eq!(report.stage, Some(CommitPipelineStage::LocalLogFlush), "{case:?}");
        assert_eq!(transaction_count(&report), 0, "{case:?}");
        assert_eq!(allocation_count(&report), 0, "{case:?}");
        assert!(!report.limitations.is_empty());
        if case == Case::Denied {
            assert!(
                report
                    .inputs
                    .iter()
                    .any(|input| input.sources.iter().any(|source| source.state == SourceState::Unavailable))
            );
        }
        if matches!(case, Case::ForeignMember | Case::Restart) {
            assert!(
                report.inputs.iter().any(|input| input.binding == culprit_core::CommitPipelineContextBinding::Foreign)
            );
        }
    }
}

#[tokio::test]
async fn role_clock_transaction_and_file_controls_are_independent_per_quantity() {
    for (index, case) in [
        Case::UnknownRole,
        Case::ChangedRole,
        Case::ReusedTransaction,
        Case::DecreasedBytes,
        Case::NativeClockBackwards,
    ]
    .into_iter()
    .enumerate()
    {
        let report = explain(&captures(case, 1_182_000 + index as u64 * 10).await);
        assert_eq!(transaction_count(&report), 0, "{case:?}");
        assert_eq!(
            allocation_count(&report),
            1,
            "a complete catalog endpoint quantity does not assert transaction/role continuity: {case:?}"
        );
    }
    let report = explain(&captures(Case::UnknownFileBasis, 1_182_100).await);
    assert_eq!(transaction_count(&report), 1);
    assert_eq!(allocation_count(&report), 0);
    for (index, case) in [Case::FileReplaced, Case::LogCapacityDenied].into_iter().enumerate() {
        let report = explain(&captures(case, 1_182_200 + index as u64 * 10).await);
        assert_eq!(
            allocation_count(&report),
            1,
            "complete type1 catalog sums survive independent log denial or file replacement: {case:?}"
        );
        assert!(!report.limitations.is_empty());
    }
    let report = explain(&captures(Case::MissingActor, 1_182_300).await);
    assert!(matches!(&report.increases[0], CommitPipelineIncrease::TransactionLogBytes { actor: None, .. }));
    let invalid = commit_pipeline_fixture(Case::DuplicateFile).originals.pop().unwrap();
    let observations = invalid
        .records
        .into_iter()
        .map(|record| {
            let TelemetryRecord::Operational(point) = record else { panic!() };
            point
        })
        .collect();
    assert!(StorageRead::from_source(&invalid.source, observations, true).is_err());
}

#[tokio::test]
async fn postgres_wal_points_keep_zero_unknown_reset_and_mysql_totals_do_not_invent_a_commit_wait_stage() {
    for (index, case) in [Case::Postgres17, Case::Postgres18, Case::MysqlTotals].into_iter().enumerate() {
        let report = explain(&captures(case, 1_183_000 + index as u64 * 10).await);
        assert_eq!(report.stage, (case != Case::MysqlTotals).then_some(CommitPipelineStage::LocalLogSync), "{case:?}");
        let counters = report
            .facts
            .iter()
            .find_map(|fact| match fact {
                CommitPipelineFact::Counters { values, .. } => Some(values),
                _ => None,
            })
            .unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].sample.value.integer(), Some(0));
        assert_eq!(counters[0].sample.reset, ResetMarker::Unknown);
        assert_eq!(counters[0].sample.scope, MeasurementScope::Server);
        assert_eq!(
            counters[0].kind,
            if case == Case::MysqlTotals {
                CommitPipelineCounterKind::WrittenBytes
            } else {
                CommitPipelineCounterKind::SyncTime
            }
        );
        assert!(report.increases.is_empty());
    }
}

#[tokio::test]
async fn missing_body_and_altered_exact_row_reference_cannot_supply_complete_context_or_current_cause() {
    let captures = captures(Case::Exact, 1_184_000).await;
    let originals = captures.iter().map(|capture| capture.verified_original().unwrap()).collect::<Vec<_>>();
    let borrowed = originals.iter().map(|original| original as &dyn OriginalProjection).collect::<Vec<_>>();
    let mut inputs = super::super::commit_pipeline::inputs(&borrowed, originals[0].original()).unwrap();
    let reference = originals[0].reference(0).unwrap();
    inputs[2].read_complete = false;
    let report = culprit_checks::commit_pipeline::explain(&inputs, &reference).unwrap();
    assert_eq!(transaction_count(&report), 0);
    assert_eq!(allocation_count(&report), 1);
    inputs[2].rows[0].reference.payload_sha256 = digest(b"different-original-body");
    assert!(culprit_checks::commit_pipeline::explain(&inputs, &reference).is_err());
}

#[tokio::test]
async fn local_recovery_epoch_supports_exact_endpoint_differences_without_a_global_database_namespace() {
    for (index, case) in [
        Case::Exact,
        Case::Partial,
        Case::Denied,
        Case::ForeignMember,
        Case::Restart,
        Case::ReusedTransaction,
        Case::UnknownRole,
        Case::ChangedRole,
    ]
    .into_iter()
    .enumerate()
    {
        let mut originals = Vec::new();
        for (offset, mut original) in commit_pipeline_fixture(case).originals.into_iter().enumerate() {
            let CollectionTarget::Database(database) = &mut original.source.target else { panic!() };
            database.incarnation = None;
            originals.push(
                Box::pin(retained(original.source, original.records, 1_186_000 + index as u64 * 10 + offset as u64))
                    .await,
            );
        }
        let initial = NativeReadContext::from_activity(originals[0].source()).unwrap();
        assert!(initial.has_qualified_local_owner());
        assert!(LocalResourceReadScope::from_context(&initial).is_ok());
        assert!(NativeSourceReadScope::from_context(&initial).is_err());
        let report = explain(&originals);
        assert_eq!(transaction_count(&report), if case == Case::Exact { 1 } else { 0 }, "{case:?}");
        let allocation = matches!(case, Case::Exact | Case::ReusedTransaction | Case::UnknownRole | Case::ChangedRole);
        assert_eq!(allocation_count(&report), if allocation { 1 } else { 0 }, "{case:?}");
        if case == Case::Exact {
            assert!(report.increases.iter().any(|increase| matches!(increase,
                CommitPipelineIncrease::TransactionLogBytes { bytes, actor: Some(_), .. } if bytes == "16")));
            let revision = history(log_episode(&observe(&originals[0], None)), 1_187_000).await;
            let mut selected = vec![&revision];
            selected.extend(originals.iter());
            let diagnosis = project(&selected, vec![]).unwrap();
            diagnosis.validate().unwrap();
            let incident = &diagnosis.incidents[0];
            assert_eq!(incident.affected_queries.len(), 1);
            assert_eq!(incident.candidate_queries.len(), 1);
            assert!(incident.culprit_queries.is_empty());
            assert!(matches!(
                incident.candidate_queries[0].sql,
                DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected }
            ));
        }
    }
}

#[tokio::test]
async fn resolved_log_incident_retains_the_original_wait_and_candidates_without_reopening_a_source() {
    let captures = captures(Case::Exact, 1_185_000).await;
    let initial = observe(&captures[0], None);
    let episode = log_episode(&initial);
    let mut negative = commit_pipeline_fixture(Case::Exact).originals.remove(0);
    let SourceObservation::Activity(header) = &mut negative.source.observation else { panic!() };
    header.at = "2020-01-01T12:00:04Z".into();
    header.observed_from = header.at.clone();
    header.observed_until = header.at.clone();
    // A still-visible faster request is uncertainty, not recovery. This complete original instead
    // proves that the exact request ended while the independently observed producer session remains.
    let ended = episode.signal.activity_key().unwrap();
    negative.records.retain(|row| !matches!(row, TelemetryRecord::Activity(actor) if &actor.key == ended));
    let remaining = u32::try_from(negative.records.len()).unwrap();
    let SourceObservation::Activity(header) = &mut negative.source.observation else { panic!() };
    header.observations = remaining;
    header.native_rows = remaining;
    header.coverage = StateSourceCoverage::collected(negative.source.source.clone(), remaining);
    assert!(!negative.records.iter().any(|row| matches!(row, TelemetryRecord::Activity(actor) if &actor.key == ended)));
    let negative = retained(negative.source, negative.records, 1_185_010).await;
    let update = observe(&negative, Some(&initial.checkpoint));
    let revision = update.revisions.into_iter().find(|revision| revision.episode.id == episode.id).unwrap();
    assert_eq!(revision.episode.lifecycle, DiagnosticLifecycle::Resolved);
    let archived = history_revision(revision, 1_185_020).await;
    let mut selected = vec![&archived, &negative];
    selected.extend(captures.iter());
    let output = project(&selected, vec![]).unwrap();
    let incident = &output.incidents[0];
    assert_eq!(incident.entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(incident.affected_queries[0].evidence[0], episode.first);
    assert_eq!(incident.candidate_queries.len(), 1);
    assert!(incident.culprit_queries.is_empty());
}

#[tokio::test]
async fn all_commit_pipeline_hops_downstream_context_and_instant_init_growth_boundaries_reach_gold_honestly() {
    // 1. Every MSSQL commit-stage hop produces its exact per-hop Gold mechanism & action without inventing culprits
    // or diagnosing a zero-initialization freeze from <= 64 MiB log allocation growth.
    for (idx, (native_wait, wait_class, expected_stage, expected_mech_key, expected_action_key)) in [
        (
            "LOGBUFFER",
            None,
            CommitPipelineStage::LogBufferSpace,
            key!("view.commit_pipeline.stage.buffer"),
            key!("view.commit_pipeline.action.local"),
        ),
        (
            "WRITELOG",
            None,
            CommitPipelineStage::LocalLogFlush,
            key!("view.commit_pipeline.stage.flush"),
            key!("view.commit_pipeline.action.local"),
        ),
        (
            "LOGMGR_RESERVE_APPEND",
            None,
            CommitPipelineStage::LogSpaceReuse,
            key!("view.commit_pipeline.stage.reuse"),
            key!("view.commit_pipeline.action.reuse"),
        ),
        (
            "HADR_SYNC_COMMIT",
            None,
            CommitPipelineStage::SynchronousHardeningAcknowledgement,
            key!("view.commit_pipeline.stage.remote_hardening"),
            key!("view.commit_pipeline.action.remote"),
        ),
        (
            "LOG_RATE_GOVERNOR",
            Some(ActivityWaitClass::LogRateGovernance),
            CommitPipelineStage::RateGovernance { component: None },
            key!("view.commit_pipeline.stage.governance"),
            key!("view.commit_pipeline.action.governance"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut fixture = commit_pipeline_fixture(Case::Exact);
        {
            let TelemetryRecord::Activity(actor) = &mut fixture.originals[0].records[0] else { panic!() };
            let wait = actor.wait.as_mut().unwrap();
            wait.native_type = Some(native_wait.into());
            wait.class = wait_class;
        }
        let primary_target = fixture.originals[0].source.target.clone();
        let primary_partition = fixture.originals[0].source.partition.clone();
        let primary_incarnation = primary_target.database().unwrap().incarnation.clone().unwrap();

        // Attach same-owner FileIo (log stall), ConfiguredLogRateLimits, and HyperscaleLogComponent context captures
        // to prove downstream context enriches reasons without turning correlated points into culprit queries.
        let (mut io_src, mut io_rows) = test_support::file_io_evidence_cases().remove(0);
        io_src.target = primary_target.clone();
        io_src.partition = primary_partition.clone();
        io_rows.retain(|r| matches!(r, TelemetryRecord::Operational(op) if matches!(&op.fact, OperationalFact::FileIo(f) if f.kind == culprit_core::FileKind::Log)));
        if let SourceObservation::Snapshot(hdr) = &mut io_src.observation {
            hdr.records = io_rows.len() as u32;
            hdr.sources[0] = StateSourceCoverage::collected(hdr.sources[0].source.clone(), io_rows.len() as u32);
        }

        let (mut rate_src, mut rate_rows) = test_support::configured_log_rate_evidence_cases().remove(0);
        rate_src.target = primary_target.clone();
        rate_src.partition = primary_partition.clone();
        rate_rows.retain(|r| matches!(r, TelemetryRecord::Operational(op) if matches!(&op.fact, OperationalFact::ConfiguredLogRateLimits(_))));
        for rec in &mut rate_rows {
            if let TelemetryRecord::Operational(op) = rec
                && let OperationalFact::ConfiguredLogRateLimits(point) = &mut op.fact
            {
                point.database_incarnation = primary_incarnation.clone();
                op.key = point.key().unwrap();
            }
        }
        if let SourceObservation::Snapshot(hdr) = &mut rate_src.observation {
            hdr.records = rate_rows.len() as u32;
            hdr.sources =
                vec![StateSourceCoverage::observed(CONFIGURED_LOG_RATES_SOURCE.into(), rate_rows.len() as u32, false)];
        }

        let (mut hs_src, mut hs_rows) = test_support::hyperscale_log_component_evidence_cases().remove(0);
        hs_src.target = primary_target;
        hs_src.partition = primary_partition;
        for rec in &mut hs_rows {
            if let TelemetryRecord::Operational(op) = rec
                && let OperationalFact::HyperscaleLogComponent(point) = &mut op.fact
            {
                point.database_incarnation = primary_incarnation.clone();
                op.key = point.key().unwrap();
            }
        }

        fixture.originals.push(test_support::CommitPipelineOriginalFixture { source: io_src, records: io_rows });
        fixture.originals.push(test_support::CommitPipelineOriginalFixture { source: rate_src, records: rate_rows });
        fixture.originals.push(test_support::CommitPipelineOriginalFixture { source: hs_src, records: hs_rows });

        let seq_base = 1_190_000 + (idx as u64) * 20;
        let mut caps = Vec::new();
        for (offset, orig) in fixture.originals.into_iter().enumerate() {
            caps.push(Box::pin(retained(orig.source, orig.records, seq_base + offset as u64)).await);
        }
        let report = explain(&caps);
        assert_eq!(report.stage, Some(expected_stage), "{native_wait}");
        let rev = history(log_episode(&observe(&caps[0], None)), seq_base + 15).await;
        let mut selected = vec![&rev];
        selected.extend(caps.iter());
        let out = project(&selected, vec![]).unwrap();
        out.validate().unwrap();
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty(), "{native_wait}");
        assert!(
            inc.mechanism.as_deref().unwrap().contains(&culprit_core::i18n::t(expected_mech_key, &[])),
            "{native_wait}"
        );
        assert!(inc.developer_action.contains(&culprit_core::i18n::t(expected_action_key, &[])), "{native_wait}");
        assert!(inc.entry.reasons.contains(&t!("view.commit_pipeline.limit.io")));
        assert!(inc.entry.reasons.contains(&t!("view.commit_pipeline.limit.configuration")));
        assert!(inc.entry.reasons.contains(&t!("view.commit_pipeline.context.hyperscale")));
        // Instant log initialization boundary: <= 64 MiB growth (131,072 bytes here) never diagnoses a zero-init freeze.
        assert!(inc.entry.reasons.contains(&t!("view.commit_pipeline.limit.growth")));
        assert!(!inc.mechanism.as_deref().unwrap().to_ascii_lowercase().contains("zero-init"));
    }

    // 2. Every PostgreSQL commit-stage hop (WALWrite, WALSync, WALInitWrite, WALInitSync, IPC: SyncRep)
    // maps to its distinct Gold stage mechanism and action while keeping culprits empty.
    for (idx, (native_wait, expected_stage, expected_mech_key, expected_action_key)) in [
        (
            "IO: WALWrite",
            CommitPipelineStage::LocalLogWrite,
            key!("view.commit_pipeline.stage.write"),
            key!("view.commit_pipeline.action.local"),
        ),
        (
            "IO: WALSync",
            CommitPipelineStage::LocalLogSync,
            key!("view.commit_pipeline.stage.sync"),
            key!("view.commit_pipeline.action.local"),
        ),
        (
            "IO: WALInitWrite",
            CommitPipelineStage::SegmentInitializationWrite,
            key!("view.commit_pipeline.stage.initialization"),
            key!("view.commit_pipeline.action.initialization"),
        ),
        (
            "IO: WALInitSync",
            CommitPipelineStage::SegmentInitializationSync,
            key!("view.commit_pipeline.stage.initialization"),
            key!("view.commit_pipeline.action.initialization"),
        ),
        (
            "IPC: SyncRep",
            CommitPipelineStage::SynchronousAcknowledgement,
            key!("view.commit_pipeline.stage.remote"),
            key!("view.commit_pipeline.action.remote"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut fixture = commit_pipeline_fixture(Case::Postgres18);
        {
            let TelemetryRecord::Activity(actor) = &mut fixture.originals[0].records[0] else { panic!() };
            actor.elapsed_ms = Some("100000".into());
            actor.wait.as_mut().unwrap().native_type = Some(native_wait.into());
        }
        let seq_base = 1_191_000 + (idx as u64) * 10;
        let mut caps = Vec::new();
        for (offset, orig) in fixture.originals.into_iter().enumerate() {
            caps.push(Box::pin(retained(orig.source, orig.records, seq_base + offset as u64)).await);
        }
        let report = explain(&caps);
        assert_eq!(report.stage, Some(expected_stage), "{native_wait}");
        let rev = history(log_episode(&observe(&caps[0], None)), seq_base + 5).await;
        let mut selected = vec![&rev];
        selected.extend(caps.iter());
        let out = project(&selected, vec![]).unwrap();
        out.validate().unwrap();
        let [inc] = out.incidents.as_slice() else { panic!() };
        assert!(inc.culprit_queries.is_empty(), "{native_wait}");
        assert!(
            inc.mechanism.as_deref().unwrap().contains(&culprit_core::i18n::t(expected_mech_key, &[])),
            "{native_wait}"
        );
        assert!(inc.developer_action.contains(&culprit_core::i18n::t(expected_action_key, &[])), "{native_wait}");
    }
}

#[cfg(feature = "collector")]
mod automatic_context;

mod intervals;
