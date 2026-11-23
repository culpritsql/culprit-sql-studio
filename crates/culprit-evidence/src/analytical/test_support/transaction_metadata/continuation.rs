//! One shared controlled corpus exercises repeated originals and canonical readback, never native wait/cost acceptance.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMetadataContinuationCase {
    Continued,
    Partial,
    ChangedRequest,
    Empty,
    Unavailable,
    UnknownReset,
    ChangedReset,
    MissingCounter,
    OtherActors,
    WideRoster,
    MultiXact,
}
impl TransactionMetadataContinuationCase {
    pub const ALL: &'static [Self] = &[
        Self::Continued,
        Self::Partial,
        Self::ChangedRequest,
        Self::Empty,
        Self::Unavailable,
        Self::UnknownReset,
        Self::ChangedReset,
        Self::MissingCounter,
        Self::OtherActors,
        Self::WideRoster,
        Self::MultiXact,
    ];
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMetadataObservation {
    Initial,
    Continued,
}
pub fn transaction_metadata_continuation_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: TransactionMetadataContinuationCase,
    observation: TransactionMetadataObservation,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    use TransactionMetadataContinuationCase::*;
    let continued = observation == TransactionMetadataObservation::Continued;
    let source_case = if continued {
        match case {
            Partial => TransactionMetadataCase::Partial,
            ChangedRequest => TransactionMetadataCase::ChangedRequest,
            Empty => TransactionMetadataCase::Empty,
            Unavailable => TransactionMetadataCase::Unavailable,
            _ => TransactionMetadataCase::Overflow,
        }
    } else {
        TransactionMetadataCase::Overflow
    };
    let (from, at, until) = if continued {
        ("2020-01-01T12:01:12Z", "2020-01-01T12:01:13Z", "2020-01-01T12:01:14Z")
    } else {
        ("2020-01-01T12:00:02Z", "2020-01-01T12:00:03Z", "2020-01-01T12:00:04Z")
    };
    let (source, mut records) = transaction_metadata_result_at(focus, execution, source_case, from, at, until);
    if let Some(TelemetryRecord::Operational(row)) = records.first_mut() {
        let OperationalFact::PostgresTransactionMetadata(point) = &mut row.fact else { unreachable!() };
        if continued {
            for counter in &mut point.slrus {
                counter.blocks_read = Some("45".into());
            }
            match case {
                UnknownReset => point.slrus[0].stats_reset = None,
                ChangedReset => point.slrus[0].stats_reset = Some("2020-01-01T10:00:00.000001Z".into()),
                MissingCounter => point.slrus[0].blocks_read = None,
                OtherActors => {
                    point.caches[1].actor = actor("49", None);
                    for holder in &mut point.retention.holders {
                        for horizon in &mut holder.horizons {
                            horizon.native_value = "9999".into();
                        }
                    }
                }
                _ => (),
            }
        }
        if case == WideRoster {
            let mut caches = vec![point.caches[0].clone()];
            for (index, pid) in (10..=40).enumerate() {
                let mut row = point.caches[1].clone();
                row.backend_id = (index + 20) as u32;
                row.actor = actor(&pid.to_string(), None);
                caches.push(row);
            }
            point.cache_rows = caches.len() as u32;
            point.caches = caches;
        }
        row.key = point.key().unwrap();
        assert!(point.fits_source_scope(&source));
    }
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}

fn original(
    source: &CollectionDescription,
    rows: &[TelemetryRecord],
    sequence: u64,
) -> (DiagnosticCaptureRef, Vec<ExportedRecord>, Vec<DiagnosticEvidenceRef>) {
    let capture = CaptureIdentity {
        observation_id: format!("metadata-continuation-{sequence}"),
        revision: Some(CaptureRevision {
            outbox_id: digest(b"metadata-continuation-fixture-outbox"),
            owner_epoch: "1".into(),
            sequence: sequence.to_string(),
        }),
    };
    let manifest = digest(format!("metadata-continuation-fixture-manifest-{sequence}").as_bytes());
    let exports =
        rows.iter().map(|row| ExportedRecord::new(source, &capture, row.clone()).unwrap()).collect::<Vec<_>>();
    let references = exports
        .iter()
        .enumerate()
        .map(|(ordinal, row)| DiagnosticEvidenceRef::from_export(source, &manifest, row, ordinal as u32).unwrap())
        .collect();
    (
        DiagnosticCaptureRef {
            capture,
            manifest_sha256: manifest,
            source_scope_id: source.captured_range().unwrap().scope_id,
        },
        exports,
        references,
    )
}

fn revise(before: &DiagnosticEpisode, next: DiagnosticEpisode, at: &str) -> DiagnosticEpisode {
    DiagnosticRevision::between(Some(before), next, at).unwrap().unwrap().episode
}

/// Both complete native answers are retained in the same episode; this helper never constructs a verified warehouse handle.
pub fn transaction_metadata_continuation_history(
    case: TransactionMetadataContinuationCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let mechanism = if case == TransactionMetadataContinuationCase::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    };
    let first = transaction_metadata_original(mechanism);
    let (first_source, first_rows) = transaction_metadata_continuation_result(
        &first.focus,
        &first.execution,
        case,
        TransactionMetadataObservation::Initial,
    );
    let (capture, rows, refs) = original(&first_source, &first_rows, 2);
    let pairs = refs.iter().zip(&rows).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        &first.focus,
        &first.source,
        &first.original,
        &first_source,
        &capture,
        &pairs,
    )
    .unwrap();
    let pending = first.episode.enroll_source_read(first.focus.clone()).unwrap();
    let episode = revise(&first.episode, pending, "2020-01-01T12:00:01Z");
    let collecting = episode.transition_source_read(&first.focus, SourceReadStageState::Collecting).unwrap();
    let episode = revise(&episode, collecting, "2020-01-01T12:00:02Z");
    let answered = episode
        .transition_source_read(
            &first.focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    let episode = revise(&episode, answered, "2020-01-01T12:00:04Z");
    let fresh = transaction_metadata_original_at(mechanism, "2020-01-01T12:01:10Z", "2020-01-01T12:01:11Z");
    let (parent, rows, refs) = original(&fresh.source, &[fresh.original.record], 3);
    let TelemetryRecord::Activity(actor) = &rows[0].record else { unreachable!() };
    let checkpoint = DiagnosticCheckpoint {
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: episode.scope.target.clone(),
        input: episode.latest.capture_ref(),
        episodes: vec![episode],
    };
    let update = observe_diagnostic_activity(
        &fresh.source,
        &parent,
        &[DiagnosticActivityRow { reference: &refs[0], observation: actor }],
        Some(&checkpoint),
        &ActivityPolicy::default(),
        "2020-01-01T12:01:11Z",
    )
    .unwrap();
    let episode = update
        .checkpoint
        .episodes
        .into_iter()
        .find(|episode| matches!(episode.signal, DiagnosticSignal::TransactionMetadataWait { .. }))
        .unwrap();
    let focus = SourceReadFocus::from_followup_input(
        SourceReadQuestion::TransactionMetadata,
        &episode,
        &fresh.source,
        &refs[0],
        &rows[0],
    )
    .unwrap()
    .unwrap();
    let execution = SourceReadExecutionScope::from_context(
        focus.question,
        &NativeReadContext::from_activity(&fresh.source).unwrap(),
    )
    .unwrap();
    let (source, values) =
        transaction_metadata_continuation_result(&focus, &execution, case, TransactionMetadataObservation::Continued);
    let (capture, results, refs) = original(&source, &values, 4);
    let pairs = refs.iter().zip(&results).collect::<Vec<_>>();
    let answer =
        TransactionMetadataAnswer::from_originals(&focus, &fresh.source, &rows[0], &source, &capture, &pairs).unwrap();
    let pending = episode.enroll_source_read(focus.clone()).unwrap();
    let episode = revise(&episode, pending, "2020-01-01T12:01:11Z");
    let collecting = episode.transition_source_read(&focus, SourceReadStageState::Collecting).unwrap();
    let episode = revise(&episode, collecting, "2020-01-01T12:01:12Z");
    let answered = episode
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    let revision = DiagnosticRevision::between(Some(&episode), answered, "2020-01-01T12:02:00Z").unwrap().unwrap();
    assert_eq!(revision.previous_revision.as_ref(), Some(&episode.revision));
    assert_eq!(revision.episode.first, first.episode.first);
    assert_eq!(revision.episode.source_reads.iter().count(), 2);
    let mut checkpoint = DiagnosticCheckpointPart {
        id: String::new(),
        controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
        target: source.target.clone(),
        input: capture.clone(),
        part: 0,
        parts: 1,
        episodes: vec![revision.episode.clone()],
    };
    checkpoint.id = checkpoint.expected_id().unwrap();
    let source = CollectionDescription {
        target: source.target,
        partition: SourcePartition::Target,
        source: DIAGNOSTICS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Diagnostics(DiagnosticCaptureDescription {
            controller_version: DIAGNOSTIC_CONTROLLER_VERSION,
            parent: capture,
            step_id: digest(b"metadata-continuation-history"),
            analyzed_at: revision.analyzed_at.clone(),
            records: 1,
            checkpoint_records: 1,
            scope_bindings: [(revision.episode.scope.key().unwrap(), revision.episode.scope.clone())].into(),
        }),
    };
    let records = vec![
        TelemetryRecord::Operational(OperationalObservation {
            source: DIAGNOSTICS_SOURCE.into(),
            key: revision.id.clone(),
            fact: OperationalFact::DiagnosticRevision(Box::new(revision)),
        }),
        TelemetryRecord::DiagnosticCheckpoint(checkpoint),
    ];
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}
