//! Controlled collector control records, never claimed as native DB availability or measured qualification.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectorConnectionCase {
    CompleteCapture,
    Success,
    Failure,
    MissingRecorder,
    Interrupted,
    Unconfirmed,
    QueryDenied,
    AuthenticatedReuse,
}
impl CollectorConnectionCase {
    pub const ALL: [Self; 8] = [
        Self::CompleteCapture,
        Self::Success,
        Self::Failure,
        Self::MissingRecorder,
        Self::Interrupted,
        Self::Unconfirmed,
        Self::QueryDenied,
        Self::AuthenticatedReuse,
    ];
}
pub fn collector_connection_evidence(case: CollectorConnectionCase) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let target = ConfiguredConnectionIdentity::new(
        "collector-fixture",
        &digest(b"saved-route-revision"),
        "approved-route",
        culprit_core::Engine::Mssql,
    )
    .unwrap();
    let capture = CaptureIdentity {
        observation_id: "a".repeat(32),
        revision: Some(CaptureRevision { outbox_id: "b".repeat(32), owner_epoch: "1".into(), sequence: "7".into() }),
    };
    let connections = if matches!(case, CollectorConnectionCase::MissingRecorder | CollectorConnectionCase::Interrupted)
    {
        None
    } else {
        let recorder = ReadConnectionRecorder::default();
        let attempt = recorder.begin();
        attempt.connection_use(if case == CollectorConnectionCase::AuthenticatedReuse {
            ReadConnectionUse::AuthenticatedReuse
        } else {
            ReadConnectionUse::FreshConnection
        });
        let acquire = attempt.phase(ReadConnectionStage::Acquire, None);
        if case == CollectorConnectionCase::Failure {
            acquire.finish(ReadConnectionOutcome::Failed {
                code: culprit_core::ErrorCode::ConnectionFailed,
                native_code: Some(18456),
            });
        } else {
            attempt.acquired(1);
            acquire.finish(ReadConnectionOutcome::Completed);
            let query = attempt.phase(ReadConnectionStage::FirstQuery, Some(digest(b"existing-tagged-template")));
            query.finish(match case {
                CollectorConnectionCase::Unconfirmed => ReadConnectionOutcome::Unconfirmed,
                CollectorConnectionCase::QueryDenied => ReadConnectionOutcome::Failed {
                    code: culprit_core::ErrorCode::PermissionDenied,
                    native_code: Some(229),
                },
                _ => ReadConnectionOutcome::Completed,
            });
        }
        recorder.snapshot()
    };
    let interrupted = case == CollectorConnectionCase::Interrupted;
    let row = CollectorConnectionObservation {
        original: DiagnosticAttemptReceipt {
            capture: capture.clone(),
            manifest_sha256: (!interrupted).then(|| digest(b"exact-controlled-terminal-attempt-manifest")),
        },
        original_status: match case {
            CollectorConnectionCase::Interrupted => None,
            CollectorConnectionCase::CompleteCapture => Some(CollectionStatus::Complete),
            _ => Some(CollectionStatus::Incomplete(CollectionStop::SourceUnavailable)),
        },
        original_state: match case {
            CollectorConnectionCase::Interrupted => OutboxCaptureState::Interrupted,
            CollectorConnectionCase::CompleteCapture => OutboxCaptureState::Ready,
            _ => OutboxCaptureState::Incomplete,
        },
        original_evidence: if interrupted {
            CollectorConnectionOriginalEvidence::ManifestUnavailable
        } else if case == CollectorConnectionCase::Failure {
            CollectorConnectionOriginalEvidence::NoNativeEvidence
        } else {
            CollectorConnectionOriginalEvidence::MayContainNativeEvidence
        },
        source_work: if matches!(case, CollectorConnectionCase::Interrupted | CollectorConnectionCase::Unconfirmed) {
            SourceWorkState::Unconfirmed
        } else {
            SourceWorkState::Quiescent
        },
        collector_outcome: if interrupted {
            CollectorCaptureOutcome::Unknown
        } else if case == CollectorConnectionCase::Failure {
            CollectorCaptureOutcome::Failed { code: culprit_core::ErrorCode::ConnectionFailed }
        } else if case == CollectorConnectionCase::QueryDenied {
            CollectorCaptureOutcome::Failed { code: culprit_core::ErrorCode::PermissionDenied }
        } else {
            CollectorCaptureOutcome::Returned
        },
        collector_started_at: "2020-01-01T06:00:00Z".into(),
        collector_finished_at: "2020-01-01T06:00:01Z".into(),
        collector_elapsed_ms: (!interrupted).then_some(1.5),
        connections,
        original_source_queries: (!interrupted).then_some(u32::from(case != CollectorConnectionCase::Failure)),
        native: None,
    };
    assert!(row.is_valid());
    let source = CollectionDescription {
        target: CollectionTarget::ConfiguredConnection(target),
        partition: SourcePartition::Target,
        source: COLLECTOR_CONNECTION_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::CollectorConnection(CollectorConnectionDescription {
            attempt: capture,
            snapshot_sha256: row.snapshot_sha256().unwrap(),
            observed_from: row.collector_started_at.clone(),
            observed_until: row.collector_finished_at.clone(),
        }),
    };
    assert!(source.partition_is_consistent());
    (source, vec![TelemetryRecord::CollectorConnection(Box::new(row))])
}
