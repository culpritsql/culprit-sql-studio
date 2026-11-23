use super::*;
use culprit_core::{
    events::{EventKind, EventSource},
    limits::OperationGuard,
    telemetry::MssqlExtendedEventKind,
};
use std::io::Write;
fn xml(at: &str) -> String {
    format!(
        "<event name=\"attention\" package=\"sqlserver\" timestamp=\"{at}\"><action name=\"session_id\" package=\"sqlserver\"><value>59</value></action><action name=\"client_app_name\" package=\"sqlserver\"><value>native-driver-label</value></action><action name=\"sql_text\" package=\"sqlserver\"><value>SELECT 'private-sql-canary'</value></action></event>"
    )
}
fn analyze(xml: &str) -> NativeLogAnalysis {
    analyze_bytes(xml.as_bytes(), NativeLogFormat::MssqlExtendedEventsXml, Default::default()).unwrap()
}
#[test]
fn explicit_xml_intake_reuses_the_native_failure_view_and_places_only_qualified_artifact_utc_without_ownership() {
    let input = format!("<events>{}{}</events>", xml("2020-01-01T14:00:00+02:00"), xml("2026-10-25T03:30:00"));
    let result = analyze(&input);
    assert!(result.is_read_complete());
    assert!(result.evidence.resource.is_none());
    assert_eq!(result.input.sha256, digest(input.as_bytes()));
    assert_eq!(
        result.report,
        culprit_checks::native_failures::explain(&result.evidence, Some(&result.source)).unwrap()
    );
    let events = result.native_events.as_ref().unwrap();
    assert_eq!(events.timeline.len(), 1);
    assert_eq!(events.unplaced_positions, [1]);
    assert_eq!(events.timeline[0].kind, EventKind::NativeEvent);
    assert_eq!(events.timeline[0].source, EventSource::ImportedArtifact);
    assert_eq!(events.timeline[0].at, "2020-01-01T12:00:00.000000000Z");
    assert!(events.timeline[0].table.is_none());
    let NativeFailureRecord::MssqlExtendedEvent(row) = &result.evidence.observations[0] else { panic!() };
    assert!(matches!(row.kind, MssqlExtendedEventKind::Attention { .. }));
    assert_eq!(row.client.as_ref().unwrap().application.as_deref(), Some("native-driver-label"));
    assert!(!serde_json::to_string(&result).unwrap().contains("private-sql-canary"));
    assert_ne!(row.timestamp_utc.as_deref(), Some(result.evidence.observed_until.as_str()));
}
#[test]
fn selected_regular_xml_file_has_the_same_artifact_and_report_and_unknown_prefixes_never_become_healthy_absence() {
    let input = xml("2020-01-01T12:00:00Z");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(input.as_bytes()).unwrap();
    file.flush().unwrap();
    let actual = analyze_file(file.path(), NativeLogFormat::MssqlExtendedEventsXml, Default::default()).unwrap();
    let expected = analyze(&input);
    assert_eq!(actual.input, expected.input);
    assert_eq!(actual.report, expected.report);
    assert_eq!(actual.native_events, expected.native_events);
    for bad in ["<events><event", "binary .xel is not XML", "<event name='not_supported' package='sqlserver'/>"] {
        let actual = analyze(bad);
        assert_eq!(actual.source.state, SourceState::Partial);
        assert!(!actual.is_read_complete());
        assert!(actual.evidence.observations.is_empty());
    }
    let partial = analyze_bytes(
        input.as_bytes(),
        NativeLogFormat::MssqlExtendedEventsXml,
        NativeLogLimits {
            input_bytes: input.len() - 1,
            record_bytes: input.len() - 1,
            field_bytes: 128,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!partial.is_read_complete());
    assert_eq!(partial.input.limitation, Some(NativeLogProblem::InputLimit));
    assert_eq!(partial.input.sha256, digest(&input.as_bytes()[..input.len() - 1]));
    assert!(!serde_json::to_string(&actual).unwrap().contains(&file.path().display().to_string()));
}
#[test]
fn caller_drop_expiry_and_mid_read_cancellation_stop_the_existing_bounded_owner_before_publication() {
    let guard = OperationGuard::new(LOG_IMPORT_TIMEOUT).unwrap();
    let handle = guard.handle();
    drop(guard);
    let result = analyze_file_with(
        Path::new("not-opened-private-path"),
        NativeLogFormat::MssqlExtendedEventsXml,
        Default::default(),
        &handle,
    )
    .unwrap_err();
    assert_eq!(result.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    let expired = OperationGuard::new(std::time::Duration::ZERO).unwrap();
    assert!(check(&expired.handle()).is_err());
    struct CancelRead {
        guard: Option<OperationGuard>,
        reads: usize,
    }
    impl Read for CancelRead {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.reads += 1;
            out[0] = b'<';
            self.guard.take();
            Ok(1)
        }
    }
    let guard = OperationGuard::new(LOG_IMPORT_TIMEOUT).unwrap();
    let operation = guard.handle();
    let mut reader = CancelRead { guard: Some(guard), reads: 0 };
    assert!(read_bounded(&mut reader, 8192, &operation).is_err());
    assert_eq!(reader.reads, 1);
}
