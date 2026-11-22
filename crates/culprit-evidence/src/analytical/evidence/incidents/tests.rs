use super::*;
use crate::analytical::test_support;
use serde_json::json;

fn original() -> DiagnosticEvidenceRef {
    let (source, rows) = test_support::diagnostic_evidence_cases().remove(0);
    let row = rows.into_iter().find(|row| matches!(row, TelemetryRecord::Operational(_))).unwrap();
    let capture = test_support::fixture().0.capture;
    let row = ExportedRecord::new(&source, &capture, row).unwrap();
    DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled original manifest"), &row, 0).unwrap()
}
fn request() -> IncidentDiscoveryRequest {
    IncidentDiscoveryRequest {
        selection: IncidentCandidateSelection::Around {
            database: IncidentDatabaseSelector::from_key(&digest(b"route hint only")).unwrap(),
            from: "2020-01-01T12:00:00Z".into(),
            until: "2020-01-01T12:10:00Z".into(),
        },
        recorded_before: "2020-01-01T14:00:00Z".into(),
        after: None,
        page_rows: 2,
    }
}
fn hint(number: u64) -> IncidentCandidateHint {
    let request = request();
    IncidentCandidateHint::from_values(json!({
        "EpisodeId":digest(b"one native episode"), "ControllerVersion":1, "Revision":number.to_string(),
        "RangeId":digest(b"one derived range"),"BatchId":digest(b"one derived batch"),"RowOrdinal":number,
        "RevisionId":digest(number.to_string().as_bytes()),"DatabaseSelector":request.selection.database().key(),
        "OriginalReference":original(),"OriginalObservedFrom":"2020-01-01T12:00:01Z","OriginalObservedUntil":"2020-01-01T12:00:02Z",
        "RecordedAt":"2020-01-01T12:20:00Z","OriginalIntervalId":null,"OriginalIntervalFrom":null,"OriginalIntervalUntil":null
    }).as_object().unwrap()).unwrap()
}
#[test]
fn candidate_lookahead_is_charged_and_cursor_orders_exact_decimal_revisions_without_signed_narrowing() {
    let first = page(&request(), vec![hint(1), hint(2), hint(10)], EvidenceReadMetrics::default()).unwrap();
    assert_eq!(first.charged_candidate_rows, 3);
    assert_eq!(first.candidates.len(), 2);
    let mut next = request();
    next.after = first.next;
    assert_eq!(next.after.as_ref().unwrap().position.revision, "2");
    let mut maximum = hint(10);
    maximum.position.revision = u64::MAX.to_string();
    let result = page(&next, vec![hint(10), maximum], EvidenceReadMetrics::default()).unwrap();
    assert!(result.next.is_none());
    assert_eq!(result.candidates[1].position.revision, u64::MAX.to_string());
    assert!(page(&next, vec![hint(2)], EvidenceReadMetrics::default()).is_err());
}
#[test]
fn foreign_route_cursor_scope_and_unordered_or_future_candidates_are_refused() {
    let mut foreign = hint(1);
    foreign.database = IncidentDatabaseSelector::from_key(&digest(b"another route")).unwrap();
    assert!(page(&request(), vec![foreign], EvidenceReadMetrics::default()).is_err());
    assert!(page(&request(), vec![hint(2), hint(1)], EvidenceReadMetrics::default()).is_err());
    let mut future = hint(1);
    future.recorded_at = "2020-01-02T00:00:00Z".into();
    assert!(page(&request(), vec![future], EvidenceReadMetrics::default()).is_err());
    let mut changed = request();
    changed.after = page(&request(), vec![hint(1), hint(2), hint(3)], EvidenceReadMetrics::default()).unwrap().next;
    changed.recorded_before = "2020-01-01T15:00:00Z".into();
    assert!(changed.validate().is_err());
}
#[test]
fn around_uses_independent_utc_capture_recording_or_history_interval_never_offset_free_native_labels() {
    let request = request();
    assert!(
        request
            .accepts_original_clocks("2020-01-01T11:00:00Z", "2020-01-01T11:00:01Z", "2020-01-01T12:01:00Z", None)
            .unwrap()
    );
    let interval = TelemetryInterval {
        native_id: "original interval".into(),
        from: "2020-01-01T12:00:00Z".into(),
        until: "2020-01-01T13:00:00Z".into(),
    };
    assert!(
        request
            .accepts_original_clocks(
                "2020-01-01T11:00:00Z",
                "2020-01-01T11:00:01Z",
                "2020-01-01T13:30:00Z",
                Some(&interval)
            )
            .unwrap()
    );
    assert!(
        !request
            .accepts_original_clocks("2020-01-01T11:00:00Z", "2020-01-01T11:00:01Z", "2020-01-01T13:30:00Z", None)
            .unwrap()
    );
    assert!(
        request
            .accepts_original_clocks("2020-01-01T12:00:00", "2020-01-01T12:00:01", "2020-01-01T13:30:00Z", None)
            .is_err()
    );
    // A rounded search hint cannot authorize an out-of-window original at the end boundary.
    assert!(
        !request
            .accepts_original_clocks(
                "2020-01-01T12:10:00.000000001Z",
                "2020-01-01T12:10:00.000000002Z",
                "2020-01-01T13:30:00Z",
                None
            )
            .unwrap()
    );
}
#[test]
fn discovery_validates_window_page_and_required_lookahead_before_any_native_store_call() {
    let request = request();
    assert_eq!(
        request
            .within_budget(EvidenceReadBudget { page_rows: 1, max_rows: 2, ..Default::default() })
            .unwrap()
            .page_rows,
        1
    );
    assert!(request.validate_budget(EvidenceReadBudget { max_rows: 2, ..Default::default() }).is_err());
    assert!(request.validate_budget(EvidenceReadBudget { max_rows: 3, ..Default::default() }).is_ok());
    let mut malformed = request.clone();
    malformed.selection = IncidentCandidateSelection::Around {
        database: request.selection.database().clone(),
        from: "2020-01-01T00:00:00Z".into(),
        until: "2020-01-03T00:00:00Z".into(),
    };
    assert!(malformed.validate().is_err());
    let empty = page(&request, vec![], EvidenceReadMetrics::default()).unwrap();
    assert!(empty.candidates.is_empty() && empty.next.is_none());
    assert_eq!(empty.charged_candidate_rows, 0, "empty candidate search supplies no native population verdict");
}
