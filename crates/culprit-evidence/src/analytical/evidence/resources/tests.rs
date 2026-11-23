use super::*;
use culprit_core::{PublishedResourceRequest, PublishedResourceWindow};
use serde_json::json;
fn contract() -> Contract {
    Contract::new("pipeline".into(), "shard".into(), "test:warehouse".into(), "a".repeat(64)).unwrap()
}
fn request() -> PublishedResourceQuery {
    PublishedResourceQuery {
        selection: PublishedResourceRequest {
            as_of: "2026-11-22T12:00:00Z".into(),
            window: None,
            after: None,
            page_rows: 2,
        },
        recovery_store_id: "b".repeat(64),
        database: None,
        clock: PublishedResourceClock::Acquisition,
        content: PublishedResourceContent::All,
    }
}
fn values(n: u32) -> Map<String, Value> {
    json!({"ResourceId":format!("{n:064x}"),"SourceId":"c".repeat(64),"RangeId":"d".repeat(64),
        "DatabaseSelector":null,"IntervalFrom":null,"IntervalUntil":null,"LogicalSourceId":"e".repeat(64),"ManifestSha256":"f".repeat(64),"ResourceKind":"database",
        "ContractId":contract().id().unwrap(),"SchemaDigest":contract().schema_digest(),"RecoveryStoreId":request().recovery_store_id,
        "CollectedFrom":"2026-11-22T10:00:00Z","CollectedUntil":"2026-11-22T10:00:01Z"}).as_object().unwrap().clone()
}
fn hint(n: u32) -> PublishedResourceHint {
    PublishedResourceHint::from_values(&contract(), &request(), &values(n)).unwrap()
}
#[test]
fn resource_lookahead_cursor_keeps_original_last_returned_position_and_charges_all_candidates() {
    let first = page(&contract(), &request(), vec![hint(1), hint(2), hint(3)], EvidenceReadMetrics::default()).unwrap();
    assert_eq!(first.candidates.len(), 2);
    assert_eq!(first.charged_candidate_rows, 3);
    let mut next = request();
    next.selection.after = first.next;
    assert_eq!(next.selection.after.as_ref().unwrap().position, hint(2).position);
    assert!(page(&contract(), &next, vec![hint(3)], EvidenceReadMetrics::default()).unwrap().next.is_none());
    assert!(page(&contract(), &next, vec![hint(2)], EvidenceReadMetrics::default()).is_err());
    assert!(page(&contract(), &request(), vec![hint(2), hint(1)], EvidenceReadMetrics::default()).is_err());
}
#[test]
fn resource_cursor_cannot_cross_namespace_contract_cutoff_or_window() {
    let cursor =
        page(&contract(), &request(), vec![hint(1), hint(2), hint(3)], EvidenceReadMetrics::default()).unwrap().next;
    let mut selected = request();
    selected.selection.after = cursor;
    assert!(selected.validate(&contract()).is_ok());
    selected.recovery_store_id = "c".repeat(64);
    assert!(selected.validate(&contract()).is_err());
    selected.recovery_store_id = request().recovery_store_id;
    selected.selection.as_of = "2026-11-22T13:00:00Z".into();
    assert!(selected.validate(&contract()).is_err());
    selected.selection.as_of = request().selection.as_of;
    selected.selection.window =
        Some(PublishedResourceWindow { from: "2026-11-22T10:00:00Z".into(), until: "2026-11-22T11:00:00Z".into() });
    assert!(selected.validate(&contract()).is_err());
    selected.selection.window = None;
    let other = Contract::new("other".into(), "shard".into(), "test:warehouse".into(), "a".repeat(64)).unwrap();
    assert!(selected.validate(&other).is_err());
}
#[test]
fn publication_hints_refuse_foreign_non_database_future_unknown_clock_and_malformed_keys() {
    for (key, value) in [
        ("ContractId", json!("a".repeat(64))),
        ("SchemaDigest", json!("b".repeat(64))),
        ("RecoveryStoreId", json!("c".repeat(64))),
        ("ResourceKind", json!("instance")),
        ("ResourceId", json!("label")),
        ("CollectedUntil", json!("2026-11-22T12:00:00.000000001Z")),
        ("CollectedFrom", json!("2026-11-22T10:00:00")),
    ] {
        let mut candidate = values(1);
        candidate.insert(key.into(), value);
        assert!(PublishedResourceHint::from_values(&contract(), &request(), &candidate).is_err(), "{key}");
    }
}
#[test]
fn resource_page_reserves_lookahead_without_inventing_a_native_population() {
    assert!(request().within_budget(&contract(), EvidenceReadBudget { max_rows: 1, ..Default::default() }).is_err());
    assert_eq!(
        request()
            .within_budget(&contract(), EvidenceReadBudget { max_rows: 2, page_rows: 1, ..Default::default() })
            .unwrap()
            .selection
            .page_rows,
        1
    );
    let empty = page(&contract(), &request(), vec![], EvidenceReadMetrics::default()).unwrap();
    assert!(empty.candidates.is_empty() && empty.next.is_none());
    assert_eq!(empty.metrics.selected_rows, 0);
}

#[test]
fn native_window_discovery_keeps_late_history_empty_analysis_and_unplaced_context_without_borrowing_acquisition_time() {
    let mut request = request();
    let database = IncidentDatabaseSelector::from_key(&"a".repeat(64)).unwrap();
    request.database = Some(database.clone());
    request.clock = PublishedResourceClock::NativeWindowCandidates;
    request.selection.window =
        Some(PublishedResourceWindow { from: "2020-01-01T12:00:00Z".into(), until: "2020-01-01T13:00:00Z".into() });
    let mut row = values(1);
    row.insert("DatabaseSelector".into(), json!(database.key()));
    row.insert("IntervalFrom".into(), json!("2020-01-01T12:00:00Z"));
    row.insert("IntervalUntil".into(), json!("2020-01-01T13:00:00Z"));
    assert!(
        PublishedResourceHint::from_values(&contract(), &request, &row).is_ok(),
        "acquired in2026, native interval2020"
    );
    request.clock = PublishedResourceClock::Acquisition;
    assert!(PublishedResourceHint::from_values(&contract(), &request, &row).is_err());
    request.clock = PublishedResourceClock::NativeWindowCandidates;
    row.insert("IntervalFrom".into(), Value::Null);
    row.insert("IntervalUntil".into(), Value::Null);
    assert!(
        PublishedResourceHint::from_values(&contract(), &request, &row).is_ok(),
        "empty Gold/unplaced context stays a candidate, not an evaluated native window"
    );
    row.insert("DatabaseSelector".into(), json!("b".repeat(64)));
    assert!(PublishedResourceHint::from_values(&contract(), &request, &row).is_err());
}
#[test]
fn native_selection_cursor_binds_database_and_clock_and_still_charges_lookahead() {
    let mut request = request();
    request.database = Some(IncidentDatabaseSelector::from_key(&"a".repeat(64)).unwrap());
    let mut hints = vec![hint(1), hint(2), hint(3)];
    for hint in &mut hints {
        hint.database = request.database.clone();
    }
    request.selection.after = page(&contract(), &request, hints, EvidenceReadMetrics::default()).unwrap().next;
    assert!(request.validate(&contract()).is_ok());
    request.database = Some(IncidentDatabaseSelector::from_key(&"b".repeat(64)).unwrap());
    assert!(request.validate(&contract()).is_err());
    request.database = Some(IncidentDatabaseSelector::from_key(&"a".repeat(64)).unwrap());
    request.clock = PublishedResourceClock::NativeWindowCandidates;
    request.selection.window =
        Some(PublishedResourceWindow { from: "2020-01-01T12:00:00Z".into(), until: "2020-01-01T13:00:00Z".into() });
    assert!(request.validate(&contract()).is_err());
    request.selection.after = None;
    assert!(request.validate(&contract()).is_ok());
    request.database = None;
    assert!(request.validate(&contract()).is_err(), "conservative native search requires one selected route");
}

#[test]
fn derived_header_selection_is_protocol_typed_and_cursor_bound_without_changing_all_reader_defaults() {
    let all = request();
    let mut selected = all.clone();
    selected.content = PublishedResourceContent::DerivedDiagnosis;
    assert_ne!(all.key(&contract()).unwrap(), selected.key(&contract()).unwrap());
    assert!(!selected.content.accepts_publication(&values(1)));
    let mut derived = values(1);
    derived.insert("SourceDescription".into(), json!({"observation":{"kind":"derived-diagnosis"}}));
    let candidate = PublishedResourceHint::from_values(&contract(), &selected, &derived).unwrap();
    let result = page(
        &contract(),
        &selected,
        vec![
            candidate.clone(),
            {
                let mut next = candidate.clone();
                next.position.resource_id = format!("{:064x}", 2);
                next
            },
            {
                let mut next = candidate;
                next.position.resource_id = format!("{:064x}", 3);
                next
            },
        ],
        EvidenceReadMetrics::default(),
    )
    .unwrap();
    let mut foreign_content = all;
    foreign_content.selection.after = result.next;
    assert!(foreign_content.validate(&contract()).is_err());
    let mut ordinary = derived.clone();
    ordinary["SourceDescription"] = json!({"observation":{"kind":"query-history"}});
    assert!(PublishedResourceHint::from_values(&contract(), &selected, &ordinary).is_err());
    assert!(request().content.accepts_publication(&ordinary));
}
