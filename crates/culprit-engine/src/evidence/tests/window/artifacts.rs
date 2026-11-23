//! Actual canonical capture/publication/index/body proofs exercise details; no source/native connection runs.
use super::*;
use crate::collected_resources::references;
use crate::evidence::EvidenceService;
use culprit_core::telemetry::digest;
use culprit_evidence::analytical::evidence::RecoveryPayloadBudget;

async fn long_sql() -> (Fixture, WarehouseArtifactQuery, String) {
    let (source, mut rows) = crate::collected_evidence::window_test_original(6, Some("artifact-native-epoch"));
    let sql =
        format!("SELECT {} FROM orders", (0..350).map(|index| format!("field_é{index}")).collect::<Vec<_>>().join(","));
    let definition = rows
        .iter_mut()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(value) => Some(value),
            _ => None,
        })
        .unwrap();
    definition.sql = DefinitionPayload::sql(&sql, source.target.engine());
    let DefinitionPayload::Available { text, sha256, .. } = &definition.sql else { panic!() };
    assert!(text.len() > 1024, "the actual1024-byte public preview must be insufficient");
    let expected = text.clone();
    let expected_sha = sha256.clone();
    let fixture = Fixture::capture(source, rows).await;
    fixture.objects.allow_payloads.store(true, Ordering::SeqCst);
    let token = CollectionCancellation::default();
    let mut session = fixture.service.session(IncidentRequest::default_budget(), &token).unwrap();
    let capture = session.read(&fixture.request.range_id, &[EvidenceFamily::QueryRuntime]).await.unwrap();
    let row =
        capture.records().iter().find(|row| matches!(row.row().record, TelemetryRecord::QueryRuntime(_))).unwrap();
    let reference = references::from_record(&capture, row).unwrap();
    let resources = fixture
        .service
        .resources(&PublishedResourceRequest::default(), EvidenceReadBudget::default(), &token)
        .await
        .unwrap();
    let query = WarehouseArtifactQuery {
        resource: resources.resources[0].selector.clone(),
        references: vec![reference],
        full_payloads: true,
        text: WarehouseArtifactTextRead::Complete,
        expected_sql_sha256: Some(expected_sha),
    };
    (fixture, query, expected)
}
fn sql_payload(report: &WarehouseArtifactReport) -> &WarehouseDefinitionArtifact {
    report.records.iter().flat_map(|row| &row.artifacts).find(|field| field.kind == DefinitionContentKind::Sql).unwrap()
}
#[tokio::test]
async fn full_sql_and_plan_follow_exact_original_native_definition_links_and_charge_complete_recovery_objects() {
    let (fixture, query, expected) = long_sql().await;
    let before = fixture.objects.snapshot();
    let calls = fixture.store.requests.load(Ordering::SeqCst);
    let report = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(
        report.gaps.is_empty(),
        "a runtime row's lack of inlineSQL is satisfied by its exact retained definition: {:?}",
        report.gaps
    );
    let artifact = sql_payload(&report);
    assert!(
        matches!(&artifact.payload,WarehouseArtifactText::Complete {payload:DefinitionPayload::Available {text,..}} if text==&expected)
    );
    assert!(artifact.matches_requested_sql, "a content match is not an actor/cause assignment");
    assert!(report.records.iter().any(|row| !row.linked_from.is_empty()));
    assert!(
        report
            .records
            .iter()
            .flat_map(|row| &row.artifacts)
            .any(|field| field.kind == DefinitionContentKind::Plan && field.plan.is_some())
    );
    assert!(report.archive_work.recovery_objects > 0 && report.archive_work.recovery_bytes > expected.len() as u64);
    assert_eq!(report.archive_work.queries as usize, fixture.store.requests.load(Ordering::SeqCst) - calls);
    assert_eq!(fixture.objects.snapshot(), before, "no writer, publisher or source lifecycle is acquired");
}
#[tokio::test]
async fn projection_only_details_keep_missing_large_sql_explicit_without_native_or_neighbor_substitution() {
    let (fixture, mut query, expected) = long_sql().await;
    query.full_payloads = false;
    let start = fixture.objects.reads.lock().unwrap().len();
    let report = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(!report.gaps.is_empty());
    assert_eq!(report.archive_work.recovery_objects, 0);
    assert_eq!(report.archive_work.recovery_bytes, 0);
    assert!(!serde_json::to_string(&report).unwrap().contains(&expected));
    assert!(fixture.objects.reads.lock().unwrap()[start..].iter().all(|key| !key.starts_with("payloads/")));
}
#[tokio::test]
async fn source_generated_unicode_chunks_reassemble_exact_full_sha_and_never_become_complete_payloads_or_parsed_plans()
{
    let (fixture, mut query, expected) = long_sql().await;
    query.text = WarehouseArtifactTextRead::Inventory { chunk_chars: MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS };
    let inventory = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    let WarehouseArtifactText::Metadata { next: Some(first), .. } = &sql_payload(&inventory).payload else { panic!() };
    let mut cursor = Some(first.clone());
    let mut restored = String::new();
    let mut previous = 0;
    while let Some(next) = cursor.take() {
        query.text = WarehouseArtifactTextRead::Chunk { cursor: next };
        let report = fixture
            .service
            .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
            .await
            .unwrap();
        let field = sql_payload(&report);
        assert!(field.plan.is_none());
        let WarehouseArtifactText::Chunk { from_character, until_character, text, next, sha256, .. } = &field.payload
        else {
            panic!()
        };
        assert_eq!(*from_character, previous);
        assert!(*until_character > *from_character);
        assert!(!text.is_empty());
        assert_eq!(Some(sha256), query.expected_sql_sha256.as_ref());
        previous = *until_character;
        restored.push_str(text);
        cursor = next.clone();
        assert!(report.archive_work.recovery_objects > 0, "each action honestly pays its complete body/EOF proof");
    }
    assert_eq!(restored, expected);
    assert_eq!(digest(restored.as_bytes()), query.expected_sql_sha256.unwrap());
}
#[tokio::test]
async fn foreign_payload_location_offsets_and_nonprogress_continuations_are_refused_without_a_path_or_current_fallback()
{
    let (fixture, mut query, _) = long_sql().await;
    query.text = WarehouseArtifactTextRead::Inventory { chunk_chars: 128 };
    let inventory = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    let WarehouseArtifactText::Metadata { next: Some(cursor), .. } = &sql_payload(&inventory).payload else { panic!() };
    for kind in 0..4 {
        let mut cursor = cursor.clone();
        match kind {
            0 => {
                cursor.previous_character = Some(cursor.next_character);
            }
            1 => {
                cursor.location = "/record/data/../../private-file".into();
            }
            2 => {
                cursor.payload_sha256 = digest(b"foreign payload");
            }
            _ => {
                cursor.previous_character = Some(u32::MAX - 128);
                cursor.next_character = u32::MAX;
            }
        }
        query.text = WarehouseArtifactTextRead::Chunk { cursor };
        assert!(
            fixture
                .service
                .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
                .await
                .is_err()
        );
    }
    let mut foreign = query.clone();
    foreign.resource.namespace_id = digest(b"foreign namespace");
    assert_eq!(
        fixture
            .service
            .artifacts(&foreign, EvidenceService::artifact_budget(&foreign), &CollectionCancellation::default())
            .await
            .unwrap_err()
            .detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::DestinationChanged))
    );
}
#[tokio::test]
async fn artifact_whole_object_budget_and_cancellation_keep_the_same_existing_owned_reader() {
    let (fixture, query, _) = long_sql().await;
    let start = fixture.objects.reads.lock().unwrap().len();
    let budget = EvidenceReadBudget {
        recovery_payloads: Some(RecoveryPayloadBudget { max_objects: 1, max_bytes: 1 }),
        ..Default::default()
    };
    assert_eq!(
        fixture.service.artifacts(&query, budget, &CollectionCancellation::default()).await.unwrap_err().detail,
        Some(ErrorDetail::Delivery(DeliveryProblem::Capacity))
    );
    assert!(
        fixture.objects.reads.lock().unwrap()[start..].iter().all(|key| !key.starts_with("payloads/")),
        "whole object admission precedes payload I/O"
    );
    fixture.objects.stall.store(true, Ordering::SeqCst);
    let token = CollectionCancellation::default();
    let read = fixture.service.artifacts(&query, EvidenceService::artifact_budget(&query), &token);
    let (result, _) = tokio::join!(read, async {
        while fixture.objects.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        token.cancel();
    });
    assert_eq!(result.unwrap_err().detail, Some(ErrorDetail::CollectionStopped(CollectionStop::Cancelled)));
    assert_eq!(fixture.objects.active.load(Ordering::SeqCst), 0);
}

#[test]
fn native_definition_binding_preserves_scope_parent_incarnation_and_independent_cache_meaning() {
    let (source, rows) = crate::collected_evidence::window_test_original(6, Some("native-binding-epoch"));
    let key = rows.iter().find_map(|row| row.measurement_key()).unwrap();
    let query = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::QueryDefinition(value)
                if Some(&value.id)
                    == key.references.query.as_ref().and_then(|reference| reference.definition_id.as_ref()) =>
            {
                Some(value)
            }
            _ => None,
        })
        .unwrap();
    let plan = rows
        .iter()
        .find_map(|row| match row {
            TelemetryRecord::PlanDefinition(value)
                if Some(&value.id)
                    == key.references.plan.as_ref().and_then(|reference| reference.definition_id.as_ref()) =>
            {
                Some(value)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(key.references.bind_query_definition(&source, Some(query)).unwrap(), Some(query));
    assert_eq!(key.references.bind_plan_definition(&source, Some(plan)).unwrap(), Some(plan));
    let mut foreign = source.clone();
    let CollectionTarget::Database(database) = &mut foreign.target else { panic!() };
    database.incarnation = Some("another-native-epoch".into());
    assert!(key.references.bind_query_definition(&foreign, Some(query)).is_err());
    assert!(key.references.bind_plan_definition(&foreign, Some(plan)).is_err());
    let mut changed = plan.clone();
    changed.query_definition_id = Some(digest(b"unrelated native query"));
    assert!(key.references.bind_plan_definition(&source, Some(&changed)).is_err());
    changed.query_definition_id = None;
    assert!(
        key.references.bind_plan_definition(&source, Some(&changed)).unwrap().is_none(),
        "an independent cached plan cannot become this runtime's plan"
    );
    changed = plan.clone();
    changed.identity.qualifiers.insert("incarnation".into(), "changed".into());
    assert!(key.references.bind_plan_definition(&source, Some(&changed)).is_err());
}
#[tokio::test]
async fn a_missing_original_stays_partial_and_manifest_tampering_never_publishes_artifacts() {
    // The secondary index can lag the authoritative head during an admitted atomic commit. Removing it
    // does not make that exact head uncommitted: the SAME original still receives full proof/body checks.
    let (fixture, query, expected) = long_sql().await;
    fixture.objects.remove_range_index(&fixture.request.range_id);
    let report = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(report.gaps.is_empty());
    assert!(
        matches!(&sql_payload(&report).payload,WarehouseArtifactText::Complete {payload:DefinitionPayload::Available {text,..}} if text==&expected)
    );
    assert!(report.records.iter().all(|record| record.reference.capture == query.references[0].capture));
    assert!(report.archive_work.proof_bytes > 0 && report.archive_work.recovery_objects > 0);

    // Move the authoritative head through the real archive writer first. A missing NON-head exact index
    // now cannot establish this original as committed; a valid neighboring capture is not a substitute.
    let (fixture, query, _) = long_sql().await;
    let (source, rows) = crate::collected_evidence::window_test_original(7, Some("artifact-native-epoch"));
    let neighbor = fixture.append_capture(source, rows, 100_001).await;
    assert_ne!(neighbor.manifest().capture, query.references[0].capture);
    fixture.objects.remove_range_index(&fixture.request.range_id);
    let read_start = fixture.objects.reads.lock().unwrap().len();
    let queries = fixture.store.requests.load(Ordering::SeqCst);
    let report = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap();
    assert!(report.records.is_empty());
    assert!(
        matches!(report.gaps.as_slice(), [WarehouseArtifactGap::OriginalNotCommitted {capture}] if capture==&query.references[0].capture_ref())
    );
    assert_eq!(report.archive_work.selected_rows, 0);
    assert_eq!(report.archive_work.recovery_objects, 0);
    assert!(report.archive_work.proof_bytes > 0, "absence still authenticates the same current root");
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), queries, "no neighboring warehouse row is requested");
    assert!(
        fixture.objects.reads.lock().unwrap()[read_start..].iter().all(|key| !key.starts_with("payloads/")),
        "absence cannot fall through to another native body"
    );
    let (fixture, query, _) = long_sql().await;
    fixture.objects.corrupt_manifest();
    let error = fixture
        .service
        .artifacts(&query, EvidenceService::artifact_budget(&query), &CollectionCancellation::default())
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Integrity)));
}
#[tokio::test]
async fn artifact_future_retains_its_one_reader_on_the_heap_and_unpolled_work_has_no_io() {
    let (fixture, query, _) = long_sql().await;
    let token = CollectionCancellation::default();
    let before = fixture.store.requests.load(Ordering::SeqCst);
    let read = fixture.service.artifacts(&query, EvidenceService::artifact_budget(&query), &token);
    assert!(std::mem::size_of_val(&read) <= 4096);
    drop(read);
    assert_eq!(fixture.store.requests.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn injected_artifact_capability_delegates_to_the_same_exact_original_reader() {
    let (fixture, query, expected) = long_sql().await;
    let report =
        crate::WarehouseIncidentReader::artifacts(&fixture.service, &query, &CollectionCancellation::default())
            .await
            .unwrap();
    assert!(
        matches!(&sql_payload(&report).payload,WarehouseArtifactText::Complete{payload:DefinitionPayload::Available{text,..}} if text==&expected)
    );
    assert!(report.archive_work.recovery_objects > 0);
}
#[tokio::test]
async fn friendly_numbered_window_detail_reuses_one_action_allowance_and_never_opens_a_source() {
    let (fixture, _, _) = long_sql().await;
    super::publish_current_findings(&fixture, 100_001, super::PublishedCase::Genuine).await;
    let window = PublishedResourceWindow { from: "2020-01-01T06:00:00Z".into(), until: "2020-01-01T08:00:00Z".into() };
    let request = crate::evidence::NamedWindowQuery {
        resource_name: "orders",
        window: &window,
        as_of: "2030-01-01T00:00:00Z",
        view: WarehouseWindowView::PublishedAnswers,
        page_rows: 1,
        after: None,
    };
    // The real selected resource label comes from the authenticated native route, not this fixture nickname.
    let resources = fixture
        .service
        .resources(
            &PublishedResourceRequest::default(),
            EvidenceReadBudget::default(),
            &CollectionCancellation::default(),
        )
        .await
        .unwrap();
    let name = resources
        .resources
        .iter()
        .find(|row| row.source_name != DERIVED_DIAGNOSIS_SOURCE)
        .unwrap()
        .database_name
        .clone();
    let request = crate::evidence::NamedWindowQuery { resource_name: &name, ..request };
    let token = CollectionCancellation::default();
    let before = fixture.objects.snapshot();
    let window_report =
        fixture.service.window_named(&request, IncidentRequest::default_budget(), &token).await.unwrap();
    let choices = crate::evidence::window_artifact_choices(&window_report);
    assert!(!choices.is_empty(), "the actual published finding exposes its original row");
    let expected = choices[0].references.iter().map(|reference| (**reference).clone()).collect::<Vec<_>>();
    let action_start = fixture.store.requests.load(Ordering::SeqCst);
    let detail =
        fixture.service.window_artifact_named(&request, 1, IncidentRequest::default_budget(), &token).await.unwrap();
    assert!(detail.query.references.iter().all(|reference| expected.contains(reference)));
    let paid = fixture.store.requests.load(Ordering::SeqCst) - action_start;
    assert_eq!(
        detail.archive_work.queries as usize, paid,
        "name discovery, window and full detail all remain paid by one action"
    );
    assert!(detail.archive_work.recovery_objects > 0);
    assert_eq!(fixture.objects.snapshot(), before);
    let action_start = fixture.store.requests.load(Ordering::SeqCst);
    let error = fixture
        .service
        .window_artifact_named(
            &request,
            1,
            EvidenceReadBudget { max_queries: 1, ..IncidentRequest::default_budget() },
            &token,
        )
        .await
        .unwrap_err();
    assert_eq!(error.detail, Some(ErrorDetail::Delivery(DeliveryProblem::Capacity)));
    assert!(fixture.store.requests.load(Ordering::SeqCst) <= action_start + 1);
}
