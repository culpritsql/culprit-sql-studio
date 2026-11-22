use super::*;
use culprit_evidence::analytical::test_support::{
    TransactionMetadataCase as Case, transaction_metadata_answer, transaction_metadata_original,
    transaction_metadata_result,
};
async fn scenario(case: Case, sequence: u64) -> (EvidenceCapture, EvidenceCapture, DiagnosticEpisode) {
    let mechanism = if case == Case::MultiXact {
        TransactionMetadataWait::MultiXact
    } else {
        TransactionMetadataWait::Subtransaction
    };
    let input = transaction_metadata_original(mechanism);
    let basis = retained(input.source, vec![input.original.record], sequence).await;
    let original = basis.verified_original().unwrap();
    let reference = original.reference(0).unwrap();
    let TelemetryRecord::Activity(actor) = &basis.records()[0].row().record else { panic!() };
    let episode = DiagnosticEpisode::start(
        DiagnosticScope::from_source(basis.source()).unwrap(),
        DiagnosticSignal::TransactionMetadataWait {
            key: actor.key.clone(),
            statement_identity: actor.statement_identity.clone(),
            mechanism,
        },
        basis.source().observation.activity().unwrap().at.clone(),
        reference,
    )
    .unwrap();
    let focus = episode.source_read_focus(SourceReadQuestion::TransactionMetadata).unwrap().unwrap();
    let mut execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(basis.source()).unwrap())
            .unwrap();
    if case == Case::Standby {
        let SourceReadExecutionScope::Local(scope) = &mut execution else { panic!() };
        scope.database_writable = false;
    }
    let (source, rows) = transaction_metadata_result(&focus, &execution, case);
    let result = retained(source, rows, sequence + 1).await;
    let current = result.verified_original().unwrap();
    let references =
        (0..result.records().len()).map(|position| current.reference(position).unwrap()).collect::<Vec<_>>();
    let rows =
        references.iter().zip(result.records()).map(|(reference, row)| (reference, row.row())).collect::<Vec<_>>();
    let answer = TransactionMetadataAnswer::from_originals(
        &focus,
        basis.source(),
        basis.records()[0].row(),
        result.source(),
        current.original(),
        &rows,
    )
    .unwrap();
    let episode = episode
        .enroll_source_read(focus.clone())
        .unwrap()
        .transition_source_read(&focus, SourceReadStageState::Collecting)
        .unwrap()
        .transition_source_read(
            &focus,
            SourceReadStageState::Answered {
                answer: Box::new(SourceReadAnswer::TransactionMetadata(Box::new(answer))),
            },
        )
        .unwrap();
    (basis, result, episode)
}
#[tokio::test]
async fn direct_cache_and_multixact_context_keeps_affected_candidate_callers_and_exact_xmin_source_without_any_culprit()
{
    for (case, sequence) in [
        (Case::Overflow, 910_000),
        (Case::ReducedCacheOverflow, 910_010),
        (Case::MultiXact, 910_020),
        (Case::Standby, 910_030),
    ] {
        let (basis, result, episode) = scenario(case, sequence).await;
        let revisions = history(episode, sequence + 2).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let [incident] = output.incidents.as_slice() else { panic!() };
        assert_eq!(incident.affected_queries.len(), 1);
        assert!(incident.caller.is_some());
        assert!(incident.culprit_queries.is_empty());
        assert_eq!(
            incident.attribution_status,
            if case == Case::Standby {
                DerivedAttributionStatus::Unestablished
            } else {
                DerivedAttributionStatus::CandidatesOnly
            }
        );
        for candidate in &incident.candidate_queries {
            assert!(candidate.causal_role.is_none());
            assert!(candidate.caller.is_some());
            assert!(candidate.sql.is_available());
            assert_ne!(
                candidate.identity, incident.affected_queries[0].identity,
                "a distinct overflowing backend remains a candidate, never a victim relabeled as culprit"
            );
            assert!(!serde_json::to_string(candidate).unwrap().contains("transaction-private-canary"));
        }
        assert!(incident.mechanism.as_ref().is_some_and(|words| !words.is_empty()));
        assert!(!incident.developer_action.is_empty());
        assert!(!incident.verification.is_empty());
        assert!(
            incident
                .entry
                .reasons
                .iter()
                .any(|reason| reason.contains("pg_replication_slots") && reason.contains("1234")),
            "the exact observed xmin source is retained without a global pin claim"
        );
        assert!(output.findings.is_empty(), "point context is not an integrity or exclusive-cause finding");
    }
}
#[tokio::test]
async fn native_false_null_partial_changed_and_empty_context_are_never_a_healthy_or_unsupported_causal_answer() {
    for (index, case) in
        [Case::Clear, Case::UnknownCache, Case::Partial, Case::ChangedRequest, Case::Empty, Case::Unavailable]
            .into_iter()
            .enumerate()
    {
        let (basis, result, episode) = scenario(case, 911_000 + index as u64 * 10).await;
        let revisions = history(episode, 911_002 + index as u64 * 10).await;
        let output = project(&[&revisions, &basis, &result], vec![]).unwrap();
        let incident = &output.incidents[0];
        assert!(incident.culprit_queries.is_empty());
        assert!(!incident.entry.reasons.is_empty());
        if matches!(case, Case::Clear | Case::UnknownCache | Case::Empty | Case::Unavailable) {
            assert!(incident.candidate_queries.is_empty());
            assert_eq!(incident.attribution_status, DerivedAttributionStatus::Unestablished);
        }
        assert!(incident.mechanism.is_some());
        if matches!(case, Case::Empty | Case::Unavailable) {
            assert_eq!(result.records().len(), 0, "no dummy row represents an empty source capture");
        }
        if matches!(case, Case::Partial | Case::UnknownCache | Case::ChangedRequest) {
            assert_eq!(result.source().observation.snapshot().unwrap().coverage_state().unwrap(), SourceState::Partial);
        }
    }
}
#[test]
fn shared_pure_view_preserves_independent_slru_reset_and_observation_clocks_and_refuses_edited_authority() {
    let (answer, point) = transaction_metadata_answer(Case::Overflow);
    let point = point.unwrap();
    let report = culprit_checks::transaction_metadata::explain(&answer, Some(&point)).unwrap();
    assert_eq!(report.observed_overflows.len(), 1);
    assert_eq!(report.observed_xmin_sources.len(), 1);
    let mut later = point.clone();
    later.slrus[0].observed_at = "2020-01-01T12:00:03.500000Z".into();
    later.slrus[0].stats_reset = Some("2020-01-01T12:00:03.600000Z".into());
    let report = culprit_checks::transaction_metadata::explain(&answer, Some(&later)).unwrap();
    assert!(
        report.limitations.len()
            > culprit_checks::transaction_metadata::explain(&answer, Some(&point)).unwrap().limitations.len()
    );
    let mut outside = point.clone();
    outside.slrus[0].observed_at = "2020-01-01T12:00:05Z".into();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&outside)).is_err());
    let mut edited = answer.clone();
    edited.overflowed_backends.clear();
    assert!(culprit_checks::transaction_metadata::explain(&edited, Some(&point)).is_err());
    let mut edited = point.clone();
    edited.database = "foreign".into();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited)).is_err());
    let mut edited = point.clone();
    edited.caches[1].actor.key.session = edited.caches[0].actor.key.session.clone();
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited)).is_err());
    let mut edited = point.clone();
    edited.slrus[0].blocks_hit = Some("1.0".into());
    assert!(culprit_checks::transaction_metadata::explain(&answer, Some(&edited)).is_err());
    assert!(
        culprit_checks::transaction_metadata::explain(&answer, None).unwrap().observed_overflows.is_empty(),
        "missing authenticated context cannot reuse compact counts as query evidence"
    );
}
#[test]
fn required_nullable_cache_and_slru_fields_roundtrip_null_without_a_compatibility_default() {
    let (_, point) = transaction_metadata_answer(Case::UnknownCache);
    let point = point.unwrap();
    let mut json = serde_json::to_value(&point).unwrap();
    assert!(json["caches"][0]["overflowed"].is_null());
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json.clone()).unwrap().is_valid());
    json["caches"][0].as_object_mut().unwrap().remove("overflowed");
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json).is_err());
    let mut json = serde_json::to_value(&point).unwrap();
    json["slrus"][0]["stats_reset"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json.clone()).unwrap().is_valid());
    json["slrus"][0].as_object_mut().unwrap().remove("stats_reset");
    assert!(serde_json::from_value::<PostgresTransactionMetadata>(json).is_err());
}
