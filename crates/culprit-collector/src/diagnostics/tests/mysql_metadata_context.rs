use super::*;
use culprit_evidence::analytical::test_support::metadata_lock_original;
#[tokio::test]
async fn sealed_native_mdl_original_enrolls_an_independent_cost_deferred_question_without_changing_primary_blocking() {
    let original = metadata_lock_original();
    let mut harness = Harness::new().await;
    let recipe = |id: &str, selector| RecipeDescriptor {
        id: id.into(),
        version: 1,
        purpose: RecipePurpose::Investigation,
        selector,
        subjects: vec![RecipeSubject::Database],
        budget: CollectionBudget::default(),
        admission: RecipeAdmission::Unqualified,
    };
    harness.policies =
        DiagnosticPolicies::for_activity("fixture", CollectionSubject::Database, ActivityPolicy::default())
            .unwrap()
            .with_recipes(
                "fixture",
                vec![
                    recipe("mysql.blocking", RecipeSelector::FocusedBlocking),
                    recipe("mysql.metadata-lock-context", RecipeSelector::FocusedMetadataLocks),
                ],
            )
            .unwrap();
    let captured = harness.capture(Frame { source: original.source, records: vec![original.original.record] }).await;
    assert_eq!(harness.work(1).await, 1);
    let checkpoint = harness.checkpoint().await;
    let episode =
        checkpoint.episodes.iter().find(|episode| matches!(episode.signal, DiagnosticSignal::LockWait { .. })).unwrap();
    assert_eq!(episode.first.capture, captured);
    assert_eq!(episode.issue, Some(DiagnosticAnalysisIssue::CostUnqualified));
    let stage = episode
        .source_reads
        .get(SourceReadQuestion::MetadataLockContext)
        .expect("the native adverse original nominates its exact independent question");
    assert!(matches!(stage.state, SourceReadStageState::Deferred { issue: DiagnosticAnalysisIssue::CostUnqualified }));
    assert_eq!(stage.focus.basis.as_ref(), &episode.first);
    assert_eq!(stage.focus.question.selector(), RecipeSelector::FocusedMetadataLocks);
    let mut primary_done = episode.clone();
    primary_done.analysis = DiagnosticAnalysisStatus::Unresolved;
    primary_done.issue = Some(DiagnosticAnalysisIssue::Unsupported);
    assert!(primary_done.is_valid());
    assert!(primary_done.has_owed_question(), "a terminal primary status cannot erase the enrolled child");
    assert!(primary_done.needs_question(&DiagnosticFocus::SourceRead { focus: Box::new(stage.focus.clone()) }));
    assert_eq!(harness.work(1).await, 0);
}
