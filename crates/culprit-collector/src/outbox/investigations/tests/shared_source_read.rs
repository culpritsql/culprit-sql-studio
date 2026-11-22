//! Structural retained-result eligibility only. Controlled receipts confer no native cost approval.
use super::*;
use culprit_evidence::analytical::test_support::{
    investigation_fixture, worker_starvation_basis, worker_starvation_result,
};
fn fixture() -> (CollectionDescription, DiagnosticInvestigationLineage) {
    let (source, rows) = worker_starvation_basis(true);
    let capture = CaptureIdentity {
        observation_id: "controlled-shared-worker-basis".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-shared-worker-owner".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    let original = ExportedRecord::new(&source, &capture, rows[0].clone()).unwrap();
    let reference =
        DiagnosticEvidenceRef::from_export(&source, &digest(b"controlled-shared-worker-manifest"), &original, 0)
            .unwrap();
    let observed = observe_diagnostic_original(
        &source,
        &reference.capture_ref(),
        &[DiagnosticOriginalRow { reference: &reference, record: &original.record }],
        None,
        &[],
        "2020-01-01T12:00:02Z",
    )
    .unwrap();
    let episode = observed.checkpoint.episodes[0].clone();
    let focus = episode.source_read_focus(SourceReadQuestion::WorkerStarvation).unwrap().unwrap();
    let execution =
        SourceReadExecutionScope::from_context(focus.question, &focus.original_context(&source).unwrap()).unwrap();
    let mut recipe = investigation_fixture().recipe;
    recipe.id = "fixture.worker-starvation-result".into();
    recipe.selector = focus.question.selector();
    recipe.subjects = vec![RecipeSubject::Shared(SharedResourceKind::Instance)];
    let RecipeAdmission::Measured(receipt) = &mut recipe.admission else { panic!() };
    receipt.recipe_id = recipe.id.clone();
    receipt.selector = recipe.selector;
    receipt.scope = RecipeCostScope::NativeRead(execution.clone());
    let component = receipt.component.clone();
    assert!(
        DiagnosticInvestigationIntent::new_source_read(
            &episode,
            &recipe,
            focus.clone(),
            execution.clone(),
            &component,
            &CollectionBudget::default(),
        )
        .is_err(),
        "a potential focus is not an enrolled question"
    );
    let episode = episode.enroll_source_read(focus.clone()).unwrap();
    let intent = DiagnosticInvestigationIntent::new_source_read(
        &episode,
        &recipe,
        focus.clone(),
        execution.clone(),
        &component,
        &CollectionBudget::default(),
    )
    .unwrap();
    let lineage = DiagnosticInvestigationLineage::from_intent(&intent).unwrap();
    let (source, _) = worker_starvation_result(&focus, &execution);
    assert!(source.partition_is_consistent());
    (source, lineage)
}
#[test]
fn shared_worker_source_result_reuses_the_input_owner_only_with_exact_original_lineage_and_native_scope() {
    let (source, lineage) = fixture();
    assert!(eligible_source(&source, Some(&lineage)).unwrap());
    assert!(
        !eligible_source(&source, None).unwrap(),
        "a dataset/shared label cannot supply the absent admission lineage"
    );
    let mut changed = lineage.clone();
    changed.intent_key = digest(b"changed-admission");
    assert!(eligible_source(&source, Some(&changed)).is_err());
    let mut changed = source.clone();
    let CollectionTarget::Shared(member) = &mut changed.target else { panic!() };
    member.native.id = "foreign-worker-member".into();
    let target = changed.target.clone();
    let SourceObservation::SourceRead { execution, .. } = &mut changed.observation else { panic!() };
    execution.target = target.clone();
    let CollectionTarget::Shared(member) = target else { panic!() };
    let SourcePartition::InstanceLocal { member: actual, .. } = &mut execution.partition else { panic!() };
    *actual = Some(member);
    assert!(changed.partition_is_consistent());
    assert!(eligible_source(&changed, Some(&lineage)).is_err(), "no current/foreign member rebinding");
    let mut changed = source.clone();
    let SourceObservation::SourceRead { snapshot, .. } = &mut changed.observation else { panic!() };
    snapshot.dataset = SnapshotDataset::QueryCompilation;
    assert!(eligible_source(&changed, Some(&lineage)).is_err(), "shared scope supports only its declared question");
}
