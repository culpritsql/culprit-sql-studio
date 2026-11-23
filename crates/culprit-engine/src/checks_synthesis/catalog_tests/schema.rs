//! Real canonical originals feed the same native catalog projections; these are controlled facts, not native runs.
use super::*;
use test_support::MssqlSchemaCatalogCase as Case;

async fn schemas(primary: &dyn OriginalProjection, case: Case) -> Vec<EvidenceCapture> {
    let mut originals = Vec::new();
    for (position, (mut source, rows)) in test_support::mssql_schema_catalog_evidence(case).into_iter().enumerate() {
        source.target = primary.source().target.clone();
        originals.push(
            capture(source, rows, 100 + position as u64, "2020-01-01T10:10:00Z", EvidenceFamily::ObjectDefinitions)
                .await,
        );
    }
    originals
}
fn project(primary: &dyn OriginalProjection, originals: &[&dyn OriginalProjection]) -> (CatalogContext, Snapshot) {
    let context = CatalogContext::from_originals(primary, originals).unwrap();
    let mut snapshot = crate::collected_evidence::settings_snapshot(
        primary,
        vec![],
        primary.collected_until(),
        &culprit_checks::Pipeline::builtin(),
    )
    .unwrap();
    context.apply(&mut snapshot);
    (context, snapshot)
}
fn state(snapshot: &Snapshot, source: &str) -> SourceState {
    snapshot.sources.iter().find(|outcome| outcome.source == source).unwrap().state
}
fn options(snapshot: &Snapshot) -> &[String] {
    &snapshot.tables[0].storage.as_ref().unwrap().options
}

#[tokio::test]
async fn sealed_schema_relations_and_options_reach_existing_checks_without_module_or_fresh_fix_authority() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 110, false).await;
    let columns = columns(&primary, "2020-01-01T10:01:00Z", 111).await;
    let table = table.verified_original().unwrap();
    let columns = columns.verified_original().unwrap();
    let frames = schemas(&primary, Case::Exact).await;
    let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
    let mut originals: Vec<&dyn OriginalProjection> = vec![&primary, &table, &columns];
    originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
    let raw = originals
        .iter()
        .map(|original| {
            serde_json::to_vec(&original.records().iter().map(|row| row.row()).collect::<Vec<_>>()).unwrap()
        })
        .collect::<Vec<_>>();
    let (context, snapshot) = project(&primary, &originals);
    assert_eq!(context.inputs().len(), 7);
    assert_eq!(snapshot.foreign_keys.len(), 1);
    let key = &snapshot.foreign_keys[0];
    assert_eq!(key.columns, ["status"]);
    assert_eq!(key.ref_columns, ["status"]);
    assert_eq!(key.delete_rule.as_deref(), Some("CASCADE"));
    assert_eq!(snapshot.constraints.len(), 2);
    assert!(
        snapshot
            .constraints
            .iter()
            .any(|issue| issue.kind == culprit_core::ConstraintKind::ForeignKey && issue.disabled)
    );
    assert!(
        snapshot.constraints.iter().any(|issue| issue.kind == culprit_core::ConstraintKind::Check && !issue.disabled)
    );
    assert!(options(&snapshot).iter().any(|option| option == "trigger=Audit:Documents:IU"));
    assert!(options(&snapshot).iter().any(|option| option == "partition_bound=VALUES LESS THAN (N'2020-02-01'):p1"));
    assert_eq!(state(&snapshot, source_id::CONSTRAINTS), SourceState::Collected);
    assert!(!snapshot.server.has(Capability::TableStats));
    let analyses = crate::collected_evidence::analyze_original_with_catalogs(
        &primary,
        &primary.original().key().unwrap(),
        &culprit_checks::Pipeline::builtin(),
        &context,
    )
    .unwrap();
    assert!(analyses.len() > 1, "the original retains independent native workload populations");
    assert!(
        analyses
            .iter()
            .all(|analysis| analysis.findings.iter().any(|finding| finding.id.starts_with("untrusted-constraint:")))
    );
    let output = policy().analyze_originals(&originals, "2020-01-01T12:02:00Z", &display(), vec![]).unwrap();
    assert_eq!(output.findings.iter().filter(|row| row.finding.id.starts_with("untrusted-constraint:")).count(), 1);
    assert_eq!(output.description.finding_evaluations.assessment_count as usize, analyses.len());
    assert_eq!(output.description.finding_evaluations.assessments.len(), analyses.len());
    assert!(output.findings.iter().all(|row| row.finding.fix.as_ref().is_none_or(|fix| fix.sql.is_none())));
    assert!(
        output
            .description
            .finding_evaluations
            .assessments
            .iter()
            .all(|assessment| assessment.catalog_inputs.len() == 7)
    );
    assert!(
        output.changes.is_empty() && output.description.change_evaluations.is_empty(),
        "offset-free object dates are not UTC DDL events"
    );
    assert_eq!(
        originals
            .iter()
            .map(|original| serde_json::to_vec(&original.records().iter().map(|row| row.row()).collect::<Vec<_>>())
                .unwrap())
            .collect::<Vec<_>>(),
        raw
    );
    assert!(!serde_json::to_string(&output).unwrap().contains("private-schema-canary"));
    output.validate().unwrap();
}

#[tokio::test]
async fn partial_or_incoherent_native_rosters_withhold_only_their_context_and_never_mean_no_constraints_or_bounds() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 120, false).await;
    let table = table.verified_original().unwrap();
    for case in [
        Case::PartialForeignKeys,
        Case::UnknownTriggerEvents,
        Case::MissingPartitionBoundary,
        Case::AmbiguousPartitionLayout,
        Case::ConflictingFunctionAcrossIndexes,
    ] {
        let frames = schemas(&primary, case).await;
        let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
        let mut originals: Vec<&dyn OriginalProjection> = vec![&primary, &table];
        originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
        let (context, snapshot) = project(&primary, &originals);
        assert!(context.issues().contains(&DerivedContextIssue::PartialCatalog), "{case:?}");
        assert!(!snapshot.server.has(Capability::TableStats));
        match case {
            Case::PartialForeignKeys => {
                assert!(snapshot.foreign_keys.is_empty());
                assert_eq!(state(&snapshot, source_id::CONSTRAINTS), SourceState::Partial);
            }
            Case::UnknownTriggerEvents => {
                assert!(!options(&snapshot).iter().any(|option| option.starts_with("trigger=")))
            }
            Case::MissingPartitionBoundary
            | Case::AmbiguousPartitionLayout
            | Case::ConflictingFunctionAcrossIndexes => {
                assert!(!options(&snapshot).iter().any(|option| option.starts_with("partition_bound=")))
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn hidden_check_text_preserves_native_trust_flags_without_reconstructing_a_predicate_or_module() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 130, false).await;
    let table = table.verified_original().unwrap();
    let frames = schemas(&primary, Case::UnavailableCheckText).await;
    let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
    let mut originals: Vec<&dyn OriginalProjection> = vec![&primary, &table];
    originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
    let (_, snapshot) = project(&primary, &originals);
    assert_eq!(snapshot.constraints.len(), 2);
    assert_eq!(state(&snapshot, source_id::CONSTRAINTS), SourceState::Collected);
    let output = policy().analyze_originals(&originals, "2020-01-01T12:02:00Z", &display(), vec![]).unwrap();
    assert!(output.findings.iter().any(|row| row.finding.id.starts_with("untrusted-constraint:")));
    assert!(output.findings.iter().all(|row| row.finding.fix.as_ref().is_none_or(|fix| fix.sql.is_none())));
    assert!(output.changes.is_empty() && output.description.change_evaluations.is_empty());
}

#[tokio::test]
async fn contradictory_parent_generation_or_object_kind_withdraws_schema_while_settings_remain_independent() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 140, false).await;
    let table = table.verified_original().unwrap();
    let setting = settings(&primary, "2020-01-01T10:01:00Z", 141, Some("8"), false).await;
    let setting = setting.verified_original().unwrap();
    for case in [Case::ChangedParent, Case::ChangedObjectKind] {
        let frames = schemas(&primary, case).await;
        let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
        let mut originals: Vec<&dyn OriginalProjection> = vec![&primary, &table, &setting];
        originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
        let (context, snapshot) = project(&primary, &originals);
        assert!(context.issues().contains(&DerivedContextIssue::ChangedCatalogSubject), "{case:?}");
        assert!(snapshot.tables.is_empty() && snapshot.foreign_keys.is_empty() && snapshot.constraints.is_empty());
        assert_eq!(snapshot.setting("MAXDOP").unwrap().value, "8");
    }
}

#[tokio::test]
async fn weak_statistic_reference_never_hides_a_later_conflicting_modification_witness_in_either_order() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let statistic = statistics(&primary, "2020-01-01T10:00:00Z", 150).await;
    let statistic = statistic.verified_original().unwrap();
    let index = index(&primary, "2020-01-01T10:01:00Z", 151, false, false).await;
    let index = index.verified_original().unwrap();
    let (source, mut rows) = table_rows(&primary, "2020-01-01T10:02:00Z", false);
    let TelemetryRecord::Operational(row) = &mut rows[0] else { panic!() };
    let OperationalFact::MssqlTable(table) = &mut row.fact else { panic!() };
    table.modified_without_offset = "2020-01-01T09:00:00.007".into();
    let changed = capture(source, rows, 152, "2020-01-01T10:02:00Z", EvidenceFamily::OperationalSamples).await;
    let changed = changed.verified_original().unwrap();
    for ordered in [
        [&statistic as &dyn OriginalProjection, &index, &changed],
        [&statistic as &dyn OriginalProjection, &changed, &index],
    ] {
        let mut originals = vec![&primary as &dyn OriginalProjection];
        originals.extend(ordered);
        let (context, snapshot) = project(&primary, &originals);
        assert!(context.issues().contains(&DerivedContextIssue::ChangedCatalogSubject));
        assert!(snapshot.tables.is_empty() && snapshot.indexes.is_empty() && snapshot.statistics.is_empty());
    }
}

#[cfg(feature = "collector")]
#[tokio::test]
async fn sealed_local_schema_originals_publish_one_source_free_diagnosis_through_the_real_writer() {
    use culprit_collector::{analytical::test_support::LocalCaptureFixture, synthesis::SynthesisRequest};
    use culprit_core::limits::OperationGuard;
    let query = query().await;
    let original = query.verified_original().unwrap();
    let (source, rows) = table_rows(&original, "2020-01-01T10:00:00Z", false);
    let mut fixture = Box::pin(LocalCaptureFixture::snapshot(source, rows)).await;
    let mut context = vec![fixture.original_reference()];
    for (mut source, rows) in test_support::mssql_schema_catalog_evidence(Case::Exact) {
        source.target = original.source().target.clone();
        context.push(Box::pin(fixture.append_original(source, rows)).await);
    }
    let query = test_support::query_compilation_followup_original();
    let primary = Box::pin(fixture.append_original(query.source, query.records)).await;
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    let outbox = fixture.outbox.as_mut().unwrap();
    let reference = outbox
        .delivery()
        .synthesize(
            SynthesisRequest { primary, context, budget: Default::default() },
            guard.handle(),
            (),
            Arc::new(policy()),
        )
        .await
        .unwrap();
    let sealed = outbox.verify(&reference.capture.observation_id).await.unwrap();
    assert_eq!(sealed.source_queries, 0);
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    let SourceObservation::DerivedDiagnosis(description) = &sealed.description.as_ref().unwrap().observation else {
        panic!()
    };
    assert!(description.counts.findings > 0);
    assert_eq!(description.inputs.len(), 7);
    assert!(
        description.inputs.iter().filter(|input| input.role == DerivedInputRole::Catalog).all(|input| input
            .catalog
            .as_ref()
            .unwrap()
            .native_time
            .is_some())
    );
}

#[tokio::test]
async fn disabled_triggers_and_nonbase_index_boundaries_never_become_active_base_storage_options() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 160, false).await;
    let table = table.verified_original().unwrap();
    for case in [Case::DisabledTrigger, Case::NonbasePartition] {
        let frames = schemas(&primary, case).await;
        let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
        let mut originals: Vec<&dyn OriginalProjection> = vec![&primary, &table];
        originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
        let (_, snapshot) = project(&primary, &originals);
        let prefix = if case == Case::DisabledTrigger { "trigger=" } else { "partition_bound=" };
        assert!(!options(&snapshot).iter().any(|option| option.starts_with(prefix)), "{case:?}");
    }
}

#[tokio::test]
async fn native_trusted_and_replication_only_constraints_match_the_ordinary_issue_policy_without_global_health() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    for case in [Case::TrustedConstraints, Case::ReplicationConstraints] {
        let frames = schemas(&primary, case).await;
        let frames = frames.iter().map(|frame| frame.verified_original().unwrap()).collect::<Vec<_>>();
        let mut originals: Vec<&dyn OriginalProjection> = vec![&primary];
        originals.extend(frames.iter().map(|frame| frame as &dyn OriginalProjection));
        let (_, snapshot) = project(&primary, &originals);
        assert!(snapshot.constraints.is_empty(), "{case:?}");
        assert_eq!(state(&snapshot, source_id::CONSTRAINTS), SourceState::Empty);
        assert_eq!(snapshot.foreign_keys.len(), 1);
        assert_eq!(state(&snapshot, source_id::TABLES), SourceState::Unavailable);
        assert!(!snapshot.server.has(Capability::TableStats));
    }
}
