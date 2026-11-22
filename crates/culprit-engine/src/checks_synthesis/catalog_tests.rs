use super::*;
use crate::collected_evidence::{RetainedEvidenceFixture as Fixture, catalogs::CatalogContext};
use culprit_evidence::{
    analytical::{
        evidence::{EvidenceCapture, EvidenceFamily, RecoveryPayloadBudget},
        test_support,
    },
    original_projection::OriginalProjection,
};
use std::sync::atomic::{AtomicBool, Ordering};

mod fixture;
mod schema;
use fixture::*;

#[tokio::test]
async fn conflicting_same_id_findings_cannot_choose_or_pool_independent_population_verdicts() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let pipeline = culprit_checks::Pipeline::builtin().with_check(Arc::new(PopulationDependentFinding));
    let policy = ChecksSynthesizer::new(Arc::new(pipeline), "conflicting-population-policy-1").unwrap();
    let result = policy.analyze_originals(&[&primary], "2020-01-01T12:02:00Z", &display(), vec![]);
    assert!(matches!(
        result.unwrap_err().detail,
        Some(culprit_core::ErrorDetail::Delivery(DeliveryProblem::Integrity))
    ));
}

struct PopulationDependentFinding;
impl Check for PopulationDependentFinding {
    fn info(&self) -> CheckInfo {
        super::test_support::CaptureFinding { many: false }.info()
    }
    fn run(&self, snapshot: &Snapshot) -> Vec<Finding> {
        let mut findings = super::test_support::CaptureFinding { many: false }.run(snapshot);
        findings[0].summary = format!(
            "Independent native population: {:?}",
            snapshot.workload.as_ref().map(|workload| &workload.population)
        );
        findings
    }
}

#[tokio::test]
async fn retained_catalogs_feed_the_installed_pipeline_once_without_current_schema_or_executable_authority() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 10, false).await;
    let index = index(&primary, "2020-01-01T10:01:00Z", 11, false, false).await;
    let columns = columns(&primary, "2020-01-01T10:02:00Z", 12).await;
    let stats = statistics(&primary, "2020-01-01T10:03:00Z", 13).await;
    let settings = settings(&primary, "2020-01-01T10:04:00Z", 14, Some("8"), false).await;
    let catalogs = [&table, &index, &columns, &stats, &settings].map(|capture| capture.verified_original().unwrap());
    let mut originals: Vec<&dyn OriginalProjection> = vec![&primary];
    originals.extend(catalogs.iter().map(|catalog| catalog as &dyn OriginalProjection));
    let entered = Arc::new(AtomicBool::new(false));
    let pipeline = culprit_checks::Pipeline::builtin().with_prepare(Arc::new(CatalogWitness(entered.clone())));
    let policy = ChecksSynthesizer::new(Arc::new(pipeline), "retained-catalog-policy-1").unwrap();
    let raw = originals.iter().map(|original| serde_json::to_vec(original.source()).unwrap()).collect::<Vec<_>>();
    let output = policy.analyze_originals(&originals, "2020-01-01T12:02:00Z", &display(), vec![]).unwrap();
    assert!(entered.load(Ordering::SeqCst));
    assert!(output.description.finding_evaluations.populations.iter().all(|population| {
        population.catalog_inputs == [1, 2, 3, 4, 5]
            && population.limitations.contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingSchema)
            && population
                .limitations
                .contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingCurrentServerMetadata)
            && !population
                .limitations
                .contains(&culprit_core::collected_analysis::CollectedAnalysisLimit::MissingSettings)
    }));
    assert!(output.description.context_issues.contains(&DerivedContextIssue::AcquisitionCatalogOnly));
    assert!(output.description.inputs.iter().skip(1).all(|input| input.role == DerivedInputRole::Catalog));
    let provenance = output.description.inputs[4].catalog.as_ref().unwrap();
    assert_eq!(provenance.observed_from, catalogs[3].collected_from());
    let native = catalogs[3].source().observation.snapshot().unwrap();
    assert_ne!(provenance.observed_from, native.observed_from);
    assert!(matches!(&provenance.native_time, Some(DerivedCatalogTime::Utc {at})
        if *at == canonical_time(&native.observed_until).unwrap()));
    assert!(output.findings.iter().all(|finding| finding.finding.fix.as_ref().is_none_or(|fix| fix.sql.is_none())));
    assert_eq!(
        originals.iter().map(|original| serde_json::to_vec(original.source()).unwrap()).collect::<Vec<_>>(),
        raw
    );
    assert!(!serde_json::to_string(&output).unwrap().contains("private-catalog-canary"));
    output.validate().unwrap();
}
struct CatalogWitness(Arc<AtomicBool>);
impl culprit_checks::pipeline::Prepare for CatalogWitness {
    fn name(&self) -> &str {
        "retained catalog contract witness"
    }
    fn prepare(&self, snapshot: &mut Snapshot) {
        self.0.store(true, Ordering::SeqCst);
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(snapshot.indexes.len(), 1);
        assert_eq!(snapshot.columns.len(), 1);
        assert_eq!(snapshot.statistics.len(), 1);
        assert_eq!(snapshot.setting("MAXDOP").unwrap().value, "8");
        assert_eq!(snapshot.columns[0].data_type, "int");
        assert!(!snapshot.server.has(Capability::TableStats));
        assert!(snapshot.indexes[0].last_read.is_none());
        assert!(snapshot.statistics[0].last_updated.is_none());
    }
}
#[tokio::test]
async fn foreign_scope_future_partial_and_changed_generation_catalogs_remain_raw_and_cannot_fill_the_snapshot() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    for case in ["foreign-incarnation", "future", "partial", "changed-generation"] {
        let (mut source, mut records) = table_rows(&primary, "2020-01-01T10:00:00Z", false);
        match case {
            "foreign-incarnation" => {
                let CollectionTarget::Database(database) = &mut source.target else { panic!() };
                database.incarnation = Some("foreign-original-database".into());
            }
            "future" => set_native_clock(&mut source, "2020-01-01T13:00:00Z"),
            "partial" => {
                let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
                header.sources[0] = StateSourceCoverage::observed(source_id::TABLES.into(), 1, false);
            }
            "changed-generation" => {
                let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
                let OperationalFact::MssqlTable(table) = &mut row.fact else { panic!() };
                table.identity.created_without_offset = "2020-01-01T09:00:00.007".into();
                row.key = table.identity.key().unwrap();
            }
            _ => unreachable!(),
        }
        let at = if case == "future" { "2020-01-01T13:00:00Z" } else { "2020-01-01T10:00:00Z" };
        let table = capture(source, records, 20, at, EvidenceFamily::OperationalSamples).await;
        let table = table.verified_original().unwrap();
        let index = index(&primary, "2020-01-01T10:01:00Z", 21, false, false).await;
        let index = index.verified_original().unwrap();
        let originals: Vec<&dyn OriginalProjection> = vec![&primary, &table, &index];
        let context = CatalogContext::from_originals(&primary, &originals).unwrap();
        let mut snapshot = crate::collected_evidence::settings_snapshot(
            &primary,
            vec![],
            primary.collected_until(),
            &culprit_checks::Pipeline::builtin(),
        )
        .unwrap();
        context.apply(&mut snapshot);
        assert!(snapshot.tables.is_empty(), "{case}");
        if case == "changed-generation" {
            assert!(snapshot.indexes.is_empty());
        }
        assert!(!context.issues().is_empty(), "{case}");
    }
}
#[tokio::test]
async fn native_filter_versions_are_distinct_after_redaction_and_unavailable_filter_is_never_unfiltered() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let first = index(&primary, "2020-01-01T10:00:00Z", 30, false, false).await;
    let second = index(&primary, "2020-01-01T10:00:00Z", 31, true, false).await;
    let unavailable = index(&primary, "2020-01-01T10:00:00Z", 32, false, true).await;
    let mut projected = Vec::new();
    for capture in [&first, &second, &unavailable] {
        let original = capture.verified_original().unwrap();
        let context = CatalogContext::from_originals(&primary, &[&primary, &original]).unwrap();
        let mut snapshot = crate::collected_evidence::settings_snapshot(
            &primary,
            vec![],
            primary.collected_until(),
            &culprit_checks::Pipeline::builtin(),
        )
        .unwrap();
        context.apply(&mut snapshot);
        projected.push(snapshot.indexes);
    }
    assert_eq!(projected[0][0].filter, projected[1][0].filter);
    assert_ne!(
        projected[0][0].filter_facts.as_ref().unwrap().fingerprint,
        projected[1][0].filter_facts.as_ref().unwrap().fingerprint
    );
    assert!(projected[2].is_empty());
}
#[tokio::test]
async fn actual_settings_comparison_uses_existing_events_and_keeps_zero_rows_scoped_to_the_same_known_roster() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let before = settings(&primary, "2020-01-01T10:00:00Z", 40, Some("0"), false).await;
    let before = before.verified_original().unwrap();
    for (value, expected) in [("8", 1), ("0", 0)] {
        let after = settings(&primary, "2020-01-01T11:00:00Z", 41, Some(value), false).await;
        let after = after.verified_original().unwrap();
        let output = policy()
            .analyze_originals(&[&primary, &before, &after], "2020-01-01T12:02:00Z", &display(), vec![])
            .unwrap();
        assert_eq!(output.changes.len(), expected);
        assert_eq!(output.description.change_evaluations.len(), 1);
        let evaluation = &output.description.change_evaluations[0];
        assert_eq!(evaluation.scope, DerivedChangeScope::Settings);
        assert_eq!((evaluation.previous_input, evaluation.current_input), (1, 2));
        assert_eq!(output.description.output_coverage.changes, DerivedOutputCoverage::Evaluated);
        assert!(!output.description.limitations().is_empty());
        if let Some(change) = output.changes.first() {
            assert_eq!(change.change.source, EventSource::Scans);
            assert_eq!(change.change.object.as_deref(), Some("MAXDOP"));
            assert_eq!(change.change.after.as_deref(), Some(evaluation.observed_after.as_str()));
            assert_eq!(change.change.at, evaluation.observed_until);
            assert_eq!(change.inputs.len(), 2);
            assert!(change.change.title.source().is_some());
        }
        output.validate().unwrap();
    }
}
#[tokio::test]
async fn null_denied_partial_ambiguous_or_changed_scope_settings_do_not_mean_no_changes() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let before = settings(&primary, "2020-01-01T10:00:00Z", 50, Some("0"), false).await;
    let before = before.verified_original().unwrap();
    for case in
        ["null", "denied", "partial", "ambiguous", "scope", "unknown-nature", "clock-regression", "not-applicable"]
    {
        let value = (case != "null").then_some("8");
        let (mut source, mut rows) = settings_rows(&primary, "2020-01-01T11:00:00Z", value, false);
        match case {
            "not-applicable" => {
                rows.clear();
                let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
                header.records = 0;
                header.sources[0] = StateSourceCoverage::not_applicable(source_id::SETTINGS.into());
            }
            "denied" => {
                rows.clear();
                let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
                header.records = 0;
                header.sources[0] =
                    StateSourceCoverage::unavailable(source_id::SETTINGS.into(), ErrorCode::PermissionDenied, None);
            }
            "partial" => {
                let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
                header.sources[0] = StateSourceCoverage::observed(source_id::SETTINGS.into(), 1, false);
            }
            "ambiguous" => {
                let TelemetryRecord::Setting(setting) = &rows[0] else { panic!() };
                let mut peer = setting.clone();
                peer.namespace = "sys.databases".into();
                rows.push(TelemetryRecord::Setting(peer));
                let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
                header.records = 2;
                header.sources[0] = StateSourceCoverage::collected(source_id::SETTINGS.into(), 2);
            }
            "scope" => {
                let CollectionTarget::Database(database) = &mut source.target else { panic!() };
                database.incarnation = Some("another-native-database".into());
            }
            "clock-regression" => set_native_clock(&mut source, "2020-01-01T09:00:00Z"),
            "unknown-nature" => {
                let TelemetryRecord::Setting(setting) = &mut rows[0] else { panic!() };
                setting.nature = SettingNature::Unknown;
            }
            _ => {}
        }
        let after = capture(source, rows, 51, "2020-01-01T11:00:00Z", EvidenceFamily::SettingObservations).await;
        let after = after.verified_original().unwrap();
        let output = policy()
            .analyze_originals(&[&primary, &before, &after], "2020-01-01T12:02:00Z", &display(), vec![])
            .unwrap();
        assert!(output.changes.is_empty(), "{case}");
        assert!(output.description.change_evaluations.is_empty(), "{case}");
        assert!(
            matches!(output.description.output_coverage.changes, DerivedOutputCoverage::ContextUnavailable { .. }),
            "{case}"
        );
        assert!(!output.description.context_issues.is_empty(), "{case}");
    }
}

#[tokio::test]
async fn a_foreign_engine_original_and_hidden_server_settings_never_supply_database_catalog_context() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let table = table(&primary, "2020-01-01T10:00:00Z", 60, false).await;
    let table = table.verified_original().unwrap();
    let (source, records) = test_support::query_evidence();
    assert_ne!(source.target.engine(), Engine::Mssql);
    let fixture = Fixture::with_collector_bounds_and_sequence(
        source,
        records,
        61,
        "2020-01-01T12:01:00Z",
        "2020-01-01T12:01:01Z",
    )
    .await
    .unwrap();
    let foreign_capture = fixture.read().await;
    let foreign = foreign_capture.verified_original().unwrap();
    let context = CatalogContext::from_originals(&foreign, &[&foreign, &table]).unwrap();
    assert!(context.inputs().is_empty());
    assert!(context.issues().contains(&DerivedContextIssue::UnqualifiedCatalogScope));
    let (source, mut rows) = settings_rows(&primary, "2020-01-01T10:00:00Z", Some("8"), false);
    let TelemetryRecord::Setting(setting) = &mut rows[0] else { panic!() };
    setting.scope = MeasurementScope::Server;
    let hidden = capture(source, rows, 62, "2020-01-01T10:00:00Z", EvidenceFamily::SettingObservations).await;
    let hidden = hidden.verified_original().unwrap();
    let context = CatalogContext::from_originals(&primary, &[&primary, &hidden]).unwrap();
    assert!(!context.settings_complete());
    assert!(context.issues().contains(&DerivedContextIssue::UnqualifiedCatalogScope));
}

#[tokio::test]
async fn identical_native_ticks_with_different_precision_join_but_one_changed_tick_or_column_identity_does_not() {
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let index = index(&primary, "2020-01-01T10:01:00Z", 70, false, false).await;
    let index = index.verified_original().unwrap();
    for (created, expected) in [("2020-01-01T09:00:00.0030000", true), ("2020-01-01T09:00:00.0030001", false)] {
        let (source, mut rows) = table_rows(&primary, "2020-01-01T10:00:00Z", false);
        let TelemetryRecord::Operational(row) = &mut rows[0] else { panic!() };
        let OperationalFact::MssqlTable(table) = &mut row.fact else { panic!() };
        table.identity.created_without_offset = created.into();
        row.key = table.identity.key().unwrap();
        let table = capture(source, rows, 71, "2020-01-01T10:00:00Z", EvidenceFamily::OperationalSamples).await;
        let table = table.verified_original().unwrap();
        let context = CatalogContext::from_originals(&primary, &[&primary, &table, &index]).unwrap();
        assert_eq!(!context.inputs().is_empty(), expected);
    }
    let (source, mut rows) = column_rows(&primary, "2020-01-01T10:02:00Z");
    let TelemetryRecord::ObjectDefinition(row) = &rows[0] else { panic!() };
    let ObjectDefinitionFact::MssqlColumn(column) = &row.fact else { panic!() };
    let mut changed = column.clone();
    changed.name = "another_native_column".into();
    rows[0] = TelemetryRecord::ObjectDefinition(
        ObjectDefinitionRecord::new(source_id::COLUMNS.into(), ObjectDefinitionFact::MssqlColumn(changed)).unwrap(),
    );
    let columns = capture(source, rows, 72, "2020-01-01T10:02:00Z", EvidenceFamily::ObjectDefinitions).await;
    let columns = columns.verified_original().unwrap();
    let context = CatalogContext::from_originals(&primary, &[&primary, &columns, &index]).unwrap();
    assert!(context.inputs().is_empty());
    assert!(context.issues().contains(&DerivedContextIssue::ChangedCatalogSubject));
}

#[cfg(feature = "collector")]
#[tokio::test]
async fn real_local_originals_pass_writer_provenance_checks_and_seal_settings_changes_with_zero_native_work() {
    use culprit_collector::{analytical::test_support::LocalCaptureFixture, synthesis::SynthesisRequest};
    use culprit_core::limits::OperationGuard;
    let query = query().await;
    let primary = query.verified_original().unwrap();
    let (source, records) = settings_rows(&primary, "2020-01-01T10:00:00Z", Some("0"), false);
    let mut fixture = Box::pin(LocalCaptureFixture::snapshot(source, records)).await;
    assert_eq!(fixture.manifest.status, CollectionStatus::Complete, "initial Settings original");
    let before = fixture.original_reference();
    let (source, records) = settings_rows(&primary, "2020-01-01T11:00:00Z", Some("8"), false);
    let after = Box::pin(fixture.append_original(source, records)).await;
    let query = test_support::query_compilation_followup_original();
    let original_source = query.source.clone();
    assert!(original_source.query_history().unwrap().acquisition_owner.is_some());
    let primary = Box::pin(fixture.append_original(query.source, query.records)).await;
    assert_eq!(fixture.manifest.description.as_ref(), Some(&original_source));
    assert!(fixture.manifest.references.complete);
    let guard = OperationGuard::new(std::time::Duration::from_secs(10)).unwrap();
    let outbox = fixture.outbox.as_mut().unwrap();
    let reference = outbox
        .delivery()
        .synthesize(
            SynthesisRequest { primary, context: vec![before.clone(), after.clone()], budget: Default::default() },
            guard.handle(),
            (),
            Arc::new(policy()),
        )
        .await
        .unwrap();
    let sealed = outbox.verify(&reference.capture.observation_id).await.unwrap();
    assert_eq!(sealed.source_queries, 0);
    assert_eq!(outbox.status(10, None).await.unwrap().source_debts, 0);
    let SourceObservation::DerivedDiagnosis(header) = sealed.description.as_ref().unwrap().observation.clone() else {
        panic!()
    };
    assert_eq!(header.change_evaluations.len(), 1);
    assert_eq!(header.counts.changes, 1);
    assert_eq!(header.output_coverage.changes, DerivedOutputCoverage::Evaluated);
    let selected =
        |capture: &DiagnosticCaptureRef| header.inputs.iter().find(|input| input.capture == *capture).unwrap();
    assert!(selected(&before).catalog.as_ref().unwrap().native_time.is_some());
    assert!(selected(&after).catalog.as_ref().unwrap().native_time.is_some());
    assert!(sealed.files.iter().any(|file| file.stream == TelemetryStream::DerivedChanges && file.rows == 1));
}
