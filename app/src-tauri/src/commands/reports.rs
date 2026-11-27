//! Desktop reports capability; native work remains with the existing Workspace owner.
use super::workspace::{Ws, blocking_workspace};
use culprit_core::{Drift, Error, Report, Result, SnapshotOptions, Verification};
use culprit_engine::export::{ExportFile, ExportFormat, ExportOptions};

/// The UI never shows raw plan XML/JSON, and on busy databases it is most of a report's size; keep it on disk only.
fn for_ui(mut report: Report) -> Report {
    // Workspace scan/read paths have already run the full analysis and plan presentation pipeline.
    // Replay evidence stays with the saved report; the UI only needs the prepared view and its provenance.
    report.evidence = None;
    if let Some(catalog) = &mut report.snapshot.runtime_populations {
        population_labels_for_ui(catalog);
    }
    if let Some(workload) = report.snapshot.workload.as_mut() {
        if let Some(catalog) = &mut workload.population_catalog {
            population_labels_for_ui(catalog);
        }
        for plan in workload.queries.iter_mut().filter_map(|q| q.plan.as_mut()) {
            plan.raw = None;
        }
    }
    report
}
/// Provider messages are rendered by the backend catalog; the app renders their resulting native labels as text.
fn population_labels_for_ui(catalog: &mut culprit_core::RuntimePopulationCatalog) {
    for population in &mut catalog.populations {
        population.label = culprit_core::i18n::Message::Text { text: population.label.render() };
    }
}
/// The workload window the app asked for, in minutes, within what a scan reads.
pub(crate) fn requested_window(window_minutes: Option<u32>) -> Option<u32> {
    window_minutes.map(|minutes| minutes.clamp(1, culprit_core::MAX_WINDOW_MINUTES))
}
#[tauri::command]
pub(crate) async fn latest_report(
    ws: Ws<'_>,
    name: String,
    window_minutes: Option<u32>,
    population: Option<culprit_core::telemetry::NativeIdentity>,
) -> Result<Option<Report>> {
    blocking_workspace(ws.inner().clone(), move |ws| {
        Ok(ws.latest_population_report(&name, requested_window(window_minutes), population.as_ref())?.map(for_ui))
    })
    .await
}
#[tauri::command]
pub(crate) async fn report_at(ws: Ws<'_>, name: String, report_id: String) -> Result<Report> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.report_at(&name, &report_id).map(for_ui)).await
}
#[tauri::command]
pub(crate) async fn scan(
    ws: Ws<'_>,
    name: String,
    window_minutes: Option<u32>,
    population: Option<culprit_core::telemetry::NativeIdentity>,
) -> Result<culprit_core::ObservedScanAttempt> {
    let defaults = SnapshotOptions::default();
    let window = requested_window(window_minutes).unwrap_or(defaults.window_in_minutes());
    let options = SnapshotOptions { workload_population: population, ..defaults.with_window_minutes(window) };
    let observation = ws.begin_provider_read(culprit_core::ProviderReadKind::Scan)?;
    let result = ws.scan_with_log(&name, &options, None, observation.operation()?).await;
    let attempt = culprit_engine::project_scan_attempt(observation.finish_call(result));
    Ok(match attempt {
        culprit_core::ObservedScanAttempt::Complete { report } => {
            culprit_core::ObservedScanAttempt::Complete { report: Box::new(for_ui(*report)) }
        }
        other => other,
    })
}
#[tauri::command]
pub(crate) async fn collection_admission_record(
    ws: Ws<'_>,
) -> Result<culprit_core::telemetry::CollectionAdmissionRecord> {
    ws.collection_admission_record().await
}

#[tauri::command]
pub(crate) async fn acknowledge_collection_quiescence(
    ws: Ws<'_>,
    observation_id: String,
    source_quiescent: bool,
) -> Result<culprit_core::telemetry::CollectionAdmissionStatus> {
    if !source_quiescent {
        return Err(Error::invalid(culprit_core::t!("outbox.cli.source_quiescent")));
    }
    ws.acknowledge_collection_quiescence(&observation_id).await
}

/// The brief a coding agent is given: in English whatever the app's language, report and all.
#[tauri::command]
pub(crate) async fn brief(ws: Ws<'_>, name: String, report_id: String, finding_id: String) -> Result<String> {
    blocking_workspace(ws.inner().clone(), move |ws| {
        culprit_core::i18n::scope("en", || {
            let report = ws.report_at(&name, &report_id)?;
            let finding =
                report.findings.iter().find(|f| f.id == finding_id).ok_or_else(|| {
                    Error::not_found(culprit_core::message!("engine.brief.no_finding", id = finding_id))
                })?;
            Ok(culprit_engine::agent_brief(&report, finding))
        })
    })
    .await
}
#[tauri::command]
pub(crate) async fn export_finding(
    ws: Ws<'_>,
    name: String,
    report_id: String,
    finding_id: String,
    format: ExportFormat,
    options: ExportOptions,
) -> Result<ExportFile> {
    blocking_workspace(ws.inner().clone(), move |ws| {
        ws.export_finding_at(&name, &report_id, &finding_id, format, &options)
    })
    .await
}
#[tauri::command]
pub(crate) async fn verify_findings(
    ws: Ws<'_>,
    name: String,
    report_id: String,
    ids: Vec<String>,
) -> Result<Verification> {
    ws.verify_at(&name, &report_id, &ids).await
}
#[tauri::command]
pub(crate) async fn drift(ws: Ws<'_>, a: String, b: String) -> Result<Drift> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.drift(&a, &b)).await
}
/// The index advisor's plan for the displayed scan.
#[tauri::command]
pub(crate) async fn index_advice(ws: Ws<'_>, name: String, report_id: String) -> Result<culprit_core::IndexAdvice> {
    blocking_workspace(ws.inner().clone(), move |ws| ws.index_advice_at(&name, &report_id)).await
}
/// The dashboard from the connection's latest report for the window; the app reads sessions itself, every few seconds.
#[tauri::command]
pub(crate) async fn dashboard(
    ws: Ws<'_>,
    name: String,
    window_minutes: Option<u32>,
    report_id: Option<String>,
) -> Result<culprit_core::Dashboard> {
    blocking_workspace(ws.inner().clone(), move |ws| {
        if let Some(report_id) = report_id {
            return ws.dashboard_for_report(&name, &report_id);
        }
        let report = ws.latest_report(&name, requested_window(window_minutes))?.ok_or_else(|| {
            Error::not_found(culprit_core::message!("engine.no_saved_report", name = &name))
                .with_hint(culprit_core::message!("engine.scan_first", name = &name))
        })?;
        let mut dashboard = culprit_engine::dashboard_report(&report);
        dashboard.connection = name;
        Ok(dashboard)
    })
    .await
}
#[cfg(test)]
mod report_projection_tests {
    #[test]
    fn ui_renders_population_messages_without_changing_native_selection_or_catalog_context() {
        use culprit_core::{RuntimePopulation, RuntimePopulationCatalog, telemetry::NativeIdentity};
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/mssql-demo.json");
        let mut report = culprit_engine::reading::open(&path).unwrap();
        let identity = NativeIdentity::new("17").qualified("native-context", "9007199254740993");
        let label = culprit_core::message!(
            "provider.mssql.query_store.population_group",
            group = "17",
            role = "source role <img>"
        );
        let rendered = label.render();
        let catalog = RuntimePopulationCatalog {
            membership: culprit_core::RuntimePopulationMembership::NativeCatalog,
            source: "fixture.runtime".into(),
            partitioned: true,
            requires_selection: false,
            populations: vec![RuntimePopulation {
                identity: identity.clone(),
                label,
                native_properties: [("native-history".into(), "opaque metadata <script>".into())].into(),
            }],
        };
        report.snapshot.runtime_populations = Some(catalog.clone());
        let workload = report.snapshot.workload.as_mut().unwrap();
        workload.population = Some(identity.clone());
        workload.population_catalog = Some(catalog);
        report.snapshot.options.as_mut().unwrap().workload_population = Some(identity.clone());
        let ui = super::for_ui(report);
        for catalog in [
            ui.snapshot.runtime_populations.as_ref().unwrap(),
            ui.snapshot.workload.as_ref().unwrap().population_catalog.as_ref().unwrap(),
        ] {
            let population = &catalog.populations[0];
            assert_eq!(population.identity, identity);
            assert_eq!(population.native_properties["native-history"], "opaque metadata <script>");
            assert_eq!(population.label, culprit_core::i18n::Message::Text { text: rendered.clone() });
        }
        assert_eq!(ui.snapshot.workload.as_ref().unwrap().population.as_ref(), Some(&identity));
        assert_eq!(ui.snapshot.options.as_ref().unwrap().workload_population.as_ref(), Some(&identity));
    }

    #[test]
    fn ui_omits_replay_copies_and_native_text_while_keeping_provenance_and_views() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/mssql-demo.json");
        let mut report = culprit_engine::reading::open(&path).unwrap();
        let collected = report.analysis_input().clone();
        report.evidence.as_mut().unwrap().collected = Some(Box::new(collected));
        let provenance = serde_json::to_value(&report.provenance).unwrap();
        for plan in report.snapshot.workload.as_mut().unwrap().queries.iter_mut().filter_map(|q| q.plan.as_mut()) {
            plan.raw = Some("PRIVATE_NATIVE_PLAN".into());
        }
        let started = std::time::Instant::now();
        let stored_bytes = serde_json::to_vec(&report).unwrap().len();
        let stored_serialization = started.elapsed();
        let ui = super::for_ui(report);
        assert!(ui.evidence.is_none());
        assert_eq!(serde_json::to_value(&ui.provenance).unwrap(), provenance);
        assert!(
            ui.snapshot.queries().iter().filter_map(|q| q.plan.as_ref()).all(|p| p.raw.is_none() && p.view.is_some())
        );
        let projection = serde_json::to_vec(&ui).unwrap();
        assert!(!String::from_utf8_lossy(&projection).contains("PRIVATE_NATIVE_PLAN"));
        assert!(projection.len() < stored_bytes);
        eprintln!(
            "report projection: {stored_bytes} stored bytes, {} UI bytes; stored serialization {stored_serialization:?}",
            projection.len()
        );
    }
}
