//! Sharing and the verify loop on top of the workspace: compare plans with what the latest scan knows, export findings,
//! reports, plans and comparisons, and check whether fixes worked.

use culprit_core::{CompareContext, Error, Plan, PlanDiff, Result, SnapshotOptions, Verification, compare_reports, t};

use crate::Workspace;
use crate::export::{self, ExportFile, ExportFormat, ExportOptions};

impl Workspace {
    /// What the latest scan of `name` knows that explains plan differences (optimizer statistics). Empty when there's
    /// no scan.
    pub fn compare_context(&self, name: &str) -> CompareContext {
        match self.latest_snapshot(name) {
            Ok(Some(snapshot)) => CompareContext { statistics: snapshot.statistics, taken_at: Some(snapshot.taken_at) },
            _ => CompareContext::default(),
        }
    }

    /// Compare two plans; with `connection`, scenarios can name stale statistics from its latest scan.
    pub fn diff_plans(&self, a: &Plan, name_a: &str, b: &Plan, name_b: &str, connection: Option<&str>) -> PlanDiff {
        let context = connection.map(|c| self.compare_context(c)).unwrap_or_default();
        culprit_plans::compare_plans(a, name_a, b, name_b, &context)
    }

    /// A finding from the latest scan as Markdown (for a PR) or a self-contained HTML page.
    pub fn export_finding(
        &self,
        name: &str,
        id: &str,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<ExportFile> {
        let report = self.latest_report(name, None)?.ok_or_else(|| not_scanned(name))?;
        finding_export(&report, name, id, format, options)
    }

    /// Export from the exact report displayed by a client. Stable finding IDs do not select a population or window.
    pub fn export_finding_at(
        &self,
        name: &str,
        report_id: &str,
        id: &str,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<ExportFile> {
        let report = self.report_service().report_at(name, report_id)?;
        finding_export(&report, name, id, format, options)
    }

    /// Every finding of the latest scan.
    pub fn export_report(&self, name: &str, format: ExportFormat, options: &ExportOptions) -> Result<ExportFile> {
        let report = self.latest_report(name, None)?.ok_or_else(|| not_scanned(name))?;
        Ok(match format {
            ExportFormat::Html => ExportFile {
                name: "culprit-report.html".into(),
                contents: export::report_html(&report, options),
                format,
            },
            _ => ExportFile {
                name: "culprit-report.md".into(),
                contents: export::report_markdown(&report, options),
                format: ExportFormat::Markdown,
            },
        })
    }

    /// A plan of a query from the latest scan, with the original plan text when the engine keeps it (Query Store).
    pub async fn export_query_plan(
        &self,
        name: &str,
        query_id: &str,
        plan_id: Option<&str>,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<ExportFile> {
        let plan = self.query_plan(name, query_id, plan_id).await?;
        query_plan_export(&plan, query_id, plan_id, format, options)
    }

    /// Export the plan selected from this exact report, including its native query/plan and workload context.
    pub async fn export_query_plan_at(
        &self,
        name: &str,
        report_id: &str,
        query_id: &str,
        plan_id: Option<&str>,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<ExportFile> {
        let plan = self.query_plan_at(name, query_id, plan_id, Some(report_id)).await?;
        query_plan_export(&plan, query_id, plan_id, format, options)
    }

    /// Re-scan `name` with the same window as its latest scan and compare, finding by finding. `ids` empty checks every
    /// finding the earlier scan had.
    pub async fn verify(&self, name: &str, ids: &[String]) -> Result<Verification> {
        Box::pin(self.verify_observation(name, None, ids)).await
    }

    /// Re-scan and compare the exact saved report's native population and window, without a default-report fallback.
    pub async fn verify_at(&self, name: &str, report_id: &str, ids: &[String]) -> Result<Verification> {
        Box::pin(self.verify_observation(name, Some(report_id), ids)).await
    }

    async fn verify_observation(&self, name: &str, report_id: Option<&str>, ids: &[String]) -> Result<Verification> {
        let report_name = name.to_owned();
        let report_id = report_id.map(str::to_owned);
        let before = self
            .report_service()
            .background(move |reports| Ok(reports.plan_report(&report_name, report_id.as_deref())?.map(Box::new)))
            .await?
            .ok_or_else(|| {
                Error::not_found(t!("engine.share.verify_unscanned", name = name))
                    .with_hint(t!("engine.share.verify_unscanned_hint", name = name))
            })?;
        let mut options = before.snapshot.options.clone().unwrap_or_else(SnapshotOptions::default);
        if let Some(population) = before.snapshot.workload.as_ref().and_then(|workload| workload.population.clone()) {
            options.workload_population = Some(population);
        }
        let observation = self.begin_provider_read(culprit_core::ProviderReadKind::Scan)?;
        let result = self.scan_populations(name, &options, observation.operation()?).await;
        let after = observation.finish_call(result).into_result()?.requested;
        let ids = ids.to_vec();
        crate::run_blocking(move || Ok(compare_reports(before.as_ref(), after.as_ref(), &ids))).await
    }
}

fn query_plan_export(
    plan: &Plan,
    query_id: &str,
    plan_id: Option<&str>,
    format: ExportFormat,
    options: &ExportOptions,
) -> Result<ExportFile> {
    let title = match plan_id {
        Some(id) => t!("engine.share.plan_of_query_id", plan = id, query = query_id),
        None => t!("engine.share.plan_of_query", query = query_id),
    };
    export_plan(plan, &title, format, options)
}

fn finding_export(
    report: &culprit_core::Report,
    name: &str,
    id: &str,
    format: ExportFormat,
    options: &ExportOptions,
) -> Result<ExportFile> {
    let finding = report.findings.iter().find(|finding| finding.id == id).ok_or_else(|| {
        Error::not_found(t!("engine.share.no_finding", id = id, name = name))
            .with_hint(t!("engine.share.no_finding_hint", name = name))
    })?;
    let stem = export::file_stem(&finding.title);
    Ok(match format {
        ExportFormat::Html => ExportFile {
            name: format!("{stem}.html"),
            contents: export::finding_html(report, finding, options),
            format,
        },
        _ => ExportFile {
            name: format!("{stem}.md"),
            contents: export::finding_markdown(report, finding, options),
            format: ExportFormat::Markdown,
        },
    })
}

/// `name` has no saved scan to export.
fn not_scanned(name: &str) -> Error {
    Error::not_found(t!("engine.share.not_scanned", name = name)).with_hint(t!("engine.scan_first", name = name))
}

/// A plan as Markdown, HTML, or its own text (.sqlplan, EXPLAIN JSON or text) when it kept it.
pub fn export_plan(plan: &Plan, title: &str, format: ExportFormat, options: &ExportOptions) -> Result<ExportFile> {
    culprit_plans::export::export_plan(plan, title, format, options, &export::exported_at())
}

/// Two plans compared, as Markdown or HTML.
pub fn export_diff(
    a: &Plan,
    b: &Plan,
    diff: &PlanDiff,
    name_a: &str,
    name_b: &str,
    format: ExportFormat,
    options: &ExportOptions,
) -> Result<ExportFile> {
    let comparison = culprit_plans::export::DiffExport {
        before: a,
        after: b,
        difference: diff,
        before_name: name_a,
        after_name: name_b,
    };
    culprit_plans::export::export_diff(&comparison, format, options, &export::exported_at())
}

#[cfg(test)]
mod context_tests {
    use super::*;

    #[test]
    fn comparisons_read_prepared_statistics_without_replaying_retained_evidence() {
        let dir = std::env::temp_dir().join(format!("culprit-compare-context-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let workspace = Workspace::open_in(&dir);
        let mut report: culprit_core::Report =
            serde_json::from_str(include_str!("../../../fixtures/mssql-demo.report.json")).unwrap();
        assert!(!report.snapshot.statistics.is_empty(), "the comparison must retain actual prepared statistics");
        report.evidence = Some(culprit_core::ReportEvidence {
            input: Box::new(report.snapshot.clone()),
            collected: None,
            recorded_changes: vec![],
        });
        let saved = workspace.paths().reports_dir(&report.snapshot.connection);
        crate::reports::save(&saved, &report).unwrap();
        let path = saved.join(format!("{}.json", report.snapshot.id));
        assert!(crate::reports::load(&path).is_ok());
        assert!(crate::reports::load_analysis_input(&path).is_ok());
        let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let version = value["_culprit_store"]["version"].clone();
        assert!(version.is_number(), "the current writer supplies the encoding version");
        value["_culprit_store"]["input"]["changes"].as_array_mut().unwrap().push(serde_json::json!({
            "kind": "set", "path": ["statistics"], "value": "not-a-statistics-array"
        }));
        assert_eq!(value["_culprit_store"]["version"], version);
        assert_eq!(value["snapshot"]["statistics"], serde_json::to_value(&report.snapshot.statistics).unwrap());
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(crate::reports::load(&path).is_err());
        assert!(crate::reports::load_analysis_input(&path).is_err());
        assert!(workspace.latest_report(&report.snapshot.connection, None).is_err());
        let context = workspace.compare_context(&report.snapshot.connection);
        assert_eq!(context.taken_at.as_deref(), Some(report.snapshot.taken_at.as_str()));
        assert_eq!(
            serde_json::to_value(context.statistics).unwrap(),
            serde_json::to_value(&report.snapshot.statistics).unwrap()
        );
        assert!(workspace.compare_context("missing").taken_at.is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
