//! Compact original fields only. The evidence service owns clocks, relationships and analysis.
use super::super::incidents::presentation::{Presentation as Incidents, preview, truncated};
use super::*;
use culprit_core::{
    CollectedWindowSection, DerivedChange, DerivedFinding, WarehouseWindowReport, WindowPublishedAnalysis,
    WindowRowPlacement, WindowWorkload, telemetry::DefinitionPayload,
};
use serde::Serialize;
const ROWS: usize = 8;
const REASONS: usize = 16;

pub(super) fn value(report: &WarehouseWindowReport) -> Result<Value> {
    let mut output = Presentation::new(report);
    while !super::super::response::admits(&output)? {
        if !output.trim() {
            return Err(DeliveryProblem::Capacity.error());
        }
    }
    serde_json::to_value(output).map_err(|_| DeliveryProblem::Integrity.error())
}
#[derive(Serialize)]
struct Presentation<'a> {
    request: &'a WarehouseWindowQuery,
    discovery_complete: bool,
    next: &'a Option<PublishedResourceCursor>,
    qualifications: Vec<String>,
    qualifications_omitted: usize,
    incidents: Incidents<'a>,
    analyses_available: usize,
    analyses_omitted: usize,
    analyses: Vec<Analysis<'a>>,
    workload_requested: bool,
    native_sections_available: usize,
    native_sections_omitted: usize,
    native_sections: Vec<Value>,
    presentation_complete: bool,
    archive_work: &'a culprit_core::collected_analysis::CollectedEvidenceMetrics,
}
impl<'a> Presentation<'a> {
    fn new(report: &'a WarehouseWindowReport) -> Self {
        let sections = report.workload.analysis().map(|analysis| analysis.sections.as_slice()).unwrap_or_default();
        let mut output = Self {
            request: &report.request,
            discovery_complete: report.discovery_complete,
            next: &report.next,
            qualifications: report.qualifications.iter().take(REASONS).map(|text| preview(text)).collect(),
            qualifications_omitted: report.qualifications.len().saturating_sub(REASONS),
            incidents: Incidents::new(&report.incidents),
            analyses_available: report.published.len(),
            analyses_omitted: 0,
            analyses: report.published.iter().take(ROWS).map(Analysis::new).collect(),
            workload_requested: matches!(report.workload, WindowWorkload::Evaluated { .. }),
            native_sections_available: sections.len(),
            native_sections_omitted: 0,
            native_sections: sections.iter().take(ROWS).map(section).collect(),
            presentation_complete: !report.qualifications.iter().take(REASONS).any(|text| truncated(text)),
            archive_work: &report.archive_work,
        };
        output.recount();
        output
    }
    fn recount(&mut self) {
        self.analyses_omitted = self.analyses_available - self.analyses.len();
        self.native_sections_omitted = self.native_sections_available - self.native_sections.len();
        self.presentation_complete &= self.qualifications_omitted == 0
            && self.analyses_omitted == 0
            && self.native_sections_omitted == 0
            && self.analyses.iter().all(Analysis::complete)
            && self.incidents.is_complete()
            && self.native_sections.iter().all(|section| section["presentation_complete"] == true);
    }
    fn trim_qualification(&mut self) -> bool {
        if self.qualifications.pop().is_none() {
            return false;
        }
        self.qualifications_omitted += 1;
        true
    }

    fn trim(&mut self) -> bool {
        let changed = self.analyses.iter_mut().rev().any(Analysis::trim)
            || self.native_sections.pop().is_some()
            || self.incidents.trim()
            || self.analyses.pop().is_some()
            || self.trim_qualification();
        if changed {
            self.presentation_complete = false;
        }
        self.recount();
        changed
    }
}
#[derive(Serialize)]
struct Analysis<'a> {
    original: &'a culprit_core::telemetry::DiagnosticCaptureRef,
    from: &'a str,
    until: &'a str,
    rules: &'a culprit_core::WindowPublishedRules,
    read_complete: bool,
    limitations: Vec<String>,
    limitations_omitted: usize,
    findings_available: usize,
    findings_omitted: usize,
    findings: Vec<Value>,
    changes_available: usize,
    changes_omitted: usize,
    changes: Vec<Value>,
    text_truncated: bool,
}
impl<'a> Analysis<'a> {
    fn new(analysis: &'a WindowPublishedAnalysis) -> Self {
        let mut output = Self {
            original: &analysis.context.capture,
            from: &analysis.context.description.observed_from,
            until: &analysis.context.description.observed_until,
            rules: &analysis.rules,
            read_complete: analysis.read_complete,
            limitations: analysis.limitations.iter().take(REASONS).map(|text| preview(text)).collect(),
            limitations_omitted: analysis.limitations.len().saturating_sub(REASONS),
            findings_available: analysis.findings.len(),
            findings_omitted: 0,
            findings: analysis.findings.iter().take(ROWS).map(|row| finding(&row.finding, &row.placement)).collect(),
            changes_available: analysis.changes.len(),
            changes_omitted: 0,
            changes: analysis.changes.iter().take(ROWS).map(|row| change(&row.change, &row.placement)).collect(),
            text_truncated: analysis.limitations.iter().take(REASONS).any(|text| truncated(text))
                || !analysis.findings.iter().take(ROWS).all(|row| finding_complete(&row.finding))
                || !analysis.changes.iter().take(ROWS).all(|row| change_complete(&row.change)),
        };
        output.recount();
        output
    }
    fn recount(&mut self) {
        self.findings_omitted = self.findings_available - self.findings.len();
        self.changes_omitted = self.changes_available - self.changes.len();
    }
    fn complete(&self) -> bool {
        self.findings_omitted == 0 && self.changes_omitted == 0 && self.limitations_omitted == 0 && !self.text_truncated
    }
    fn trim(&mut self) -> bool {
        let changed = self.changes.pop().is_some() || self.findings.pop().is_some();
        self.recount();
        changed
    }
}
fn sql(payload: &DefinitionPayload) -> Option<String> {
    match payload {
        DefinitionPayload::Available { text, .. } => Some(preview(text)),
        _ => None,
    }
}
fn finding(row: &DerivedFinding, placement: &WindowRowPlacement) -> Value {
    let finding = &row.finding;
    json!({"id":finding.id,"check":finding.check,"severity":finding.severity,"confidence":finding.confidence,
        "title":preview(&finding.title),"summary":preview(&finding.summary),
        "fix":finding.fix.as_ref().map(|fix| json!({"summary":preview(&fix.summary),"sql":fix.sql.as_deref().map(preview)})),
        "verification":finding.verify.iter().take(REASONS).map(|text| preview(text)).collect::<Vec<_>>(),
        "verification_omitted":finding.verify.len().saturating_sub(REASONS),
        "query":row.query.as_ref().map(|query| json!({"identity":query.identity,"reference":query.reference,"sql":sql(&query.sql)})),
        "cause":row.cause.as_ref().map(|cause| preview(&cause.summary)),"guidance":row.guidance,
        "placement":placement,"inputs":row.inputs.iter().take(REASONS).collect::<Vec<_>>(),
        "inputs_omitted":row.inputs.len().saturating_sub(REASONS),
        "presentation_complete":finding_complete(row)})
}
fn change(row: &DerivedChange, placement: &WindowRowPlacement) -> Value {
    let change = &row.change;
    json!({"id":change.id,"kind":change.kind,"source":change.source,"at":change.at,"after":change.after,"until":change.until,
        "title":preview(change.title.text()),"detail":change.detail.as_ref().map(|text| preview(text.text())),
        "placement":placement,"inputs":row.inputs.iter().take(REASONS).collect::<Vec<_>>(),
        "inputs_omitted":row.inputs.len().saturating_sub(REASONS),
        "presentation_complete":change_complete(row)})
}
fn finding_complete(row: &DerivedFinding) -> bool {
    let finding = &row.finding;
    row.inputs.len() <= REASONS
        && finding.verify.len() <= REASONS
        && !truncated(&finding.title)
        && !truncated(&finding.summary)
        && !finding.verify.iter().take(REASONS).any(|text| truncated(text))
        && !finding.fix.as_ref().is_some_and(|fix| truncated(&fix.summary) || fix.sql.as_deref().is_some_and(truncated))
        && !row.query.as_ref().is_some_and(|query| match &query.sql {
            DefinitionPayload::Available { text, .. } => truncated(text),
            _ => false,
        })
        && !row.cause.as_ref().is_some_and(|cause| truncated(&cause.summary))
}
fn change_complete(row: &DerivedChange) -> bool {
    row.inputs.len() <= REASONS
        && !truncated(row.change.title.text())
        && !row.change.detail.as_ref().is_some_and(|text| truncated(text.text()))
}
fn section(section: &CollectedWindowSection) -> Value {
    let complete = section.analysis.findings.len() <= ROWS
        && section.analysis.findings.iter().all(|finding| !truncated(&finding.title) && !truncated(&finding.summary))
        && section.comparison.changes.as_ref().is_none_or(|changes| {
            changes.queries.len() <= ROWS
                && !truncated(&changes.summary)
                && changes.queries.iter().all(|query| {
                    !truncated(&query.headline)
                        && !truncated(&query.text)
                        && !query.reason.as_deref().is_some_and(truncated)
                })
        });
    json!({"presentation_complete":complete,"analysis_id":section.analysis_id,"scope_id":section.scope_id,"population":section.population,
        "native_scope_qualified":section.native_scope_qualified,"observed":section.observed,
        "runtime_totals":section.analysis.runtime_totals,
        "findings_available":section.analysis.findings.len(),
        "findings":section.analysis.findings.iter().take(ROWS).map(|finding| json!({"id":finding.id,
            "title":preview(&finding.title),"summary":preview(&finding.summary)})).collect::<Vec<_>>(),
        "findings_omitted":section.analysis.findings.len().saturating_sub(ROWS),
        "comparison":section.comparison.changes.as_ref().map(|changes| json!({"source":changes.source,
            "baseline":changes.baseline,"recent":changes.recent,"baseline_total":changes.baseline_total,
            "recent_total":changes.recent_total,"summary":preview(&changes.summary),
            "queries_available":changes.queries.len(),"queries_omitted":changes.queries.len().saturating_sub(ROWS),
            "queries":changes.queries.iter().take(ROWS).map(|query| json!({"status":query.status,
                "headline":preview(&query.headline),"query":preview(&query.text),"baseline":query.baseline,
                "recent":query.recent,"reason":query.reason.as_deref().map(preview)})).collect::<Vec<_>>()}))})
}
