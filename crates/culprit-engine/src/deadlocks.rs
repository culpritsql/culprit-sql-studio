//! Link native deadlock fixes to findings in the latest report. Parsing is owned by culprit-plans.
use culprit_core::{DeadlockReport, Report};

/// Keep a fix's finding link only when the latest report has that finding, so links never lead nowhere.
pub fn link_findings(report: &mut DeadlockReport, latest: Option<&Report>) {
    for d in &mut report.deadlocks {
        for fix in &mut d.fixes {
            if let Some(id) = fix.finding.as_deref()
                && !latest.is_some_and(|r| r.findings.iter().any(|f| f.id == id))
            {
                fix.finding = None;
            }
        }
    }
}
