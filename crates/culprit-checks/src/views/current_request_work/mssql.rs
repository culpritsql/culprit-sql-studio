//! Native request counters keep their own lifetime; cache-generation samples do not attest an earlier executor.
use super::*;

pub(super) fn limitations() -> Vec<String> {
    vec![
        t!("view.current_request_work.mssql.limit.visibility"),
        t!("view.current_request_work.mssql.limit.parallel"),
        t!("view.current_request_work.mssql.limit.clock"),
        t!("view.current_request_work.mssql.limit.plan"),
    ]
}
pub(super) fn details(work: &MssqlCurrentRequestWork) -> Details {
    let mut metrics = counter_metrics([
        (key!("view.current_request_work.mssql.metric.reads"), &work.reads),
        (key!("view.current_request_work.mssql.metric.writes"), &work.writes),
        (key!("view.current_request_work.mssql.metric.logical_reads"), &work.logical_reads),
        (key!("view.current_request_work.mssql.metric.rows"), &work.rows_returned),
    ]);
    for (label, value) in [
        (key!("view.current_request_work.mssql.metric.elapsed"), work.elapsed_ms),
        (key!("view.current_request_work.mssql.metric.cpu"), work.cpu_ms),
    ] {
        if let Some(value) = value.filter(|value| *value >= 0) {
            metrics.push(Metric::said_text(label, format::duration(f64::from(value))));
        }
    }
    let mut words = vec![t!("view.current_request_work.mssql.observed")];
    if [work.elapsed_ms, work.cpu_ms].into_iter().flatten().any(|value| value < 0) {
        words.push(t!("view.current_request_work.mssql.negative_time"));
    }
    words.push(match &work.current_plan {
        MssqlCurrentRequestPlan::Observed { observed_from, observed_until, context, plan } => {
            metrics.extend([
                Metric::said_text(
                    key!("view.current_request_work.mssql.metric.plan_generation"),
                    format::digits(context.identity.plan_generation.clone()),
                ),
                Metric::said_text(
                    key!("view.current_request_work.mssql.metric.compiled"),
                    &context.identity.created_without_offset,
                ),
            ]);
            t!(
                "view.current_request_work.mssql.plan",
                from = observed_from,
                until = observed_until,
                payload = if plan.is_available() { "available" } else { "unavailable" }
            )
        }
        MssqlCurrentRequestPlan::Unavailable { reason } => t!(
            "view.current_request_work.mssql.plan_unavailable",
            reason = match reason {
                MssqlCurrentPlanUnavailable::NotObserved => "not-observed",
                MssqlCurrentPlanUnavailable::ChangedDuringRead => "changed",
                MssqlCurrentPlanUnavailable::Ambiguous => "ambiguous",
                MssqlCurrentPlanUnavailable::PermissionDenied => "denied",
                MssqlCurrentPlanUnavailable::Unsupported => "unsupported",
                MssqlCurrentPlanUnavailable::NotCollected => "not-collected",
            }
        ),
    });
    Details {
        explanation: words.join(" "),
        metrics,
        investigation: t!("view.current_request_work.mssql.investigate"),
        verify: t!("view.current_request_work.mssql.verify"),
    }
}
