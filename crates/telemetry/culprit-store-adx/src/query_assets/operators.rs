//! ADX query models for an existing Grafana data source. All rows still enter through namespace eligibility.
use super::*;
use culprit_evidence::analytical::query_helpers::operators::{
    self, OperatorAlert, OperatorDeploymentAssets, OperatorPanel,
};
use serde_json::{Value, json};

const PLUGIN: &str = "grafana-azure-data-explorer-datasource";

pub(super) fn assets(binding: &QueryHelperBinding) -> Result<OperatorDeploymentAssets> {
    operators::configured(
        binding,
        PLUGIN,
        |panel| model(panel_query(&binding.policy.name, panel), operators::DASHBOARD_ROW_LIMIT),
        |alert| model(alert_query(&binding.policy.name, alert), operators::ALERT_ROW_LIMIT),
    )
}
fn model(query: String, rows: u32) -> Value {
    json!({"refId": "A", "query": format!("set truncationmaxrecords={rows};\nset truncationmaxsize={};\n{query}", operators::QUERY_RESULT_BYTES),
        "clusterUri": "", "database": "", "resultFormat": "table", "rawMode": true, "querySource": "raw", "queryType": "KQL"})
}
fn panel_query(n: &str, panel: OperatorPanel) -> String {
    let window = "$__timeFrom,$__timeTo,now()";
    let query = match panel {
        OperatorPanel::Resources => {
            format!("{n}Resources(now()) | order by Alias asc,SourceId asc,CollectedUntil desc")
        }
        OperatorPanel::Collection => format!(
            "{n}CollectionStatus(now(),{}s) | order by CollectedUntil desc,SourceId asc",
            operators::OBSERVATION_MAX_AGE_SECONDS
        ),
        OperatorPanel::Availability => format!(
            "{n}StatusRows(now(),{}s) | order by Alias asc,SourceId asc",
            operators::OBSERVATION_MAX_AGE_SECONDS
        ),
        OperatorPanel::Incidents => {
            format!("{n}IncidentWindow({window}) | order by OriginalRecordedAt desc,EpisodeId asc")
        }
        OperatorPanel::History => {
            format!("{n}HistoryWindow({window}) | order by OriginalRecordedAt desc,EpisodeId asc,Revision desc")
        }
        OperatorPanel::Findings => format!(
            "{n}FindingWindow({window}) | where Severity == \"high\" and Actionability in (\"needs-attention\",\"optional-improvement\") and not(Inactive) | order by OriginalObservedUntil desc,FindingId asc"
        ),
        OperatorPanel::Changes => {
            format!("{n}ChangeWindow({window}) | order by OriginalObservedUntil desc,ChangeId asc")
        }
        OperatorPanel::Deadlocks => format!("{n}DeadlockWindow({window}) | order by EventAt desc,EventPosition asc"),
    };
    format!("{query} | take {}", operators::DASHBOARD_ROW_LIMIT)
}
fn alert_query(n: &str, alert: OperatorAlert) -> String {
    let window = operators::NOTIFICATION_WINDOW_SECONDS;
    match alert {
        OperatorAlert::IncidentRecorded => format!(
            "{n}NewIncidentWindow(ago({window}s),now(),now())\n| project ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion=tostring(ControllerVersion),Value=1.0\n| distinct *"
        ),
        OperatorAlert::DeadlockRecorded => format!(
            "{n}DeadlockWindow(ago({window}s),now(),now())\n| project ResourceId,SourceId,EventPosition,Value=1.0\n| distinct *"
        ),
        OperatorAlert::DataUnavailable => availability_query(n),
    }
}
fn availability_query(n: &str) -> String {
    format!(
        r#"let Published=materialize({n}Published(now()));
let Collection=materialize({n}CollectionStatus(now(),{age}s));
let Diagnosed={n}Headers(now()) | distinct ResourceId;
union
({n}StatusRows(now(),{age}s) | project ResourceId,SourceId,Kind="diagnosis",Value=iff(State == "observed",0.0,1.0)),
(Collection | project ResourceId,SourceId,Kind="collection",Value=iff(State == "observed-acquisition",0.0,1.0)),
(Collection | join kind=leftanti Diagnosed on ResourceId | project ResourceId,SourceId,Kind="diagnosis-unavailable",Value=1.0),
(print ResourceId="",SourceId="",Kind="namespace-unavailable",Value=1.0 | where toscalar(Published | count) == 0),
(print ResourceId="",SourceId="",Kind="acquisition-unavailable",Value=1.0 | where toscalar(Collection | count) == 0 and toscalar(Published | count) > 0)
| summarize Value=max(Value) by ResourceId,SourceId,Kind"#,
        age = operators::OBSERVATION_MAX_AGE_SECONDS
    )
}
