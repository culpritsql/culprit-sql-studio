//! ClickHouse query models use the same eligible publication and Gold views as ordinary readers.
use super::*;
use culprit_evidence::analytical::query_helpers::operators::{
    self, OperatorAlert, OperatorDeploymentAssets, OperatorPanel,
};
use serde_json::{Value, json};

const PLUGIN: &str = "grafana-clickhouse-datasource";

pub(super) fn assets(binding: &QueryHelperBinding) -> Result<OperatorDeploymentAssets> {
    operators::configured(
        binding,
        PLUGIN,
        |panel| model(panel_query(&binding.policy.name, panel), operators::DASHBOARD_ROW_LIMIT),
        |alert| model(alert_query(&binding.policy.name, alert), operators::ALERT_ROW_LIMIT),
    )
}
fn model(query: String, rows: u32) -> Value {
    json!({"refId": "A", "editorType": "sql", "queryType": "table",
        "rawSql": format!("{query} SETTINGS max_execution_time={},max_result_rows={rows},max_result_bytes={},result_overflow_mode='throw'", operators::QUERY_TIMEOUT_SECONDS, operators::QUERY_RESULT_BYTES)})
}
fn panel_query(n: &str, panel: OperatorPanel) -> String {
    // Grafana supplies numeric Unix milliseconds. No untrusted resource text is interpolated into SQL.
    let window = "from=fromUnixTimestamp64Milli(${__from}),until=fromUnixTimestamp64Milli(${__to}),asof=now64(9,'UTC')";
    let query = match panel {
        OperatorPanel::Resources => {
            format!("SELECT * FROM {n}Resources(asof=now64(9,'UTC')) ORDER BY Alias,SourceId,CollectedUntil DESC")
        }
        OperatorPanel::Collection => format!(
            "SELECT * FROM {n}CollectionStatus(asof=now64(9,'UTC'),max_age_seconds={}) ORDER BY CollectedUntil DESC,SourceId",
            operators::OBSERVATION_MAX_AGE_SECONDS
        ),
        OperatorPanel::Availability => format!(
            "SELECT * FROM {n}StatusRows(asof=now64(9,'UTC'),max_age_seconds={}) ORDER BY Alias,SourceId",
            operators::OBSERVATION_MAX_AGE_SECONDS
        ),
        OperatorPanel::Incidents => {
            format!("SELECT * FROM {n}IncidentWindow({window}) ORDER BY OriginalRecordedAt DESC,EpisodeId")
        }
        OperatorPanel::History => {
            format!("SELECT * FROM {n}HistoryWindow({window}) ORDER BY OriginalRecordedAt DESC,EpisodeId,Revision DESC")
        }
        OperatorPanel::Findings => format!(
            "SELECT * FROM {n}FindingWindow({window}) WHERE Severity='high' AND Actionability IN ('needs-attention','optional-improvement') AND NOT Inactive ORDER BY OriginalObservedUntil DESC,FindingId"
        ),
        OperatorPanel::Changes => {
            format!("SELECT * FROM {n}ChangeWindow({window}) ORDER BY OriginalObservedUntil DESC,ChangeId")
        }
        OperatorPanel::Deadlocks => {
            format!("SELECT * FROM {n}DeadlockWindow({window}) ORDER BY EventAt DESC,EventPosition")
        }
    };
    format!("{query} LIMIT {}", operators::DASHBOARD_ROW_LIMIT)
}
fn alert_query(n: &str, alert: OperatorAlert) -> String {
    let window = format!(
        "from=now64(9,'UTC')-INTERVAL {} SECOND,until=now64(9,'UTC'),asof=now64(9,'UTC')",
        operators::NOTIFICATION_WINDOW_SECONDS
    );
    match alert {
        OperatorAlert::IncidentRecorded => format!(
            "SELECT DISTINCT ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,toString(ControllerVersion) AS ControllerVersion,toFloat64(1) AS Value FROM {n}NewIncidentWindow({window})"
        ),
        OperatorAlert::DeadlockRecorded => format!(
            "SELECT DISTINCT ResourceId,SourceId,EventPosition,toFloat64(1) AS Value FROM {n}DeadlockWindow({window})"
        ),
        OperatorAlert::DataUnavailable => availability_query(n),
    }
}
fn availability_query(n: &str) -> String {
    format!(
        r#"WITH Published AS (SELECT * FROM {n}Published(asof=now64(9,'UTC'))),
Collection AS (SELECT * FROM {n}CollectionStatus(asof=now64(9,'UTC'),max_age_seconds={age})),
Diagnosed AS (SELECT DISTINCT ResourceId FROM {n}Headers(asof=now64(9,'UTC'))),
States AS (
    SELECT ResourceId,SourceId,'diagnosis' AS Kind,toFloat64(State!='observed') AS Value FROM {n}StatusRows(asof=now64(9,'UTC'),max_age_seconds={age})
    UNION ALL SELECT ResourceId,SourceId,'collection' AS Kind,toFloat64(State!='observed-acquisition') AS Value FROM Collection
    UNION ALL SELECT ResourceId,SourceId,'diagnosis-unavailable' AS Kind,toFloat64(1) AS Value FROM Collection WHERE ResourceId NOT IN (SELECT ResourceId FROM Diagnosed)
    UNION ALL SELECT '' AS ResourceId,'' AS SourceId,'namespace-unavailable' AS Kind,toFloat64(1) AS Value WHERE (SELECT count() FROM Published)=0
    UNION ALL SELECT '' AS ResourceId,'' AS SourceId,'acquisition-unavailable' AS Kind,toFloat64(1) AS Value WHERE (SELECT count() FROM Collection)=0 AND (SELECT count() FROM Published)>0
)
SELECT ResourceId,SourceId,Kind,max(Value) AS Value FROM States GROUP BY ResourceId,SourceId,Kind"#,
        age = operators::OBSERVATION_MAX_AGE_SECONDS
    )
}
