//! Import documents for existing Grafana installations. Adapters own their query syntax.
use super::*;
use culprit_core::t;
use serde_json::{Value, json};

pub const DASHBOARD_ROW_LIMIT: u32 = 501;
pub const ALERT_ROW_LIMIT: u32 = 10_000;
pub const QUERY_RESULT_BYTES: u32 = 8 * 1024 * 1024;
pub const NOTIFICATION_WINDOW_SECONDS: u32 = 5 * 60;
pub const QUERY_TIMEOUT_SECONDS: u32 = 30;
pub const OBSERVATION_MAX_AGE_SECONDS: u32 = 30 * 60;

#[derive(Serialize)]
pub struct OperatorDeploymentAssets {
    /// Classic dashboard JSON. Import selects an existing data source once.
    pub dashboard: Value,
    /// Self-managed Grafana file provisioning. All rules begin paused; no notification routes are installed.
    pub alerts: Value,
}

#[derive(Clone, Copy)]
pub enum OperatorPanel {
    Resources,
    Collection,
    Availability,
    Incidents,
    History,
    Findings,
    Changes,
    Deadlocks,
}
impl OperatorPanel {
    pub const ALL: [Self; 8] = [
        Self::Resources,
        Self::Collection,
        Self::Availability,
        Self::Incidents,
        Self::History,
        Self::Findings,
        Self::Changes,
        Self::Deadlocks,
    ];
    fn id(self) -> u32 {
        match self {
            Self::Resources => 1,
            Self::Collection => 2,
            Self::Availability => 3,
            Self::Incidents => 4,
            Self::History => 5,
            Self::Findings => 6,
            Self::Changes => 7,
            Self::Deadlocks => 8,
        }
    }
    fn title(self) -> String {
        match self {
            Self::Resources => t!("query_helpers.operator.panel.resources"),
            Self::Collection => t!("query_helpers.operator.panel.collection"),
            Self::Availability => t!("query_helpers.operator.panel.availability"),
            Self::Incidents => t!("query_helpers.operator.panel.incidents"),
            Self::History => t!("query_helpers.operator.panel.history"),
            Self::Findings => t!("query_helpers.operator.panel.findings"),
            Self::Changes => t!("query_helpers.operator.panel.changes"),
            Self::Deadlocks => t!("query_helpers.operator.panel.deadlocks"),
        }
    }
    fn description(self) -> String {
        let limit = culprit_core::format::count(f64::from(DASHBOARD_ROW_LIMIT));
        match self {
            Self::Resources | Self::Incidents => t!("query_helpers.operator.panel.scope_note", limit = limit),
            Self::Collection => t!("query_helpers.operator.panel.collection_note", limit = limit),
            Self::Availability => t!("query_helpers.operator.panel.availability_note", limit = limit),
            Self::History => t!("query_helpers.operator.panel.history_note", limit = limit),
            Self::Findings => t!("query_helpers.operator.panel.findings_note", limit = limit),
            Self::Changes => t!("query_helpers.operator.panel.changes_note", limit = limit),
            Self::Deadlocks => t!("query_helpers.operator.panel.deadlocks_note", limit = limit),
        }
    }
}

#[derive(Clone, Copy)]
pub enum OperatorAlert {
    IncidentRecorded,
    DeadlockRecorded,
    DataUnavailable,
}
impl OperatorAlert {
    const ALL: [Self; 3] = [Self::IncidentRecorded, Self::DeadlockRecorded, Self::DataUnavailable];
    fn panel(self) -> OperatorPanel {
        match self {
            Self::IncidentRecorded => OperatorPanel::Incidents,
            Self::DeadlockRecorded => OperatorPanel::Deadlocks,
            Self::DataUnavailable => OperatorPanel::Availability,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::IncidentRecorded => "incident",
            Self::DeadlockRecorded => "deadlock",
            Self::DataUnavailable => "coverage",
        }
    }
    fn title(self) -> String {
        match self {
            Self::IncidentRecorded => t!("query_helpers.operator.alert.incident"),
            Self::DeadlockRecorded => t!("query_helpers.operator.alert.deadlock"),
            Self::DataUnavailable => t!("query_helpers.operator.alert.coverage"),
        }
    }
    fn description(self) -> String {
        match self {
            Self::IncidentRecorded => t!("query_helpers.operator.alert.incident_note"),
            Self::DeadlockRecorded => t!("query_helpers.operator.alert.deadlock_note"),
            Self::DataUnavailable => t!("query_helpers.operator.alert.coverage_note"),
        }
    }
}

/// Only the external document shape is shared. No adapter, credentials or rule execution lives here.
pub fn configured(
    binding: &QueryHelperBinding,
    plugin: &str,
    panel: impl Fn(OperatorPanel) -> Value,
    alert: impl Fn(OperatorAlert) -> Value,
) -> Result<OperatorDeploymentAssets> {
    binding.validate()?;
    let id = identity(binding)?;
    Ok(OperatorDeploymentAssets {
        dashboard: dashboard(binding, plugin, &id, panel),
        alerts: alerts(binding, &id, alert)?,
    })
}
fn identity(binding: &QueryHelperBinding) -> Result<String> {
    // Reanalysis, later revisions and rules changes must not rename the installed notification rule.
    let bytes = serde_json::to_vec(&(
        "culprit-operator-pack-v1",
        &binding.contract_id,
        &binding.recovery_store_id,
        &binding.policy.name,
    ))
    .map_err(|_| invalid())?;
    Ok(culprit_core::telemetry::digest(&bytes)[..16].to_string())
}
fn dashboard(binding: &QueryHelperBinding, plugin: &str, id: &str, query: impl Fn(OperatorPanel) -> Value) -> Value {
    let datasource = json!({"type": plugin, "uid": "${DS_CULPRIT}"});
    let panels = OperatorPanel::ALL
        .iter()
        .enumerate()
        .map(|(index, kind)| {
            json!({
                "id": kind.id(), "type": "table", "title": kind.title(), "description": kind.description(),
                "datasource": datasource, "gridPos": {"x": 0, "y": index * 9, "w": 24, "h": 9},
                "targets": [query(*kind)], "options": {"showHeader": true, "cellHeight": "sm"},
                "fieldConfig": {"defaults": {"custom": {"filterable": true}}, "overrides": []},
                "transformations": []
            })
        })
        .collect::<Vec<_>>();
    json!({
        "__inputs": [{"name": "DS_CULPRIT", "label": t!("query_helpers.operator.datasource"),
            "description": t!("query_helpers.operator.datasource_note"), "type": "datasource", "pluginId": plugin, "pluginName": plugin}],
        "id": null, "uid": format!("culprit-{id}"), "title": t!("query_helpers.operator.dashboard.title", name = &binding.policy.name),
        "description": t!("query_helpers.operator.dashboard.note"), "tags": ["culprit", "retained-evidence"], "timezone": "utc", "editable": true,
        "schemaVersion": 39, "version": 1, "refresh": "1m", "time": {"from": "now-6h", "to": "now"},
        "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "1h"]}, "templating": {"list": []},
        "annotations": {"list": []}, "links": [], "panels": panels
    })
}
fn alerts(binding: &QueryHelperBinding, id: &str, query: impl Fn(OperatorAlert) -> Value) -> Result<Value> {
    let rules = OperatorAlert::ALL.iter().map(|kind| {
        let mut model = query(*kind);
        model.as_object_mut().ok_or_else(invalid)?.insert("refId".into(), json!("A"));
        Ok(json!({
            "uid": format!("culprit-{id}-{}", kind.name()), "title": kind.title(), "condition": "C", "for": "0s", "isPaused": true,
            "noDataState": if matches!(kind, OperatorAlert::DataUnavailable) {"Alerting"} else {"OK"}, "execErrState": "Error",
            "data": [
                {"refId": "A", "relativeTimeRange": {"from": NOTIFICATION_WINDOW_SECONDS, "to": 0}, "datasourceUid": "REPLACE_WITH_EXISTING_DATASOURCE_UID", "model": model},
                {"refId": "C", "relativeTimeRange": {"from": 0, "to": 0}, "datasourceUid": "__expr__", "model": {
                    "refId": "C", "type": "threshold", "expression": "A", "conditions": [{"type": "query",
                        "evaluator": {"type": "gt", "params": [0]}, "operator": {"type": "and"},
                        "query": {"params": ["C"]}, "reducer": {"type": "last", "params": []}}],
                    "datasource": {"type": "__expr__", "uid": "__expr__"}}}
            ],
            "labels": {"culprit_policy": binding.policy.name, "culprit_notification": kind.name()},
            "annotations": {"summary": kind.title(), "description": kind.description()}, "dashboardUid": format!("culprit-{id}"), "panelId": kind.panel().id()
        }))
    }).collect::<Result<Vec<_>>>()?;
    Ok(json!({"apiVersion": 1, "groups": [{"orgId": 1, "name": format!("culprit-{}", binding.policy.name),
        "folder": t!("query_helpers.operator.folder"), "interval": "1m", "rules": rules}]}))
}

#[cfg(test)]
mod tests;
