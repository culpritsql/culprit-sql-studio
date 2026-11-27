//! Source-free estate triage over authenticated archive publications and derived diagnoses.
use crate::{
    Engine, PublishedResourceSelector, PublishedResourceWindow, Result,
    collected_analysis::CollectedEvidenceMetrics,
    telemetry::{DeliveryProblem, SharedResourceKind, valid_digest},
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const MAX_WAREHOUSE_FLEET_PAGE_ROWS: u32 = 24;
pub const DEFAULT_WAREHOUSE_FLEET_MAX_AGE_SECONDS: u32 = 1800;
pub const MAX_WAREHOUSE_FLEET_AGE_SECONDS: u32 = 7 * 86_400;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseFleetFilter {
    pub environment: Option<String>,
    pub engine: Option<Engine>,
    pub attention_only: bool,
    pub shared_group: Option<String>,
}

impl WarehouseFleetFilter {
    pub fn is_valid(&self) -> bool {
        self.environment.as_deref().is_none_or(valid_filter_label)
            && self.shared_group.as_deref().is_none_or(valid_filter_label)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseFleetSort {
    #[default]
    Attention,
    Resource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseFleetCursor {
    /// Consistency binding to the exact contract, recovery namespace and fleet request parameters.
    pub selection_key: String,
    pub resource_id: String,
}

impl WarehouseFleetCursor {
    pub fn is_valid(&self) -> bool {
        valid_digest(&self.selection_key) && valid_digest(&self.resource_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseFleetRequest {
    pub as_of: String,
    pub window: Option<PublishedResourceWindow>,
    pub max_age_seconds: u32,
    pub filter: WarehouseFleetFilter,
    pub sort: WarehouseFleetSort,
    pub page_rows: u32,
    pub after: Option<WarehouseFleetCursor>,
}

impl Default for WarehouseFleetRequest {
    fn default() -> Self {
        Self {
            as_of: chrono::Utc::now().to_rfc3339(),
            window: None,
            max_age_seconds: DEFAULT_WAREHOUSE_FLEET_MAX_AGE_SECONDS,
            filter: WarehouseFleetFilter::default(),
            sort: WarehouseFleetSort::default(),
            page_rows: MAX_WAREHOUSE_FLEET_PAGE_ROWS,
            after: None,
        }
    }
}

impl WarehouseFleetRequest {
    pub fn validate(&self) -> Result<()> {
        if utc(&self.as_of).is_none()
            || self.window.as_ref().is_some_and(|window| !window.is_valid())
            || !(1..=MAX_WAREHOUSE_FLEET_AGE_SECONDS).contains(&self.max_age_seconds)
            || !self.filter.is_valid()
            || !(1..=MAX_WAREHOUSE_FLEET_PAGE_ROWS).contains(&self.page_rows)
            || self.after.as_ref().is_some_and(|cursor| !cursor.is_valid())
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }

    pub fn accepts_acquisition(&self, from: &str, until: &str) -> bool {
        let (Some(from), Some(until), Some(as_of)) = (utc(from), utc(until), utc(&self.as_of)) else {
            return false;
        };
        from <= until
            && until <= as_of
            && self.window.as_ref().is_none_or(|window| {
                utc(&window.from).zip(utc(&window.until)).is_some_and(|(start, end)| from < end && until >= start)
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseFleetFreshness {
    Observed,
    Stale,
    Partial,
    NoDerivedDiagnosis,
    ClockBoundary,
    UnknownClock,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseFleetSharedGroup {
    pub member_id: String,
    pub member_kind: SharedResourceKind,
    pub label: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WarehouseFleetRowTelemetry {
    pub cpu_pct: Option<f64>,
    pub memory_pct: Option<f64>,
    pub data_io_pct: Option<f64>,
    pub workers_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub load_sparkline: Vec<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cpu_sparkline: Vec<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dominant_wait: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WarehouseCollectorTarget {
    pub connection_id: String,
    pub connection_name: String,
    pub engine: Engine,
    pub connection_outcome: crate::telemetry::CollectorConnectionOutcome,
    pub original_status: Option<String>,
    pub original_stop: Option<String>,
    pub started_at: String,
    pub finished_at: String,
    pub elapsed_ms: Option<f64>,
    pub source_queries: Option<u32>,
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseFleetRow {
    pub resource_id: String,
    pub selector: PublishedResourceSelector,
    pub engine: Engine,
    pub database_name: String,
    pub connection_alias: Option<String>,
    pub environment: Option<String>,
    pub shared_group: Option<WarehouseFleetSharedGroup>,
    pub freshness: WarehouseFleetFreshness,
    pub latest_collected_from: String,
    pub latest_collected_until: String,
    pub latest_observed_from: Option<String>,
    pub latest_observed_until: Option<String>,
    pub active_incidents: u32,
    pub uncertain_incidents: u32,
    pub high_findings: u32,
    pub actionable_findings: u32,
    pub recent_regressions: u32,
    pub recent_changes: u32,
    pub input_read_complete: bool,
    pub limitations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub telemetry: Option<WarehouseFleetRowTelemetry>,
}

impl WarehouseFleetRow {
    pub fn needs_attention(&self) -> bool {
        self.active_incidents > 0
            || self.uncertain_incidents > 0
            || self.high_findings > 0
            || self.recent_regressions > 0
            || !matches!(self.freshness, WarehouseFleetFreshness::Observed)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub struct WarehouseFleetSummary {
    pub evaluated_resources: u32,
    pub attention_resources: u32,
    pub active_incidents: u32,
    pub high_finding_resources: u32,
    pub regression_resources: u32,
    pub stale_or_partial_resources: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseFleetGap {
    OriginalUnavailable,
    NotPublished,
    AcquisitionClockBoundary,
    IncompleteDerivedAnalysis,
    MoreCandidates,
}

#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseFleetReport {
    pub request: WarehouseFleetRequest,
    pub summary: WarehouseFleetSummary,
    pub rows: Vec<WarehouseFleetRow>,
    pub next: Option<WarehouseFleetCursor>,
    pub discovery_complete: bool,
    pub gaps: Vec<WarehouseFleetGap>,
    pub candidate_rows: u32,
    pub archive_work: CollectedEvidenceMetrics,
    pub qualifications: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub collector_targets: Option<Vec<WarehouseCollectorTarget>>,
}

fn valid_filter_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn utc(value: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    if value.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(value).ok().filter(|at| at.offset().local_minus_utc() == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseIdentity, IncidentDatabaseSelector};

    #[test]
    fn fleet_request_validates_bounds_digests_and_filter_labels() {
        let mut request = WarehouseFleetRequest { as_of: "2026-04-01T12:00:00Z".into(), ..Default::default() };
        assert!(request.validate().is_ok());

        request.page_rows = 0;
        assert!(request.validate().is_err());
        request.page_rows = MAX_WAREHOUSE_FLEET_PAGE_ROWS + 1;
        assert!(request.validate().is_err());
        request.page_rows = MAX_WAREHOUSE_FLEET_PAGE_ROWS;

        request.max_age_seconds = 0;
        assert!(request.validate().is_err());
        request.max_age_seconds = DEFAULT_WAREHOUSE_FLEET_MAX_AGE_SECONDS;

        request.filter.environment = Some("prod\n".into());
        assert!(request.validate().is_err());
        request.filter.environment = Some("production".into());
        assert!(request.validate().is_ok());

        request.after = Some(WarehouseFleetCursor { selection_key: "bad".into(), resource_id: "b".repeat(64) });
        assert!(request.validate().is_err());
        request.after = Some(WarehouseFleetCursor { selection_key: "a".repeat(64), resource_id: "b".repeat(64) });
        assert!(request.validate().is_ok());
    }

    #[test]
    fn fleet_row_attention_tracks_incidents_high_findings_regressions_and_freshness() {
        let database = IncidentDatabaseSelector::from_route(&DatabaseIdentity::server(
            Engine::Mssql,
            "orders",
            "sql.example.internal",
            1433,
            Some("inc-1".to_string()),
        ))
        .unwrap();
        let mut row = WarehouseFleetRow {
            resource_id: "a".repeat(64),
            selector: PublishedResourceSelector { namespace_id: "b".repeat(64), database },
            engine: Engine::Mssql,
            database_name: "orders".into(),
            connection_alias: Some("orders-prod".into()),
            environment: Some("production".into()),
            shared_group: None,
            freshness: WarehouseFleetFreshness::Observed,
            latest_collected_from: "2026-04-01T11:55:00Z".into(),
            latest_collected_until: "2026-04-01T11:56:00Z".into(),
            latest_observed_from: Some("2026-04-01T11:50:00Z".into()),
            latest_observed_until: Some("2026-04-01T11:55:00Z".into()),
            active_incidents: 0,
            uncertain_incidents: 0,
            high_findings: 0,
            actionable_findings: 2,
            recent_regressions: 0,
            recent_changes: 1,
            input_read_complete: true,
            limitations: vec![],
            telemetry: None,
        };
        assert!(!row.needs_attention());
        row.high_findings = 1;
        assert!(row.needs_attention());
        row.high_findings = 0;
        row.freshness = WarehouseFleetFreshness::Stale;
        assert!(row.needs_attention());
    }
}
