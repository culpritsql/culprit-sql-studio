use crate::{
    AlertCategory, AlertSeverity, AlertTransition, RedactedAlertEvent, deterministic_dedup_key,
    store::{DeduplicationState, EpisodeAlertStatus, TrackedAvailability, TrackedEpisode, TrackedFinding},
};
use chrono::{DateTime, Duration, Utc};
use culprit_core::{Result, t};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertEvaluationPolicy {
    /// Suppress newly discovered captures older than `evaluated_at - novelty_horizon_seconds` during catch-up/backfill.
    #[serde(default = "default_novelty_horizon_seconds")]
    pub novelty_horizon_seconds: u64,
    /// Consecutive distinct evaluation windows required before alerting on a persistent high-severity finding.
    #[serde(default = "default_min_consecutive_windows")]
    pub min_consecutive_windows: u32,
    /// Optional validated HTTPS archive viewer base URL for deep links (`https://archives.example.com`).
    #[serde(default)]
    pub archive_viewer_origin: Option<String>,
}

fn default_novelty_horizon_seconds() -> u64 {
    3_600
}

fn default_min_consecutive_windows() -> u32 {
    2
}

impl Default for AlertEvaluationPolicy {
    fn default() -> Self {
        Self {
            novelty_horizon_seconds: default_novelty_horizon_seconds(),
            min_consecutive_windows: default_min_consecutive_windows(),
            archive_viewer_origin: None,
        }
    }
}

impl AlertEvaluationPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.novelty_horizon_seconds == 0 || self.min_consecutive_windows == 0 || self.min_consecutive_windows > 32 {
            return Err(culprit_core::Error::invalid(t!("notify.config.invalid_policy")));
        }
        if let Some(origin) = &self.archive_viewer_origin
            && culprit_auth::https_origin(origin).is_none()
        {
            return Err(culprit_core::Error::invalid(t!("notify.config.invalid_origin")));
        }
        Ok(())
    }

    fn deep_link(&self, resource_alias: &str) -> Option<String> {
        let base = self.archive_viewer_origin.as_deref().and_then(culprit_auth::https_origin)?;
        let encoded: String = resource_alias
            .bytes()
            .flat_map(|byte| {
                if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                    vec![byte as char]
                } else {
                    format!("%{byte:02X}").chars().collect()
                }
            })
            .collect();
        crate::validate_deep_link(&format!("{base}/?resource={encoded}"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncidentAlertInput {
    #[serde(default)]
    pub engine: Option<culprit_core::Engine>,
    pub target_id: String,
    pub original_scope_id: String,
    pub original_source: String,
    pub episode_id: String,
    pub controller_version: i64,
    pub revision: String,
    pub lifecycle: String,
    pub first_recorded_observation: String,
    pub observed_until: String,
    pub connection_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub title: String,
    pub victim_summary: String,
    pub culprit_query: Option<String>,
    pub developer_action: String,
    pub verification: String,
}

impl IncidentAlertInput {
    pub fn canonical_storage_key(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            self.target_id, self.original_scope_id, self.original_source, self.episode_id, self.controller_version
        )
    }

    pub fn dedup_key(&self) -> String {
        let cv = self.controller_version.to_string();
        deterministic_dedup_key(
            AlertCategory::Incident,
            &[&self.target_id, &self.original_scope_id, &self.original_source, &self.episode_id, &cv],
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FindingAlertInput {
    #[serde(default)]
    pub engine: Option<culprit_core::Engine>,
    pub resource_id: String,
    pub finding_id: String,
    pub check_id: String,
    pub severity: String,
    pub actionability: String,
    pub inactive: bool,
    pub observed_from: String,
    pub observed_until: String,
    pub connection_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub title: String,
    pub summary: String,
    pub query_sql: Option<String>,
    pub developer_action: Option<String>,
    pub verification: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegressionAlertInput {
    #[serde(default)]
    pub engine: Option<culprit_core::Engine>,
    pub resource_id: String,
    pub regression_id: String,
    pub observed_until: String,
    pub connection_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    pub title: String,
    pub summary: String,
    pub query_sql: Option<String>,
    pub developer_action: Option<String>,
    pub verification: Option<String>,
    /// Co-occurring timeline events (`culprit mark` deploy markers, index/setting changes) in the window.
    #[serde(default)]
    pub proximity_marks: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvailabilityAlertInput {
    pub resource_id: String,
    pub connection_alias: String,
    pub database_name: Option<String>,
    pub environment: Option<String>,
    /// StatusNow state (`"observed"`, `"stale"`, `"partial"`, `"incomplete-output"`, `"collector-failing"`, etc.).
    pub state: String,
    pub input_read_complete: bool,
    #[serde(default)]
    pub context_issues: Vec<String>,
    pub observed_at: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertBatchInput {
    pub evaluated_at: String,
    #[serde(default)]
    pub incidents: Vec<IncidentAlertInput>,
    #[serde(default)]
    pub findings: Vec<FindingAlertInput>,
    #[serde(default)]
    pub regressions: Vec<RegressionAlertInput>,
    /// Resources whose active findings were completely evaluated in this batch, allowing absent findings to resolve.
    #[serde(default)]
    pub assessed_finding_resources: BTreeSet<String>,
    #[serde(default)]
    pub availability: Vec<AvailabilityAlertInput>,
}

/// Pure, deterministic alert evaluator (`(AlertBatchInput, &mut DeduplicationState) -> Vec<RedactedAlertEvent>`).
pub struct AlertEvaluator {
    policy: AlertEvaluationPolicy,
}

impl AlertEvaluator {
    pub fn new(policy: AlertEvaluationPolicy) -> Result<Self> {
        policy.validate()?;
        Ok(Self { policy })
    }

    pub fn evaluate(&self, batch: &AlertBatchInput, state: &mut DeduplicationState) -> Result<Vec<RedactedAlertEvent>> {
        let now = parse_utc(&batch.evaluated_at)?;
        let horizon_cutoff = now - Duration::seconds(self.policy.novelty_horizon_seconds as i64);
        let mut emitted = Vec::new();

        self.evaluate_incidents(&batch.incidents, horizon_cutoff, &batch.evaluated_at, state, &mut emitted)?;
        self.evaluate_findings(
            &batch.findings,
            &batch.assessed_finding_resources,
            horizon_cutoff,
            &batch.evaluated_at,
            state,
            &mut emitted,
        )?;
        self.evaluate_regressions(&batch.regressions, horizon_cutoff, state, &mut emitted)?;
        self.evaluate_availability(&batch.availability, state, &mut emitted);

        state.enforce_bounds();
        Ok(emitted)
    }

    fn evaluate_incidents(
        &self,
        incidents: &[IncidentAlertInput],
        horizon_cutoff: DateTime<Utc>,
        evaluated_at: &str,
        state: &mut DeduplicationState,
        emitted: &mut Vec<RedactedAlertEvent>,
    ) -> Result<()> {
        for incident in incidents {
            let storage_key = incident.canonical_storage_key();
            let dedup_key = incident.dedup_key();
            let first_obs = parse_utc(&incident.first_recorded_observation)?;

            match incident.lifecycle.as_str() {
                "active" => match state.episodes.get(&storage_key).map(|entry| &entry.status) {
                    Some(EpisodeAlertStatus::Triggered | EpisodeAlertStatus::SuppressedBackfill) => {
                        // Revised enrichments, newer Revision numbers, AnalysisId, or rules_version updates never re-page.
                        if let Some(existing) = state.episodes.get_mut(&storage_key) {
                            existing.updated_at = incident.observed_until.clone();
                            existing.headline = incident.title.clone();
                        }
                    }
                    Some(EpisodeAlertStatus::Resolved) | None => {
                        if first_obs < horizon_cutoff {
                            state.episodes.insert(
                                storage_key,
                                TrackedEpisode {
                                    dedup_key,
                                    status: EpisodeAlertStatus::SuppressedBackfill,
                                    first_observed_at: incident.first_recorded_observation.clone(),
                                    updated_at: incident.observed_until.clone(),
                                    resource_alias: incident.connection_alias.clone(),
                                    database_name: incident.database_name.clone(),
                                    environment: incident.environment.clone(),
                                    headline: incident.title.clone(),
                                },
                            );
                            continue;
                        }
                        state.episodes.insert(
                            storage_key,
                            TrackedEpisode {
                                dedup_key: dedup_key.clone(),
                                status: EpisodeAlertStatus::Triggered,
                                first_observed_at: incident.first_recorded_observation.clone(),
                                updated_at: incident.observed_until.clone(),
                                resource_alias: incident.connection_alias.clone(),
                                database_name: incident.database_name.clone(),
                                environment: incident.environment.clone(),
                                headline: incident.title.clone(),
                            },
                        );
                        emitted.push(
                            RedactedAlertEvent {
                                dedup_key,
                                category: AlertCategory::Incident,
                                transition: AlertTransition::Triggered,
                                severity: AlertSeverity::Critical,
                                engine: incident.engine,
                                resource_alias: incident.connection_alias.clone(),
                                database_name: incident.database_name.clone(),
                                environment: incident.environment.clone(),
                                headline: incident.title.clone(),
                                summary: incident.victim_summary.clone(),
                                culprit_query: incident.culprit_query.clone(),
                                victim_summary: Some(incident.victim_summary.clone()),
                                developer_action: Some(incident.developer_action.clone()),
                                verification: Some(incident.verification.clone()),
                                proximity_marks: Vec::new(),
                                observed_at: incident.observed_until.clone(),
                                deep_link: self.policy.deep_link(&incident.connection_alias),
                            }
                            .sanitized(),
                        );
                    }
                },
                "resolved" => {
                    if let Some(tracked) = state.episodes.get_mut(&storage_key)
                        && tracked.status == EpisodeAlertStatus::Triggered
                    {
                        tracked.status = EpisodeAlertStatus::Resolved;
                        tracked.updated_at = incident.observed_until.clone();
                        emitted.push(
                            RedactedAlertEvent {
                                dedup_key: tracked.dedup_key.clone(),
                                category: AlertCategory::Incident,
                                transition: AlertTransition::Resolved,
                                severity: AlertSeverity::Info,
                                engine: incident.engine,
                                resource_alias: incident.connection_alias.clone(),
                                database_name: incident.database_name.clone(),
                                environment: incident.environment.clone(),
                                headline: incident.title.clone(),
                                summary: t!("notify.event.incident_resolved", alias = &incident.connection_alias),
                                culprit_query: incident.culprit_query.clone(),
                                victim_summary: None,
                                developer_action: None,
                                verification: Some(incident.verification.clone()),
                                proximity_marks: Vec::new(),
                                observed_at: if incident.observed_until.is_empty() {
                                    evaluated_at.to_string()
                                } else {
                                    incident.observed_until.clone()
                                },
                                deep_link: self.policy.deep_link(&incident.connection_alias),
                            }
                            .sanitized(),
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn evaluate_findings(
        &self,
        findings: &[FindingAlertInput],
        assessed_resources: &BTreeSet<String>,
        horizon_cutoff: DateTime<Utc>,
        evaluated_at: &str,
        state: &mut DeduplicationState,
        emitted: &mut Vec<RedactedAlertEvent>,
    ) -> Result<()> {
        let mut active_keys_seen = BTreeSet::new();
        for finding in findings {
            let actionable = matches!(finding.actionability.as_str(), "needs-attention" | "optional-improvement");
            if finding.inactive || finding.severity != "high" || !actionable {
                continue;
            }
            let obs_until = parse_utc(&finding.observed_until)?;
            if obs_until < horizon_cutoff {
                continue;
            }
            let storage_key = format!("{}|{}", finding.resource_id, finding.finding_id);
            active_keys_seen.insert(storage_key.clone());
            let dedup_key =
                deterministic_dedup_key(AlertCategory::HighFinding, &[&finding.resource_id, &finding.finding_id]);

            let entry = state.findings.entry(storage_key).or_insert_with(|| TrackedFinding {
                dedup_key: dedup_key.clone(),
                consecutive_windows: 0,
                last_window_until: String::new(),
                alerted: false,
                resource_alias: finding.connection_alias.clone(),
                database_name: finding.database_name.clone(),
                environment: finding.environment.clone(),
                headline: finding.title.clone(),
            });
            if entry.last_window_until != finding.observed_until {
                entry.consecutive_windows = entry.consecutive_windows.saturating_add(1);
                entry.last_window_until = finding.observed_until.clone();
            }
            entry.headline = finding.title.clone();
            if !entry.alerted && entry.consecutive_windows >= self.policy.min_consecutive_windows {
                entry.alerted = true;
                emitted.push(
                    RedactedAlertEvent {
                        dedup_key,
                        category: AlertCategory::HighFinding,
                        transition: AlertTransition::Triggered,
                        severity: AlertSeverity::High,
                        engine: finding.engine,
                        resource_alias: finding.connection_alias.clone(),
                        database_name: finding.database_name.clone(),
                        environment: finding.environment.clone(),
                        headline: finding.title.clone(),
                        summary: finding.summary.clone(),
                        culprit_query: finding.query_sql.clone(),
                        victim_summary: None,
                        developer_action: finding.developer_action.clone(),
                        verification: finding.verification.clone(),
                        proximity_marks: Vec::new(),
                        observed_at: finding.observed_until.clone(),
                        deep_link: self.policy.deep_link(&finding.connection_alias),
                    }
                    .sanitized(),
                );
            }
        }

        // Resolve previously alerted findings whose resource was completely evaluated without the finding recurring.
        let mut cleared = Vec::new();
        for (storage_key, tracked) in &state.findings {
            let Some((resource_id, _)) = storage_key.split_once('|') else { continue };
            if assessed_resources.contains(resource_id) && !active_keys_seen.contains(storage_key) {
                if tracked.alerted {
                    emitted.push(
                        RedactedAlertEvent {
                            dedup_key: tracked.dedup_key.clone(),
                            category: AlertCategory::HighFinding,
                            transition: AlertTransition::Resolved,
                            severity: AlertSeverity::Info,
                            engine: None,
                            resource_alias: tracked.resource_alias.clone(),
                            database_name: tracked.database_name.clone(),
                            environment: tracked.environment.clone(),
                            headline: tracked.headline.clone(),
                            summary: t!("notify.event.finding_resolved", alias = &tracked.resource_alias),
                            culprit_query: None,
                            victim_summary: None,
                            developer_action: None,
                            verification: None,
                            proximity_marks: Vec::new(),
                            observed_at: evaluated_at.to_string(),
                            deep_link: self.policy.deep_link(&tracked.resource_alias),
                        }
                        .sanitized(),
                    );
                }
                cleared.push(storage_key.clone());
            }
        }
        for key in cleared {
            state.findings.remove(&key);
        }
        Ok(())
    }

    fn evaluate_regressions(
        &self,
        regressions: &[RegressionAlertInput],
        horizon_cutoff: DateTime<Utc>,
        state: &mut DeduplicationState,
        emitted: &mut Vec<RedactedAlertEvent>,
    ) -> Result<()> {
        for regression in regressions {
            let obs_until = parse_utc(&regression.observed_until)?;
            let storage_key = format!("{}|{}", regression.resource_id, regression.regression_id);
            if state.regressions.contains_key(&storage_key) {
                continue;
            }
            state.regressions.insert(storage_key, regression.observed_until.clone());
            if obs_until < horizon_cutoff {
                continue;
            }
            let dedup_key = deterministic_dedup_key(
                AlertCategory::Regression,
                &[&regression.resource_id, &regression.regression_id],
            );
            emitted.push(
                RedactedAlertEvent {
                    dedup_key,
                    category: AlertCategory::Regression,
                    transition: AlertTransition::Triggered,
                    severity: AlertSeverity::High,
                    engine: regression.engine,
                    resource_alias: regression.connection_alias.clone(),
                    database_name: regression.database_name.clone(),
                    environment: regression.environment.clone(),
                    headline: regression.title.clone(),
                    summary: regression.summary.clone(),
                    culprit_query: regression.query_sql.clone(),
                    victim_summary: None,
                    developer_action: regression.developer_action.clone(),
                    verification: regression.verification.clone(),
                    proximity_marks: regression.proximity_marks.clone(),
                    observed_at: regression.observed_until.clone(),
                    deep_link: self.policy.deep_link(&regression.connection_alias),
                }
                .sanitized(),
            );
        }
        Ok(())
    }

    fn evaluate_availability(
        &self,
        availability: &[AvailabilityAlertInput],
        state: &mut DeduplicationState,
        emitted: &mut Vec<RedactedAlertEvent>,
    ) {
        for item in availability {
            let healthy = item.state == "observed" && item.input_read_complete && item.context_issues.is_empty();
            let dedup_key = deterministic_dedup_key(AlertCategory::EvidenceAvailability, &[&item.resource_id]);

            match state.availability.get_mut(&item.resource_id) {
                Some(tracked) => {
                    if !healthy && !tracked.degraded {
                        tracked.degraded = true;
                        tracked.last_state = item.state.clone();
                        tracked.updated_at = item.observed_at.clone();
                        emitted.push(availability_event(&self.policy, item, dedup_key, true));
                    } else if healthy && tracked.degraded {
                        tracked.degraded = false;
                        tracked.last_state = item.state.clone();
                        tracked.updated_at = item.observed_at.clone();
                        emitted.push(availability_event(&self.policy, item, dedup_key, false));
                    } else {
                        tracked.last_state = item.state.clone();
                        tracked.updated_at = item.observed_at.clone();
                    }
                }
                None => {
                    state.availability.insert(
                        item.resource_id.clone(),
                        TrackedAvailability {
                            dedup_key: dedup_key.clone(),
                            degraded: !healthy,
                            last_state: item.state.clone(),
                            updated_at: item.observed_at.clone(),
                            resource_alias: item.connection_alias.clone(),
                            database_name: item.database_name.clone(),
                            environment: item.environment.clone(),
                        },
                    );
                    if !healthy {
                        emitted.push(availability_event(&self.policy, item, dedup_key, true));
                    }
                }
            }
        }
    }
}

fn availability_event(
    policy: &AlertEvaluationPolicy,
    item: &AvailabilityAlertInput,
    dedup_key: String,
    degraded: bool,
) -> RedactedAlertEvent {
    let (transition, severity, headline, summary) = if degraded {
        let issues = if item.context_issues.is_empty() {
            item.state.clone()
        } else {
            format!("{} ({})", item.state, item.context_issues.join(", "))
        };
        (
            AlertTransition::AvailabilityDegraded,
            AlertSeverity::Warning,
            t!("notify.event.availability_degraded_title", alias = &item.connection_alias, state = &item.state),
            t!("notify.event.availability_degraded_summary", alias = &item.connection_alias, detail = &issues),
        )
    } else {
        (
            AlertTransition::AvailabilityRecovered,
            AlertSeverity::Info,
            t!("notify.event.availability_recovered_title", alias = &item.connection_alias),
            t!("notify.event.availability_recovered_summary", alias = &item.connection_alias),
        )
    };
    RedactedAlertEvent {
        dedup_key,
        category: AlertCategory::EvidenceAvailability,
        transition,
        severity,
        engine: None,
        resource_alias: item.connection_alias.clone(),
        database_name: item.database_name.clone(),
        environment: item.environment.clone(),
        headline,
        summary,
        culprit_query: None,
        victim_summary: None,
        developer_action: None,
        verification: None,
        proximity_marks: Vec::new(),
        observed_at: item.observed_at.clone(),
        deep_link: policy.deep_link(&item.connection_alias),
    }
    .sanitized()
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| culprit_core::Error::invalid(t!("notify.config.invalid_timestamp")))
}
