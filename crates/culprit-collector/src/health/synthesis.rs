//! Analysis health stays distinct from source, archive and analytical transport health.
use super::*;
use crate::synthesis::{SynthesisBacklog, SynthesisDeferral, SynthesisFailureWatermark, SynthesisTurn};
use std::io::Write;

#[derive(Clone, Default)]
pub(super) struct SynthesisHealth {
    pending: u32,
    failed: u32,
    failure_watermark: SynthesisFailureWatermark,
    oldest_queued_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: Option<Instant>,
    completed: u64,
    failures: u64,
    retirement_pending: u64,
    deferred: Option<SynthesisDeferral>,
}
impl CollectorHealth {
    pub fn synthesis_registered(&self) -> Result<()> {
        self.with(|state| state.synthesis = Some(SynthesisHealth::default()))
    }
    pub fn synthesis(&self, turn: &SynthesisTurn) -> Result<()> {
        self.with(|state| {
            let synthesis = state.synthesis.get_or_insert_default();
            synthesis.deferred = None;
            match turn {
                SynthesisTurn::Committed { queue_retired, .. } => {
                    synthesis.completed = synthesis.completed.saturating_add(1);
                    if !queue_retired {
                        synthesis.retirement_pending = synthesis.retirement_pending.saturating_add(1);
                    }
                }
                SynthesisTurn::Failed { .. } => synthesis.failures = synthesis.failures.saturating_add(1),
                SynthesisTurn::Deferred { reason } => synthesis.deferred = Some(*reason),
                SynthesisTurn::Idle => {}
            }
        })
    }
    pub fn synthesis_backlog(&self, backlog: &SynthesisBacklog) -> Result<()> {
        let oldest_queued_at = backlog
            .oldest_pending
            .as_ref()
            .map(|oldest| {
                chrono::DateTime::parse_from_rfc3339(&oldest.queued_at)
                    .map(|at| at.with_timezone(&chrono::Utc))
                    .map_err(|_| DeliveryProblem::Integrity.error())
            })
            .transpose()?;
        if (backlog.pending > 0) != oldest_queued_at.is_some() {
            return Err(DeliveryProblem::Integrity.error());
        }
        if !backlog.failure_watermark.is_valid() {
            return Err(DeliveryProblem::Integrity.error());
        }
        let mut state = self.state.lock().map_err(|_| DeliveryProblem::Storage.error())?;
        let synthesis = state.synthesis.get_or_insert_default();
        let previous = &synthesis.failure_watermark;
        if backlog.failure_watermark.total < previous.total
            || backlog.failure_watermark.total == previous.total && backlog.failure_watermark != *previous
        {
            return Err(DeliveryProblem::Integrity.error());
        }
        synthesis.pending = backlog.pending;
        synthesis.failed = backlog.failed;
        synthesis.failure_watermark = backlog.failure_watermark.clone();
        synthesis.oldest_queued_at = oldest_queued_at;
        synthesis.updated_at = Some(Instant::now());
        Ok(())
    }
}
impl SynthesisHealth {
    pub(super) fn ready(&self, now: Instant) -> bool {
        self.failed == 0
            && self.failures == 0
            && self.failure_watermark.total == 0
            && self.updated_at.is_some_and(|at| now.duration_since(at) <= MAX_STATUS_AGE)
            && !matches!(
                self.deferred,
                Some(
                    SynthesisDeferral::QueueBackpressure
                        | SynthesisDeferral::PolicyMismatch
                        | SynthesisDeferral::LocalBudget
                )
            )
    }
    pub(super) fn write_metrics(&self, body: &mut impl Write) -> Result<()> {
        for (name, value) in [
            ("pending_captures", u64::from(self.pending)),
            ("failed_captures", u64::from(self.failed)),
            ("completed_analyses_total", self.completed),
            ("failed_analyses_total", self.failures),
            ("durable_failed_analyses_total", self.failure_watermark.total),
            ("pending_retirement_total", self.retirement_pending),
        ] {
            writeln!(body, "culprit_collector_synthesis_{name} {value}").map_err(metric_error)?;
        }
        if let Some(oldest) = self.oldest_queued_at {
            let elapsed = (chrono::Utc::now() - oldest).num_seconds();
            writeln!(body, "culprit_collector_synthesis_oldest_pending_queue_clock_ahead {}", u8::from(elapsed < 0))
                .map_err(metric_error)?;
            writeln!(body, "culprit_collector_synthesis_oldest_pending_queue_age_seconds {}", elapsed.max(0))
                .map_err(metric_error)?;
        }
        if let Some(latest) = &self.failure_watermark.latest {
            let at = chrono::DateTime::parse_from_rfc3339(&latest.failed_at)
                .map_err(|_| DeliveryProblem::Integrity.error())?;
            let elapsed = (chrono::Utc::now() - at.with_timezone(&chrono::Utc)).num_seconds();
            writeln!(body, "culprit_collector_synthesis_last_failure_settlement_clock_ahead {}", u8::from(elapsed < 0))
                .map_err(metric_error)?;
            writeln!(body, "culprit_collector_synthesis_last_failure_settlement_age_seconds {}", elapsed.max(0))
                .map_err(metric_error)?;
            let code = serde_json::to_value(latest.code).map_err(|_| DeliveryProblem::Integrity.error())?;
            let code = code.as_str().ok_or_else(|| DeliveryProblem::Integrity.error())?;
            writeln!(body, "culprit_collector_synthesis_last_failure{{code=\"{code}\"}} 1").map_err(metric_error)?;
        }
        if let Some(reason) = self.deferred {
            let reason = match reason {
                SynthesisDeferral::NativeCaptureOwned => "native-capture-owned",
                SynthesisDeferral::QueueBackpressure => "queue-backpressure",
                SynthesisDeferral::PolicyMismatch => "policy-mismatch",
                SynthesisDeferral::LocalBudget => "local-budget",
            };
            writeln!(body, "culprit_collector_synthesis_deferred{{reason=\"{reason}\"}} 1").map_err(metric_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthesis::SynthesisQueuedCapture;

    fn initialized() -> CollectorHealth {
        let health = super::super::tests::health();
        health.source_disabled().unwrap();
        health.outbox(&super::super::tests::status(0)).unwrap();
        health.initialized().unwrap();
        health
    }
    fn empty() -> SynthesisBacklog {
        SynthesisBacklog { pending: 0, failed: 0, oldest_pending: None, failure_watermark: Default::default() }
    }

    #[test]
    fn failed_analyses_stay_visible_after_an_idle_turn_and_do_not_become_healthy_absence() {
        let health = initialized();
        assert!(health.ready().unwrap());
        health.synthesis_registered().unwrap();
        assert!(!health.ready().unwrap(), "a registered worker has not observed its durable queue yet");
        health.synthesis_backlog(&empty()).unwrap();
        assert!(health.ready().unwrap());
        health.synthesis_backlog(&SynthesisBacklog { failed: 1, ..empty() }).unwrap();
        health.synthesis(&SynthesisTurn::Idle).unwrap();
        assert!(!health.ready().unwrap());
        let body = String::from_utf8(health.reply().unwrap().metrics).unwrap();
        assert!(body.contains("culprit_collector_synthesis_failed_captures 1"));
    }

    #[test]
    fn pending_analysis_reports_its_queue_clock_without_exposing_original_capture_identifiers() {
        let health = initialized();
        let (_, diagnosis) = culprit_evidence::analytical::test_support::derived_diagnosis_output(false, false);
        let primary = diagnosis.description.inputs[0].capture.clone();
        health.synthesis_registered().unwrap();
        health
            .synthesis_backlog(&SynthesisBacklog {
                pending: 1,
                failed: 0,
                failure_watermark: Default::default(),
                oldest_pending: Some(SynthesisQueuedCapture {
                    primary: primary.clone(),
                    queued_at: (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
                }),
            })
            .unwrap();
        let body = String::from_utf8(health.reply().unwrap().metrics).unwrap();
        assert!(body.contains("culprit_collector_synthesis_pending_captures 1"));
        assert!(body.contains("culprit_collector_synthesis_oldest_pending_queue_clock_ahead 1"));
        assert!(!body.contains(&primary.capture.observation_id));
        assert!(!body.contains(&primary.manifest_sha256));
        assert!(health.synthesis_backlog(&SynthesisBacklog { pending: 1, ..empty() }).is_err());
        let body = String::from_utf8(health.reply().unwrap().metrics).unwrap();
        assert!(body.contains("culprit_collector_synthesis_pending_captures 1"));
    }

    #[test]
    fn persistent_policy_or_capacity_deferral_is_unready_while_an_owned_native_capture_is_expected() {
        let health = initialized();
        health.synthesis_registered().unwrap();
        health.synthesis_backlog(&empty()).unwrap();
        health.synthesis(&SynthesisTurn::Deferred { reason: SynthesisDeferral::NativeCaptureOwned }).unwrap();
        assert!(health.ready().unwrap());
        for reason in [SynthesisDeferral::PolicyMismatch, SynthesisDeferral::QueueBackpressure] {
            health.synthesis(&SynthesisTurn::Deferred { reason }).unwrap();
            assert!(!health.ready().unwrap());
        }
        health.synthesis(&SynthesisTurn::Idle).unwrap();
        assert!(health.ready().unwrap());
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::synthesis::{SynthesisFailure, SynthesisFailureWatermark};

    fn empty() -> SynthesisBacklog {
        SynthesisBacklog { pending: 0, failed: 0, oldest_pending: None, failure_watermark: Default::default() }
    }
    fn health() -> CollectorHealth {
        let health = super::super::tests::health();
        health.source_disabled().unwrap();
        health.outbox(&super::super::tests::status(0)).unwrap();
        health.initialized().unwrap();
        health.synthesis_registered().unwrap();
        health
    }
    fn failure() -> SynthesisFailure {
        let (_, diagnosis) = culprit_evidence::analytical::test_support::derived_diagnosis_output(false, false);
        SynthesisFailure {
            primary: diagnosis.description.inputs[0].capture.clone(),
            code: culprit_core::ErrorCode::Timeout,
            failed_at: canonical_time(&chrono::Utc::now().to_rfc3339()).unwrap(),
        }
    }
    #[test]
    fn an_observed_failed_turn_cannot_become_ready_when_its_marker_is_evicted_before_health_refresh() {
        let health = health();
        health.synthesis_backlog(&empty()).unwrap();
        assert!(health.ready().unwrap());
        let failure = failure();
        health.synthesis(&SynthesisTurn::Failed { primary: failure.primary, code: failure.code }).unwrap();
        health.synthesis_backlog(&empty()).unwrap();
        health.synthesis(&SynthesisTurn::Idle).unwrap();
        assert!(!health.ready().unwrap());
    }
    #[test]
    fn durable_failure_health_survives_fresh_process_state_and_never_prints_original_keys_or_resets() {
        let failure = failure();
        let health = health();
        let state = SynthesisBacklog {
            failure_watermark: SynthesisFailureWatermark { total: 1, latest: Some(failure.clone()) },
            ..empty()
        };
        health.synthesis_backlog(&state).unwrap();
        assert!(!health.ready().unwrap());
        let body = String::from_utf8(health.reply().unwrap().metrics).unwrap();
        assert!(body.contains("culprit_collector_synthesis_durable_failed_analyses_total 1"));
        assert!(body.contains("culprit_collector_synthesis_last_failure_settlement_age_seconds"));
        assert!(!body.contains(&failure.primary.capture.observation_id));
        assert!(!body.contains(&failure.primary.manifest_sha256));
        assert!(health.synthesis_backlog(&empty()).is_err());
        let mut changed = state;
        changed.failure_watermark.latest.as_mut().unwrap().code = culprit_core::ErrorCode::QueryFailed;
        assert!(health.synthesis_backlog(&changed).is_err());
        assert!(!health.ready().unwrap());
    }
}
