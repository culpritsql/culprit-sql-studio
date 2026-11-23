//! Positive original acquisition proof only; neither admission charges nor unavailable costs imply dispatch.
use super::*;

impl ReadWorkMetrics {
    /// A complete original roster proves that every fresh acquisition failed before an authenticated
    /// reader was available. The actual source owner must independently confirm quiescence. This proof
    /// neither changes charged source queries nor authorizes deleting any retained native evidence.
    pub fn proves_pre_dispatch_connection_failure(&self, settlement: SourceWorkState) -> bool {
        settlement == SourceWorkState::Quiescent
            && self.is_valid()
            && self.active_tasks == 0
            && self.untracked_tasks == 0
            && self.templates.is_empty()
            && self.connection_work.as_ref().is_some_and(|work| {
                work.has_complete_observation_roster()
                    && !work.has_unconfirmed_work()
                    && !work.attempts.is_empty()
                    && work.attempts.iter().all(failed_fresh_acquisition)
            })
            && self.native_session_work.as_ref().is_none_or(|work| work.is_unobserved_after_failed_acquisition())
    }
}

fn failed_fresh_acquisition(attempt: &ReadConnectionAttempt) -> bool {
    attempt.connection_use == ReadConnectionUse::FreshConnection
        && attempt.reader_generation.is_none()
        && attempt.omitted_phases == 0
        && attempt.phases.iter().all(|phase| match phase.stage {
            ReadConnectionStage::Acquire | ReadConnectionStage::HandshakeAndLogin => {
                matches!(phase.outcome, ReadConnectionOutcome::Failed { .. })
            }
            ReadConnectionStage::RouteSelection | ReadConnectionStage::Transport => {
                matches!(phase.outcome, ReadConnectionOutcome::Completed | ReadConnectionOutcome::Failed { .. })
            }
            ReadConnectionStage::SessionSetup | ReadConnectionStage::FirstQuery => false,
        })
}

#[cfg(test)]
mod tests;
