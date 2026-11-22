//! Automatic work remains local and serialized by the existing Files owner. Native authority is unchanged.
use super::artifacts::VerificationObserver;
use super::completed_capture::{CaptureWork, CompletedCaptureRead, ReadControl};
use super::completed_synthesis::SynthesisCapture;
use super::journal as capture_journal;
use super::{Files, Outbox, OutboxDelivery, problem, storage};
use crate::synthesis::*;
use culprit_core::{Result, limits::OperationHandle, telemetry::*};
use tokio::time::Instant;
mod catalog;
mod context;
mod journal;
use context::{ContextPlan, decode_plan, encode_plan};
pub(super) use journal::{SCHEMA, archived, before_remove, charged_bytes, committed, reserve_allowance};

impl Outbox {
    /// Install the host's pure analyzer before its first source reservation. The durable policy and pending
    /// work survive restart; the executable callback and source authority are never serialized.
    pub async fn register_automatic_synthesis(
        &mut self,
        automatic: &AutomaticSynthesis,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
    ) -> Result<()> {
        let control = control(operation, automatic.budget())?;
        automatic.analyzer()?;
        let registration = automatic.registration().clone();
        control.check()?;
        self.delivery()
            .with_read_operation(control, worker_guard, move |files, control| {
                control.check()?;
                files.register_synthesis(&registration)?;
                Ok(CaptureWork::Read(()))
            })
            .await
    }
}

impl OutboxDelivery {
    /// Failed terminal attempts remain visible after pending work is exhausted; Idle is not a health verdict.
    pub async fn synthesis_backlog(
        &self,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
    ) -> Result<SynthesisBacklog> {
        let control = control(operation, super::CompletedCaptureBudget::default())?;
        self.with_read_operation(control, worker_guard, |files, control| {
            journal::backlog(&files.journal.conn, files.journal.limits.max_captures, &|| control.check())
                .map(CaptureWork::Read)
        })
        .await
    }

    /// One turn owns metadata selection, every selected EOF proof, analysis and the ordinary Ready writer.
    /// The host drives this alongside its existing delivery work; no new source/scheduler is constructed.
    pub async fn synthesize_pending(
        &self,
        automatic: &AutomaticSynthesis,
        operation: OperationHandle,
        worker_guard: impl Send + 'static,
    ) -> Result<SynthesisTurn> {
        let budget = automatic.budget();
        let control = control(operation, budget)?;
        let analyzer = automatic.analyzer()?;
        let registration = automatic.registration().clone();
        control.check()?;
        self.with_read_operation(control, worker_guard, move |files, control| {
            if !files.synthesis_mutation_ready()? {
                return Ok(CaptureWork::Read(SynthesisTurn::Deferred {
                    reason: SynthesisDeferral::NativeCaptureOwned,
                }));
            }
            if !files.synthesis_registered
                || journal::registration(&files.journal.conn)?.as_ref() != Some(&registration)
            {
                return Ok(CaptureWork::Read(SynthesisTurn::Deferred { reason: SynthesisDeferral::PolicyMismatch }));
            }
            journal::backfill(&mut files.journal.conn, &registration, &|| control.check())?;
            let Some(job) = journal::next(&files.journal.conn, &registration)? else {
                return Ok(CaptureWork::Read(SynthesisTurn::Idle));
            };
            let attempted = (|| {
                let mut read = CompletedCaptureRead::synthesis_control(job.primary.clone(), budget, control.clone())?;
                let primary = files.synthesis_capture(&mut read)?;
                journal::retain_verified(&mut files.journal.conn, &primary)?;
                let input = match &job.plan {
                    Some(plan) => files.replay_synthesis_plan(primary, plan, &mut read)?,
                    None => {
                        let (input, plan) = files.plan_synthesis_context(primary, &mut read)?;
                        read.check()?;
                        journal::store_plan(&mut files.journal.conn, &job, &plan)?;
                        input
                    }
                };
                files.synthesize_input(&mut read, input, analyzer.as_ref(), Some(&registration))
            })();
            match attempted {
                Ok(output) => {
                    // Once output commit is known, cancellation or an acknowledgement failure cannot erase it.
                    let queue_retired = journal::complete(&mut files.journal.conn, &job, &output).is_ok();
                    Ok(CaptureWork::Committed(SynthesisTurn::Committed { primary: job.primary, output, queue_retired }))
                }
                Err(error) => {
                    if control.is_cancelled() {
                        return Err(CollectionStop::Cancelled.error());
                    }
                    let stopped = control.check().err();
                    if stopped.is_none()
                        && error.detail == Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::RecoveryRequired))
                    {
                        return Ok(CaptureWork::Read(SynthesisTurn::Deferred {
                            reason: SynthesisDeferral::NativeCaptureOwned,
                        }));
                    }
                    // An allocated local output may already be durably complete even if its caller lost a receipt.
                    // Keep the job for the existing recovery/idempotence owner; never record a guessed failure.
                    if files.synthesis_output_started(&job.primary.capture.observation_id)? {
                        return Err(stopped.unwrap_or(error));
                    }
                    if stopped.is_none()
                        && error.detail == Some(culprit_core::ErrorDetail::Outbox(OutboxProblem::Capacity))
                    {
                        return Ok(CaptureWork::Read(SynthesisTurn::Deferred {
                            reason: SynthesisDeferral::QueueBackpressure,
                        }));
                    }
                    // No output owner exists and canonical/native work has already returned. Only the
                    // bounded terminal transaction remains; it cannot restart a read or renew its allowance.
                    if control.is_cancelled() {
                        return Err(CollectionStop::Cancelled.error());
                    }
                    let failure = stopped.unwrap_or(error);
                    journal::failed(&mut files.journal.conn, &job, failure.code)?;
                    // This is a known FAILED metadata commit, never a successful diagnosis. Its receipt
                    // must survive the read deadline just as a known output commit survives cancellation.
                    Ok(CaptureWork::Committed(SynthesisTurn::Failed { primary: job.primary, code: failure.code }))
                }
            }
        })
        .await
    }
}

fn control(operation: OperationHandle, budget: super::CompletedCaptureBudget) -> Result<ReadControl> {
    budget.validate()?;
    let deadline = Instant::now()
        .checked_add(budget.timeout.min(operation.remaining()))
        .ok_or_else(|| CollectionStop::TimedOut.error())?;
    let control = ReadControl::Operation { operation, deadline };
    control.check()?;
    Ok(control)
}

impl Files {
    fn register_synthesis(&mut self, registration: &SynthesisRegistration) -> Result<()> {
        if self.native_reservation_started || !self.synthesis_mutation_ready()? {
            return Err(problem(OutboxProblem::RecoveryRequired));
        }
        let headroom = MAX_DERIVED_OUTPUT_BYTES as u64 + super::CAPTURE_ALLOWANCE + super::MAX_REQUEST_BYTES as u64;
        if self.journal.limits.max_captures < 2 || self.journal.limits.max_bytes <= headroom + super::CAPTURE_ALLOWANCE
        {
            return Err(problem(OutboxProblem::Capacity));
        }
        journal::install(&mut self.journal.conn, registration)?;
        self.synthesis_registered = true;
        Ok(())
    }
    pub(super) fn synthesis_host_ready(&self, request: &CollectionRequest) -> Result<()> {
        let configured = journal::registration(&self.journal.conn)?.is_some();
        if self.synthesis_registered && !configured {
            return Err(integrity());
        }
        if !request.collection().is_local_output() && configured && !self.synthesis_registered {
            return Err(problem(OutboxProblem::RecoveryRequired));
        }
        Ok(())
    }
    fn synthesis_output_started(&self, primary: &str) -> Result<bool> {
        self.journal
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM synthesized_outputs WHERE primary_observation_id=?1)",
                [primary],
                |row| row.get(0),
            )
            .map_err(storage)
    }
}

fn integrity() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
