//! Fixture dispatch/retirement accounting is independent of cache evidence and pure interpretation.
use super::Actor;
use crate::{read::invalid_data, test_support::owned_captures::Captured};
use culprit_core::{Result, telemetry::*};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct WorkState {
    application_pending: AtomicBool,
    readers_active: AtomicUsize,
    reader_unconfirmed: AtomicBool,
}

#[derive(Clone)]
pub(in super::super) struct NativeWork(Arc<WorkState>);
impl NativeWork {
    /// Called only after Probe's original setup commands and identity read returned completely.
    pub(super) fn after_setup() -> Self {
        Self(Arc::new(WorkState::default()))
    }
    pub(super) fn application_dispatch(&self) -> Result<ApplicationDispatch> {
        self.0
            .application_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
        Ok(ApplicationDispatch(self.0.clone()))
    }
    pub(in super::super) fn reader(&self) -> ReaderWork {
        self.0.readers_active.fetch_add(1, Ordering::AcqRel);
        ReaderWork { state: self.0.clone(), finished: false }
    }
    pub(super) fn withhold(&self) {
        self.0.reader_unconfirmed.store(true, Ordering::Release);
    }
    pub(super) fn retired(self, database: String, actor: Actor, retirement: Result<()>) -> Result<SettledScalarActor> {
        // A failed physical close cannot mint a receipt, and a successful one cannot settle pending work.
        retirement?;
        if self.0.application_pending.load(Ordering::Acquire)
            || self.0.readers_active.load(Ordering::Acquire) != 0
            || self.0.reader_unconfirmed.load(Ordering::Acquire)
        {
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        let actor = NativeActivityIdentity {
            id: actor.id.to_string(),
            generation: crate::sql_time::native_time(&actor.generation)?,
        };
        Ok(SettledScalarActor { database, actor })
    }
}

/// Dropping or unwinding an admitted dispatch leaves it pending. Only the same fully drained native API
/// return may consume this guard; evidence matching, elapsed time and client close cannot complete it.
pub(super) struct ApplicationDispatch(Arc<WorkState>);
impl ApplicationDispatch {
    pub(super) fn complete(self) {
        self.0.application_pending.store(false, Ordering::Release);
    }
}

pub(in super::super) struct ReaderWork {
    state: Arc<WorkState>,
    finished: bool,
}
impl ReaderWork {
    pub(in super::super) fn closed(mut self, shutdown: SourceShutdown) {
        if shutdown != SourceShutdown::Quiescent {
            self.state.reader_unconfirmed.store(true, Ordering::Release);
        }
        self.finished = true;
        self.state.readers_active.fetch_sub(1, Ordering::AcqRel);
    }
    pub(in super::super) fn captured(self, captured: &Captured) -> Result<()> {
        // The canonical capture owner already checks EOF, status, accounting and the exact durable original.
        // Its source's actual settlement is still required here; missing metrics are never guessed quiescent.
        let native = captured
            .manifest
            .read_metrics
            .as_ref()
            .and_then(|metrics| metrics.native_session_work.as_ref())
            .ok_or_else(invalid_data)?;
        if native.settlement() != NativeReadSettlement::Quiescent {
            return Err(CollectionStop::CancellationUnconfirmed.error());
        }
        self.closed(SourceShutdown::Quiescent);
        Ok(())
    }
}
impl Drop for ReaderWork {
    fn drop(&mut self) {
        if !self.finished {
            self.state.reader_unconfirmed.store(true, Ordering::Release);
            self.state.readers_active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Private construction requires this actor's acknowledged native work, all own reader settlements and its
/// completed retirement. The receipt reopens only the exact registered nonce-owned fixture actor's cleanup.
pub(crate) struct SettledScalarActor {
    database: String,
    actor: NativeActivityIdentity,
}
impl SettledScalarActor {
    pub(crate) fn matches(&self, database: &str, actor: &NativeActivityIdentity) -> bool {
        self.database == database && self.actor == *actor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> Actor {
        Actor { id: 42, generation: "2020-01-01T07:59:00.0033333".into() }
    }
    fn retire(work: NativeWork) -> Result<SettledScalarActor> {
        work.retired("owned-native-fixture".into(), actor(), Ok(()))
    }
    #[test]
    fn acknowledged_native_work_can_retire_after_a_pure_evidence_failure() {
        let work = NativeWork::after_setup();
        let evidence: Result<()> = {
            let dispatched = work.application_dispatch().unwrap();
            let reader = work.reader();
            dispatched.complete();
            reader.closed(SourceShutdown::Quiescent);
            Err(CollectionStop::SourceUnavailable.error())
        };
        assert_eq!(CollectionStop::from_error(&evidence.unwrap_err()), CollectionStop::SourceUnavailable);
        let receipt = retire(work).unwrap();
        let expected = NativeActivityIdentity { id: "42".into(), generation: "2020-01-01T07:59:00.0033333".into() };
        assert!(receipt.matches("owned-native-fixture", &expected));
        assert!(!receipt.matches("another-fixture", &expected));
        let mut replaced = expected;
        replaced.generation = "2020-01-01T07:59:00.0066667".into();
        assert!(!receipt.matches("owned-native-fixture", &replaced));
    }
    #[test]
    fn a_later_pure_panic_does_not_change_the_acknowledged_original_native_work() {
        let work = NativeWork::after_setup();
        let outcome = std::panic::catch_unwind(|| {
            work.application_dispatch().unwrap().complete();
            work.reader().closed(SourceShutdown::Quiescent);
            panic!("later cache/plan assertion failed");
        });
        assert!(outcome.is_err());
        assert!(retire(work).is_ok());
    }
    #[test]
    fn pending_or_dropped_native_dispatches_cannot_be_settled_by_physical_close_or_another_dispatch() {
        for drop_original in [false, true] {
            let work = NativeWork::after_setup();
            let mut dispatched = Some(work.application_dispatch().unwrap());
            if drop_original {
                drop(dispatched.take());
            }
            assert!(work.application_dispatch().is_err());
            assert!(retire(work).is_err());
            drop(dispatched);
        }
    }
    #[test]
    fn a_native_panic_keeps_its_original_dispatch_unconfirmed() {
        let work = NativeWork::after_setup();
        let outcome = std::panic::catch_unwind(|| {
            let _pending = work.application_dispatch().unwrap();
            panic!("original native operation did not return");
        });
        assert!(outcome.is_err());
        assert!(retire(work).is_err());
    }
    #[test]
    fn every_reader_requires_its_own_positive_close_and_uncertainty_is_sticky() {
        for failure in ["active", "lost", "unconfirmed"] {
            let work = NativeWork::after_setup();
            let first = work.reader();
            let mut second = Some(work.reader());
            first.closed(SourceShutdown::Quiescent);
            match failure {
                "active" => {}
                "lost" => drop(second.take()),
                "unconfirmed" => second.take().unwrap().closed(SourceShutdown::Unconfirmed),
                _ => unreachable!(),
            }
            work.reader().closed(SourceShutdown::Quiescent);
            assert!(retire(work).is_err(), "{failure}");
            drop(second);
        }
    }
    #[test]
    fn acknowledged_responses_do_not_override_a_failed_client_retirement() {
        let work = NativeWork::after_setup();
        work.application_dispatch().unwrap().complete();
        let error =
            work.retired("owned-native-fixture".into(), actor(), Err(CollectionStop::TimedOut.error())).err().unwrap();
        assert_eq!(CollectionStop::from_error(&error), CollectionStop::TimedOut);
    }
}
