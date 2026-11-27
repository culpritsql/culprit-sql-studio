//! One native-work permit is shared by report scans and telemetry captures.
use culprit_core::{
    Result,
    telemetry::{CollectionStop, SourceShutdown},
};
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct SourceGate {
    generation: Mutex<Arc<Semaphore>>,
}
impl Default for SourceGate {
    fn default() -> Self {
        Self { generation: Mutex::new(Arc::new(Semaphore::new(1))) }
    }
}
impl SourceGate {
    fn generation(&self) -> MutexGuard<'_, Arc<Semaphore>> {
        self.generation.lock().unwrap_or_else(|poison| poison.into_inner())
    }
    pub(super) fn resume(&self) {
        let mut generation = self.generation();
        if generation.is_closed() {
            *generation = Arc::new(Semaphore::new(1));
        }
    }
    pub async fn acquire(&self) -> Result<SourcePermit<'_>> {
        let generation = self.generation().clone();
        let permit = generation.acquire_owned().await.map_err(|_| CollectionStop::CancellationUnconfirmed.error())?;
        Ok(SourcePermit { permit, state: State::Ready, owner: PhantomData })
    }
    pub fn is_closed(&self) -> bool {
        self.generation().is_closed()
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Ready,
    InFlight,
    Settled,
}
#[must_use]
pub struct SourcePermit<'gate> {
    // Keep the public permit scoped to its gate while owning the exact semaphore generation it can close.
    owner: PhantomData<&'gate SourceGate>,
    permit: OwnedSemaphorePermit,
    state: State,
}
impl SourcePermit<'_> {
    /// Call before native work may start; cancellation after this point retains the uncertainty.
    pub fn begin(&mut self) -> Result<()> {
        if self.state != State::Ready {
            return Err(CollectionStop::InvalidPage.error());
        }
        self.state = State::InFlight;
        Ok(())
    }
    pub fn settle(&mut self, shutdown: SourceShutdown) -> Result<()> {
        if self.state != State::InFlight {
            return Err(CollectionStop::InvalidPage.error());
        }
        if shutdown == SourceShutdown::Unconfirmed {
            self.permit.semaphore().close();
        }
        self.state = State::Settled;
        Ok(())
    }
}
impl Drop for SourcePermit<'_> {
    fn drop(&mut self) {
        if self.state == State::InFlight {
            self.permit.semaphore().close();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unused_and_confirmed_permits_release_admission_but_uncertain_or_dropped_work_closes_it() {
        let gate = SourceGate::default();
        drop(gate.acquire().await.unwrap());
        let mut permit = gate.acquire().await.unwrap();
        permit.begin().unwrap();
        permit.settle(SourceShutdown::Quiescent).unwrap();
        drop(permit);
        let mut permit = gate.acquire().await.unwrap();
        permit.begin().unwrap();
        permit.settle(SourceShutdown::Unconfirmed).unwrap();
        drop(permit);
        assert!(gate.acquire().await.is_err());
        let gate = SourceGate::default();
        let mut permit = gate.acquire().await.unwrap();
        permit.begin().unwrap();
        drop(permit);
        assert!(gate.is_closed());
        assert!(gate.acquire().await.is_err());
    }
    #[tokio::test]
    async fn duplicate_state_transitions_never_authorize_more_work() {
        let gate = SourceGate::default();
        let mut permit = gate.acquire().await.unwrap();
        assert!(permit.settle(SourceShutdown::Quiescent).is_err());
        permit.begin().unwrap();
        assert!(permit.begin().is_err());
        drop(permit);
        assert!(gate.is_closed());
    }
}
