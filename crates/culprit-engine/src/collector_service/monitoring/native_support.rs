//! Source-neutral native acceptance reuses the SAME Outbox/archive EOF and installed Gold synthesis owners.
use super::*;
use std::{path::Path, sync::Arc, time::Duration};
pub(super) mod archive;
pub(super) mod history;
pub(super) mod synthesis;

mod case {
    use super::*;
    pub fn require_active(cancellation: &CollectionCancellation) -> Result<()> {
        if cancellation.is_cancelled() { Err(CollectionStop::Cancelled.error()) } else { Ok(()) }
    }
    pub async fn admitted<T>(
        cancellation: &CollectionCancellation,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        require_active(cancellation)?;
        work.await
    }
}
