//! Shared caller cancellation notification; cancellation does not attest native retirement.
use tokio::sync::watch;
#[derive(Clone)]
pub struct CollectionCancellation {
    sender: watch::Sender<bool>,
}
impl Default for CollectionCancellation {
    fn default() -> Self {
        Self { sender: watch::channel(false).0 }
    }
}
impl CollectionCancellation {
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}
