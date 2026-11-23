use super::*;
#[async_trait::async_trait]
pub(super) trait SnapshotIo: crate::read::OwnedRead {
    fn snapshot_generation(&self) -> Option<u64>;
    async fn begin_stream(&mut self, sql: &str, parameters: Vec<mysql_async::Value>, row_limit: usize) -> Result<()>;
    async fn next_batch(&mut self) -> Result<ReadBatch<Rec>>;
}
#[async_trait::async_trait]
impl SnapshotIo for ReadSession {
    fn snapshot_generation(&self) -> Option<u64> {
        ReadSession::snapshot_generation(self)
    }
    async fn begin_stream(&mut self, sql: &str, parameters: Vec<mysql_async::Value>, row_limit: usize) -> Result<()> {
        ReadSession::begin_stream(self, sql, parameters, row_limit).await
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Rec>> {
        ReadSession::next_batch(self).await
    }
}
