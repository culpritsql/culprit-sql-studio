//! The existing owned native reader exposes streaming to the shared snapshot pager; no extra connection owner.
use super::*;

#[async_trait::async_trait]
pub(super) trait SnapshotIo: NativeRead {
    async fn begin_stream(&mut self, sql: &str, row_limit: usize) -> Result<()>;
    async fn next_batch(&mut self) -> Result<ReadBatch<Row>>;
}
#[async_trait::async_trait]
impl SnapshotIo for ReadSession {
    async fn begin_stream(&mut self, sql: &str, row_limit: usize) -> Result<()> {
        ReadSession::begin_stream(self, sql, row_limit).await
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Row>> {
        ReadSession::next_batch(self).await
    }
}
