//! Source-only worker questions reuse FrozenSource paging/finish and the same held native Activity owner.
use crate::{
    activity::{ActivityReader, IdentityDetail, ReadOptions},
    conn::Pool,
    local_source::Context,
    read::{NativeRead, ReadSession, invalid_data},
};
use culprit_core::{Result, telemetry::*};
use std::sync::Arc;

pub(super) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    caller.intersection(&CollectionBudget {
        page_rows: 1,
        max_snapshot_rows: (MAX_WORKER_TASKS + MAX_WORKER_WAITERS) as u32,
        max_pages: 1,
        page_bytes: 1024 * 1024,
        total_bytes: 4 * 1024 * 1024,
        max_sql_bytes: 4096,
        max_queries: 13 + ReadSession::SESSION_SETUP_QUERIES,
        statement_timeout_ms: 1000,
        total_timeout_ms: 5000,
        ..CollectionBudget::default()
    })
}
pub(super) struct WorkerStarvationSource<R: NativeRead = ReadSession> {
    inner: culprit_read::FrozenSource<Reader<R>>,
}
struct Reader<R: NativeRead> {
    reader: ActivityReader<R>,
    expected: SourceReadExecutionScope,
    focus_key: String,
    owner: Option<Context>,
}
impl WorkerStarvationSource {
    pub fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        Self::with_io(ReadSession::collection(pool, request.budget.clone())?, &request, host, port)
    }
}
fn bounded_request(request: &CollectionRequest) -> Result<CollectionRequest> {
    request.validate()?;
    let CollectionSelection::SourceRead { focus, execution } = &request.selection else { return Err(invalid_data()) };
    if focus.question != SourceReadQuestion::WorkerStarvation
        || !execution.accepts_question(focus.question)
        || execution.target() != focus.target
    {
        return Err(invalid_data());
    }
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request.budget.validate()?;
    Ok(request)
}
impl<R: NativeRead> WorkerStarvationSource<R> {
    pub(super) fn with_io(io: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        let CollectionSelection::SourceRead { focus, execution } = &request.selection else {
            return Err(invalid_data());
        };
        let reader = ActivityReader::new(
            io,
            ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint },
            host,
            port,
        );
        Ok(Self {
            inner: culprit_read::FrozenSource::new(
                Reader { reader, expected: execution.as_ref().clone(), focus_key: focus.key()?, owner: None },
                &request,
            )?,
        })
    }
}
#[async_trait::async_trait]
impl<R: NativeRead> culprit_read::ObservationReader for Reader<R> {
    async fn observe(&mut self, _: &CollectionRequest) -> Result<culprit_read::FrozenObservation> {
        let capture = self.reader.capture_worker_starvation(&self.expected).await?;
        let point = capture.point;
        let context = point.context.clone();
        let complete = !point.tasks_limited
            && !point.waits_limited
            && point.requests.iter().all(MssqlWorkerRequest::is_qualified)
            && point.tasks.iter().all(|task| !task.worker_binding_unavailable());
        let key = point.key()?;
        let description = CollectionDescription {
            target: context.target.clone(),
            partition: SourcePartition::Target,
            source: culprit_core::source_id::WORKER_QUEUE.into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::SourceRead {
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::WorkerStarvation,
                    observed_from: context.observed_from.clone(),
                    observed_until: context.observed_until.clone(),
                    records: 1,
                    sources: vec![StateSourceCoverage::observed(
                        culprit_core::source_id::WORKER_QUEUE.into(),
                        1,
                        complete,
                    )],
                },
                focus_key: self.focus_key.clone(),
                execution: Box::new(context),
            },
        };
        self.owner = Some(capture.owner);
        Ok(culprit_read::FrozenObservation {
            description,
            records: vec![TelemetryRecord::Operational(OperationalObservation {
                source: culprit_core::source_id::WORKER_QUEUE.into(),
                key,
                fact: OperationalFact::MssqlWorkerStarvation(Box::new(point)),
            })],
        })
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        self.reader
            .verify_worker_starvation(
                self.owner.as_ref().ok_or_else(invalid_data)?,
                &description.observation.snapshot().ok_or_else(invalid_data)?.observed_until,
            )
            .await
    }
    fn query_count(&self) -> u32 {
        self.reader.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.reader.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.reader.close().await
    }
}
#[async_trait::async_trait]
impl<R: NativeRead> TelemetrySource for WorkerStarvationSource<R> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.inner.describe().await
    }
    fn query_count(&self) -> u32 {
        self.inner.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.inner.read_metrics()
    }
    async fn read_page(&mut self, stream: TelemetryStream, cursor: Option<&str>) -> Result<CollectionRead> {
        self.inner.read_page(stream, cursor).await
    }
    async fn finish(&mut self) -> Result<SourceCompletion> {
        self.inner.finish().await
    }
    async fn close(&mut self) -> SourceShutdown {
        self.inner.close().await
    }
}
#[cfg(test)]
pub(super) mod tests;
