//! Existing focused selection, one FrozenSource and one held Activity reader; no ordinary scan work changes.
use crate::{
    activity::{ActivityReader, IdentityDetail, ReadOptions},
    conn::Pool,
    read::{NativeRead, ReadSession, invalid_data},
};
use culprit_core::{Result, telemetry::*};
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};
use std::sync::Arc;
const MAX_READER_QUERIES: u32 = 10;
const MAX_QUERIES: u32 = MAX_READER_QUERIES + ReadSession::SESSION_SETUP_QUERIES;

pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        max_queries: requested.max_queries.min(MAX_QUERIES),
        max_snapshot_rows: 1,
        page_rows: 1,
        max_sql_bytes: requested.max_sql_bytes.min(MAX_CURRENT_REQUEST_WORK_SQL_BYTES as u32),
        max_plan_bytes: requested.max_plan_bytes.min(MAX_CURRENT_REQUEST_WORK_PLAN_BYTES as u32),
        statement_timeout_ms: requested.statement_timeout_ms.min(1000),
        total_timeout_ms: requested.total_timeout_ms.min(5000),
        ..requested.clone()
    }
}
pub(super) struct Source<R: NativeRead = ReadSession> {
    inner: FrozenSource<Reader<R>>,
}
struct Reader<R: NativeRead> {
    activity: ActivityReader<R>,
    verified: Option<CollectionDescription>,
}
impl Source {
    pub(super) fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = validated(request)?;
        Self::with_io(ReadSession::collection(pool, request.budget.clone())?, &request, host, port)
    }
}
fn validated(request: &CollectionRequest) -> Result<CollectionRequest> {
    request.validate()?;
    request.subject.require_database()?;
    let CollectionSelection::CurrentRequestWork { focus } = &request.selection else {
        return Err(invalid_data());
    };
    crate::activity::current_request_work::validate_focus(focus)?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request.budget.validate()?;
    Ok(request)
}
impl<R: NativeRead> Source<R> {
    fn with_io(io: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = validated(request)?;
        let activity = ActivityReader::new(
            io,
            ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint },
            host,
            port,
        );
        Ok(Self { inner: FrozenSource::new(Reader { activity, verified: None }, &request)? })
    }
}
#[async_trait::async_trait]
impl<R: NativeRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let CollectionSelection::CurrentRequestWork { focus } = &request.selection else {
            return Err(invalid_data());
        };
        let capture = self.activity.capture_current_request_work(focus).await?;
        let context = capture.context;
        let records = capture
            .observations
            .into_iter()
            .map(|point| {
                Ok(TelemetryRecord::Operational(OperationalObservation {
                    source: capture.source.clone(),
                    key: point.key()?,
                    fact: OperationalFact::CurrentRequestWork(Box::new(point)),
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        let description = CollectionDescription {
            target: context.target,
            partition: context.partition,
            source: capture.source,
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::CurrentRequestWork {
                profile: CurrentRequestWorkProfile::MssqlRequest,
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::CurrentRequestWork,
                    observed_from: context.observed_from,
                    observed_until: context.observed_until,
                    records: records.len() as u32,
                    sources: vec![capture.coverage],
                },
                result: Box::new(capture.result),
            },
        };
        self.verified = Some(description.clone());
        Ok(FrozenObservation { description, records })
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        Ok(self.verified.as_ref() == Some(description))
    }
    fn query_count(&self) -> u32 {
        self.activity.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.activity.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.activity.close().await
    }
}
#[async_trait::async_trait]
impl<R: NativeRead> TelemetrySource for Source<R> {
    async fn describe(&mut self) -> Result<CollectionDescription> {
        self.inner.describe().await
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
    fn query_count(&self) -> u32 {
        self.inner.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.inner.read_metrics()
    }
}
#[cfg(test)]
mod tests;
