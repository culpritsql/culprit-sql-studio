//! Selected profiling shares the held Activity/current-work/task/SQL owners and FrozenSource lifecycle.
use crate::{
    activity::{ActivityReader, IdentityDetail, ReadOptions},
    conn::Pool,
    read::{NativeRead, ReadSession, invalid_data},
};
use culprit_core::{Result, telemetry::*};
use std::sync::Arc;

pub(super) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    // Boxed/MI: initial3 +request2 +tasks2 +layout1 +cache2 +profiles1 +bindings1 +final3 =15 reader calls.
    // Azure: initial2 +the same selected9 +final1 =12. SET is separately reserved exactly once.
    caller.intersection(&CollectionBudget {
        page_rows: 1,
        max_snapshot_rows: (MAX_EXECUTION_PROFILE_ROWS + 4 * MAX_WORKER_TASKS) as u32,
        max_pages: 1,
        page_bytes: 1024 * 1024,
        total_bytes: 4 * 1024 * 1024,
        max_sql_bytes: 4096,
        max_plan_bytes: MAX_CURRENT_REQUEST_WORK_PLAN_BYTES as u32,
        max_queries: 15 + ReadSession::SESSION_SETUP_QUERIES,
        statement_timeout_ms: 1000,
        total_timeout_ms: 5000,
    })
}
pub(super) struct CurrentExecutionProfileSource<R: NativeRead = ReadSession> {
    inner: culprit_read::FrozenSource<Reader<R>>,
}
struct Reader<R: NativeRead> {
    reader: ActivityReader<R>,
    focus: Box<ActivityRequestFocus>,
    execution: SourceReadExecutionScope,
    focus_key: String,
    prerequisite_until: String,
    verified: Option<CollectionDescription>,
}
impl CurrentExecutionProfileSource {
    pub fn new(pool: Arc<Pool>, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        Self::with_io(ReadSession::collection(pool, request.budget.clone())?, &request, host, port)
    }
}
fn bounded_request(request: &CollectionRequest) -> Result<CollectionRequest> {
    request.validate()?;
    let CollectionSelection::SourceRead { focus, execution } = &request.selection else { return Err(invalid_data()) };
    if focus.question != SourceReadQuestion::CurrentExecutionProfile
        || !execution.accepts_question(focus.question)
        || execution.target() != focus.target
    {
        return Err(invalid_data());
    }
    let participant = focus.execution_profile_request().ok_or_else(invalid_data)?;
    crate::activity::current_request_work::validate_focus(participant)?;
    let mut request = request.clone();
    request.budget = bounded_budget(&request.budget);
    request.budget.validate()?;
    Ok(request)
}
impl<R: NativeRead> CurrentExecutionProfileSource<R> {
    pub(super) fn with_io(io: R, request: &CollectionRequest, host: String, port: u16) -> Result<Self> {
        let request = bounded_request(request)?;
        let CollectionSelection::SourceRead { focus, execution } = &request.selection else {
            return Err(invalid_data());
        };
        let participant = focus.execution_profile_request().ok_or_else(invalid_data)?.clone();
        let reader = ActivityReader::new(
            io,
            ReadOptions { budget: request.budget.clone(), identity: IdentityDetail::Endpoint },
            host,
            port,
        );
        Ok(Self {
            inner: culprit_read::FrozenSource::new(
                Reader {
                    reader,
                    focus: Box::new(participant),
                    execution: execution.as_ref().clone(),
                    focus_key: focus.key()?,
                    prerequisite_until: focus.execution_profile_observed_until().ok_or_else(invalid_data)?.into(),
                    verified: None,
                },
                &request,
            )?,
        })
    }
}
#[async_trait::async_trait]
impl<R: NativeRead> culprit_read::ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<culprit_read::FrozenObservation> {
        let point = self.reader.capture_current_execution_profile(&self.focus).await?;
        if !self.execution.matches_context(&point.context)
            || ObservationBounds::new(&self.prerequisite_until, &point.context.observed_from).is_none()
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        let key = point.key()?;
        let context = point.context.clone();
        let description = CollectionDescription {
            target: context.target.clone(),
            partition: SourcePartition::Target,
            source: culprit_core::source_id::CURRENT_EXECUTION_PROFILE.into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::SourceRead {
                snapshot: StateSnapshotDescription {
                    dataset: SnapshotDataset::CurrentExecutionProfile,
                    observed_from: context.observed_from.clone(),
                    observed_until: context.observed_until.clone(),
                    records: 1,
                    // Dynamic operator/thread visibility never attests a whole execution or finished siblings.
                    sources: vec![StateSourceCoverage::observed(
                        culprit_core::source_id::CURRENT_EXECUTION_PROFILE.into(),
                        1,
                        false,
                    )],
                },
                focus_key: self.focus_key.clone(),
                execution: Box::new(context),
            },
        };
        if !description.matches_request(request) {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.verified = Some(description.clone());
        Ok(culprit_read::FrozenObservation {
            description,
            records: vec![TelemetryRecord::Operational(OperationalObservation {
                source: culprit_core::source_id::CURRENT_EXECUTION_PROFILE.into(),
                key,
                fact: OperationalFact::MssqlCurrentExecutionProfile(point),
            })],
        })
    }
    async fn verify(&mut self, description: &CollectionDescription) -> Result<bool> {
        // The capture already completed the held final native source/actor checks. As focused blocking does,
        // retain that exact proof rather than dispatching another identity query during finish.
        Ok(self.verified.as_ref() == Some(description))
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
impl<R: NativeRead> TelemetrySource for CurrentExecutionProfileSource<R> {
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
