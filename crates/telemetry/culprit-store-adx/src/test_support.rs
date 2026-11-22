#[cfg(feature = "write")]
use crate::{
    client::{QueryRequest, QueryTransport},
    response::QueryTable,
};
#[cfg(feature = "write")]
use async_trait::async_trait;
#[cfg(feature = "write")]
use culprit_core::{Result, telemetry::DeliveryProblem};
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::Blobs;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::Fault;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::Recovery;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::cumulative_queries;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::fixture;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::interval_queries;
#[cfg(feature = "write")]
pub(crate) use culprit_evidence::analytical::test_support::waits;
use serde_json::{Value, json};
#[cfg(feature = "write")]
use std::collections::VecDeque;
#[cfg(feature = "write")]
use std::sync::Mutex;

#[cfg(feature = "write")]
pub(crate) fn event_grid_sink(
    contract: culprit_evidence::delivery::contract::Contract,
    recovery: std::sync::Arc<dyn culprit_evidence::delivery::recovery::RecoveryRead>,
    blobs: std::sync::Arc<Blobs>,
    query: std::sync::Arc<dyn QueryTransport>,
    scratch: &std::path::Path,
) -> Result<culprit_collector::analytical::sink::AnalyticalSink> {
    crate::sink::new(
        contract,
        recovery,
        blobs.clone(),
        std::sync::Arc::new(crate::ready::ReadyDispatcher::new(blobs)),
        query,
        scratch,
    )
}

#[cfg(feature = "write")]
pub(super) enum Reply {
    Audit(u64),
    Rows(Vec<Vec<Value>>),
    Failure,
}
#[cfg(feature = "write")]
#[derive(Default)]
pub(super) struct Queries {
    pub(super) replies: Mutex<VecDeque<Reply>>,
    pub(super) requests: Mutex<Vec<QueryRequest>>,
}
#[cfg(feature = "write")]
#[async_trait]
impl QueryTransport for Queries {
    fn destination(&self) -> &str {
        "adx:test"
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryTable> {
        let reply = self.replies.lock().unwrap().pop_front().expect("unexpected query");
        let columns = request.columns.clone();
        self.requests.lock().unwrap().push(request);
        let rows = match reply {
            Reply::Audit(count) => vec![vec![json!(count)]],
            Reply::Rows(rows) => rows,
            Reply::Failure => return Err(DeliveryProblem::Transport.error()),
        };
        Ok(QueryTable { columns, response_bytes: serde_json::to_vec(&rows).unwrap().len(), rows })
    }
}

pub(crate) fn returned(schema: &crate::schema::Schema, row: &Value) -> Vec<Value> {
    schema
        .columns()
        .iter()
        .map(|column| {
            let value = &row[&column.name];
            match column.kind {
                crate::projection::ScalarType::JsonContainer => {
                    value.as_str().map_or_else(|| json!(""), |text| json!(text))
                }
                crate::projection::ScalarType::ExactCounter => json!(value.as_str().unwrap_or_default()),
                _ => value.clone(),
            }
        })
        .collect()
}
