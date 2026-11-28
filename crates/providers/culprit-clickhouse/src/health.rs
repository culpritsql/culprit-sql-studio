//! The desktop snapshot and the collector decode identical native records.
use crate::{
    client::{Client, invalid_response},
    identity::Identity,
    queries, sources,
    workload::Window,
};
use culprit_core::{Result, Source, telemetry::ClickHouseObservation as Fact};
use serde::de::DeserializeOwned;

#[derive(Clone, Copy)]
pub enum Read {
    Failures,
    Parts,
    Merges,
    Mutations,
    Replicas,
    Queue,
    DistributionQueue,
    AsyncInserts,
}
impl Read {
    pub const ALL: &[Self] = &[
        Self::Failures,
        Self::Parts,
        Self::Merges,
        Self::Mutations,
        Self::Replicas,
        Self::Queue,
        Self::DistributionQueue,
        Self::AsyncInserts,
    ];
    pub fn source(self) -> &'static Source {
        match self {
            Self::Failures => &sources::FAILURES,
            Self::Parts => &sources::PARTS,
            Self::Merges => &sources::MERGES,
            Self::Mutations => &sources::MUTATIONS,
            Self::Replicas => &sources::REPLICAS,
            Self::Queue => &sources::REPLICATION_QUEUE,
            Self::DistributionQueue => &sources::DISTRIBUTION_QUEUE,
            Self::AsyncInserts => &sources::ASYNC_INSERTS,
        }
    }
    pub async fn capture(self, client: &Client, identity: &Identity, window: &Window, limit: u32) -> Result<Capture> {
        let mut params = window.parameters(limit + 1);
        async fn collect<T: DeserializeOwned>(
            client: &Client,
            query: &'static str,
            params: crate::client::Parameters,
            wrap: fn(T) -> Fact,
        ) -> Result<Vec<Fact>> {
            Ok(client.rows(query, params).await?.into_iter().map(wrap).collect())
        }
        let mut rows = match self {
            Self::Failures => {
                params.push(queries::profile_events());
                collect(client, queries::FAILURES, params, Fact::Failure).await?
            }
            Self::Parts => collect(client, queries::ACTIVE_PARTS, params, Fact::Parts).await?,
            Self::Merges => collect(client, queries::MERGES, params, Fact::Merge).await?,
            Self::Mutations => collect(client, queries::MUTATIONS, params, Fact::Mutation).await?,
            Self::Replicas => collect(client, queries::REPLICAS, params, Fact::Replica).await?,
            Self::Queue => collect(client, queries::REPLICATION_QUEUE, params, Fact::ReplicationTask).await?,
            Self::DistributionQueue => {
                collect(client, queries::DISTRIBUTION_QUEUE, params, Fact::DistributedQueue).await?
            }
            Self::AsyncInserts => collect(client, queries::ASYNC_INSERTS, params, Fact::AsyncInsert).await?,
        };
        if rows.iter().any(|fact| {
            let key = fact.identity();
            !fact.is_valid()
                || key.qualifiers.get("node") != Some(&identity.node)
                || key.qualifiers.get("database") != Some(&identity.database)
        }) {
            return Err(invalid_response());
        }
        let complete = rows.len() <= limit as usize;
        rows.truncate(limit as usize);
        Ok(Capture { rows, complete })
    }
}
pub struct Capture {
    pub rows: Vec<Fact>,
    pub complete: bool,
}
