//! Disposable PostgreSQL 18 comparable commit-batching workload helper; keeps `tokio_postgres` inside `culprit-postgres`.
use super::{owner, specification};
use crate::{connect::Connector, rows::quote_ident};
use culprit_core::{ConnectionSpec, telemetry::*};
use std::time::Duration;
use tokio_postgres::Client;

pub const COMMIT_INTERVENTION_ROWS: i32 = 160;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CommitPipelineFixture {
    pub specification: ConnectionSpec,
    schema: String,
    admin: Option<Client>,
}

impl CommitPipelineFixture {
    pub async fn open() -> Self {
        let database = std::env::var("CULPRIT_PREVIEW_PG_DATABASE").unwrap();
        let specification = Box::pin(specification(&database, CollectionBudget::default())).await;
        let connector = Connector::new(&specification).unwrap();
        let admin =
            tokio::time::timeout(CONTROL_TIMEOUT, connector.connect_unconfigured()).await.unwrap().unwrap().client;
        let schema = format!("culprit_commit_{}", owner());
        admin.execute("/* culprit */ ALTER SYSTEM SET wal_sync_method = 'fdatasync'", &[]).await.unwrap();
        admin
            .batch_execute(&format!(
                "/* culprit */ SET statement_timeout='5s'; SET lock_timeout='2s'; SET synchronous_commit='on';
                 SELECT pg_catalog.pg_reload_conf();
                 CREATE SCHEMA {};
                 CREATE TABLE {}.events(id integer NOT NULL, payload text NOT NULL);
                 SELECT pg_catalog.pg_stat_reset_shared('io');
                 SELECT pg_catalog.pg_stat_reset_shared('wal');
                 SELECT pg_catalog.pg_stat_force_next_flush();",
                quote_ident(&schema),
                quote_ident(&schema),
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        Self { specification, schema, admin: Some(admin) }
    }

    pub async fn run_unbatched_arm(&self) -> String {
        let client = self.admin.as_ref().unwrap();
        let row = client
            .query_one("/* culprit */ SELECT (pg_catalog.to_jsonb(pg_catalog.clock_timestamp()) #>> '{}')::text", &[])
            .await
            .unwrap();
        let at = canonical_time(&row.get::<_, String>(0)).unwrap();
        let sql = format!(
            "INSERT INTO {}.events(id, payload) VALUES ($1, pg_catalog.repeat('a', 128))",
            quote_ident(&self.schema)
        );
        for id in 0..COMMIT_INTERVENTION_ROWS {
            client.execute(&sql, &[&id]).await.unwrap();
        }
        client.batch_execute("/* culprit */ SELECT pg_catalog.pg_stat_force_next_flush();").await.unwrap();
        at
    }

    pub async fn run_batched_arm(&self) -> String {
        let client = self.admin.as_ref().unwrap();
        let row = client
            .query_one("/* culprit */ SELECT (pg_catalog.to_jsonb(pg_catalog.clock_timestamp()) #>> '{}')::text", &[])
            .await
            .unwrap();
        let at = canonical_time(&row.get::<_, String>(0)).unwrap();
        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {}.events(id, payload)
                 SELECT g, pg_catalog.repeat('b', 128) FROM pg_catalog.generate_series(1, {COMMIT_INTERVENTION_ROWS}) AS g;
                 COMMIT;
                 /* culprit */ SELECT pg_catalog.pg_stat_force_next_flush();",
                quote_ident(&self.schema)
            ))
            .await
            .unwrap();
        at
    }

    pub async fn close(&mut self) {
        if let Some(client) = self.admin.take() {
            client
                .batch_execute(&format!("/* culprit */ DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&self.schema)))
                .await
                .unwrap();
            client.execute("/* culprit */ ALTER SYSTEM RESET wal_sync_method", &[]).await.unwrap();
            client.batch_execute("/* culprit */ SELECT pg_catalog.pg_reload_conf()").await.unwrap();
            drop(client);
        }
    }
}
