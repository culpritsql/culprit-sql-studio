//! Controlled parser fixtures shared with provider integration tests.
pub use super::parameters::comparison::original_plan_hash;

pub fn test_read_context(database: &str) -> culprit_core::telemetry::NativeReadContext {
    use culprit_core::{Engine, telemetry::*};
    NativeReadContext {
        target: culprit_core::DatabaseIdentity {
            engine: Engine::Mssql,
            database: database.into(),
            location: culprit_core::DatabaseLocation::Server {
                host: "controlled-fixture.invalid".into(),
                port: 1433,
                instance: None,
            },
            incarnation: None,
        }
        .into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Mssql,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("controlled-test"),
                native: NativeIdentity::new("instance"),
            }),
            epoch: Some("controlled-native-epoch".into()),
        },
        observed_from: "2026-11-14T12:00:00Z".into(),
        observed_until: "2026-11-14T12:00:01Z".into(),
        database_writable: None,
    }
}

pub fn test_source(
    dataset: culprit_core::telemetry::SnapshotDataset,
) -> culprit_core::telemetry::CollectionDescription {
    use culprit_core::telemetry::*;
    let read = test_read_context("app");
    CollectionDescription {
        target: read.target,
        partition: read.partition,
        source: "controlled-cache-source".into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Snapshot(StateSnapshotDescription {
            dataset,
            observed_from: read.observed_from,
            observed_until: read.observed_until,
            records: 1,
            sources: vec![StateSourceCoverage::observed("controlled-native".into(), 1, true)],
        }),
    }
}

pub mod deadlocks;
pub const SAMPLE_EVENTS_XEL: &[u8] = include_bytes!("test_support/mssql-events.sample.xel");
