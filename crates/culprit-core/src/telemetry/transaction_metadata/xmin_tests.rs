use super::*;
use crate::{CleanupHolder, MeasurementScope, SourceState, TransactionActivity};

fn holder() -> RetentionObservation {
    RetentionObservation {
        source: "pg_stat_activity".into(),
        identity: NativeIdentity::new("backend-xmin"),
        observed_at: "2020-01-01T12:00:00Z".into(),
        scope: MeasurementScope::Database,
        holder: CleanupHolder::Session,
        name: "backend 77".into(),
        horizons: vec![RetentionHorizon {
            protects: vec![CleanupHorizon::Data],
            native_name: "backend_xmin".into(),
            native_value: "1234".into(),
            transactions_behind: Some("100".into()),
        }],
        session: Some(NativeActivityIdentity { id: "77".into(), generation: "2020-01-01T11:00:00Z".into() }),
        unresolved_session_id: None,
        transaction: None,
        query: None,
        client: None,
        activity: TransactionActivity::IdleInTransaction,
        native_state: Some("idle in transaction".into()),
        age_seconds: None,
        database: Some("db".into()),
        code_ownership: PayloadUnavailable::NotCollected,
    }
}
fn point(holder: RetentionObservation) -> PostgresTransactionMetadata {
    let cache = super::tests::cache(Some(0), Some(false));
    PostgresTransactionMetadata {
        observed_at: "2020-01-01T12:00:00Z".into(),
        database_oid: "42".into(),
        database: "db".into(),
        focus: cache.actor.key.clone(),
        request_end: BlockingRequestState::SameStatement,
        caches: vec![cache],
        cache_rows: 1,
        cache_limited: false,
        slrus: vec![super::tests::slru()],
        retention: RetentionSnapshot {
            holders: vec![holder],
            sources: vec![RetentionSourceCoverage { source: "pg_stat_activity".into(), state: SourceState::Collected }],
            coverage: RetentionCoverage {
                native_sources: vec!["pg_stat_activity".into()],
                qualified_horizons: vec![CleanupHorizon::Data],
            },
        },
        retainer_rows: 1,
        retainer_limited: false,
    }
}

#[test]
fn exact_native_retainer_and_partial_positive_source_keep_only_the_observed_session_fact() {
    let original = holder();
    let mut point = point(original.clone());
    assert!(point.is_valid());
    assert_eq!(point.xmin_retaining_session(&original), Some(&original));
    point.retention.sources[0].state = SourceState::Partial;
    assert!(point.is_valid());
    assert!(!point.retention.is_horizon_complete(CleanupHorizon::Data));
    assert_eq!(point.xmin_retaining_session(&original), Some(&original));
}

#[test]
fn missing_generation_unresolved_pid_and_future_or_opaque_backend_start_never_become_known_sessions() {
    for change in 0..5 {
        let mut original = holder();
        match change {
            0 => {
                original.session = None;
                original.unresolved_session_id = Some("77".into());
            }
            1 => original.session.as_mut().unwrap().id = "0".into(),
            2 => original.session.as_mut().unwrap().id = "2147483648".into(),
            3 => original.session.as_mut().unwrap().generation = "unknown-native-generation".into(),
            _ => original.session.as_mut().unwrap().generation = "2020-01-01T12:00:01Z".into(),
        }
        let point = point(original.clone());
        assert!(point.is_valid());
        assert!(point.xmin_retaining_session(&original).is_none());
    }
}

#[test]
fn server_or_foreign_database_slot_and_prepared_facts_are_not_cast_to_database_queries() {
    for change in 0..4 {
        let mut original = holder();
        match change {
            0 => {
                original.scope = MeasurementScope::Server;
                original.database = Some("other".into());
            }
            1 => original.database = Some("other".into()),
            2 => original.holder = CleanupHolder::ReplicationSlot,
            _ => original.holder = CleanupHolder::PreparedTransaction,
        }
        let point = point(original.clone());
        assert_eq!(point.is_valid(), change != 1, "a foreign database-scoped retainer remains an invalid original");
        assert!(point.xmin_retaining_session(&original).is_none());
    }
}

#[test]
fn unqualified_data_horizon_and_changed_selected_body_withhold_session_projection_without_repair() {
    let original = holder();
    let mut point = point(original.clone());
    point.retention.coverage.qualified_horizons.clear();
    assert!(point.is_valid());
    assert!(point.xmin_retaining_session(&original).is_none());
    point.retention.coverage.qualified_horizons.push(CleanupHorizon::Data);
    let mut changed = original.clone();
    changed.name = "another recorded body".into();
    assert!(point.xmin_retaining_session(&changed).is_none());
    assert_eq!(point.retention.holders, vec![original]);
}
