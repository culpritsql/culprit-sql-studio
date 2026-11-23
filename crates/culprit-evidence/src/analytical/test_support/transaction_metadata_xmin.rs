//! One canonical corpus for existing native xmin-retainer fields. These are controlled facts, not native acceptance.
use super::*;
use culprit_core::{CleanupHolder, MeasurementScope, SourceState, TransactionActivity};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMetadataXminCase {
    Session,
    PreviousStatement,
    Partial,
    UnresolvedSession,
    FutureGeneration,
    ForeignServerSession,
    PreparedTransaction,
    UnqualifiedHorizon,
    ReusedPid,
}
impl TransactionMetadataXminCase {
    pub const ALL: &'static [Self] = &[
        Self::Session,
        Self::PreviousStatement,
        Self::Partial,
        Self::UnresolvedSession,
        Self::FutureGeneration,
        Self::ForeignServerSession,
        Self::PreparedTransaction,
        Self::UnqualifiedHorizon,
        Self::ReusedPid,
    ];
}

pub fn transaction_metadata_xmin_result(
    focus: &SourceReadFocus,
    execution: &SourceReadExecutionScope,
    case: TransactionMetadataXminCase,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    use TransactionMetadataXminCase::*;
    let base = if case == ReusedPid { TransactionMetadataCase::Overflow } else { TransactionMetadataCase::Clear };
    let (mut source, mut records) = transaction_metadata_result(focus, execution, base);
    let TelemetryRecord::Operational(row) = &mut records[0] else { panic!() };
    let OperationalFact::PostgresTransactionMetadata(point) = &mut row.fact else { panic!() };
    // Preserve the native tied xmin distance rather than inventing a global oldest or a new counter window.
    let mut holder = point.retention.holders[0].clone();
    holder.source = "pg_stat_activity".into();
    holder.identity = NativeIdentity::new("native-xmin-retaining-session");
    holder.holder = CleanupHolder::Session;
    holder.scope = MeasurementScope::Database;
    holder.name = "backend 77".into();
    holder.database = Some(point.database.clone());
    holder.session = Some(NativeActivityIdentity { id: "77".into(), generation: "2020-01-01T11:58:00Z".into() });
    holder.unresolved_session_id = None;
    holder.query = None;
    holder.transaction = Some(RetentionTransactionContext {
        native_id: None,
        started_at: Some("2020-01-01T11:58:00Z".into()),
        prepared_at: None,
    });
    holder.client = Some(ActivityClient {
        application: Some("native-retainer-application".into()),
        user: Some("native-retainer-user".into()),
        host: None,
    });
    holder.age_seconds = None;
    holder.activity = TransactionActivity::IdleInTransaction;
    holder.native_state = Some("idle in transaction".into());
    holder.horizons.retain(|horizon| horizon.protects.contains(&CleanupHorizon::Data));
    for horizon in &mut holder.horizons {
        horizon.protects = vec![CleanupHorizon::Data];
        horizon.native_name = "backend_xmin".into();
    }
    match case {
        PreviousStatement => {
            holder.query = Some(RetentionQueryContext {
                identity: Some(NativeQueryIdentity { query_id: "9999".into(), qualifiers: Default::default() }),
                started_at: Some("2020-01-01T11:58:30Z".into()),
                relationship: RetentionQueryRelationship::PreviousStatement,
            })
        }
        UnresolvedSession => {
            holder.session = None;
            holder.unresolved_session_id = Some("77".into());
        }
        FutureGeneration => holder.session.as_mut().unwrap().generation = "2020-01-01T12:10:00Z".into(),
        ForeignServerSession => {
            holder.scope = MeasurementScope::Server;
            holder.database = Some("another-native-database".into());
        }
        PreparedTransaction => holder.holder = CleanupHolder::PreparedTransaction,
        UnqualifiedHorizon => {
            point.retention.coverage.qualified_horizons.retain(|horizon| *horizon != CleanupHorizon::Data)
        }
        ReusedPid => {
            holder.session.as_mut().unwrap().id = "43".into();
            holder.session.as_mut().unwrap().generation = "2020-01-01T12:00:03Z".into();
            holder.name = "backend 43".into();
        }
        _ => {}
    }
    point.retention.holders.push(holder);
    point.retainer_rows = point.retention.holders.len() as u32;
    for source in &mut point.retention.sources {
        if source.source == "pg_stat_activity" {
            source.state = SourceState::Collected;
        }
        if case == Partial {
            source.state = SourceState::Partial;
        }
    }
    if case == Partial {
        let SourceObservation::SourceRead { snapshot, .. } = &mut source.observation else { panic!() };
        snapshot.sources[0] = StateSourceCoverage::observed(snapshot.sources[0].source.clone(), 1, false);
    }
    row.key = point.key().unwrap();
    assert!(point.fits_source_scope(&source));
    assert!(records.iter().all(TelemetryRecord::is_valid));
    (source, records)
}
