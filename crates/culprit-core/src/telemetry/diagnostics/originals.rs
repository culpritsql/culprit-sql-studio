//! Original source signals reuse the incident journal without inventing an Activity participant or event lifetime.
use super::*;
mod qualification;
pub use qualification::*;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DiagnosticSourceKind {
    ClickHouseOperational,
    MemoryFailures,
    NativeFailures,
    Cleanup,
    TaskSpace,
    QueryHistory,
    WorkerQueues,
    Deadlocks,
}
impl DiagnosticSourceKind {
    /// Eligible collection families only; native original headers/rows still establish actual applicability.
    pub fn supports_collection(collection: TelemetryCollection) -> bool {
        matches!(
            collection,
            TelemetryCollection::QueryHistory
                | TelemetryCollection::DeadlockHistory
                | TelemetryCollection::Snapshot(
                    SnapshotDataset::Operational
                        | SnapshotDataset::Cleanup
                        | SnapshotDataset::TaskSpace
                        | SnapshotDataset::MemoryFailures
                        | SnapshotDataset::NativeFailures
                )
        )
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DiagnosticPopulation {
    Activity { population: ActivityPopulation },
    Source { source: DiagnosticSourceKind, range: TelemetryRange },
}

impl DiagnosticPopulation {
    pub(super) fn is_valid(&self) -> bool {
        match self {
            Self::Activity { .. } => true,
            Self::Source {
                source: DiagnosticSourceKind::QueryHistory,
                range: TelemetryRange::NativeInterval { interval },
            } => valid_key(&interval.native_id) && ObservationBounds::new(&interval.from, &interval.until).is_some(),
            Self::Source {
                source: DiagnosticSourceKind::Deadlocks,
                range: TelemetryRange::TimeWindow { from, until },
            } => ObservationBounds::new(from, until).is_some(),
            Self::Source { source, range: TelemetryRange::Current } => {
                !matches!(source, DiagnosticSourceKind::QueryHistory | DiagnosticSourceKind::Deadlocks)
            }
            _ => false,
        }
    }
}

/// Native failure records and historical counts remain captured observations, not current/global incident counts.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DiagnosticOriginalCondition {
    ClickHouse {
        incident: ClickHouseIncident,
    },
    MssqlOutOfMemory,
    /// Positive native worker wait/work queue only; no actor or selected-database ownership is invented.
    MssqlWorkerQueue,
    MysqlError,
    PostgresFailure,
    /// Qualified failure, deadlock or client-attention Extended Event; routine handled user errors are excluded.
    MssqlExtendedEvent,
    /// Retained deadlock graph occurrence from a qualified shared or database deadlock source.
    DeadlockEvent,
    MssqlCleanupBoundary {
        session: NativeActivityIdentity,
        transaction: NativeActivityIdentity,
    },
    /// Checked request context only, never the statement which allocated earlier retained pages.
    MssqlTemporarySpace {
        request: ActivityKey,
        statement_identity: NativeIdentity,
    },
    /// A native Query Store population fragment, not new errors or distinct failed current executions.
    MssqlQueryHistory {
        execution_type: u8,
        query: NativeIdentity,
        plan: NativeIdentity,
        cohort: NativeIdentity,
    },
}
impl DiagnosticOriginalCondition {
    fn source_kind(&self) -> DiagnosticSourceKind {
        match self {
            Self::MssqlOutOfMemory => DiagnosticSourceKind::MemoryFailures,
            Self::MssqlWorkerQueue => DiagnosticSourceKind::WorkerQueues,
            Self::ClickHouse { .. } => DiagnosticSourceKind::ClickHouseOperational,
            Self::MysqlError | Self::PostgresFailure | Self::MssqlExtendedEvent => DiagnosticSourceKind::NativeFailures,
            Self::DeadlockEvent => DiagnosticSourceKind::Deadlocks,
            Self::MssqlCleanupBoundary { .. } => DiagnosticSourceKind::Cleanup,
            Self::MssqlTemporarySpace { .. } => DiagnosticSourceKind::TaskSpace,
            Self::MssqlQueryHistory { .. } => DiagnosticSourceKind::QueryHistory,
        }
    }
    fn is_valid(&self) -> bool {
        match self {
            Self::MssqlCleanupBoundary { session, transaction } => {
                known_generation(session) && known_generation(transaction)
            }
            Self::MssqlTemporarySpace { request, statement_identity } => {
                request.is_valid()
                    && known_generation(&request.session)
                    && request.request.as_ref().is_some_and(known_generation)
                    && statement_identity.is_valid()
            }
            Self::MssqlQueryHistory { execution_type, query, plan, cohort } => {
                matches!(execution_type, 3 | 4)
                    && query_store_query_identity_is_complete(&NativeQueryIdentity {
                        query_id: query.id.clone(),
                        qualifiers: query.qualifiers.clone(),
                    })
                    && plan.is_valid()
                    && positive(&plan.id).is_some()
                    && plan.qualifiers.get("initial_compile_at").is_some_and(|at| canonical_time(at).is_ok())
                    && cohort.is_valid()
                    && cohort.qualifiers.get("execution_type") == Some(&execution_type.to_string())
            }
            Self::MssqlOutOfMemory
            | Self::MssqlWorkerQueue
            | Self::ClickHouse { .. }
            | Self::MysqlError
            | Self::PostgresFailure
            | Self::MssqlExtendedEvent
            | Self::DeadlockEvent => true,
        }
    }
    pub(super) fn accepts_identity(&self, key: &NativeRecordIdentity) -> bool {
        if !self.is_valid() || !key.is_valid() {
            return false;
        }
        match (self, key) {
            (Self::MssqlQueryHistory { plan, cohort, .. }, NativeRecordIdentity::Runtime(native)) => {
                let mut expected = cohort.qualifiers.clone();
                expected.insert("plan_id".into(), plan.id.clone());
                positive(&native.id).is_some() && native.qualifiers == expected
            }
            (Self::DeadlockEvent, NativeRecordIdentity::Deadlock { .. }) => true,
            (
                Self::MssqlOutOfMemory
                | Self::MssqlWorkerQueue
                | Self::ClickHouse { .. }
                | Self::MysqlError
                | Self::PostgresFailure
                | Self::MssqlExtendedEvent
                | Self::MssqlCleanupBoundary { .. }
                | Self::MssqlTemporarySpace { .. },
                NativeRecordIdentity::Operational { .. },
            ) => true,
            _ => false,
        }
    }
    pub(super) fn has_current_lifecycle(&self) -> bool {
        matches!(self, Self::MssqlCleanupBoundary { .. })
    }
    /// This classification is shared by the observer and authenticated journal validation.
    /// It cannot grant source admission or restore an unavailable body.
    pub fn from_record(source: &CollectionDescription, record: &TelemetryRecord) -> Option<Self> {
        let source_kind = diagnostic_original_source_kind(source)?;
        let condition = match (source_kind, record) {
            (DiagnosticSourceKind::ClickHouseOperational, TelemetryRecord::Operational(row)) => {
                let OperationalFact::ClickHouse(point) = &row.fact else { return None };
                if !point.fits_source(source)
                    || row.source != point.source_name()
                    || row.key != point.identity().key().ok()?
                {
                    return None;
                }
                Self::ClickHouse { incident: point.incident()? }
            }
            (DiagnosticSourceKind::WorkerQueues, TelemetryRecord::Operational(row))
                if worker_starvation::positive_worker_queue(row) =>
            {
                Self::MssqlWorkerQueue
            }
            (
                DiagnosticSourceKind::MemoryFailures,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlOutOfMemory(_), ..
                }),
            ) if source.target.engine() == crate::Engine::Mssql => Self::MssqlOutOfMemory,
            (
                DiagnosticSourceKind::NativeFailures,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MysqlErrorLog(row), ..
                }),
            ) if source.target.engine() == crate::Engine::Mysql && row.priority == "Error" => Self::MysqlError,
            (
                DiagnosticSourceKind::NativeFailures,
                TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::PostgresLog(_), .. }),
            ) if source.target.engine() == crate::Engine::Postgres => Self::PostgresFailure,
            (
                DiagnosticSourceKind::NativeFailures,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlExtendedEvent(row),
                    ..
                }),
            ) if source.target.engine() == crate::Engine::Mssql && mssql_event_is_incident_condition(row) => {
                Self::MssqlExtendedEvent
            }
            (
                DiagnosticSourceKind::Cleanup,
                TelemetryRecord::Operational(OperationalObservation {
                    fact: OperationalFact::MssqlCleanupHorizon(row),
                    ..
                }),
            ) if source.target.engine() == crate::Engine::Mssql => {
                let boundary = row.oldest_confirmed.as_deref()?;
                Self::MssqlCleanupBoundary {
                    session: boundary.session.clone(),
                    transaction: boundary.transaction.clone(),
                }
            }
            (
                DiagnosticSourceKind::TaskSpace,
                TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::TaskSpace(row), .. }),
            ) if source.target.engine() == crate::Engine::Mssql
                && row.request_context() == TaskRequestContext::CheckedAtBounds
                && retained_temporary_pages(row).is_some_and(|pages| pages > 0) =>
            {
                let request = row.request.as_ref()?;
                if !request.has_session_generation_authority() {
                    return None;
                }
                // The same original row and both native checks must retain the same statement token.
                let statement = row.statement_identity.as_ref()?;
                if request.statement_identity.as_ref() != Some(statement) {
                    return None;
                }
                Self::MssqlTemporarySpace { request: request.key.clone(), statement_identity: statement.clone() }
            }
            (DiagnosticSourceKind::QueryHistory, TelemetryRecord::QueryRuntime(row))
                if source.target.engine() == crate::Engine::Mssql
                    && source.source == crate::source_id::QUERY_STORE
                    && positive(&row.executions).is_some()
                    && positive(&row.key.native.id).is_some() =>
            {
                // The supported native parser owns this code; unknown values remain unclassified.
                // https://learn.microsoft.com/sql/relational-databases/system-catalog-views/sys-query-store-runtime-stats-transact-sql
                let code = row.key.native.qualifiers.get("execution_type")?;
                let execution_type = code.parse::<u8>().ok()?;
                let query = row.key.references.query.as_ref()?.native.clone();
                let plan = row.key.references.plan.as_ref()?.native.clone();
                let cohort = row.key.cohort.as_ref()?.clone();
                let mut expected = row.key.native.clone();
                expected.id = "query-store-execution-population".into();
                expected.qualifiers.remove("plan_id");
                if cohort != expected || row.key.native.qualifiers.get("plan_id") != Some(&plan.id) {
                    return None;
                }
                Self::MssqlQueryHistory { execution_type, query, plan, cohort }
            }
            (DiagnosticSourceKind::Deadlocks, TelemetryRecord::Deadlock(row)) if row.is_valid() => Self::DeadlockEvent,
            _ => return None,
        };
        condition.is_valid().then_some(condition)
    }
}
fn known_generation(identity: &NativeActivityIdentity) -> bool {
    identity.is_valid() && !identity.generation.is_empty()
}
fn retained_temporary_pages(row: &TaskSpaceObservation) -> Option<u64> {
    let remaining = |allocated: &Option<String>, released: &Option<String>| {
        allocated.as_deref()?.parse::<u64>().ok()?.checked_sub(released.as_deref()?.parse::<u64>().ok()?)
    };
    remaining(&row.user_allocated_pages, &row.user_deallocated_pages)?
        .checked_add(remaining(&row.internal_allocated_pages, &row.internal_deallocated_pages)?)
}

/// Distinguish observed resource/deadlock/cancellation events from informational or routine handled user errors.
pub fn mssql_event_is_incident_condition(row: &MssqlExtendedEventObservation) -> bool {
    match row.kind {
        MssqlExtendedEventKind::Attention { .. } | MssqlExtendedEventKind::DeadlockReport { .. } => true,
        MssqlExtendedEventKind::ErrorReported { number, severity, intercepted, .. } => {
            if intercepted == Some(true) && severity <= 16 {
                return false;
            }
            severity >= 17
                || matches!(
                    number,
                    701 | 802 | 1101 | 1105 | 1204 | 1205 | 1222 | 3960 | 3961 | 8628 | 8645 | 8651 | 9002
                )
        }
    }
}

/// One native acquisition owner is necessary; a name or ordinary target-only snapshot is insufficient.
/// The target identifies the original database route, not ownership of shared error-log/OOM resources.
pub fn diagnostic_original_source_kind(source: &CollectionDescription) -> Option<DiagnosticSourceKind> {
    diagnostic_original_input_qualification(Some(source)).source()
}
pub(super) fn scope(source: &CollectionDescription) -> Result<DiagnosticScope> {
    let kind = diagnostic_original_source_kind(source).ok_or_else(invalid)?;
    let scope = DiagnosticScope {
        target: source.target.clone(),
        partition: source.partition.clone(),
        source: source.source.clone(),
        population: DiagnosticPopulation::Source { source: kind, range: source.observation.range()?.canonical()? },
        measurement_scope: match (&source.target, kind) {
            (CollectionTarget::Database(_), DiagnosticSourceKind::NativeFailures | DiagnosticSourceKind::Deadlocks) => {
                crate::MeasurementScope::Database
            }
            (
                _,
                DiagnosticSourceKind::MemoryFailures
                | DiagnosticSourceKind::NativeFailures
                | DiagnosticSourceKind::WorkerQueues
                | DiagnosticSourceKind::Deadlocks,
            ) => crate::MeasurementScope::Server,
            _ => crate::MeasurementScope::Database,
        },
        // Query history remains in its data namespace. The present acquisition process is not a historical executor.
        native_epoch: source.partition.native_epoch().map(str::to_owned),
    };
    scope.is_valid().then_some(scope).ok_or_else(invalid)
}
impl DiagnosticSignal {
    pub(crate) fn is_capture_point(&self) -> bool {
        self.is_task_point()
            || matches!(self, Self::OriginalObservation { condition, .. } if !condition.has_current_lifecycle())
    }
}
impl DiagnosticEpisode {
    /// Stable native grouping, independent of a later capture receipt or source-read answer. This is a lookup
    /// key only; an immutable original revision must be authenticated before its episode can be reused.
    /// Finish local classification after the one question-planning pass. Native question state is independent
    /// and still owes its admitted work; the original observation does not become an established cause.
    pub fn finish_original_observation_planning(&self) -> Result<Self> {
        if !self.is_valid() {
            return Err(invalid());
        }
        let mut settled = self.clone();
        if matches!(settled.signal, DiagnosticSignal::OriginalObservation { .. })
            && settled.analysis == DiagnosticAnalysisStatus::Pending
        {
            settled.analysis = DiagnosticAnalysisStatus::Unresolved;
            settled.issue = Some(DiagnosticAnalysisIssue::Unsupported);
        }
        if !settled.is_valid() {
            return Err(invalid());
        }
        Ok(settled)
    }
    pub fn original_group_key(&self) -> Result<Option<String>> {
        if !self.is_valid() {
            return Err(invalid());
        }
        match &self.signal {
            DiagnosticSignal::OriginalObservation { .. } if self.signal.is_capture_point() => {
                self.scope.original_group_key(&self.signal).map(Some)
            }
            _ => Ok(None),
        }
    }
    pub(super) fn original_point_is_consistent(&self) -> bool {
        let DiagnosticSignal::OriginalObservation { condition, .. } = &self.signal else {
            return true;
        };
        matches!(&self.scope.population, DiagnosticPopulation::Source { source, .. } if *source == condition.source_kind())
            && match (&self.scope.target, &self.scope.partition, condition.source_kind()) {
                (
                    CollectionTarget::Database(db),
                    SourcePartition::Target,
                    DiagnosticSourceKind::ClickHouseOperational,
                ) => {
                    db.engine == crate::Engine::Clickhouse
                        && db.incarnation.is_some()
                        && self.scope.measurement_scope == crate::MeasurementScope::Database
                }
                (
                    CollectionTarget::Shared(owner),
                    SourcePartition::InstanceLocal { member: Some(member), epoch: Some(_) },
                    DiagnosticSourceKind::MemoryFailures
                    | DiagnosticSourceKind::NativeFailures
                    | DiagnosticSourceKind::WorkerQueues
                    | DiagnosticSourceKind::Deadlocks,
                ) => {
                    owner == member
                        && (owner.kind == SharedResourceKind::Instance
                            || condition.source_kind() == DiagnosticSourceKind::WorkerQueues
                                && owner.kind == SharedResourceKind::Node)
                        && self.scope.measurement_scope == crate::MeasurementScope::Server
                }
                (
                    CollectionTarget::Database(db),
                    SourcePartition::InstanceLocal { member: None, epoch: Some(_) },
                    DiagnosticSourceKind::NativeFailures,
                ) => {
                    db.engine == crate::Engine::Mssql
                        && self.scope.source == MSSQL_DATABASE_EVENTS_SOURCE
                        && db.incarnation.is_some()
                        && self.scope.measurement_scope == crate::MeasurementScope::Database
                }
                (CollectionTarget::Database(_), SourcePartition::Target, DiagnosticSourceKind::Deadlocks) => {
                    self.scope.measurement_scope == crate::MeasurementScope::Database
                }
                (
                    CollectionTarget::Database(_),
                    _,
                    DiagnosticSourceKind::Cleanup
                    | DiagnosticSourceKind::TaskSpace
                    | DiagnosticSourceKind::QueryHistory
                    | DiagnosticSourceKind::WorkerQueues,
                ) => true,
                _ => false,
            }
            && (condition.has_current_lifecycle()
                || (self.peak.is_none() && self.lifecycle == DiagnosticLifecycle::Uncertain && self.recovery.is_none()))
    }
    pub fn original_observation_matches(&self, source: &CollectionDescription, row: &TelemetryRecord) -> bool {
        let DiagnosticSignal::OriginalObservation { condition, native_key } = &self.signal else {
            return false;
        };
        condition.accepts_identity(&row.native_identity())
            && (matches!(condition, DiagnosticOriginalCondition::MssqlQueryHistory { .. })
                || native_key == &row.native_identity())
            && DiagnosticScope::from_source(source).is_ok_and(|scope| scope.key().ok() == self.scope.key().ok())
            && DiagnosticOriginalCondition::from_record(source, row).as_ref() == Some(condition)
    }
}

/// Acquisition bounds qualify when Culprit observed the original; they are never the event or query interval.
pub fn diagnostic_original_observed_until(source: &CollectionDescription) -> Option<&str> {
    diagnostic_original_source_kind(source)?;
    source
        .observation
        .snapshot()
        .map(|snapshot| snapshot.observed_until.as_str())
        .or_else(|| source.observation.events().map(|events| events.observed_at.as_str()))
        .or_else(|| source.query_history()?.acquisition_owner.as_deref().map(|owner| owner.observed_until.as_str()))
}
impl DiagnosticEpisode {
    pub(crate) fn original_scope_can_resolve(&self, source: &CollectionDescription) -> bool {
        let DiagnosticSignal::OriginalObservation {
            condition: DiagnosticOriginalCondition::MssqlCleanupBoundary { .. },
            native_key: NativeRecordIdentity::Operational { source: native_source, .. },
        } = &self.signal
        else {
            return false;
        };
        DiagnosticScope::from_source(source).is_ok_and(|scope| scope.key().ok() == self.scope.key().ok())
            && diagnostic_original_observed_until(source)
                .is_some_and(|at| ObservationBounds::new(&self.last_seen_at, at).is_some())
            && source.observation.snapshot().is_some_and(|snapshot| {
                snapshot.sources.iter().any(|coverage| {
                    coverage.source == *native_source
                        && diagnostic_baseline_can_resolve(&self.scope, coverage.state, coverage.complete_scope)
                        && coverage.error_code.is_none()
                        && coverage.stopped.is_none()
                })
            })
    }
}

impl DiagnosticScope {
    pub fn original_group_key(&self, signal: &DiagnosticSignal) -> Result<String> {
        if !self.is_valid() || !matches!(signal, DiagnosticSignal::OriginalObservation { .. }) || !signal.is_valid() {
            return Err(invalid());
        }
        digest_value(&("diagnostic-original-group-v1", self.key()?, signal.key()?))
    }
}
