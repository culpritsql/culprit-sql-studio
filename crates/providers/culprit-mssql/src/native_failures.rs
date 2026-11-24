//! Non-flushing Extended Events acquisition for SQL Server `system_health` files and approved Azure SQL Database
//! `culprit_events` ring buffers. Never queries `sys.dm_xe_session_targets` on server sessions, never touches Azure's
//! internal `dl` session, and never starts or alters an Extended Events session.
use crate::{
    conn::Pool,
    database_identity, instance,
    probe::ENGINE_EDITION_AZURE_SQL_DATABASE,
    read::{
        NativeRead, Parameter, ReadError, ReadSession, integer, invalid_data, optional_integer, optional_text, text,
    },
    sources,
};
use culprit_core::{
    Coverage, Engine, Error, ErrorCode, NativeLogLimits, Result, Snapshot, t,
    telemetry::{
        CollectionBudget, CollectionDescription, CollectionRequest, CollectionSelection, CollectionStop,
        CollectionSubject, CollectionTarget, MSSQL_DATABASE_EVENTS_SOURCE, MSSQL_SYSTEM_HEALTH_EVENTS_SOURCE,
        MssqlExtendedEventObservation, NativeFailureRecord, NativeFailureSnapshot, ObservationBounds, OperationalFact,
        OperationalObservation, ReadWorkMetrics, SharedResourceKind, SnapshotDataset, SourceContinuity,
        SourceObservation, SourcePartition, SourceShutdown, StateSnapshotDescription, StateSourceCoverage,
        TelemetryRecord, TelemetrySource, canonical_time, digest,
    },
};
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};
use std::sync::Arc;

const MIN_NATIVE_MAJOR: u32 = 14;
const MAX_CONFIGURED_PATH_BYTES: i64 = 8_000;
const MAX_FILE_SIZE_MB: i64 = 512;
const MAX_ROLLOVER_FILES: i64 = 64;
pub(crate) const MAX_EVENT_ROWS: usize = 128;
const MAX_EVENT_XML_BYTES: i64 = 256 * 1024;
const MAX_DATABASE_MEMORY_KIB: i64 = 1_024;
const MAX_DATABASE_EVENTS: usize = 256;
const MAX_DATABASE_RING_BYTES: i64 = 4 * 1024 * 1024;
const MAX_SOURCE_QUERIES: u32 = 8;
pub(crate) const DATABASE_SESSION: &str = "culprit_events";

// Persistent server catalog views do not read target_data and never force session-target flushing.
const SYSTEM_HEALTH_CONFIGURATION: &str = r#"SELECT TOP(2) /* culprit */
 CONVERT(varchar(40),SYSUTCDATETIME(),127)+'Z' AS observed_at,
 c.event_session_id,target.target_id,target.package AS target_package,
 CONVERT(varchar(128),SQL_VARIANT_PROPERTY(f.value,'BaseType')) AS file_path_type,
 DATALENGTH(CONVERT(nvarchar(max),f.value)) AS file_path_bytes,
 CASE WHEN DATALENGTH(CONVERT(nvarchar(max),f.value))<=8000 THEN CONVERT(nvarchar(4000),f.value) END AS file_path,
 TRY_CONVERT(bigint,CONVERT(nvarchar(128),sz.value)) AS max_file_size_mb,
 TRY_CONVERT(bigint,CONVERT(nvarchar(128),ro.value)) AS max_rollover_files,
 CASE WHEN EXISTS(SELECT 1 FROM sys.server_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'error_reported') THEN 1 ELSE 0 END AS has_error_reported,
 CASE WHEN EXISTS(SELECT 1 FROM sys.server_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'attention') THEN 1 ELSE 0 END AS has_attention,
 CASE WHEN EXISTS(SELECT 1 FROM sys.server_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'xml_deadlock_report') THEN 1 ELSE 0 END AS has_deadlock_report,
 CASE WHEN EXISTS(SELECT 1 FROM sys.server_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver'
   AND e.name IN (N'error_reported',N'attention',N'xml_deadlock_report') AND e.predicate IS NOT NULL) THEN 1 ELSE 0 END AS has_event_predicate
FROM sys.server_event_sessions c
JOIN sys.server_event_session_targets target ON target.event_session_id=c.event_session_id
JOIN sys.server_event_session_fields f ON f.event_session_id=c.event_session_id
 AND f.object_id=target.target_id AND f.name=N'filename'
LEFT JOIN sys.server_event_session_fields sz ON sz.event_session_id=c.event_session_id
 AND sz.object_id=target.target_id AND sz.name=N'max_file_size'
LEFT JOIN sys.server_event_session_fields ro ON ro.event_session_id=c.event_session_id
 AND ro.object_id=target.target_id AND ro.name=N'max_rollover_files'
WHERE c.name=N'system_health' AND target.name=N'event_file' AND target.package=N'package0'
OPTION(MAXDOP 1,LOOP JOIN)"#;

// Sequential native buffer/file order without ORDER BY avoids scanning and sorting every rollover file.
fn system_health_events_sql(has_watermark: bool) -> String {
    let resume = if has_watermark { "@P4,@P5" } else { "NULL,NULL" };
    format!(
        r#"SELECT TOP (@P1) /* culprit */
 x.file_name,x.file_offset,
 CONVERT(varchar(40),CAST(x.timestamp_utc AS datetime2(7)),127)+'Z' AS timestamp_utc,
 DATALENGTH(CAST(x.event_data AS nvarchar(max))) AS event_bytes,
 CASE WHEN DATALENGTH(CAST(x.event_data AS nvarchar(max)))<=@P2 THEN CAST(x.event_data AS nvarchar(max)) END AS event_xml
FROM sys.fn_xe_file_target_read_file(@P3,NULL,{resume}) x
WHERE x.object_name IN (N'error_reported',N'attention',N'xml_deadlock_report')
OPTION(MAXDOP 1)"#
    )
}

const DATABASE_CONFIGURATION: &str = r#"SELECT TOP (2) /* culprit */
 CONVERT(varchar(40),SYSUTCDATETIME(),127)+'Z' AS observed_at,
 c.event_session_id,
 CONVERT(varchar(40),CAST(s.create_time AS datetime2(7)),126) AS generation,
 CONVERT(bigint,m.value) AS memory_kib,CONVERT(bigint,n.value) AS maximum_events,
 CASE WHEN EXISTS(SELECT 1 FROM sys.database_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'error_reported') THEN 1 ELSE 0 END AS has_error_reported,
 CASE WHEN EXISTS(SELECT 1 FROM sys.database_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'attention') THEN 1 ELSE 0 END AS has_attention,
 CASE WHEN EXISTS(SELECT 1 FROM sys.database_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver' AND e.name=N'database_xml_deadlock_report') THEN 1 ELSE 0 END AS has_deadlock_report,
 CASE WHEN EXISTS(SELECT 1 FROM sys.database_event_session_events e
  WHERE e.event_session_id=c.event_session_id AND e.package=N'sqlserver'
   AND e.name IN (N'error_reported',N'attention',N'database_xml_deadlock_report') AND e.predicate IS NOT NULL) THEN 1 ELSE 0 END AS has_event_predicate
FROM sys.database_event_sessions c
JOIN sys.dm_xe_database_sessions s ON s.name=c.name
JOIN sys.database_event_session_targets target ON target.event_session_id=c.event_session_id
JOIN sys.database_event_session_fields m ON m.event_session_id=c.event_session_id AND m.object_id=target.target_id AND m.name=N'max_memory'
JOIN sys.database_event_session_fields n ON n.event_session_id=c.event_session_id AND n.object_id=target.target_id AND n.name=N'max_events_limit'
WHERE c.name=@P1 AND target.name=N'ring_buffer' AND target.package=N'package0'
 AND (SELECT COUNT_BIG(*) FROM sys.database_event_session_targets x WHERE x.event_session_id=c.event_session_id)=1
OPTION(MAXDOP 1)"#;

const PLATFORM_EDITION: &str = "SELECT /* culprit */ CAST(SERVERPROPERTY('EngineEdition') AS int) AS edition";

/// Exact buffer-level checkpoint for `sys.fn_xe_file_target_read_file`. `file_offset` identifies a buffer that
/// can hold multiple events; `initial_offset` resumes after that entire buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct XelBufferWatermark {
    pub file_name: String,
    pub file_offset: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct XelEventBatch {
    pub observations: Vec<MssqlExtendedEventObservation>,
    pub completed_watermark: Option<XelBufferWatermark>,
    pub truncated: bool,
    pub partial_buffer: bool,
    pub has_row_gap: bool,
    pub complete_scope: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SystemHealthEventTarget {
    observed_at: String,
    configuration_fingerprint: String,
    pattern: String,
    has_error_reported: bool,
    has_attention: bool,
    has_deadlock_report: bool,
    has_event_predicate: bool,
}

impl SystemHealthEventTarget {
    pub(crate) async fn read<R: NativeRead>(io: &mut R) -> Result<Self> {
        let rows = io.query(SYSTEM_HEALTH_CONFIGURATION, vec![], 2).await.map_err(source_error)?;
        let [row] = rows.as_slice() else { return Err(configured_file_unavailable()) };
        let path_bytes = optional_integer(row, "file_path_bytes")?.ok_or_else(configured_file_unavailable)?;
        if path_bytes < 0 {
            return Err(invalid_data());
        }
        if path_bytes > MAX_CONFIGURED_PATH_BYTES {
            return Err(CollectionStop::ByteLimit.error());
        }
        if let Some(max_file_size_mb) = optional_integer(row, "max_file_size_mb")?
            && !(1..=MAX_FILE_SIZE_MB).contains(&max_file_size_mb)
        {
            return Err(CollectionStop::ByteLimit.error());
        }
        if let Some(max_rollover_files) = optional_integer(row, "max_rollover_files")?
            && !(0..=MAX_ROLLOVER_FILES).contains(&max_rollover_files)
        {
            return Err(CollectionStop::ByteLimit.error());
        }
        let observed_at = canonical_time(&text(row, "observed_at")?)?;
        let session_id = integer(row, "event_session_id")?;
        let target_id = integer(row, "target_id")?;
        if session_id < 0 || target_id < 0 || text(row, "target_package")? != "package0" {
            return Err(invalid_data());
        }
        let path_type = optional_text(row, "file_path_type")?.ok_or_else(configured_file_unavailable)?;
        if !matches!(path_type.as_str(), "char" | "varchar" | "nchar" | "nvarchar") {
            return Err(configured_file_unavailable());
        }
        let path = optional_text(row, "file_path")?.ok_or_else(configured_file_unavailable)?;
        let pattern = configured_pattern(&path)?;
        let has_error_reported = integer(row, "has_error_reported")? == 1;
        let has_attention = integer(row, "has_attention")? == 1;
        let has_deadlock_report = integer(row, "has_deadlock_report")? == 1;
        if !(has_error_reported || has_attention || has_deadlock_report) {
            return Err(configured_file_unavailable());
        }
        let has_event_predicate = integer(row, "has_event_predicate")? == 1;
        let configuration_fingerprint = digest(
            &serde_json::to_vec(&(
                "system-health-native-failures-v1",
                session_id,
                target_id,
                "package0",
                &path,
                optional_integer(row, "max_file_size_mb")?,
                optional_integer(row, "max_rollover_files")?,
                has_error_reported,
                has_attention,
                has_deadlock_report,
                has_event_predicate,
            ))
            .map_err(|_| invalid_data())?,
        );
        Ok(Self {
            observed_at,
            configuration_fingerprint,
            pattern,
            has_error_reported,
            has_attention,
            has_deadlock_report,
            has_event_predicate,
        })
    }

    pub(crate) fn observed_at(&self) -> &str {
        &self.observed_at
    }

    pub(crate) fn confirmed_by(&self, current: &Self) -> bool {
        self.configuration_fingerprint == current.configuration_fingerprint
            && ObservationBounds::new(&self.observed_at, &current.observed_at).is_some()
    }

    pub(crate) fn capture_epoch(&self, instance: &instance::Context) -> Result<String> {
        serde_json::to_vec(&(instance.capture_epoch()?, &self.configuration_fingerprint))
            .map(|bytes| digest(&bytes))
            .map_err(|_| invalid_data())
    }

    fn covers_unfiltered_event_set(&self) -> bool {
        self.has_error_reported && self.has_attention && self.has_deadlock_report && !self.has_event_predicate
    }

    pub(crate) async fn read_incremental<R: NativeRead>(
        &self,
        io: &mut R,
        watermark: Option<&XelBufferWatermark>,
        limit: usize,
    ) -> Result<XelEventBatch> {
        let limit = limit.min(MAX_EVENT_ROWS);
        if limit == 0 {
            return Err(CollectionStop::RowLimit.error());
        }
        if let Some(watermark) = watermark
            && (watermark.file_name.is_empty() || watermark.file_offset < 0)
        {
            return Err(invalid_data());
        }
        let mut parameters = vec![
            Parameter::Integer((limit + 1) as i64),
            Parameter::Integer(MAX_EVENT_XML_BYTES),
            Parameter::Text(self.pattern.clone()),
        ];
        if let Some(watermark) = watermark {
            parameters.push(Parameter::Text(watermark.file_name.clone()));
            parameters.push(Parameter::Integer(watermark.file_offset));
        }
        let rows = io
            .query(&system_health_events_sql(watermark.is_some()), parameters, limit + 1)
            .await
            .map_err(source_error)?;
        let truncated = rows.len() > limit;
        let mut raw_buffers = Vec::with_capacity(rows.len());
        for row in &rows {
            let file_name = text(row, "file_name")?;
            let file_offset = integer(row, "file_offset")?;
            if file_name.is_empty() || file_offset < 0 {
                return Err(invalid_data());
            }
            raw_buffers.push((file_name, file_offset));
        }
        let kept_len = rows.len().min(limit);
        let partial_buffer = truncated && kept_len > 0 && raw_buffers[kept_len - 1] == raw_buffers[kept_len];
        let completed_watermark = if partial_buffer {
            let incomplete = &raw_buffers[kept_len];
            raw_buffers[..kept_len]
                .iter()
                .rev()
                .find(|buffer| *buffer != incomplete)
                .map(|(file_name, file_offset)| XelBufferWatermark {
                    file_name: file_name.clone(),
                    file_offset: *file_offset,
                })
                .or_else(|| watermark.cloned())
        } else if let Some((file_name, file_offset)) = raw_buffers[..kept_len].last() {
            Some(XelBufferWatermark { file_name: file_name.clone(), file_offset: *file_offset })
        } else {
            watermark.cloned()
        };

        let mut observations = Vec::new();
        let mut has_row_gap = false;
        for row in &rows[..kept_len] {
            let event_bytes = optional_integer(row, "event_bytes")?.ok_or_else(invalid_data)?;
            if event_bytes < 0 {
                return Err(invalid_data());
            }
            if event_bytes > MAX_EVENT_XML_BYTES {
                has_row_gap = true;
                continue;
            }
            let Some(xml) = optional_text(row, "event_xml")? else {
                has_row_gap = true;
                continue;
            };
            let row_timestamp_utc = canonical_time(&text(row, "timestamp_utc")?)?;
            let position = u32::try_from(observations.len()).map_err(|_| invalid_data())?;
            match decode_single_event(&xml, Some(&row_timestamp_utc), position) {
                Some(observation) => observations.push(observation),
                None => has_row_gap = true,
            }
        }
        let complete_scope =
            !truncated && !partial_buffer && !has_row_gap && watermark.is_none() && self.covers_unfiltered_event_set();
        Ok(XelEventBatch { observations, completed_watermark, truncated, partial_buffer, has_row_gap, complete_scope })
    }
}

pub(crate) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    caller.intersection(&CollectionBudget {
        page_rows: MAX_EVENT_ROWS as u32,
        max_snapshot_rows: MAX_EVENT_ROWS as u32,
        max_queries: MAX_SOURCE_QUERIES,
        max_pages: 16,
        page_bytes: 1024 * 1024,
        total_bytes: 8 * 1024 * 1024,
        statement_timeout_ms: 1_000,
        total_timeout_ms: 5_000,
        ..Default::default()
    })
}

pub(crate) fn collection_budget(caller: &CollectionBudget) -> CollectionBudget {
    ReadSession::budget_including_setup(caller, bounded_budget(caller))
}

pub(crate) fn source(
    pool: Arc<Pool>,
    request: &CollectionRequest,
    host: String,
    port: u16,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    if request.selection != (CollectionSelection::Snapshot { dataset: SnapshotDataset::NativeFailures }) {
        return Err(invalid_data());
    }
    match &request.subject {
        CollectionSubject::Database => {}
        CollectionSubject::Shared { resource } if resource.kind == SharedResourceKind::Instance => {}
        _ => return Err(unsupported()),
    }
    let mut bounded = request.clone();
    bounded.budget = collection_budget(&request.budget);
    let reader = Reader { io: ReadSession::collection(pool, bounded.budget.clone())?, host, port, state: None };
    Ok(Box::new(FrozenSource::new(reader, &bounded)?))
}

enum VerifiedOwner {
    SystemHealth { instance: instance::Context, target: SystemHealthEventTarget },
    DatabaseSession { database: database_identity::Context, session_id: i64, generation: String },
}

struct Reader<R = ReadSession> {
    io: R,
    host: String,
    port: u16,
    state: Option<VerifiedOwner>,
}

#[async_trait::async_trait]
impl<R: NativeRead> ObservationReader for Reader<R> {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let limit = MAX_EVENT_ROWS.min(request.budget.max_snapshot_rows as usize);
        match &request.subject {
            CollectionSubject::Shared { .. } => {
                let (instance, at) = instance::read(&mut self.io).await?;
                if instance.major < MIN_NATIVE_MAJOR {
                    return Err(unsupported());
                }
                let target_resource = CollectionTarget::Shared(instance.identity.clone());
                if !request.subject.accepts(&target_resource) {
                    return Err(CollectionStop::SourceChanged.error());
                }
                let target = SystemHealthEventTarget::read(&mut self.io).await?;
                if ObservationBounds::new(&at, target.observed_at()).is_none() {
                    return Err(CollectionStop::SourceChanged.error());
                }
                let batch = target.read_incremental(&mut self.io, None, limit).await?;
                let epoch = target.capture_epoch(&instance)?;
                let description = CollectionDescription {
                    partition: SourcePartition::InstanceLocal {
                        member: Some(instance.identity.clone()),
                        epoch: Some(epoch),
                    },
                    target: target_resource,
                    source: MSSQL_SYSTEM_HEALTH_EVENTS_SOURCE.into(),
                    continuity: SourceContinuity::Unverified,
                    observation: SourceObservation::Snapshot(StateSnapshotDescription {
                        dataset: SnapshotDataset::NativeFailures,
                        observed_from: at,
                        observed_until: target.observed_at().to_owned(),
                        records: batch.observations.len() as u32,
                        sources: vec![StateSourceCoverage::observed(
                            sources::NATIVE_FAILURES.name.into(),
                            batch.observations.len() as u32,
                            batch.complete_scope,
                        )],
                    }),
                };
                if !description.matches_request(request) {
                    return Err(invalid_data());
                }
                let records = to_operational_records(batch.observations)?;
                self.state = Some(VerifiedOwner::SystemHealth { instance, target });
                Ok(FrozenObservation { description, records })
            }
            CollectionSubject::Database => {
                let edition = read_edition(&mut self.io).await?;
                if edition != ENGINE_EDITION_AZURE_SQL_DATABASE {
                    return Err(Error::new(ErrorCode::Unsupported, t!("collection.azure_sql_only")));
                }
                let database = database_identity::read_azure(&mut self.io).await?;
                let captured = read_database_session(&mut self.io, &database, &self.host, self.port, limit).await?;
                if !captured.description.matches_request(request) {
                    return Err(invalid_data());
                }
                self.state = Some(VerifiedOwner::DatabaseSession {
                    database,
                    session_id: captured.session_id,
                    generation: captured.generation,
                });
                Ok(FrozenObservation { description: captured.description, records: captured.records })
            }
            CollectionSubject::ConfiguredConnection { .. } => Err(invalid_data()),
        }
    }

    async fn verify(&mut self, _: &CollectionDescription) -> Result<bool> {
        match self.state.as_ref().ok_or_else(invalid_data)? {
            VerifiedOwner::SystemHealth { instance: original_instance, target: original_target } => {
                let current_target = SystemHealthEventTarget::read(&mut self.io).await?;
                let (current_instance, at) = instance::read(&mut self.io).await?;
                Ok(original_instance == &current_instance
                    && original_target.confirmed_by(&current_target)
                    && ObservationBounds::new(current_target.observed_at(), &at).is_some())
            }
            VerifiedOwner::DatabaseSession { database: original_db, session_id, generation } => {
                let current_db = database_identity::read_azure(&mut self.io).await?;
                let rows = self
                    .io
                    .query(DATABASE_CONFIGURATION, vec![Parameter::Text(DATABASE_SESSION.into())], 2)
                    .await
                    .map_err(source_error)?;
                let [row] = rows.as_slice() else { return Ok(false) };
                let current_generation = crate::sql_time::native_time(&text(row, "generation")?)?;
                Ok(original_db == &current_db
                    && integer(row, "event_session_id")? == *session_id
                    && &current_generation == generation)
            }
        }
    }

    fn query_count(&self) -> u32 {
        self.io.queries()
    }

    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.read_metrics()
    }

    async fn close(&mut self) -> SourceShutdown {
        self.io.close().await
    }
}

struct DatabaseCapture {
    description: CollectionDescription,
    records: Vec<TelemetryRecord>,
    session_id: i64,
    generation: String,
    observed_from: String,
    observed_until: String,
    observations: Vec<MssqlExtendedEventObservation>,
    complete_scope: bool,
}

async fn read_database_session<R: NativeRead>(
    io: &mut R,
    database: &database_identity::Context,
    host: &str,
    port: u16,
    limit: usize,
) -> Result<DatabaseCapture> {
    let limit = limit.min(MAX_EVENT_ROWS);
    if limit == 0 {
        return Err(CollectionStop::RowLimit.error());
    }
    let rows = io
        .query(DATABASE_CONFIGURATION, vec![Parameter::Text(DATABASE_SESSION.into())], 2)
        .await
        .map_err(source_error)?;
    let [config] = rows.as_slice() else { return Err(database_session_required()) };
    let observed_from = canonical_time(&text(config, "observed_at")?)?;
    let memory_kib = integer(config, "memory_kib")?;
    let maximum_events = integer(config, "maximum_events")?;
    if !(1..=MAX_DATABASE_MEMORY_KIB).contains(&memory_kib)
        || !(1..=MAX_DATABASE_EVENTS as i64).contains(&maximum_events)
    {
        return Err(database_session_required());
    }
    let has_error_reported = integer(config, "has_error_reported")? == 1;
    let has_attention = integer(config, "has_attention")? == 1;
    let has_deadlock_report = integer(config, "has_deadlock_report")? == 1;
    if !(has_error_reported || has_attention || has_deadlock_report) {
        return Err(database_session_required());
    }
    let has_event_predicate = integer(config, "has_event_predicate")? == 1;
    let session_id = integer(config, "event_session_id")?;
    let generation = crate::sql_time::native_time(&text(config, "generation")?)?;
    let generation_matches = crate::sql_time::native_time_matches("s.create_time", "@P3");
    let target_sql = format!(
        r#"SELECT TOP (2) /* culprit */
 CONVERT(varchar(40),SYSUTCDATETIME(),127)+'Z' AS observed_until,
 DATALENGTH(t.target_data) AS target_bytes,
 CASE WHEN DATALENGTH(t.target_data)<=@P4 THEN t.target_data END AS target_xml
FROM sys.dm_xe_database_sessions s
JOIN sys.dm_xe_database_session_targets t ON t.event_session_address=s.address
JOIN sys.database_event_sessions c ON c.name=s.name
WHERE s.name=@P1 AND c.event_session_id=@P2 AND {generation_matches}
 AND t.target_name=N'ring_buffer' OPTION(MAXDOP 1)"#
    );
    let target_rows = io
        .query(
            &target_sql,
            vec![
                Parameter::Text(DATABASE_SESSION.into()),
                Parameter::Integer(session_id),
                Parameter::Text(generation.clone()),
                Parameter::Integer(MAX_DATABASE_RING_BYTES),
            ],
            2,
        )
        .await
        .map_err(source_error)?;
    let [target_row] = target_rows.as_slice() else { return Err(CollectionStop::SourceChanged.error()) };
    let observed_until = canonical_time(&text(target_row, "observed_until")?)?;
    if ObservationBounds::new(&observed_from, &observed_until).is_none() {
        return Err(CollectionStop::SourceChanged.error());
    }
    if integer(target_row, "target_bytes")? > MAX_DATABASE_RING_BYTES {
        return Err(CollectionStop::ByteLimit.error());
    }
    let xml = text(target_row, "target_xml")?;
    if xml.len() > MAX_DATABASE_RING_BYTES as usize {
        return Err(CollectionStop::ByteLimit.error());
    }
    let parsed = culprit_plans::mssql::events::parse_xml(
        xml.as_bytes(),
        NativeLogLimits {
            input_bytes: MAX_DATABASE_RING_BYTES as usize,
            records: limit,
            record_bytes: MAX_EVENT_XML_BYTES as usize,
            field_bytes: 64 * 1024,
        },
    )
    .map_err(|_| invalid_data())?;
    let mut observations = parsed.records;
    for (position, row) in observations.iter_mut().enumerate() {
        row.position = u32::try_from(position).map_err(|_| invalid_data())?;
    }
    let complete_scope = parsed.stopped.is_none()
        && parsed.issues.is_empty()
        && has_error_reported
        && has_attention
        && has_deadlock_report
        && !has_event_predicate;
    let physical = database.physical_incarnation().ok_or_else(invalid_data)?;
    let epoch = digest(
        &serde_json::to_vec(&("mssql-database-events-epoch-v1", physical, DATABASE_SESSION, session_id, &generation))
            .map_err(|_| invalid_data())?,
    );
    let description = CollectionDescription {
        partition: SourcePartition::InstanceLocal { member: None, epoch: Some(epoch) },
        target: CollectionTarget::Database(database.identity(host, port)),
        source: MSSQL_DATABASE_EVENTS_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::Snapshot(StateSnapshotDescription {
            dataset: SnapshotDataset::NativeFailures,
            observed_from: observed_from.clone(),
            observed_until: observed_until.clone(),
            records: observations.len() as u32,
            sources: vec![StateSourceCoverage::observed(
                sources::NATIVE_FAILURES.name.into(),
                observations.len() as u32,
                complete_scope,
            )],
        }),
    };
    let records = to_operational_records(observations.clone())?;
    Ok(DatabaseCapture {
        description,
        records,
        session_id,
        generation,
        observed_from,
        observed_until,
        observations,
        complete_scope,
    })
}

pub(crate) async fn collect(pool: Arc<Pool>, host: &str, port: u16, snapshot: &mut Snapshot) {
    let budget = bounded_budget(&CollectionBudget::default());
    let mut io = ReadSession::new(pool, budget);
    let (captured, coverage) = collect_from(&mut io, host, port).await;
    snapshot.operations.native_failures = captured;
    coverage.append_to(snapshot);
}

async fn collect_from<R: NativeRead>(io: &mut R, host: &str, port: u16) -> (Option<NativeFailureSnapshot>, Coverage) {
    let mut coverage = Coverage::default();
    let outcome = collect_staged(io, host, port).await;
    let outcome = if io.close().await == SourceShutdown::Unconfirmed {
        Err(CollectionStop::CancellationUnconfirmed.error())
    } else {
        outcome
    };
    match outcome {
        Ok((snapshot, complete_scope)) => {
            if complete_scope {
                coverage.completed(&sources::NATIVE_FAILURES, snapshot.observations.len());
            } else {
                coverage.partial(
                    &sources::NATIVE_FAILURES,
                    culprit_core::message!("provider.mssql.native_failures.partial"),
                );
            }
            (Some(snapshot), coverage)
        }
        Err(error) => {
            coverage.source_failed(&sources::NATIVE_FAILURES, &error);
            if let Some(hint) = error.hint_source() {
                coverage.note(sources::NATIVE_FAILURES.name, hint);
            }
            (None, coverage)
        }
    }
}

async fn collect_staged<R: NativeRead>(io: &mut R, host: &str, port: u16) -> Result<(NativeFailureSnapshot, bool)> {
    let edition = read_edition(io).await?;
    if edition == ENGINE_EDITION_AZURE_SQL_DATABASE {
        let before = database_identity::read_azure(io).await?;
        let captured = read_database_session(io, &before, host, port, MAX_EVENT_ROWS).await?;
        let after = database_identity::read_azure(io).await?;
        if before != after {
            return Err(CollectionStop::SourceChanged.error());
        }
        let snapshot = NativeFailureSnapshot {
            source: sources::NATIVE_FAILURES.name.into(),
            observed_from: captured.observed_from,
            observed_until: captured.observed_until,
            resource: None,
            observations: captured
                .observations
                .into_iter()
                .map(|row| NativeFailureRecord::MssqlExtendedEvent(Box::new(row)))
                .collect(),
        };
        if !snapshot.is_valid() {
            return Err(invalid_data());
        }
        return Ok((snapshot, captured.complete_scope));
    }
    if !instance::SUPPORTED_EDITIONS.contains(&edition) {
        return Err(unsupported());
    }
    let (before_instance, observed_from) = instance::read(io).await?;
    if before_instance.major < MIN_NATIVE_MAJOR {
        return Err(unsupported());
    }
    let before_target = SystemHealthEventTarget::read(io).await?;
    let batch = before_target.read_incremental(io, None, MAX_EVENT_ROWS).await?;
    let after_target = SystemHealthEventTarget::read(io).await?;
    let (after_instance, observed_until) = instance::read(io).await?;
    if before_instance != after_instance
        || !before_target.confirmed_by(&after_target)
        || ObservationBounds::new(&observed_from, &observed_until).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    let snapshot = NativeFailureSnapshot {
        source: sources::NATIVE_FAILURES.name.into(),
        observed_from,
        observed_until,
        resource: Some(before_instance.identity),
        observations: batch
            .observations
            .into_iter()
            .map(|row| NativeFailureRecord::MssqlExtendedEvent(Box::new(row)))
            .collect(),
    };
    if !snapshot.is_valid() {
        return Err(invalid_data());
    }
    Ok((snapshot, batch.complete_scope))
}

fn to_operational_records(observations: Vec<MssqlExtendedEventObservation>) -> Result<Vec<TelemetryRecord>> {
    observations
        .into_iter()
        .map(|mut observation| {
            if let culprit_core::telemetry::DefinitionPayload::Available { text, .. } = &observation.statement {
                observation.statement = culprit_core::telemetry::DefinitionPayload::sql(text, Engine::Mssql);
            }
            Ok(TelemetryRecord::Operational(OperationalObservation {
                source: sources::NATIVE_FAILURES.name.into(),
                key: observation.key()?,
                fact: OperationalFact::MssqlExtendedEvent(Box::new(observation)),
            }))
        })
        .collect()
}

fn decode_single_event(
    xml: &str,
    fallback_timestamp_utc: Option<&str>,
    position: u32,
) -> Option<MssqlExtendedEventObservation> {
    let parsed = culprit_plans::mssql::events::parse_xml(
        xml.as_bytes(),
        NativeLogLimits {
            input_bytes: MAX_EVENT_XML_BYTES as usize,
            records: 1,
            record_bytes: MAX_EVENT_XML_BYTES as usize,
            field_bytes: 64 * 1024,
        },
    )
    .ok()?;
    if !parsed.issues.is_empty() || parsed.stopped.is_some() || parsed.records.len() != 1 {
        return None;
    }
    let mut row = parsed.records.into_iter().next()?;
    row.position = position;
    if row.timestamp.is_none()
        && let Some(at) = fallback_timestamp_utc
    {
        row.timestamp = Some(at.to_owned());
        row.timestamp_utc = MssqlExtendedEventObservation::qualified_timestamp(at);
    }
    row.is_valid().then_some(row)
}

async fn read_edition<R: NativeRead>(io: &mut R) -> Result<i64> {
    let rows = io.query(PLATFORM_EDITION, vec![], 1).await.map_err(ReadError::into_error)?;
    let [row] = rows.as_slice() else { return Err(invalid_data()) };
    integer(row, "edition")
}

fn configured_pattern(path: &str) -> Result<String> {
    let windows_drive = path.as_bytes().get(0..3).is_some_and(|prefix| {
        prefix[0].is_ascii_alphabetic() && prefix[1] == b':' && matches!(prefix[2], b'\\' | b'/')
    });
    let unix_absolute = path.starts_with('/');
    let unc_absolute = path.strip_prefix(r"\\").is_some_and(|tail| {
        let mut parts = tail.split('\\');
        parts.next().is_some_and(|server| !server.is_empty())
            && parts.next().is_some_and(|share| !share.is_empty())
            && parts.next().is_some_and(|file| !file.is_empty())
    });
    if !(windows_drive || unix_absolute || unc_absolute)
        || path.chars().any(char::is_control)
        || path.contains(['*', '?', '#'])
    {
        return Err(configured_file_unavailable());
    }
    let (stem, extension) = path.rsplit_once('.').ok_or_else(configured_file_unavailable)?;
    if !extension.eq_ignore_ascii_case("xel") || stem.ends_with(['/', '\\']) {
        return Err(configured_file_unavailable());
    }
    Ok(format!("{stem}_*.{extension}"))
}

fn source_error(error: ReadError) -> Error {
    let error = error.into_error();
    if error.code == ErrorCode::PermissionDenied && error.detail.is_none() && error.hint.is_none() {
        error.with_hint(culprit_core::message!("provider.mssql.native_failures.permissions"))
    } else {
        error
    }
}

fn configured_file_unavailable() -> Error {
    Error::new(ErrorCode::Unsupported, t!("provider.mssql.deadlocks.configured_file_unavailable"))
}

fn database_session_required() -> Error {
    Error::new(ErrorCode::Unsupported, t!("provider.mssql.native_failures.session_required"))
}

fn unsupported() -> Error {
    Error::new(ErrorCode::Unsupported, t!("collection.unsupported"))
}

#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod tests;
