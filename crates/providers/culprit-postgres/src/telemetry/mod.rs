//! Provider-owned recipes over the common capture contract and bounded native reader.
mod activity;
mod blocking;
#[cfg(test)]
mod native_tests;
#[cfg(test)]
mod profile_native_tests;
mod readable_secondary_recovery;
mod recipes;
mod settings_profile;
mod snapshot_io;
mod statement_profile;
mod transaction_metadata;
use crate::connect::Connector;
use crate::native;
use crate::native::metadata;
use crate::native::metadata::Metadata;
use crate::read::{NativeRead, ReadSession};
use crate::rows::Row;
use culprit_core::{Error, ErrorCode, Result, telemetry::*};
use culprit_read::{ReadBatch, SnapshotReader, SnapshotRecheck, SnapshotRecord, SnapshotScope, SnapshotSource};
pub(crate) use recipes::{declarations, open as source, profile_selectors, profile_source};
use snapshot_io::SnapshotIo;
#[derive(Clone, Copy)]
enum Recipe {
    Statements,
    Definitions,
    Operational,
    InstanceOperational,
    Settings,
    Tables,
    Indexes,
    Replication,
    Sequences,
    Cleanup,
    Catalog(native::catalog::Kind),
}
impl Recipe {
    fn dataset(self) -> SnapshotDataset {
        match self {
            Self::Statements => SnapshotDataset::StatementCounters,
            Self::Definitions => SnapshotDataset::StatementDefinitions,
            Self::Operational | Self::InstanceOperational => SnapshotDataset::Operational,
            Self::Settings => SnapshotDataset::Settings,
            Self::Tables => SnapshotDataset::Tables,
            Self::Indexes => SnapshotDataset::Indexes,
            Self::Replication => SnapshotDataset::Replication,
            Self::Sequences => SnapshotDataset::Sequences,
            Self::Cleanup => SnapshotDataset::Cleanup,
            Self::Catalog(kind) => kind.dataset(),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Statements => "postgres-statement-counters",
            Self::Definitions => "postgres-statement-definitions",
            Self::Operational => "postgres-operational-state",
            Self::InstanceOperational => "postgres-instance-state",
            Self::Settings => "postgres-settings",
            Self::Tables => "postgres-tables",
            Self::Indexes => "postgres-indexes",
            Self::Replication => "postgres-replication",
            Self::Sequences => "postgres-sequences",
            Self::Cleanup => "postgres-cleanup",
            Self::Catalog(kind) => kind.object_type(),
        }
    }
    fn sources(self, meta: &Metadata) -> Vec<String> {
        match self {
            Self::Statements | Self::Definitions => vec!["pg_stat_statements".into()],
            Self::Operational => native::operational::selected_sources(meta, native::operational::Selection::Database)
                .into_iter()
                .map(str::to_owned)
                .collect(),
            Self::InstanceOperational => {
                native::operational::selected_sources(meta, native::operational::Selection::Instance)
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            }
            Self::Settings => vec!["pg_settings".into()],
            Self::Tables => vec!["pg_stat_user_tables".into()],
            Self::Indexes => vec!["pg_index".into()],
            Self::Replication => native::replication::sources(meta.standby),
            Self::Sequences => vec!["pg_sequences".into()],
            Self::Cleanup => native::cleanup::sources(),
            Self::Catalog(kind) => vec![kind.source().into()],
        }
    }
    fn query(
        self,
        meta: &Metadata,
        budget: &CollectionBudget,
        memory_access: native::memory::Access,
    ) -> Result<String> {
        let limit = budget.max_snapshot_rows as usize + 1;
        match self {
            Self::Statements | Self::Definitions => {
                native::statements::query(meta, matches!(self, Self::Definitions), limit, budget.max_sql_bytes as usize)
            }
            Self::Operational => Ok(native::operational::selected_query(
                meta,
                limit,
                native::operational::Selection::Database,
                memory_access,
            )),
            Self::InstanceOperational => Ok(native::operational::selected_query(
                meta,
                limit,
                native::operational::Selection::Instance,
                memory_access,
            )),
            Self::Settings => Ok(native::settings::capture_query(limit)),
            Self::Catalog(kind) => Ok(native::catalog::query(kind, meta.version, false, limit)),
            Self::Sequences => Ok(native::sequences::query(Some(limit))),
            Self::Cleanup => native::cleanup::capture_query(meta.version, meta.standby, limit),
            Self::Replication => native::replication::query(
                meta.version,
                meta.standby,
                &meta.database_oid,
                meta.replay_pause_access,
                limit,
            ),
            Self::Indexes => Ok(native::indexes::query(meta.version, false, false, Some(limit))),
            Self::Tables => Ok(native::relations::table_query(
                meta.version,
                native::relations::SizeRead::CatalogEstimate,
                Some(limit),
            )),
        }
    }
    fn decode(
        self,
        row: &Row,
        meta: &Metadata,
        source: &CollectionDescription,
        budget: &CollectionBudget,
    ) -> Result<(String, TelemetryRecord)> {
        match self {
            Self::Statements => {
                let sample = native::statements::counters(row, meta)?;
                let record = OperationalObservation {
                    source: "pg_stat_statements".into(),
                    key: sample.key()?,
                    fact: OperationalFact::StatementCounters(Box::new(sample)),
                };
                Ok((record.source.clone(), TelemetryRecord::Operational(record)))
            }
            Self::Definitions => {
                let definition = native::statements::definition(row, meta, source, budget.max_sql_bytes as usize)?;
                let record = ObjectDefinitionRecord::new(
                    "pg_stat_statements".into(),
                    ObjectDefinitionFact::Statement(Box::new(definition)),
                )?;
                Ok((record.source.clone(), TelemetryRecord::ObjectDefinition(record)))
            }
            Self::Catalog(kind) => {
                let definition = native::catalog::decode(
                    kind,
                    row.clone(),
                    Some(&meta.database_oid),
                    budget.max_sql_bytes as usize,
                    culprit_core::Engine::Postgres,
                )?
                .definition;
                let source = kind.source().to_owned();
                let record = if matches!(kind, native::catalog::Kind::Statistics) {
                    let counters = NativeCounterSet {
                        ratios: Vec::new(),
                        ratio_gaps: Vec::new(),
                        identity: definition.identity.clone(),
                        scope: culprit_core::MeasurementScope::Database,
                        counters: vec![],
                        properties: std::collections::BTreeMap::from([(
                            "object_type".into(),
                            kind.object_type().into(),
                        )]),
                    };
                    let key = definition.key()?;
                    TelemetryRecord::Operational(OperationalObservation {
                        source: source.clone(),
                        key,
                        fact: OperationalFact::CatalogCounters(Box::new(NativeCatalogCounters {
                            catalog: definition,
                            counters,
                        })),
                    })
                } else {
                    TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord::new(
                        source.clone(),
                        ObjectDefinitionFact::NativeCatalog(Box::new(definition)),
                    )?)
                };
                Ok((source, record))
            }
            Self::Cleanup => {
                if native::text(row, "native_source")? == RetentionCoverage::SOURCE {
                    let coverage = native::cleanup::decode_coverage(row, meta.standby)?;
                    let source = RetentionCoverage::SOURCE.to_owned();
                    return Ok((
                        source.clone(),
                        TelemetryRecord::Operational(OperationalObservation {
                            source,
                            key: coverage.key()?,
                            fact: OperationalFact::RetentionCoverage(coverage),
                        }),
                    ));
                }
                let holder = native::cleanup::decode(row)?;
                let source = holder.source.clone();
                Ok((
                    source.clone(),
                    TelemetryRecord::Operational(OperationalObservation {
                        source,
                        key: holder.key()?,
                        fact: OperationalFact::Retention(Box::new(holder)),
                    }),
                ))
            }
            Self::Sequences => {
                let sequence = native::sequences::decode(row.clone())?;
                let source = "pg_sequences".to_owned();
                Ok((
                    source.clone(),
                    TelemetryRecord::Operational(OperationalObservation {
                        source,
                        key: sequence.catalog.key()?,
                        fact: OperationalFact::CatalogCounters(Box::new(sequence)),
                    }),
                ))
            }
            Self::Replication => {
                let row = native::replication::decode(row)?;
                Ok((row.source.clone(), TelemetryRecord::Replication(row)))
            }
            Self::Indexes => {
                let index = native::indexes::decode(row.clone(), Some(&meta.database_oid))?.native;
                let record = OperationalObservation {
                    source: "pg_index".into(),
                    key: index.catalog.key()?,
                    fact: OperationalFact::CatalogCounters(Box::new(index)),
                };
                Ok((record.source.clone(), TelemetryRecord::Operational(record)))
            }
            Self::Tables => {
                let set = native::relations::table(row, &meta.database_oid)?;
                let record = OperationalObservation {
                    source: "pg_stat_user_tables".into(),
                    key: set.key()?,
                    fact: OperationalFact::CounterSet(Box::new(set)),
                };
                Ok((record.source.clone(), TelemetryRecord::Operational(record)))
            }
            Self::Settings => Ok(("pg_settings".into(), TelemetryRecord::Setting(native::settings::decode(row)?))),
            Self::Operational | Self::InstanceOperational => {
                let selection = if matches!(self, Self::InstanceOperational) {
                    native::operational::Selection::Instance
                } else {
                    native::operational::Selection::Database
                };
                let record = native::operational::decode(row, meta, selection)?;
                let TelemetryRecord::Operational(observation) = &record else { unreachable!() };
                Ok((observation.source.clone(), record))
            }
        }
    }
}
pub(crate) async fn resources(
    connector: Connector,
    request: &ResourceDiscoveryRequest,
) -> Result<Vec<CollectionResource>> {
    request.budget.validate()?;
    if request.kind != SharedResourceKind::Instance {
        return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
    }
    let host = connector.host.clone();
    let port = connector.port;
    let mut reader = ReadSession::new(connector, request.budget.clone());
    let result = async {
        let metadata = metadata::read(&mut reader, &host, port).await?;
        let instance = native::instance::read(&mut reader, metadata.version).await?;
        let collections = recipes::collections_for(&instance.identity);
        let resources =
            vec![CollectionResource { identity: instance.identity, observed_at: instance.observed_at, collections }];
        request.validate_result(&resources)?;
        Ok(resources)
    }
    .await;
    if reader.close().await == SourceShutdown::Unconfirmed {
        return Err(CollectionStop::CancellationUnconfirmed.error());
    }
    result
}

fn snapshot_source(
    connector: Connector,
    request: &CollectionRequest,
    recipe: Recipe,
) -> Result<Box<dyn TelemetrySource>> {
    snapshot_source_with_profile(connector, request, recipe, None)
}
fn snapshot_source_with_profile(
    connector: Connector,
    request: &CollectionRequest,
    recipe: Recipe,
    profile: Option<culprit_read::SourceProfileGuard>,
) -> Result<Box<dyn TelemetrySource>> {
    let io = ReadSession::new(connector.clone(), request.budget.clone());
    let io = match &profile {
        Some(profile) => io.with_profile(profile.clone()),
        None => io,
    };
    let reader = PostgresSnapshotReader {
        host: connector.host.clone(),
        port: connector.port,
        io,
        budget: request.budget.clone(),
        recipe,
        metadata: None,
        instance: None,
        memory_access: native::memory::Access::Unsupported,
        acquisition: None,
        generation: None,
        profile,
    };
    Ok(Box::new(SnapshotSource::new(reader, request)?))
}
struct PostgresSnapshotReader<R = ReadSession> {
    io: R,
    budget: CollectionBudget,
    recipe: Recipe,
    host: String,
    port: u16,
    metadata: Option<Metadata>,
    instance: Option<SharedResourceIdentity>,
    memory_access: native::memory::Access,
    acquisition: Option<native::local_context::Context>,
    generation: Option<u64>,
    profile: Option<culprit_read::SourceProfileGuard>,
}
impl<R: SnapshotIo> PostgresSnapshotReader<R> {
    fn verify_profile_native(&self, profile: &culprit_read::SourceProfileGuard, metadata: &Metadata) -> Result<()> {
        if metadata.version / 10_000 != 18 {
            return Err(Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported")));
        }
        match self.recipe {
            Recipe::Settings => (),
            Recipe::Statements | Recipe::Definitions => {
                native::statements::validate_guarded_counters(metadata, statement_profile::MAX_NATIVE_ENTRIES)?
            }
            _ => return Err(native::invalid()),
        }
        if let Some(policy) = profile.builtin_policy() {
            crate::builtin_safety::verify_native(policy, metadata.version)?;
        }
        Ok(())
    }
    fn check_profile(&self) -> Result<()> {
        let current = self.io.connection_generation();
        if let Some(profile) = &self.profile {
            profile.check_connection(current)?;
        }
        if self.generation.is_some_and(|original| current != Some(original)) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl<R: SnapshotIo> SnapshotReader for PostgresSnapshotReader<R> {
    type Row = Row;
    fn check_lifetime(&self) -> Result<()> {
        self.check_profile()
    }
    async fn describe(&mut self, request: &CollectionRequest) -> Result<CollectionDescription> {
        self.check_profile()?;
        let opening = self.io.connection_generation();
        let meta = metadata::read(&mut self.io, &self.host, self.port).await?;
        let acquisition = if let Some(profile) = &self.profile {
            self.verify_profile_native(profile, &meta)?;
            let generation = self
                .io
                .connection_generation()
                .filter(|generation| *generation > 0)
                .ok_or_else(|| CollectionStop::SourceChanged.error())?;
            if opening.is_some_and(|original| original != generation) {
                return Err(CollectionStop::SourceChanged.error());
            }
            self.generation = Some(generation);
            let owner = native::local_context::Context::observe(&mut self.io, &meta)
                .await?
                .ok_or_else(|| CollectionStop::SourceChanged.error())?;
            self.check_profile()?;
            profile.bind_native(&owner.read_context()?, self.io.connection_generation())?;
            Some(owner)
        } else if matches!(self.recipe, Recipe::Statements | Recipe::Definitions)
            && request.budget.max_queries >= 6
            && native::statements::validate_guarded_counters(&meta, statement_profile::MAX_NATIVE_ENTRIES).is_ok()
        {
            let generation = self.io.connection_generation().filter(|generation| *generation > 0);
            if let (Some(original), Some(current)) = (opening, generation)
                && original != current
            {
                return Err(CollectionStop::SourceChanged.error());
            }
            let observed = native::local_context::Context::observe(&mut self.io, &meta).await?;
            if observed.is_some() {
                self.generation = generation;
                self.check_profile()?;
            }
            observed
        } else {
            None
        };
        let instance = if matches!(self.recipe, Recipe::InstanceOperational) {
            Some(native::instance::read(&mut self.io, meta.version).await?.identity)
        } else {
            None
        };
        let target = instance.clone().map(CollectionTarget::Shared).unwrap_or_else(|| meta.database.clone().into());
        if !request.subject.accepts(&target) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let memory_access = if matches!(self.recipe, Recipe::InstanceOperational) {
            native::memory::access(&mut self.io, meta.version).await?
        } else {
            native::memory::Access::Unsupported
        };
        // Validate native support and permissions before admitting the statistics statement.
        self.recipe.query(&meta, &request.budget, memory_access)?;
        if matches!(self.recipe, Recipe::Replication | Recipe::Cleanup) && !meta.full_visibility {
            return Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
        }
        if matches!(self.recipe, Recipe::Catalog(native::catalog::Kind::Statistics))
            && !native::catalog::statistics_visible(&mut self.io).await?
        {
            return Err(Error::new(ErrorCode::PermissionDenied, culprit_core::t!("collection.invalid_source")));
        }
        let execution = acquisition.as_ref().map(native::local_context::Context::read_context).transpose()?;
        let description = CollectionDescription {
            partition: if matches!(self.recipe, Recipe::Statements | Recipe::Definitions) {
                execution.as_ref().map_or(SourcePartition::Target, |execution| execution.partition.clone())
            } else if let Some(owner) = &instance {
                SourcePartition::InstanceLocal { member: Some(owner.clone()), epoch: Some(meta.activity_epoch()) }
            } else {
                SourcePartition::Target
            },
            target,
            source: self.recipe.name().into(),
            continuity: SourceContinuity::Unverified,
            observation: {
                let snapshot = PagedSnapshotDescription {
                    dataset: self.recipe.dataset(),
                    observed_from: meta.at.clone(),
                    sources: self.recipe.sources(&meta),
                };
                match execution {
                    Some(execution) => SourceObservation::PagedSnapshotAcquisition {
                        snapshot,
                        execution: Box::new(execution),
                        settings_context: matches!(self.recipe, Recipe::Settings).then(|| NativeSettingsContext {
                            format: NativeSettingsFormat::PostgresSettings,
                            version: meta.version.to_string(),
                        }),
                    },
                    None => SourceObservation::PagedSnapshot(snapshot),
                }
            },
        };
        if !description.matches_request(request) {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.instance = instance;
        self.memory_access = memory_access;
        self.metadata = Some(meta);
        self.acquisition = acquisition;
        Ok(description)
    }
    async fn begin(&mut self, request: &CollectionRequest) -> Result<()> {
        self.check_profile()?;
        let meta = self.metadata.as_ref().ok_or_else(native::invalid)?;
        let query = self.recipe.query(meta, &request.budget, self.memory_access)?;
        self.io.begin_stream(&query, request.budget.max_snapshot_rows as usize + 1).await
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Row>> {
        self.check_profile()?;
        let batch = self.io.next_batch().await?;
        self.check_profile()?;
        Ok(batch)
    }
    fn decode(&self, row: &Row, description: &CollectionDescription) -> Result<SnapshotRecord> {
        let meta = self.metadata.as_ref().ok_or_else(native::invalid)?;
        let (source, record) = self.recipe.decode(row, meta, description, &self.budget)?;
        if self.acquisition.is_some() {
            match self.recipe {
                Recipe::Statements => {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::StatementCounters(sample),
                        ..
                    }) = &record
                    else {
                        return Err(native::invalid());
                    };
                    native::statements::validate_guarded_sample(sample, meta)?;
                }
                Recipe::Definitions => {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::Statement(definition),
                        ..
                    }) = &record
                    else {
                        return Err(native::invalid());
                    };
                    let observed_at = native::text(row, "observed_at")?;
                    native::statements::validate_guarded_definition(definition, &observed_at, meta, description)?;
                }
                _ => (),
            }
        }
        let complete_scope =
            !matches!(self.recipe, Recipe::Cleanup) || native::cleanup::scope_complete(&source, meta.standby);
        Ok(SnapshotRecord { source, record, complete_scope })
    }
    async fn recheck(&mut self, _: &CollectionDescription) -> Result<SnapshotRecheck> {
        self.check_profile()?;
        let after = metadata::read(&mut self.io, &self.host, self.port).await?;
        let before = self.metadata.as_ref().ok_or_else(native::invalid)?;
        let same_metadata = if matches!(self.recipe, Recipe::Replication) {
            before.same_replication_source(&after)
        } else {
            before.same_source(&after)
        };
        if self.acquisition.is_some() && !same_metadata {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.check_profile()?;
        let same_instance = match &self.instance {
            Some(before) => native::instance::read(&mut self.io, after.version).await?.identity == *before,
            None => true,
        };
        let complete_scope = if matches!(self.recipe, Recipe::Catalog(native::catalog::Kind::Statistics)) {
            native::catalog::statistics_visible(&mut self.io).await?
        } else {
            true
        };
        let memory_after = if matches!(self.recipe, Recipe::InstanceOperational) {
            native::memory::access(&mut self.io, after.version).await?
        } else {
            native::memory::Access::Unsupported
        };
        let execution = if let Some(owner) = &self.acquisition {
            let context = native::local_context::complete_observed(&mut self.io, before, Some(owner), &after)
                .await?
                .ok_or_else(|| CollectionStop::SourceChanged.error())?
                .context;
            if let Some(profile) = &self.profile {
                profile.verify_final(&context, self.io.connection_generation())?;
            }
            Some(Box::new(context))
        } else {
            None
        };
        let observed_until = execution.as_ref().map_or(after.at.clone(), |context| context.observed_until.clone());
        Ok(SnapshotRecheck {
            settings_context: self.acquisition.as_ref().filter(|_| matches!(self.recipe, Recipe::Settings)).map(|_| {
                NativeSettingsContext {
                    format: NativeSettingsFormat::PostgresSettings,
                    version: after.version.to_string(),
                }
            }),
            execution,
            unchanged: same_metadata && same_instance,
            observed_until,
            scopes: self
                .recipe
                .sources(before)
                .into_iter()
                .map(|source| {
                    if source == native::memory::SOURCE_NAME {
                        return match self.memory_access {
                            native::memory::Access::Available => SnapshotScope::Available {
                                source,
                                complete: memory_after == native::memory::Access::Available,
                                error_code: (memory_after != native::memory::Access::Available)
                                    .then_some(ErrorCode::PermissionDenied),
                            },
                            native::memory::Access::Restricted => {
                                SnapshotScope::Unavailable { source, error_code: ErrorCode::PermissionDenied }
                            }
                            native::memory::Access::Unsupported => SnapshotScope::NotApplicable { source },
                        };
                    }
                    let recovery_scope = !matches!(self.recipe, Recipe::Cleanup)
                        || native::cleanup::scope_complete(&source, before.standby);
                    SnapshotScope::Available {
                        source,
                        complete: complete_scope && recovery_scope,
                        error_code: if !recovery_scope {
                            Some(ErrorCode::Unsupported)
                        } else {
                            (!complete_scope).then_some(ErrorCode::PermissionDenied)
                        },
                    }
                })
                .collect(),
        })
    }
    fn query_count(&self) -> u32 {
        self.io.queries()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.io.metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.io.close().await
    }
}

pub(crate) fn safety_source(
    connector: Connector,
    request: &CollectionRequest,
    profile: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    match &request.selection {
        CollectionSelection::Activity => activity::profile_source(connector, request, profile),
        CollectionSelection::FocusedBlocking { .. } => blocking::profile_source(connector, request, profile),
        CollectionSelection::SourceRead { focus, .. }
            if focus.question == SourceReadQuestion::ReadableSecondaryRecovery =>
        {
            readable_secondary_recovery::profile_source(connector, request, profile)
        }
        CollectionSelection::SourceRead { focus, .. } if focus.question == SourceReadQuestion::TransactionMetadata => {
            transaction_metadata::profile_source(connector, request, profile)
        }
        CollectionSelection::MaintenanceProgress { .. } | CollectionSelection::CurrentRequestWork { .. } => {
            crate::progress::profile_source(connector, request, profile)
        }
        _ => Err(CollectionStop::SourceChanged.error()),
    }
}
