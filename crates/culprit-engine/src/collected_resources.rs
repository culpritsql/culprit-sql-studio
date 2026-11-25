//! Authenticated resource observations need neither a live provider nor invented query ownership.
use culprit_core::{
    Engine, Result, SourceOutcome, SourceState, collected_resource_analysis::CollectedRetainedFailureAnalysis,
    operations::MemoryFailureSnapshot, telemetry::*,
};
use culprit_evidence::analytical::evidence::{EvidenceCapture, EvidenceFamily};
mod incident_history;
pub use incident_history::incident_history;
mod source_overhead;
pub use source_overhead::source_overhead;
mod state;
use state::CapturedState;
pub(crate) use state::read_outcome;
mod activity;
mod blocking;
pub(crate) mod references;
pub use blocking::blocking;
mod cached_plans;
pub use cached_plans::cached_plans;
pub(crate) use cached_plans::explain_cached_plan_original;
mod cache_entry_memory;
pub use cache_entry_memory::cache_entry_memory;
mod backup_history;
pub use backup_history::backup_history;
mod cdc;
mod change_feed;
pub use change_feed::change_feed;
mod columnstore_rowgroups;
mod full_text_populations;
pub use full_text_populations::full_text_populations;
mod table_memory;
pub use columnstore_rowgroups::columnstore_rowgroups;
pub use table_memory::table_memory;
pub(crate) mod cleanup;
pub use cdc::cdc;
pub use cleanup::cleanup;
mod tuning;
pub use tuning::tuning;
mod compilation;
#[cfg(feature = "collector")]
mod completed_compilation;
#[cfg(feature = "collector")]
pub use completed_compilation::{CompletedCompilationAnalysis, completed_compilation};
mod completed_spills;
pub use compilation::compilation;
pub use completed_spills::completed_spills;
mod compiler_admission;
mod instance_resources;
pub use instance_resources::{instance_resources, process_cpu_interval};
pub(crate) mod table_full_text;
pub use table_full_text::table_full_text;
pub(crate) mod table_temporal;
pub use table_temporal::table_temporal;
mod database_properties;
mod suspect_pages;
pub use suspect_pages::suspect_pages;
mod memory_clerks;
mod tempdb_metadata_memory;
pub use database_properties::database_properties;
pub use memory_clerks::memory_clerks;
pub use tempdb_metadata_memory::tempdb_metadata_memory;
mod counter_ratios;
mod performance_counters;
pub use performance_counters::performance_counters;
mod wait_counters;
pub use compiler_admission::compiler_admission;
pub use counter_ratios::counter_ratios;
pub use wait_counters::wait_counters;
mod maintenance_progress;
pub use maintenance_progress::maintenance_progress;
mod maintenance_blocking;
pub use maintenance_blocking::maintenance_blocking;
mod current_request_work;
pub use current_request_work::current_request_work;
pub(crate) mod jobs;
pub use jobs::jobs;
mod replication;
#[cfg(feature = "collector")]
pub(crate) use replication::explain_read as explain_replication_read;
pub use replication::replication;
mod configured_log_rates;
mod connection_outcomes;
mod hyperscale_log_components;
pub use configured_log_rates::configured_log_rates;
pub use connection_outcomes::connection_outcomes;
pub use hyperscale_log_components::hyperscale_log_components;
mod maintenance;
pub use maintenance::maintenance;
mod request_memory;
pub use request_memory::request_memory;
mod session_memory;
pub use session_memory::session_memory;
mod page_waits;
pub use page_waits::page_waits;
pub(crate) mod page_server_reads;
pub use page_server_reads::page_server_reads;
pub(crate) mod query_wait_fragments;
pub use query_wait_fragments::query_wait_fragments;
mod query_history_fragments;
mod query_runtime_fragments;
pub use query_runtime_fragments::query_runtime_fragments;
mod page_wait_writers;
pub use page_wait_writers::page_wait_writers;
mod page_task_holders;
pub use page_task_holders::page_task_holders;
mod statement_work;
pub use statement_work::statement_work;
mod statement_grants;
pub use statement_grants::statement_grants;
mod task_space;
pub use task_space::task_space;
mod runtime_distributions;
pub use runtime_distributions::runtime_distributions;

fn memory_failures_target(
    source: &CollectionDescription,
) -> Result<(&StateSnapshotDescription, &SharedResourceIdentity)> {
    let native = source.observation.snapshot().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    if native.dataset != SnapshotDataset::MemoryFailures
        || native.dataset.stream() != TelemetryStream::OperationalSamples
    {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    let CollectionTarget::Shared(resource) = &source.target else {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    };
    if !matches!(resource.kind, SharedResourceKind::Instance | SharedResourceKind::Pool)
        || resource.engine != Engine::Mssql
    {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    Ok((native, resource))
}

/// Positions identify captured rows, never incidents across observations. Native offset-free event time remains
/// distinct from UTC capture and delivery clocks.
pub fn memory_failures(capture: &EvidenceCapture) -> Result<CollectedRetainedFailureAnalysis> {
    if !capture.families().contains(&EvidenceFamily::OperationalSamples) {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    memory_failures_target(capture.source())?;
    let original = capture.verified_original()?;
    let report = memory_failures_from_original(&original)?;
    Ok(CollectedRetainedFailureAnalysis { context: crate::collected_context::from_capture(capture), report })
}

pub(crate) fn memory_failures_from_original(
    original: &dyn culprit_evidence::original_projection::OriginalProjection,
) -> Result<culprit_core::RetainedFailureReport> {
    let source = original.source();
    let (native, resource) = memory_failures_target(source)?;
    if !original.selected_streams().any(|stream| stream == TelemetryStream::OperationalSamples) {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    if !source.target.is_valid() || !native.is_valid() {
        return Err(invalid());
    }
    let [coverage] = native.sources.as_slice() else {
        return Err(invalid());
    };
    let mut observations = Vec::new();
    for record in original.records() {
        let TelemetryRecord::Operational(observation) = &record.row().record else {
            return Err(invalid());
        };
        if !observation.is_valid() || !native.sources.iter().any(|entry| entry.source == observation.source) {
            return Err(invalid());
        }
        if observation.source != coverage.source {
            continue;
        }
        let OperationalFact::MssqlOutOfMemory(failure) = &observation.fact else {
            return Err(invalid());
        };
        observations.push(failure.as_ref().clone());
    }
    observations.sort_by_key(|observation| observation.position);
    let source_outcome = read_outcome(coverage, observations.len(), original.is_read_complete())?;
    let mut report = culprit_checks::memory_failures::explain(
        &MemoryFailureSnapshot {
            source: coverage.source.clone(),
            observed_from: native.observed_from.clone(),
            observed_until: native.observed_until.clone(),
            scope: source.target.measurement_scope(),
            resource: Some(resource.clone()),
            observations,
        },
        Some(&source_outcome),
    )?;
    report.qualify_incident_input(source);
    Ok(report)
}

fn native_failures_target(
    source: &CollectionDescription,
) -> Result<(&StateSnapshotDescription, Option<SharedResourceIdentity>)> {
    let native = source.observation.snapshot().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    if native.dataset != SnapshotDataset::NativeFailures
        || native.dataset.stream() != TelemetryStream::OperationalSamples
    {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    let resource = match &source.target {
        CollectionTarget::Shared(resource) if resource.kind == SharedResourceKind::Instance => Some(resource.clone()),
        CollectionTarget::Database(database)
            if database.engine == Engine::Mssql && source.source == MSSQL_DATABASE_EVENTS_SOURCE =>
        {
            None
        }
        _ => return Err(DeliveryProblem::UnsupportedSchema.error()),
    };
    Ok((native, resource))
}

/// Only exact declared operational rows from this authenticated capture are admitted. Current sessions and
/// neighboring captures are never consulted to fill missing query, resource-owner or native-clock information.
pub fn native_failures(capture: &EvidenceCapture) -> Result<CollectedRetainedFailureAnalysis> {
    if !capture.families().contains(&EvidenceFamily::OperationalSamples) {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    native_failures_target(capture.source())?;
    let original = capture.verified_original()?;
    let report = native_failures_from_original(&original)?;
    Ok(CollectedRetainedFailureAnalysis { context: crate::collected_context::from_capture(capture), report })
}

pub(crate) fn native_failures_from_original(
    original: &dyn culprit_evidence::original_projection::OriginalProjection,
) -> Result<culprit_core::RetainedFailureReport> {
    let source = original.source();
    let (native, resource) = native_failures_target(source)?;
    if !original.selected_streams().any(|stream| stream == TelemetryStream::OperationalSamples) {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    if !source.target.is_valid() || !native.is_valid() {
        return Err(invalid());
    }
    let [coverage] = native.sources.as_slice() else {
        return Err(invalid());
    };
    let mut observations = Vec::new();
    for record in original.records() {
        let TelemetryRecord::Operational(observation) = &record.row().record else {
            return Err(invalid());
        };
        if !observation.is_valid() || !native.sources.iter().any(|entry| entry.source == observation.source) {
            return Err(invalid());
        }
        if observation.source != coverage.source {
            continue;
        }
        let failure = match &observation.fact {
            OperationalFact::MysqlErrorLog(row) => NativeFailureRecord::MysqlErrorLog(row.as_ref().clone()),
            OperationalFact::PostgresLog(row) => NativeFailureRecord::PostgresLog(row.clone()),
            OperationalFact::MssqlExtendedEvent(row) => NativeFailureRecord::MssqlExtendedEvent(row.clone()),
            _ => return Err(invalid()),
        };
        observations.push(failure);
    }
    observations.sort_by_key(NativeFailureRecord::position);
    let source_outcome = read_outcome(coverage, observations.len(), original.is_read_complete())?;
    let mut report = culprit_checks::native_failures::explain(
        &NativeFailureSnapshot {
            source: coverage.source.clone(),
            observed_from: native.observed_from.clone(),
            observed_until: native.observed_until.clone(),
            resource,
            observations,
        },
        Some(&source_outcome),
    )?;
    report.qualify_incident_input(source);
    Ok(report)
}

fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

mod query_store_definitions;

mod session_census;
mod storage;
pub use session_census::session_census;
pub use storage::storage;

mod group_replication;
pub use group_replication::group_replication;

mod galera;
pub use galera::galera;

mod pool_settings;
pub use pool_settings::pool_settings;

mod managed_instance_resources;
pub use managed_instance_resources::managed_instance_resources;

mod index_configuration;
pub use index_configuration::index_configuration;

mod indexed_views;
pub use indexed_views::indexed_views;
