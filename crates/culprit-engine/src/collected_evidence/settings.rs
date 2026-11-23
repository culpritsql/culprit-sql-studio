//! One native Settings original feeds the installed pure pipeline without query-history or current catalog claims.
use super::*;
use culprit_core::{MeasurementScope, Report, telemetry::*};
use std::collections::BTreeSet;

pub(crate) struct RetainedSettingsAnalysis {
    pub report: Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
}

pub(crate) fn analyze_settings_original(
    original: &dyn OriginalProjection,
    pipeline: &culprit_checks::Pipeline,
) -> Result<RetainedSettingsAnalysis> {
    let header = validate_original(original)?;
    let projection = project_settings(original)?;
    let mut limitations = vec![
        limit(None, CollectedAnalysisLimit::MissingCurrentServerMetadata),
        limit(None, CollectedAnalysisLimit::MissingSchema),
        limit(None, CollectedAnalysisLimit::MissingResourceMeasurements),
    ];
    if !projection.complete {
        limitations.push(limit(None, CollectedAnalysisLimit::MissingSettings));
    }
    let mut snapshot = settings_snapshot(original, projection.settings, &header.observed_until, pipeline)?;
    let metadata = original.source().observation.snapshot_settings_context().ok_or_else(invalid)?;
    if !metadata.apply_server_metadata(&mut snapshot.server) {
        return Err(invalid());
    }
    snapshot.operations.postgres_settings = projection.postgres;
    let empty = snapshot.settings.is_empty();
    let outcome =
        snapshot.sources.iter_mut().find(|outcome| outcome.source == source_id::SETTINGS).ok_or_else(invalid)?;
    outcome.state = if !projection.complete {
        SourceState::Partial
    } else if empty {
        SourceState::Empty
    } else {
        SourceState::Collected
    };
    outcome.reason = if projection.recovery_limited {
        Some(culprit_core::message!("engine.collected_analysis.recovery_settings").into())
    } else {
        (!projection.complete).then(|| culprit_core::message!("engine.collected_analysis.partial_settings").into())
    };
    let report =
        run_retained_snapshot(snapshot, pipeline, culprit_core::AnalysisOrigin::RetainedInput, &mut limitations);
    Ok(RetainedSettingsAnalysis { report, limitations })
}

fn validate_original(original: &dyn OriginalProjection) -> Result<&StateSnapshotDescription> {
    let source = original.source();
    let header = source.observation.snapshot().ok_or_else(invalid)?;
    let execution = source.observation.snapshot_acquisition_context().ok_or_else(invalid)?;
    if header.dataset != SnapshotDataset::Settings
        || !source.partition_is_consistent()
        || !header.is_valid()
        || !execution.has_qualified_local_owner()
        || execution.database_writable.is_none()
        || execution.target != source.target
        || canonical_time(&header.observed_from)? != canonical_time(&execution.observed_from)?
        || canonical_time(&header.observed_until)? != canonical_time(&execution.observed_until)?
        || !source
            .observation
            .snapshot_settings_context()
            .is_some_and(|context| context.is_valid_for(source.target.engine()))
        || !original.original().is_valid()
        || original.records().len() > header.records as usize
    {
        return Err(invalid());
    }
    Ok(header)
}
/// The proof reader still authenticates each row. This attaches those same native facts, not a new source.
pub(crate) fn settings_read(original: &dyn OriginalProjection) -> Result<PostgresSettingsRead> {
    validate_original(original)?;
    let source = original.source();
    let observations = observations(original)?;
    let read = PostgresSettingsRead {
        settings_context: source.observation.snapshot_settings_context().ok_or_else(invalid)?.clone(),
        observations,
        population_complete: catalogs::complete(original),
        acquisition: PostgresSettingsAcquisition::Completed {
            context: Box::new(source.observation.snapshot_acquisition_context().ok_or_else(invalid)?.clone()),
        },
    };
    if !read.is_valid() {
        return Err(invalid());
    }
    Ok(read)
}

struct SettingsProjection {
    settings: Vec<culprit_core::Setting>,
    postgres: Option<Box<PostgresSettingsRead>>,
    complete: bool,
    recovery_limited: bool,
}
fn project_settings(original: &dyn OriginalProjection) -> Result<SettingsProjection> {
    let context = original.source().observation.snapshot_settings_context().ok_or_else(invalid)?;
    match context.format {
        NativeSettingsFormat::PostgresSettings => {
            let read = settings_read(original)?;
            let advice = read.configuration(original.source().target.database());
            let complete = advice.is_complete();
            let recovery_limited =
                advice.limitations.contains(&PostgresSettingsAdviceLimit::RecoveryMaintenanceInactive);
            // Keep the original PG cohort. ConfigCheck alone applies its same-read recovery policy.
            let settings = read
                .observations
                .iter()
                .filter(|setting| setting.scope == MeasurementScope::Server)
                .filter_map(|setting| context.project(setting))
                .collect();
            Ok(SettingsProjection { settings, postgres: Some(Box::new(read)), complete, recovery_limited })
        }
        NativeSettingsFormat::MysqlGlobalVariables | NativeSettingsFormat::MariaDbGlobalVariables => {
            let observations = observations(original)?;
            let settings: Vec<_> = observations.iter().filter_map(|setting| context.project(setting)).collect();
            let complete =
                catalogs::complete(original) && !observations.is_empty() && settings.len() == observations.len();
            Ok(SettingsProjection { settings, postgres: None, complete, recovery_limited: false })
        }
    }
}
fn observations(original: &dyn OriginalProjection) -> Result<Vec<SettingObservation>> {
    let source = original.source();
    let mut keys = BTreeSet::new();
    let mut observations = Vec::new();
    for row in original.records() {
        let TelemetryRecord::Setting(native) = &row.row().record else { return Err(invalid()) };
        if !native.is_valid()
            || !keys.insert((&native.namespace, &native.name))
            || !source.observation.accepts_snapshot_record(TelemetryStream::SettingObservations, &native.source)
        {
            return Err(invalid());
        }
        observations.push(native.clone());
    }
    Ok(observations)
}
