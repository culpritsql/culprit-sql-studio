//! Cleanup explanations use one authenticated capture and the same pure view as native scans.
use super::*;
use culprit_core::{CleanupReport, collected_resource_analysis::CollectedCleanupAnalysis, source_id};
use culprit_evidence::original_projection::OriginalProjection;

pub fn cleanup(capture: &EvidenceCapture) -> Result<CollectedCleanupAnalysis> {
    if !matches!(capture.source().observation, SourceObservation::Snapshot(_) | SourceObservation::LocalResource { .. })
        || !capture.families().contains(&EvidenceFamily::OperationalSamples)
    {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    let original = capture.verified_original()?;
    let report = from_original(&original)?;
    Ok(CollectedCleanupAnalysis { context: crate::collected_context::from_capture(capture), report })
}

pub(crate) fn from_original(original: &dyn OriginalProjection) -> Result<CleanupReport> {
    let source = original.source();
    let native = source.observation.snapshot().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    if !matches!(native.dataset, SnapshotDataset::Cleanup | SnapshotDataset::Operational)
        || native.dataset.stream() != TelemetryStream::OperationalSamples
        || !original.selected_streams().any(|stream| stream == TelemetryStream::OperationalSamples)
    {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    if !source.target.is_valid() || !native.is_valid() {
        return Err(invalid());
    }
    for record in original.records() {
        let TelemetryRecord::Operational(observation) = &record.row().record else {
            return Err(invalid());
        };
        if !observation.is_valid() || !native.sources.iter().any(|coverage| coverage.source == observation.source) {
            return Err(invalid());
        }
    }
    let local_cleanup =
        native.dataset == SnapshotDataset::Cleanup && matches!(source.partition, SourcePartition::InstanceLocal { .. });
    if local_cleanup {
        let context = NativeReadContext::from_snapshot(source)?;
        if context.target.engine() != Engine::Mssql
            || !context.has_qualified_local_owner()
            || !matches!(context.partition, SourcePartition::InstanceLocal { member: Some(ref member), .. }
                if member.kind == SharedResourceKind::Instance)
        {
            return Err(invalid());
        }
    }
    let database = source.target.database().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    let bounds = ObservationBounds::new(&native.observed_from, &native.observed_until).ok_or_else(invalid)?;
    let mut observations = Vec::new();
    let mut sources = Vec::new();
    for coverage in &native.sources {
        let rows = original
            .records()
            .iter()
            .filter_map(|record| match &record.row().record {
                TelemetryRecord::Operational(observation) if observation.source == coverage.source => Some(observation),
                _ => None,
            })
            .collect::<Vec<_>>();
        let outcome = read_outcome(coverage, rows.len(), original.is_read_complete())?;
        match native.dataset {
            SnapshotDataset::Cleanup if local_cleanup => {
                if rows.len() > 1 {
                    return Err(invalid());
                }
                let expected = match coverage.source.as_str() {
                    source_id::OPEN_TRANSACTIONS => {
                        rows.iter().all(|row| matches!(row.fact, OperationalFact::MssqlCleanupHorizon(_)))
                    }
                    source_id::PERSISTENT_VERSION_STORE => {
                        rows.iter().all(|row| matches!(row.fact, OperationalFact::MssqlPersistentVersionStore(_)))
                    }
                    source_id::VERSION_STORE => rows.iter().all(|row| {
                        matches!(&row.fact, OperationalFact::Counter(counter)
                        if counter.scope == culprit_core::MeasurementScope::Database
                            && counter.key == "tempdb-version-store"
                            && counter.unit == CounterUnit::Kibibytes
                            && counter.nature == CounterNature::Gauge
                            && counter.native_object == "sys.dm_tran_version_store_space_usage"
                            && counter.native_name == "reserved_space_kb")
                    }),
                    _ => false,
                };
                if !expected {
                    return Err(invalid());
                }
                if coverage.source != source_id::VERSION_STORE {
                    sources.push(outcome);
                    observations.extend(rows);
                }
            }
            SnapshotDataset::Cleanup => {
                for row in &rows {
                    match &row.fact {
                        OperationalFact::Retention(holder)
                            if bounds.contains(&holder.observed_at)
                                && holder.fits_database_capture(&database.database) => {}
                        OperationalFact::RetentionCoverage(_) => (),
                        _ => return Err(invalid()),
                    }
                }
                sources.push(outcome);
                observations.extend(rows);
            }
            SnapshotDataset::Operational => {
                let selected = rows.iter().copied().filter(|row| is_native_cleanup(&row.fact)).collect::<Vec<_>>();
                let declared_cleanup = matches!(
                    coverage.source.as_str(),
                    source_id::PERSISTENT_VERSION_STORE | source_id::OPEN_TRANSACTIONS
                );
                if declared_cleanup || !selected.is_empty() {
                    if declared_cleanup && selected.len() != rows.len() {
                        return Err(invalid());
                    }
                    sources.push(outcome);
                    observations.extend(selected);
                }
            }
            _ => unreachable!("dataset admitted before reading rows"),
        }
    }
    if sources.is_empty() {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    culprit_checks::cleanup::explain(
        culprit_checks::cleanup::CleanupEvidence { retention: None, native: &observations },
        &sources,
    )
    .map_err(|_| invalid())
}

fn is_native_cleanup(fact: &OperationalFact) -> bool {
    matches!(fact, OperationalFact::MssqlPersistentVersionStore(_) | OperationalFact::MssqlCleanupHorizon(_))
}
