//! Keep original scope/clock/coverage per fact. Independent points never establish the current wait's cause.
use super::*;
pub(super) fn append(originals: &[Original<'_>], report: &mut CommitPipelineReport) -> Result<()> {
    for original in originals {
        if !relevant_target(&original.source.target, &report.context) {
            continue;
        }
        report.examined_rows = report
            .examined_rows
            .checked_add(u32::try_from(original.rows.len()).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let Some(header) = original.source.observation.snapshot() else { continue };
        let context = original.context().ok();
        let binding = context
            .as_ref()
            .map(|context| context_binding(context, &report.context))
            .unwrap_or(CommitPipelineContextBinding::Unqualified);
        if report.inputs.len() < MAX_COMMIT_PIPELINE_FACTS {
            report.inputs.push(CommitPipelineCoverage {
                original: original.original.clone(),
                context: context.clone().filter(|_| {
                    matches!(
                        binding,
                        CommitPipelineContextBinding::SameNativeOwner | CommitPipelineContextBinding::SharedNativeOwner
                    )
                }),
                binding,
                sources: header.sources.clone(),
                read_complete: original.read_complete,
            });
        } else {
            report.facts_limited = true;
        }
        let Some(context) = context.filter(|context| compatible_owner(context, &report.context)) else { continue };
        if header.dataset == SnapshotDataset::Replication {
            replica(original, &context, report)?;
            continue;
        }
        if header.dataset.stream() != TelemetryStream::OperationalSamples {
            continue;
        }
        let read = original.operational()?;
        let references = read.captured_references()?;
        for (row, reference) in read.observations.iter().zip(references) {
            let coverage = read.source_coverage(&row.source).ok_or_else(invalid)?;
            let origin = CommitPipelineOriginal {
                reference,
                context: context.clone(),
                source_state: coverage.state,
                complete_source_scope: coverage.complete_scope,
                read_complete: original.read_complete,
            };
            let fact = match &row.fact {
                OperationalFact::CounterSet(set) => {
                    let values = counter_values(original.source.target.engine(), &row.source, set);
                    (!values.is_empty()).then_some(CommitPipelineFact::Counters { original: origin, values })
                }
                OperationalFact::FileIo(point) if point.kind == FileKind::Log => {
                    Some(CommitPipelineFact::FileIo { original: origin, point: point.clone() })
                }
                OperationalFact::Space(point) if point.population == StoragePopulation::DatabaseTransactionLog => {
                    Some(CommitPipelineFact::Allocation { original: origin, point: Box::new(point.clone()) })
                }
                OperationalFact::MssqlDatabaseFile(point) if point.native_type == "1" => {
                    Some(CommitPipelineFact::CatalogLogFile { original: origin, point: point.clone() })
                }
                OperationalFact::MssqlCleanupHorizon(point) => point
                    .oldest_confirmed
                    .as_deref()
                    .filter(|transaction| {
                        transaction
                            .log_record_count
                            .as_deref()
                            .and_then(|value| value.parse::<u64>().ok())
                            .is_some_and(|value| value > 0)
                    })
                    .map(|transaction| CommitPipelineFact::Transaction {
                        original: origin,
                        point: Box::new(transaction.clone()),
                    }),
                OperationalFact::MaintenanceProgress(point) => {
                    Some(CommitPipelineFact::Maintenance { original: origin, point: point.clone() })
                }
                OperationalFact::ConfiguredLogRateLimits(point) => {
                    Some(CommitPipelineFact::ConfiguredRates { original: origin, point: point.clone() })
                }
                OperationalFact::HyperscaleLogComponent(point) => {
                    Some(CommitPipelineFact::HyperscaleComponent { original: origin, point: point.clone() })
                }
                _ => None,
            };
            if let Some(fact) = fact {
                admit(report, fact);
            }
        }
    }
    Ok(())
}
fn replica(original: &Original<'_>, context: &NativeReadContext, report: &mut CommitPipelineReport) -> Result<()> {
    let observations = original
        .rows
        .iter()
        .map(|row| match row.record {
            TelemetryRecord::Replication(point) => Ok(point.clone()),
            _ => Err(invalid()),
        })
        .collect::<Result<Vec<_>>>()?;
    let snapshot = ReplicationSnapshot::from_source(original.source, observations)?;
    let interpreted = super::super::replication::explain(&snapshot, original.read_complete)?;
    for entry in interpreted.entries {
        let row=original.rows.iter().find(|row|matches!(row.record,TelemetryRecord::Replication(point) if point.source==entry.reference.source && point.fact.key()==entry.reference.observation_key)).ok_or_else(invalid)?;
        let coverage =
            snapshot.sources.iter().find(|source| source.source == entry.reference.source).ok_or_else(invalid)?;
        admit(
            report,
            CommitPipelineFact::Replica {
                original: CommitPipelineOriginal {
                    reference: row.reference.clone(),
                    context: context.clone(),
                    source_state: coverage.state,
                    complete_source_scope: coverage.complete_scope,
                    read_complete: original.read_complete,
                },
                entry: Box::new(entry),
            },
        );
    }
    report.facts_limited |= interpreted.entries_limited;
    Ok(())
}
fn admit(report: &mut CommitPipelineReport, fact: CommitPipelineFact) {
    if report.facts.len() < MAX_COMMIT_PIPELINE_FACTS {
        report.facts.push(fact);
    } else {
        report.facts_limited = true;
    }
}

// One semantic selector serves original points and the interval companion.
pub(super) fn counter_values(engine: Engine, source: &str, set: &NativeCounterSet) -> Vec<CommitPipelineCounter> {
    set.counters
        .iter()
        .filter_map(|sample| {
            engine
                .spec()
                .commit_counter_kind(source, set, sample)
                .map(|kind| CommitPipelineCounter { kind, sample: sample.clone() })
        })
        .collect()
}
