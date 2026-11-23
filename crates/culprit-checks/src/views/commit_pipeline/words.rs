//! Render typed stages/observations once. Advice stays conditional; values do not drive logic through prose.
use super::*;
pub(super) fn render(report: &mut CommitPipelineReport, qualified: bool) {
    report.summary = if report.stage.is_some() {
        t!("view.commit_pipeline.summary.observed")
    } else if qualified {
        t!("view.commit_pipeline.summary.unknown")
    } else {
        t!("view.commit_pipeline.summary.unqualified")
    };
    report.mechanism = report.stage.map(|stage| culprit_core::i18n::t(mechanism_key(stage), &[]));
    report.investigation = report
        .stage
        .map(|stage| culprit_core::i18n::t(action_key(stage), &[]))
        .unwrap_or_else(|| t!("view.commit_pipeline.action.unknown"));
    report.limitations = vec![
        t!("view.commit_pipeline.limit.point"),
        t!("view.commit_pipeline.limit.cause"),
        t!("view.commit_pipeline.limit.totals"),
        t!("view.commit_pipeline.limit.flush_size"),
        t!("view.commit_pipeline.limit.clocks"),
        t!("view.commit_pipeline.limit.absence"),
        t!("view.commit_pipeline.limit.maintenance"),
    ];
    if report.inputs.iter().any(|input| {
        matches!(input.binding, CommitPipelineContextBinding::Unqualified | CommitPipelineContextBinding::Foreign)
    }) {
        report.limitations.push(t!("view.commit_pipeline.limit.owner"));
    }
    if report.inputs.iter().any(|input| {
        !input.read_complete
            || input
                .sources
                .iter()
                .any(|source| matches!(source.state, SourceState::Partial | SourceState::Unavailable))
    }) {
        report.limitations.push(t!("view.commit_pipeline.limit.partial"));
    }
    if report.facts.is_empty() {
        report.limitations.push(t!("view.commit_pipeline.limit.context"));
    }
    if report.facts.iter().any(|fact| {
        !fact.original().read_complete
            || !fact.original().complete_source_scope
            || matches!(fact.original().source_state, SourceState::Partial | SourceState::Unavailable)
    }) {
        report.limitations.push(t!("view.commit_pipeline.limit.partial"));
    }
    if report.facts.iter().any(|fact| matches!(fact, CommitPipelineFact::FileIo { .. })) {
        report.limitations.push(t!("view.commit_pipeline.limit.io"));
    }
    if report.facts.iter().any(|fact| matches!(fact, CommitPipelineFact::Replica { .. })) {
        report.limitations.push(t!("view.commit_pipeline.limit.replica"));
    }
    if report.facts.iter().any(|fact| matches!(fact, CommitPipelineFact::ConfiguredRates { .. })) {
        report.limitations.push(t!("view.commit_pipeline.limit.configuration"));
    }
    let mut summaries = Vec::new();
    let mut limited = report.facts_limited;
    for fact in &report.facts {
        match fact {
            CommitPipelineFact::Counters { values, .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.counters"));
                for counter in values {
                    append(&mut summaries, &mut limited, counter_summary(&counter.sample));
                }
            }
            CommitPipelineFact::Transaction { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.transaction"))
            }
            CommitPipelineFact::FileIo { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.file_io"))
            }
            CommitPipelineFact::Allocation { .. } | CommitPipelineFact::CatalogLogFile { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.allocation"))
            }
            CommitPipelineFact::Maintenance { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.maintenance"))
            }
            CommitPipelineFact::ConfiguredRates { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.configuration"))
            }
            CommitPipelineFact::HyperscaleComponent { .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.hyperscale"))
            }
            CommitPipelineFact::Replica { entry, .. } => {
                append(&mut summaries, &mut limited, t!("view.commit_pipeline.context.replica"));
                append(&mut summaries, &mut limited, entry.explanation.clone());
            }
        }
    }
    for increase in &report.increases {
        append(
            &mut summaries,
            &mut limited,
            match increase {
                CommitPipelineIncrease::TransactionLogBytes { bytes, .. } => {
                    t!("view.commit_pipeline.increase.transaction", bytes = format::digits(bytes.clone()))
                }
                CommitPipelineIncrease::CatalogLogAllocationBytes { bytes, .. } => {
                    t!("view.commit_pipeline.increase.allocation", bytes = format::digits(bytes.clone()))
                }
            },
        );
    }
    report.context_summaries = summaries;
    report.facts_limited = limited;
    super::intervals::render(report);
    if report.facts_limited {
        report.limitations.push(t!("view.commit_pipeline.limit.presentation"));
    }
    if report.increases.iter().any(|increase| matches!(increase, CommitPipelineIncrease::TransactionLogBytes { .. })) {
        report.investigation.push_str("\n\n");
        report.investigation.push_str(&t!("view.commit_pipeline.action.producer"));
    }
    if report
        .increases
        .iter()
        .any(|increase| matches!(increase, CommitPipelineIncrease::CatalogLogAllocationBytes { .. }))
    {
        report.limitations.push(t!("view.commit_pipeline.limit.growth"));
    }
}
fn mechanism_key(stage: CommitPipelineStage) -> &'static str {
    match stage {
        CommitPipelineStage::LogBufferSpace => key!("view.commit_pipeline.stage.buffer"),
        CommitPipelineStage::LocalLogFlush => key!("view.commit_pipeline.stage.flush"),
        CommitPipelineStage::LocalLogWrite => key!("view.commit_pipeline.stage.write"),
        CommitPipelineStage::LocalLogSync => key!("view.commit_pipeline.stage.sync"),
        CommitPipelineStage::LogSpaceReuse => key!("view.commit_pipeline.stage.reuse"),
        CommitPipelineStage::SegmentInitializationWrite | CommitPipelineStage::SegmentInitializationSync => {
            key!("view.commit_pipeline.stage.initialization")
        }
        CommitPipelineStage::SynchronousAcknowledgement => key!("view.commit_pipeline.stage.remote"),
        CommitPipelineStage::SynchronousHardeningAcknowledgement => key!("view.commit_pipeline.stage.remote_hardening"),
        CommitPipelineStage::RateGovernance { .. } => key!("view.commit_pipeline.stage.governance"),
    }
}
fn action_key(stage: CommitPipelineStage) -> &'static str {
    match stage {
        CommitPipelineStage::SynchronousAcknowledgement | CommitPipelineStage::SynchronousHardeningAcknowledgement => {
            key!("view.commit_pipeline.action.remote")
        }
        CommitPipelineStage::LogSpaceReuse => key!("view.commit_pipeline.action.reuse"),
        CommitPipelineStage::SegmentInitializationWrite | CommitPipelineStage::SegmentInitializationSync => {
            key!("view.commit_pipeline.action.initialization")
        }
        CommitPipelineStage::RateGovernance { .. } => key!("view.commit_pipeline.action.governance"),
        _ => key!("view.commit_pipeline.action.local"),
    }
}

pub(super) fn append(summaries: &mut Vec<String>, limited: &mut bool, summary: String) {
    if summaries.len() < MAX_COMMIT_PIPELINE_FACTS {
        summaries.push(summary);
    } else {
        *limited = true;
    }
}
fn counter_summary(counter: &NativeCounterSample) -> String {
    let raw = match &counter.value {
        MeasurementValue::Integer(value) => value.clone(),
        MeasurementValue::Real(value) => value.to_string(),
    };
    t!(
        "view.commit_pipeline.context.counter",
        object = format::escaped_preview(&counter.native_object, 256).text,
        name = format::escaped_preview(&counter.native_name, 256).text,
        value = format::digits(raw),
        unit = match counter.unit {
            CounterUnit::Bytes => "bytes",
            CounterUnit::Milliseconds => "milliseconds",
            CounterUnit::Count => "count",
            _ => "other",
        }
    )
}
