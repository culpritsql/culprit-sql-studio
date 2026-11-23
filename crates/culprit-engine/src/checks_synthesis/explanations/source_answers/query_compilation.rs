//! Retained Query Store compilation totals supply bounded per-query compiler context and limitations,
//! never a historical execution's caller, runtime inputs or a harmful plan-reuse verdict.
use super::*;
use culprit_checks::compilation::CompilationEvidence;

pub(super) fn append(
    originals: &[&dyn OriginalProjection],
    answer: &QueryCompilationAnswer,
    explanation: &mut Explanation,
) -> Result<()> {
    if !answer.is_valid() {
        return Err(invalid());
    }
    let Some(result_original) =
        originals.iter().copied().find(|candidate| candidate.capture() == &answer.result.capture)
    else {
        return Ok(());
    };
    if result_original.original() != &answer.result
        || result_original.source().observation.execution_context() != Some(&answer.result_context)
    {
        return Err(invalid());
    }
    let read = result_original.operational_read()?;
    let outcome = super::super::source(&read, culprit_core::source_id::QUERY_COMPILATION)?;
    let context = NativeReadContext::from_snapshot(result_original.source())?;
    let observations = read.observations.iter().collect::<Vec<_>>();
    let report = culprit_checks::compilation::explain(CompilationEvidence {
        context: Some(&context),
        observations: &observations,
        source: Some(&outcome),
        definitions: None,
    })
    .map_err(|_| invalid())?;

    match &answer.selection {
        QueryCompilationSelection::Observed { observation, counters } => {
            let Some((selected, position)) = resolve(originals, observation)? else { return Ok(()) };
            if selected.original() != result_original.original() {
                return Err(invalid());
            }
            let TelemetryRecord::Operational(point) = &selected.records()[position].row().record else {
                return Err(invalid());
            };
            if point.key != counters.observation_key {
                return Err(invalid());
            }
            let entry = report.entries.iter().find(|entry| entry.observation_key == point.key).ok_or_else(invalid)?;
            explanation.mechanism = Some(entry.explanation.clone());
            explanation.action = Some(entry.investigation.clone());
            explanation.verify = Some(entry.verify.clone());
            for limitation in report.limitations {
                push_once(&mut explanation.limitations, limitation);
            }
        }
        QueryCompilationSelection::NotObserved | QueryCompilationSelection::IncarnationChanged => match outcome.state {
            SourceState::Partial => {
                push_once(&mut explanation.limitations, t!("view.compilation.limit.source_partial"));
            }
            SourceState::Unavailable | SourceState::NotApplicable => {
                push_once(&mut explanation.limitations, t!("view.compilation.limit.source_unavailable"));
            }
            SourceState::Collected | SourceState::Empty => (),
        },
    }
    Ok(())
}

fn push_once(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
