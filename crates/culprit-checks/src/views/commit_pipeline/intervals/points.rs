//! Select native-source captures before counter identities, including genuine zero-row or incomplete endpoints.
use super::*;
pub(super) fn collect<'a>(originals: &'a [Original<'a>], report: &mut CommitPipelineReport) -> Result<Vec<Frame<'a>>> {
    let mut frames = Vec::new();
    for original in originals {
        if !relevant_target(&original.source.target, &report.context) {
            continue;
        }
        let Some(header) = original.source.observation.snapshot() else { continue };
        if header.dataset.stream() != TelemetryStream::OperationalSamples {
            continue;
        }
        // Native compatibility is checked only after the nearest declared-resource captures are selected.
        // A changed or missing owner retains its original clocks/coverage and cannot expose an older fallback.
        let context = original.context().ok();
        let read = original.operational()?;
        for native_coverage in &header.sources {
            let native_source = native_coverage.source.as_str();
            let coverage = read.source_coverage(native_source).cloned().ok_or_else(invalid)?;
            let mut points = Vec::new();
            for row in &original.rows {
                let TelemetryRecord::Operational(observation) = row.record else { return Err(invalid()) };
                if observation.source != coverage.source {
                    continue;
                }
                match &observation.fact {
                    OperationalFact::CounterSet(set) => points.push(Point {
                        reference: &row.reference,
                        set,
                        values: facts::counter_values(original.source.target.engine(), &observation.source, set),
                    }),
                    OperationalFact::FileIo(file) if file.kind == FileKind::Log => {
                        // The current file DTO has no independently qualified file-incarnation/reset companion.
                        // Original cumulative points survive without substituting catalog/name/process guesses.
                        gap(
                            report,
                            None,
                            Some(&row.reference),
                            None,
                            if file.reset == ResetMarker::Unknown {
                                CommitPipelineIntervalIssue::UnknownReset
                            } else {
                                CommitPipelineIntervalIssue::FileCounterIdentityUnavailable
                            },
                        );
                    }
                    _ => (),
                }
            }
            let key = digest(
                &serde_json::to_vec(&(&original.source.target, &original.source.source, &coverage.source))
                    .map_err(|_| invalid())?,
            );
            frames.push(Frame {
                original,
                context: context.clone(),
                observed_from: &header.observed_from,
                observed_until: &header.observed_until,
                native_source,
                coverage,
                points,
                key,
            });
        }
    }
    let relevant = frames
        .iter()
        .filter(|frame| frame.points.iter().any(|point| !point.values.is_empty()))
        .map(|frame| frame.key.clone())
        .collect::<BTreeSet<_>>();
    frames.retain(|frame| relevant.contains(&frame.key));
    if frames.len() > MAX_COMMIT_PIPELINE_FACTS {
        gap(report, None, None, None, CommitPipelineIntervalIssue::PresentationLimited);
        report.facts_limited = true;
        return Ok(Vec::new());
    }
    Ok(frames)
}
