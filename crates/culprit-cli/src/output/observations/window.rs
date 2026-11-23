//! Plain bounded archive presentation reuses the existing incident, workload and native comparison text owners.
use culprit_core::{Result, WarehouseWindowReport, WindowWorkload, format, t, telemetry::DeliveryProblem};
const TEXT_CHARS: usize = 2048;
fn text(value: &str) -> String {
    format::escaped_preview(value, TEXT_CHARS).text
}
pub(crate) fn window(report: &WarehouseWindowReport) -> Result<String> {
    let mut paragraphs = vec![t!(
        "window.cli.scope",
        from = text(&report.request.window.from),
        until = text(&report.request.window.until),
        as_of = text(&report.request.as_of)
    )];
    paragraphs.extend(report.qualifications.iter().map(|reason| text(reason)));
    if !report.incidents.incidents.is_empty() || !report.incidents.history.entries.is_empty() {
        paragraphs.push(super::incidents(&report.incidents));
    }
    for analysis in &report.published {
        paragraphs.push(t!(
            "window.cli.analysis",
            alias = text(&analysis.context.description.display.connection_alias),
            from = text(&analysis.context.description.observed_from),
            until = text(&analysis.context.description.observed_until)
        ));
        paragraphs.extend(analysis.limitations.iter().map(|reason| text(reason)));
        for row in &analysis.findings {
            let finding = &row.finding.finding;
            paragraphs.push(t!("window.cli.finding", title = text(&finding.title), summary = text(&finding.summary)));
            paragraphs.push(text(&row.placement.qualification));
            if let Some(fix) = &finding.fix {
                paragraphs.push(t!("window.cli.action", action = text(&fix.summary)));
                if let Some(sql) = &fix.sql {
                    paragraphs.push(text(sql));
                }
            }
            for verify in &finding.verify {
                paragraphs.push(t!("window.cli.verify", verify = text(verify)));
            }
            paragraphs.push(t!("window.reader.historical_guidance"));
        }
        for row in &analysis.changes {
            let change = &row.change.change;
            paragraphs.push(t!("window.cli.change", at = text(&change.at), title = text(change.title.text())));
            if let Some(detail) = &change.detail {
                paragraphs.push(text(detail.text()));
            }
            paragraphs.push(text(&row.placement.qualification));
        }
    }
    match &report.workload {
        WindowWorkload::NotRequested => paragraphs.push(t!("window.reader.workload_not_requested")),
        WindowWorkload::Evaluated { analysis } => {
            for section in &analysis.sections {
                paragraphs.push(t!(
                    "window.cli.native_section",
                    from = text(&section.observed.from),
                    until = text(&section.observed.until)
                ));
                paragraphs.push(super::query_result(&section.analysis));
                if let Some(changes) = &section.comparison.changes {
                    paragraphs.push(text(&changes.summary));
                    for query in &changes.queries {
                        paragraphs.push(text(&query.headline));
                        if let Some(reason) = &query.reason {
                            paragraphs.push(text(reason));
                        }
                        paragraphs.push(text(&query.text));
                    }
                }
            }
        }
    }
    for (index, choice) in culprit_engine::evidence::window_artifact_choices(report).iter().enumerate() {
        paragraphs.push(t!(
            "artifact.cli.choice",
            index = format::digits((index + 1).to_string()),
            title = text(choice.title)
        ));
    }
    if let Some(next) = &report.next {
        // This is an opaque copied continuation, not a manually supplied capture identity or read authority.
        let cursor = serde_json::to_string(next).map_err(|_| DeliveryProblem::Integrity.error())?;
        paragraphs.push(t!("window.cli.next", as_of = text(&report.request.as_of), cursor = cursor));
    }
    Ok(paragraphs.join("\n\n") + "\n")
}

pub(crate) fn artifacts(report: &culprit_core::WarehouseArtifactReport) -> String {
    use culprit_core::{WarehouseArtifactText, telemetry::DefinitionPayload};
    let mut paragraphs = report.qualifications.iter().map(|reason| text(reason)).collect::<Vec<_>>();
    for row in &report.records {
        paragraphs.push(t!(
            "artifact.cli.original",
            source = text(&row.source),
            from = text(&row.collected_from),
            until = text(&row.collected_until)
        ));
        for artifact in &row.artifacts {
            paragraphs.push(text(&artifact.label));
            match &artifact.payload {
                WarehouseArtifactText::Complete { payload: DefinitionPayload::Available { text, .. } } => {
                    paragraphs.push(format::escaped_preview(text, text.chars().count()).text)
                }
                WarehouseArtifactText::Complete { .. } => paragraphs.push(t!("artifact.ui.unavailable")),
                _ => paragraphs.push(t!("artifact.ui.slice")),
            }
            paragraphs.extend(artifact.limitations.iter().map(|reason| text(reason)));
        }
    }
    paragraphs.join("\n\n") + "\n"
}
