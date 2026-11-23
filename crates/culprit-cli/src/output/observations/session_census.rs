use super::{SourceQualification, Summary, render_summary};
use culprit_core::{SessionCensusReport, format, t};

const MAX_FIELD_PREVIEW_CHARACTERS: usize = 512;

pub(crate) fn session_census(report: &SessionCensusReport) -> String {
    render_summary(
        Summary {
            title: &report.title,
            summary: &report.summary,
            source: SourceQualification::Multiple(&report.sources),
            limitations: &report.limitations,
        },
        |text| {
            text.push('\n');
            text.push_str(&t!(
                "view.wait_counters.bounds",
                from = &report.context.observed_from,
                until = &report.context.observed_until
            ));
            text.push('\n');
            text.push_str(&t!("ui.memory_participants.observed_at", at = &report.observed_at));
            text.push('\n');
            text.push_str(&t!(
                "view.wait_counters.cli.counts",
                native = format::digits(report.native_rows.to_string()),
                observed = format::digits(report.observed_rows.to_string()),
                presented = format::digits(report.presented_rows.to_string()),
                printed = format::digits(report.entries.len().min(super::super::BRIEF_FINDING_LIMIT).to_string())
            ));
            render_client_groups(text, &report.client_groups, None);
            render_client_groups(
                text,
                &report.recent_clients.groups,
                Some(std::slice::from_ref(&report.recent_clients.source)),
            );
            for entry in report.entries.iter().take(super::super::BRIEF_FINDING_LIMIT) {
                text.push_str("\n\n");
                text.push_str(&entry.title);
                text.push('\n');
                text.push_str(&entry.state);
                render_fields(text, &entry.fields);
            }
        },
    )
}

fn render_client_groups(
    text: &mut String,
    groups: &culprit_core::SessionCensusClientGroups,
    sources: Option<&[culprit_core::SourceOutcome]>,
) {
    text.push_str("\n\n");
    text.push_str(&groups.title);
    text.push('\n');
    text.push_str(&groups.summary);
    if let Some(sources) = sources {
        super::append_native_sources(text, sources);
    }
    text.push('\n');
    text.push_str(&t!(
        "view.session_census.groups.cli.presented",
        available = format::digits(groups.available_groups.to_string()),
        printed = format::digits(groups.entries.len().min(super::super::BRIEF_FINDING_LIMIT).to_string())
    ));
    for limitation in &groups.limitations {
        text.push('\n');
        text.push_str(limitation);
    }
    render_group_entries(text, groups);
}
fn render_group_entries(text: &mut String, groups: &culprit_core::SessionCensusClientGroups) {
    for group in groups.entries.iter().take(super::super::BRIEF_FINDING_LIMIT) {
        text.push('\n');
        for limitation in &group.limitations {
            text.push_str(limitation);
            text.push('\n');
        }
        render_fields(text, &group.fields);
    }
}

fn render_fields(text: &mut String, fields: &[culprit_core::SessionCensusField]) {
    for field in fields {
        text.push('\n');
        text.push_str(&t!(
            "view.counter_ratios.cli.metric",
            label = &field.label,
            value = field
                .value
                .as_deref()
                .map(|value| culprit_core::format::escaped_preview(value, MAX_FIELD_PREVIEW_CHARACTERS).text)
                .unwrap_or_else(|| t!("ui.memory_failures.not_reported"))
        ));
    }
}

#[cfg(test)]
mod tests;
