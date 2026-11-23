//! A named filter of one original census; the window selects sessions, never one hour of query work.
use super::*;
use culprit_core::{RecentClientPopulationCoverage, SessionCensusRecentClients};
mod context;
pub(super) use context::SessionContext;
pub(super) use context::fields;

const COMPLETION_WINDOW_MS: u32 = 3_600_000;
const REPORTED_LABEL_POLICY: &str = "culprit-reported-client-tools-v1";
const EXCLUDED_PROGRAM_PREFIXES: [&str; 3] = ["microsoft sql server management studio", "sqlserverextension", "azdata"];

enum Disposition {
    Recent,
    OutsideWindow,
    Excluded,
    Unknown,
}
#[derive(Default)]
struct SelectedSessions<'source, 'session> {
    selected: Vec<&'session ClientSession<'source>>,
    excluded: u32,
    outside_window: u32,
    unknown: u32,
}
impl<'source: 'session, 'session> SelectedSessions<'source, 'session> {
    fn from_sessions(sessions: &'session [ClientSession<'source>], observed: u32) -> Self {
        let mut selection = Self { unknown: observed - sessions.len() as u32, ..Default::default() };
        for session in sessions {
            match disposition(session) {
                Disposition::Recent => selection.selected.push(session),
                Disposition::OutsideWindow => selection.outside_window += 1,
                Disposition::Excluded => selection.excluded += 1,
                Disposition::Unknown => selection.unknown += 1,
            }
        }
        selection
    }
}
fn disposition(session: &ClientSession<'_>) -> Disposition {
    let Some(native) = &session.recent else { return Disposition::Unknown };
    let program = session.facts.key.application.as_deref();
    if program.is_some_and(excluded_label) {
        return Disposition::Excluded;
    }
    match native.last_end_age_ms.and_then(|age| age.parse::<u64>().ok()) {
        Some(age) if age >= u64::from(COMPLETION_WINDOW_MS) => Disposition::OutsideWindow,
        Some(_) if program.is_some() => Disposition::Recent,
        _ => Disposition::Unknown,
    }
}
fn excluded_label(program: &str) -> bool {
    let folded = program.to_ascii_lowercase();
    EXCLUDED_PROGRAM_PREFIXES.iter().any(|prefix| folded.starts_with(prefix))
}
pub(super) fn explain(
    frame: &ActivityFrameEvidence<'_>,
    sessions: &[ClientSession<'_>],
    observed: u32,
) -> SessionCensusRecentClients {
    let selected = SelectedSessions::from_sessions(sessions, observed);
    let coverage = coverage(frame, sessions, observed);
    let groups = groups(selected.selected.iter().copied());
    let available_groups = groups.len() as u32;
    let grouped_sessions = selected.selected.len() as u32;
    let requests = groups.values().map(|group| group.requests).sum::<u32>();
    let entries_limited = groups.len() > MAX_PRESENTED_CLIENT_GROUPS;
    let mut limitations = limitations(coverage);
    if entries_limited {
        limitations.push(t!(
            "view.session_census.groups.limit.presentation",
            presented = format::digits(MAX_PRESENTED_CLIENT_GROUPS.to_string()),
            available = format::digits(available_groups.to_string())
        ));
    }
    SessionCensusRecentClients {
        window_ms: COMPLETION_WINDOW_MS,
        reported_label_policy: REPORTED_LABEL_POLICY.into(),
        source: view_source(frame, coverage, grouped_sessions, selected.unknown),
        coverage,
        observed_sessions: observed,
        excluded_sessions: selected.excluded,
        outside_window_sessions: selected.outside_window,
        unknown_sessions: selected.unknown,
        groups: SessionCensusClientGroups {
            title: t!("view.session_census.recent.title"),
            summary: t!(
                "view.session_census.recent.summary",
                selected = format::digits(grouped_sessions.to_string()),
                observed = format::digits(observed.to_string()),
                requests = format::digits(requests.to_string()),
                excluded = format::digits(selected.excluded.to_string()),
                older = format::digits(selected.outside_window.to_string()),
                unknown = format::digits(selected.unknown.to_string())
            ),
            limitations,
            grouped_sessions,
            ungrouped_sessions: selected.unknown,
            available_groups,
            entries_limited,
            entries: groups
                .into_iter()
                .take(MAX_PRESENTED_CLIENT_GROUPS)
                .map(|(key, group)| entry(key, group, GroupDetails::Recent))
                .collect(),
        },
    }
}
fn coverage(
    frame: &ActivityFrameEvidence<'_>,
    sessions: &[ClientSession<'_>],
    observed: u32,
) -> RecentClientPopulationCoverage {
    use RecentClientPopulationCoverage::*;
    if !frame.is_qualified() || !frame.read_complete() {
        return UnverifiedOriginalCensus;
    }
    if observed > 0 && !sessions.iter().any(|session| session.recent.is_some()) {
        return NativeRecencyUnavailable;
    }
    let context = frame.context();
    if !context.has_qualified_local_owner()
        || (context.target.database().is_some() && context.database_writable.is_none())
    {
        return UnverifiedOriginalCensus;
    }
    if frame.native().population != ActivityPopulation::UserSessionsCurrentRequestsAndBlockers
        || includes_uncovered_database(frame, sessions)
    {
        return OriginalPopulationSubset;
    }
    if !frame.native().coverage.complete_scope {
        return PartialOriginalCensus;
    }
    CompleteOriginalCensus
}
/// A request in this DB can belong to a session whose default DB is elsewhere. Its context is useful,
/// but the selected-DB query never declared a full idle-session census of that other database.
fn includes_uncovered_database(frame: &ActivityFrameEvidence<'_>, sessions: &[ClientSession<'_>]) -> bool {
    let Some(target) = frame.source().target.database() else { return false };
    sessions.iter().any(|session| {
        matches!(&session.facts.key.database,
        SessionClientDatabase::CurrentSession { name, .. } if name.as_deref() != Some(target.database.as_str()))
    })
}

fn limitations(coverage: RecentClientPopulationCoverage) -> Vec<String> {
    let mut limitations = vec![
        t!("view.session_census.recent.limit.selection"),
        t!("view.session_census.recent.limit.labels"),
        t!("view.session_census.recent.limit.lifetimes"),
        t!("view.session_census.recent.limit.connections"),
        t!("view.session_census.recent.limit.clocks"),
        t!("view.session_census.limit.client"),
    ];
    limitations.push(coverage_message(coverage));
    limitations
}
fn coverage_message(coverage: RecentClientPopulationCoverage) -> String {
    use RecentClientPopulationCoverage::*;
    match coverage {
        CompleteOriginalCensus => t!("view.session_census.recent.coverage.complete"),
        PartialOriginalCensus => t!("view.session_census.recent.coverage.partial"),
        OriginalPopulationSubset => t!("view.session_census.recent.coverage.subset"),
        UnverifiedOriginalCensus => t!("view.session_census.recent.coverage.unverified"),
        NativeRecencyUnavailable => t!("view.session_census.recent.coverage.unavailable"),
    }
}
fn view_source(
    frame: &ActivityFrameEvidence<'_>,
    coverage: RecentClientPopulationCoverage,
    selected: u32,
    unknown: u32,
) -> culprit_core::SourceOutcome {
    use RecentClientPopulationCoverage::*;
    use culprit_core::SourceState;
    let state = match (coverage, unknown, selected) {
        (CompleteOriginalCensus, 0, 0) => SourceState::Empty,
        (CompleteOriginalCensus, 0, _) => SourceState::Collected,
        (NativeRecencyUnavailable, _, _) => SourceState::Unavailable,
        _ => SourceState::Partial,
    };
    let reason = if coverage == CompleteOriginalCensus && unknown > 0 {
        t!("view.session_census.recent.coverage.unknown_selection")
    } else {
        coverage_message(coverage)
    };
    culprit_core::SourceOutcome {
        source: frame.native().coverage.source.clone(),
        state,
        reason: Some(culprit_core::LocalizedText::raw(reason)),
    }
}

#[cfg(test)]
mod tests;
