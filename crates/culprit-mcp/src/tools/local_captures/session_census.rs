//! Bounded native context only: no raw source/endpoint, SQL, manifest or private authentication material.
use super::*;
use culprit_core::telemetry::digest;
use culprit_engine::LocalSessionCensus;

const ENTRY_LIMIT: usize = 5;

pub(super) fn captured(analysis: &LocalSessionCensus) -> Result<Value> {
    let report = analysis.report();
    let original = analysis.original();
    let reference = digest(&serde_json::to_vec(original.capture()).map_err(|_| DeliveryProblem::Integrity.error())?);
    let entries = report
        .entries
        .iter()
        .take(ENTRY_LIMIT)
        .map(|entry| {
            json!({
                "reference":entry.reference.as_ref().map(|row|digest(row.record_id.as_bytes())),
                "session":entry.activity.session.id,
                "request":entry.activity.request.as_ref().map(|request|request.id.as_str()),
                "scope":entry.scope,"idle":entry.idle,"title":entry.title,"state":entry.state,
                "fields":entry.fields,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "session_census":{
            "title":report.title,"summary":report.summary,"limitations":report.limitations,
            "historical_context":true,"reference":reference,
            "logical_database":report.context.target.database().map(|database|database.database.as_str()),
            "native_read_bounds":{"from":report.context.observed_from,"until":report.context.observed_until},
            "native_observed_at":report.observed_at,"scope":report.scope,
            "database_writable":report.context.database_writable,
            "qualified_local_owner":report.context.has_qualified_local_owner(),
            "native_population":report.population,"source":report.native_source,
            "sources":report.sources,"read_complete":report.read_complete,
            "native_rows":report.native_rows,"observed_rows":report.observed_rows,
            "observed_sessions":report.observed_sessions,"observed_current_requests":report.observed_requests,
            "entries_available":report.entries.len(),"entries_presented":entries.len(),
            "entries_omitted":report.entries.len().saturating_sub(entries.len()),
            "entries_limited":report.entries_limited,"entries":entries,
            "client_groups":client_groups(&report.client_groups),
            "recent_clients":{
                "window_ms":report.recent_clients.window_ms,
                "reported_label_policy":report.recent_clients.reported_label_policy,
                "coverage":report.recent_clients.coverage,
                "source":report.recent_clients.source,
                "observed_sessions":report.recent_clients.observed_sessions,
                "excluded_sessions":report.recent_clients.excluded_sessions,
                "outside_window_sessions":report.recent_clients.outside_window_sessions,
                "unknown_sessions":report.recent_clients.unknown_sessions,
                "groups":client_groups(&report.recent_clients.groups),
            },
        },
        "collected_from":original.collected_from(),"collected_until":original.collected_until(),
        "local_verification_bytes":original.bytes_read(),
    }))
}

fn client_groups(groups: &culprit_core::SessionCensusClientGroups) -> Value {
    let entries = groups
        .entries
        .iter()
        .take(ENTRY_LIMIT)
        .map(|group| {
            json!({
                "observed_sessions":group.observed_sessions,"observed_current_requests":group.observed_requests,
                "fields":group.fields,"limitations":group.limitations,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "title":groups.title,"summary":groups.summary,"limitations":groups.limitations,
        "grouped_sessions":groups.grouped_sessions,"ungrouped_sessions":groups.ungrouped_sessions,
        "groups_available":groups.available_groups,"entries_limited":groups.entries_limited,
        "groups_presented":entries.len(),"groups_omitted":groups.available_groups as usize-entries.len(),
        "entries":entries,
    })
}
