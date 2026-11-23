use super::*;
use culprit_evidence::analytical::test_support::{
    RecentClientCase, SessionCensusCase, recent_client_evidence, session_census_evidence,
};

fn read(case: RecentClientCase) -> ActivityCensusRead {
    original(recent_client_evidence(case))
}
fn original((source, rows): (CollectionDescription, Vec<TelemetryRecord>)) -> ActivityCensusRead {
    ActivityCensusRead::new(
        source,
        rows.into_iter()
            .map(|row| match row {
                TelemetryRecord::Activity(row) => *row,
                _ => unreachable!(),
            })
            .collect(),
        true,
    )
    .unwrap()
}
fn report(read: &ActivityCensusRead) -> culprit_core::SessionCensusReport {
    crate::session_census::explain(read).unwrap()
}
fn value<'a>(group: &'a SessionCensusClientGroup, key: &str) -> Option<&'a str> {
    group.fields.iter().find(|field| field.key == key).unwrap().value.as_deref()
}
fn balanced(report: &culprit_core::SessionCensusRecentClients) {
    assert_eq!(
        report.observed_sessions,
        report.groups.grouped_sessions
            + report.excluded_sessions
            + report.outside_window_sessions
            + report.unknown_sessions
    );
}
#[test]
fn idle_pool_and_mars_keep_sessions_requests_native_connections_and_lifetime_totals_distinct() {
    for (case, requests, primary, unknown_primary, tokens) in
        [(RecentClientCase::IdlePool, 0, "1", "0", "0"), (RecentClientCase::Mars, 2, "0", "1", "2")]
    {
        let report = report(&read(case));
        let recent = &report.recent_clients;
        assert_eq!(recent.coverage, RecentClientPopulationCoverage::CompleteOriginalCensus);
        assert_eq!(recent.window_ms, 3_600_000);
        assert_eq!(recent.source.state, culprit_core::SourceState::Collected);
        assert_eq!(recent.groups.grouped_sessions, 1);
        let group = &recent.groups.entries[0];
        assert_eq!(group.observed_requests, requests);
        assert_eq!(value(group, "session_logical_reads"), Some("9007199254740993"));
        assert_eq!(value(group, "session_physical_reads"), Some("7"));
        assert_eq!(value(group, "session_writes"), Some("2"));
        assert_eq!(value(group, "session_age_ms"), Some("600000"));
        assert_eq!(value(group, "known_idle_primary_connections"), Some(primary));
        assert_eq!(value(group, "unconfirmed_primary_sessions"), Some(unknown_primary));
        assert_eq!(value(group, "request_connection_tokens"), Some(tokens));
        assert_eq!(value(group, "reported_login"), Some("shared-login"));
        assert_eq!(value(group, "native_tds_protocol"), Some("1946157060"));
        assert!(report.entries.iter().all(|row| row.activity.request.is_none()) || requests == 2);
        balanced(recent);
    }
}
#[test]
fn strict_native_completion_age_selects_the_hour_without_reinterpreting_offset_free_clocks_or_request_work() {
    let mut original = read(RecentClientCase::Boundary);
    let first = report(&original);
    assert_eq!((first.recent_clients.groups.grouped_sessions, first.recent_clients.outside_window_sessions), (1, 2));
    assert_eq!(first.recent_clients.groups.entries[0].observed_sessions, 1);
    assert_eq!(value(&first.recent_clients.groups.entries[0], "latest_end"), Some("2020-01-01T11:00:00.0010000"));
    let SourceObservation::Activity(native) = &mut original.source.observation else { unreachable!() };
    native.observed_from = "2020-01-01T18:00:00Z".into();
    native.at = "2020-01-01T18:00:00.0001Z".into();
    native.observed_until = "2020-01-01T18:00:00.0002Z".into();
    let changed = report(&original);
    assert_eq!(
        serde_json::to_value(first.recent_clients).unwrap(),
        serde_json::to_value(changed.recent_clients).unwrap()
    );
}
#[test]
fn reported_tool_prefixes_are_explicit_and_unknown_program_or_completion_never_becomes_a_recent_client() {
    let report = report(&read(RecentClientCase::ExcludedAndUnknown));
    let recent = report.recent_clients;
    assert_eq!(recent.reported_label_policy, "culprit-reported-client-tools-v1");
    assert_eq!(
        (recent.observed_sessions, recent.groups.grouped_sessions, recent.excluded_sessions, recent.unknown_sessions),
        (7, 1, 3, 3)
    );
    assert_eq!(recent.groups.entries[0].key.application.as_deref(), Some("customer-azdata-workload"));
    assert_eq!(recent.source.state, culprit_core::SourceState::Partial);
    assert!(recent.groups.limitations.contains(&t!("view.session_census.recent.limit.labels")));
    assert!(excluded_label("MICROSOFT SQL SERVER MANAGEMENT STUDIO - Query"));
    assert!(!excluded_label("customer-SqlServerExtension"));
    balanced(&recent);
}
#[test]
fn subset_partial_unverified_and_empty_populations_do_not_gain_full_recent_client_coverage() {
    for (case, expected, selected) in [
        (RecentClientCase::Subset, RecentClientPopulationCoverage::OriginalPopulationSubset, 1),
        (RecentClientCase::Partial, RecentClientPopulationCoverage::PartialOriginalCensus, 1),
        (RecentClientCase::Empty, RecentClientPopulationCoverage::CompleteOriginalCensus, 0),
        (RecentClientCase::EmptySubset, RecentClientPopulationCoverage::OriginalPopulationSubset, 0),
    ] {
        let report = report(&read(case));
        assert_eq!(report.recent_clients.coverage, expected);
        assert_eq!(report.recent_clients.groups.grouped_sessions, selected);
        assert_eq!(
            report.recent_clients.source.state,
            if matches!(case, RecentClientCase::Empty) {
                culprit_core::SourceState::Empty
            } else {
                culprit_core::SourceState::Partial
            }
        );
        balanced(&report.recent_clients);
    }
    let mut incomplete = read(RecentClientCase::IdlePool);
    incomplete.read_complete = false;
    let report = report(&incomplete);
    assert_eq!(report.recent_clients.coverage, RecentClientPopulationCoverage::UnverifiedOriginalCensus);
    assert_eq!((report.recent_clients.groups.grouped_sessions, report.recent_clients.unknown_sessions), (0, 1));
    assert!(report.recent_clients.groups.entries.is_empty());
}
#[test]
fn native_recent_context_conflict_withdraws_only_recent_interpretation_and_keeps_original_census_groups() {
    let report = report(&read(RecentClientCase::ConflictingMars));
    assert_eq!(report.client_groups.grouped_sessions, 1);
    assert_eq!(report.client_groups.entries[0].observed_requests, 2);
    assert_eq!((report.recent_clients.groups.grouped_sessions, report.recent_clients.unknown_sessions), (0, 1));
    assert_eq!(report.recent_clients.coverage, RecentClientPopulationCoverage::NativeRecencyUnavailable);
    assert_eq!(report.entries.len(), 2);
    assert_eq!(report.recent_clients.source.state, culprit_core::SourceState::Unavailable);
}
#[test]
fn missing_counters_login_and_protocol_preserve_exact_other_metrics_without_zero_or_a_representative() {
    let report = report(&read(RecentClientCase::UnknownCounters));
    let group = &report.recent_clients.groups.entries[0];
    assert_eq!(group.observed_sessions, 2);
    assert_eq!(value(group, "session_logical_reads"), None);
    assert_eq!(value(group, "session_physical_reads"), None);
    assert_eq!(value(group, "session_age_ms"), None);
    assert_eq!(value(group, "session_writes"), Some("2"));
    assert_eq!(value(group, "reported_login"), None);
    assert_eq!(value(group, "native_tds_protocol"), None);
    assert_eq!(value(group, "reported_login_labels"), Some("1"));
    assert_eq!(value(group, "unknown_login_sessions"), Some("1"));
    assert_eq!(value(group, "native_tds_codes"), Some("1"));
    assert_eq!(value(group, "unknown_tds_sessions"), Some("1"));
}
#[test]
fn native_backend_lifetimes_do_not_supply_mssql_last_completion_or_transport_provenance() {
    for case in [SessionCensusCase::PostgresSharedLabels, SessionCensusCase::MysqlShared] {
        let report = report(&original(session_census_evidence(case)));
        assert!(!report.client_groups.entries.is_empty());
        assert_eq!(report.recent_clients.coverage, RecentClientPopulationCoverage::NativeRecencyUnavailable);
        assert_eq!(report.recent_clients.groups.grouped_sessions, 0);
        assert_eq!(report.recent_clients.source.state, culprit_core::SourceState::Unavailable);
        assert_eq!(report.recent_clients.unknown_sessions, report.observed_sessions);
    }
}
#[test]
fn request_context_from_another_default_database_cannot_declare_its_idle_client_population_complete() {
    let mut original = read(RecentClientCase::Mars);
    for row in &mut original.observations {
        let Some(SessionNativeContext::Mssql(native)) =
            row.session_context.as_deref_mut().unwrap().native.as_deref_mut()
        else {
            unreachable!()
        };
        native.session_database_id = Some("4".into());
        native.session_database_name = Some("session-default".into());
    }
    let report = report(&original);
    assert_eq!(report.recent_clients.coverage, RecentClientPopulationCoverage::OriginalPopulationSubset);
    assert_eq!(report.recent_clients.groups.grouped_sessions, 1);
    assert_eq!(report.recent_clients.groups.entries[0].key.database.name(), Some("session-default"));
}
#[test]
fn recent_group_cap_preserves_available_counts_and_unknown_primary_connection_states() {
    let mut original = read(RecentClientCase::IdlePool);
    let first = original.observations[0].clone();
    original.observations = (0..MAX_PRESENTED_CLIENT_GROUPS + 1)
        .map(|index| {
            let mut row = first.clone();
            row.key.session.id = (61 + index).to_string();
            row.client.as_mut().unwrap().application = Some(format!("fixture-client-{index}"));
            let Some(SessionNativeContext::Mssql(native)) =
                row.session_context.as_deref_mut().unwrap().native.as_deref_mut()
            else {
                unreachable!()
            };
            native.idle_connection = Some(MssqlIdleSessionConnection::VisibilityUnavailable { permitted: None });
            row
        })
        .collect();
    let SourceObservation::Activity(native) = &mut original.source.observation else { unreachable!() };
    native.native_rows = original.observations.len() as u32;
    native.observations = native.native_rows;
    native.coverage.records = native.native_rows;
    let report = report(&original);
    let recent = report.recent_clients;
    assert_eq!(recent.groups.available_groups, (MAX_PRESENTED_CLIENT_GROUPS + 1) as u32);
    assert_eq!(recent.groups.grouped_sessions, recent.groups.available_groups);
    assert_eq!(recent.groups.entries.len(), MAX_PRESENTED_CLIENT_GROUPS);
    assert!(recent.groups.entries_limited);
    for group in &recent.groups.entries {
        assert_eq!(value(group, "known_idle_primary_connections"), Some("0"));
        assert_eq!(value(group, "unconfirmed_primary_sessions"), Some("1"));
    }
    balanced(&recent);
}

#[test]
fn unknown_member_process_or_database_role_cannot_make_a_complete_recent_population() {
    for missing in ["member", "epoch", "role"] {
        let mut original = read(RecentClientCase::IdlePool);
        match missing {
            "member" => {
                let SourcePartition::InstanceLocal { member, .. } = &mut original.source.partition else {
                    unreachable!()
                };
                *member = None;
            }
            "epoch" => {
                let SourcePartition::InstanceLocal { epoch, .. } = &mut original.source.partition else {
                    unreachable!()
                };
                *epoch = None;
                let SourceObservation::Activity(native) = &mut original.source.observation else { unreachable!() };
                native.epoch = None;
                native.coverage.complete_scope = false;
                native.coverage.state = culprit_core::SourceState::Partial;
            }
            "role" => {
                let SourceObservation::Activity(native) = &mut original.source.observation else { unreachable!() };
                native.database_writable = None;
            }
            _ => unreachable!(),
        }
        let SourceObservation::Activity(native) = &mut original.source.observation else { unreachable!() };
        native.coverage.complete_scope = false;
        native.coverage.state = culprit_core::SourceState::Partial;
        let report = report(&original);
        assert_eq!(
            report.recent_clients.coverage,
            RecentClientPopulationCoverage::UnverifiedOriginalCensus,
            "{missing}"
        );
        assert!(
            report.recent_clients.groups.limitations.contains(&t!("view.session_census.recent.coverage.unverified"))
        );
    }
}

#[test]
fn known_readonly_database_preserves_local_role_without_a_global_incarnation() {
    let mut readonly = read(RecentClientCase::IdlePool);
    let SourceObservation::Activity(native) = &mut readonly.source.observation else { unreachable!() };
    native.database_writable = Some(false);
    let report = report(&readonly);
    assert_eq!(report.context.database_writable, Some(false));
    assert!(report.context.target.database().unwrap().incarnation.is_none());
    assert!(report.context.has_qualified_local_owner());
    assert_eq!(report.recent_clients.coverage, RecentClientPopulationCoverage::CompleteOriginalCensus);
}
