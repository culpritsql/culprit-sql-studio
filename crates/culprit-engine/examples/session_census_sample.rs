//! Controlled native-shaped cases through the actual pure view; no live owner or source-cost claim.
use culprit_core::telemetry::*;
use culprit_evidence::analytical::test_support::session_census_evidence;
use culprit_evidence::analytical::test_support::{RecentClientCase, SessionCensusCase, recent_client_evidence};
use std::{collections::BTreeMap, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    culprit_core::i18n::scope("en", generate)
}
fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let mut cases = BTreeMap::new();
    let inputs = [
        ("idle", SessionCensusCase::Idle),
        ("idle-connection", SessionCensusCase::IdleConnection),
        ("idle-connection-missing", SessionCensusCase::IdleConnectionMissing),
        ("idle-connection-scope-unavailable", SessionCensusCase::IdleConnectionScopeUnavailable),
        ("idle-connection-ambiguous", SessionCensusCase::IdleConnectionAmbiguous),
        ("idle-connection-visibility-unknown", SessionCensusCase::IdleConnectionVisibilityUnknown),
        ("idle-connection-visibility-denied", SessionCensusCase::IdleConnectionVisibilityDenied),
        ("dormant", SessionCensusCase::Dormant),
        ("mars", SessionCensusCase::Mars),
        ("unknown", SessionCensusCase::Unknown),
        ("partial", SessionCensusCase::Partial),
        ("unavailable", SessionCensusCase::Unavailable),
        ("empty", SessionCensusCase::Empty),
        ("client-groups", SessionCensusCase::GroupedClients),
        ("group-conflict", SessionCensusCase::ContradictoryMars),
        ("group-unknown", SessionCensusCase::GroupUnknownTotals),
        ("signed-elapsed", SessionCensusCase::NativeSignedElapsed),
        ("postgres-shared", SessionCensusCase::PostgresSharedLabels),
        ("postgres-worker", SessionCensusCase::PostgresWorker),
        ("mysql-shared", SessionCensusCase::MysqlShared),
        ("mysql-switched", SessionCensusCase::MysqlSwitched),
        ("mysql-worker", SessionCensusCase::MysqlWorker),
    ]
    .into_iter()
    .map(|(name, case)| (name, session_census_evidence(case)))
    .chain(RecentClientCase::ALL.into_iter().map(|case| (case.name(), recent_client_evidence(case))));
    for (name, (source, rows)) in inputs {
        let observations = rows
            .into_iter()
            .map(|row| match row {
                TelemetryRecord::Activity(row) => Ok(*row),
                _ => Err(DeliveryProblem::Integrity.error()),
            })
            .collect::<culprit_core::Result<Vec<_>>>()?;
        let read = ActivityCensusRead::new(source, observations, true)?;
        let report = culprit_checks::session_census::explain(&read)?;
        cases.insert(name, serde_json::json!({ "read": read, "report": report }));
    }
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/session-census.sample.json"));
    std::fs::write(path, serde_json::to_string_pretty(&cases)? + "\n")?;
    Ok(())
}
