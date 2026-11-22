//! What blocked sessions wait for, and the latest deadlock InnoDB remembers (`SHOW ENGINE INNODB STATUS`, which needs
//! PROCESS). InnoDB prints the locked rows' contents too; Culprit keeps only tables, indexes and lock modes.

use std::collections::{HashMap, HashSet};

use culprit_core::{DeadlockReport, LockWait, MeasurementScope, Result, t};
use mysql_async::Value;

use crate::rows::{query, query_one, query_with};
use crate::{connect::Flavor, native::locks};
use culprit_plans::mysql::deadlocks::{local_time, parse_at};
pub(crate) const LATEST_SQL: &str = "SHOW ENGINE INNODB STATUS";

pub(crate) struct BlockingEvidence {
    pub blockers: HashMap<String, String>,
    pub locks: Vec<LockWait>,
    pub resources: HashMap<String, Vec<culprit_core::ObservedWaitResource>>,
    pub notes: Vec<String>,
}

pub(crate) async fn blocking_evidence(conn: &mut impl crate::read::QueryRead, flavor: Flavor) -> BlockingEvidence {
    let mut coverage = culprit_core::Coverage::default();
    let mut rows = coverage.read(&crate::sources::LIVE_LOCKS, wait_pairs(conn, flavor).await).unwrap_or_default();
    rows.extend(
        coverage
            .read(
                &crate::sources::METADATA_LOCKS,
                query(conn, locks::METADATA_LOCK_WAITS_SQL).await.and_then(locks::decode),
            )
            .unwrap_or_default(),
    );
    if let Some(Some(instrument)) = coverage.read(&crate::sources::METADATA_LOCKS, query_one(conn, "SELECT ENABLED AS enabled FROM performance_schema.setup_instruments WHERE NAME = 'wait/lock/metadata/sql/mdl'").await)
        && instrument.text("enabled").as_deref() != Some("YES")
    {
        coverage.note("metadata_locks", metadata_instrument_off());
    }
    let pairs = rows.iter().map(|row| (row.session.to_string(), row.blocking.to_string())).collect::<Vec<_>>();
    let locks = rows.iter().map(locks::LockPair::lock_wait).collect();
    let mut resources: HashMap<String, Vec<culprit_core::ObservedWaitResource>> = HashMap::new();
    for row in rows {
        if let Some(resource) = row.identified_resource {
            let identified = resources.entry(row.session.to_string()).or_default();
            if !identified.contains(&resource) {
                identified.push(resource);
            }
        }
    }
    BlockingEvidence {
        blockers: direct_blockers(&pairs),
        locks,
        resources,
        notes: coverage.finish().2.into_iter().map(|n| format!("{}: {}", n.collector, n.message)).collect(),
    }
}

/// Every (waiting, blocking) pair with what the waiter asked for, from whichever lock views this server has.
async fn wait_pairs(conn: &mut impl crate::read::QueryRead, flavor: Flavor) -> Result<Vec<locks::LockPair>> {
    let version = query_one(conn, "SELECT VERSION() AS version").await?.ok_or_else(crate::native::invalid)?;
    let version = crate::native::text(&version, "version")?;
    locks::decode(query(conn, locks::row_waits(flavor, &version).sql).await?)
}

/// A waiter queued behind several sessions is reported once per session ahead of it. The one it waits on directly is
/// the session that is itself waiting (next in the queue) if there is one, so chains read in queue order.
fn direct_blockers(pairs: &[(String, String)]) -> HashMap<String, String> {
    let waiting: HashSet<&str> = pairs.iter().map(|(session, _)| session.as_str()).collect();
    let mut out: HashMap<String, String> = HashMap::new();
    for (session, blocking) in pairs {
        if session.is_empty() || blocking.is_empty() || session == blocking {
            continue;
        }
        match out.get(session) {
            Some(current) if waiting.contains(current.as_str()) => {}
            _ => {
                out.insert(session.clone(), blocking.clone());
            }
        }
    }
    out
}

pub(crate) async fn lock_waits(conn: &mut impl crate::read::QueryRead, flavor: Flavor) -> Result<Vec<LockWait>> {
    Ok(blocking_evidence(conn, flavor).await.locks)
}

/// The most recent statement of each sleeping connection (performance_schema keeps it after it finished). MariaDB
/// turns the current-statement consumer off by default; the per-thread history, when on, has it too.
pub(crate) async fn last_statements(conn: &mut impl crate::read::QueryRead) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for sql in [
        "SELECT t.PROCESSLIST_ID AS id, s.SQL_TEXT AS sql_text
           FROM performance_schema.events_statements_current s
           JOIN performance_schema.threads t ON t.THREAD_ID = s.THREAD_ID
          WHERE t.PROCESSLIST_COMMAND = 'Sleep' AND s.SQL_TEXT IS NOT NULL",
        "SELECT t.PROCESSLIST_ID AS id, s.SQL_TEXT AS sql_text
           FROM performance_schema.events_statements_history s
           JOIN performance_schema.threads t ON t.THREAD_ID = s.THREAD_ID
          WHERE t.PROCESSLIST_COMMAND = 'Sleep' AND s.SQL_TEXT IS NOT NULL
          ORDER BY s.THREAD_ID, s.EVENT_ID DESC",
    ] {
        for r in query(conn, sql).await.unwrap_or_default() {
            if let (Some(id), Some(text)) = (
                r.text("id"),
                r.text("sql_text").filter(|t| {
                    !culprit_core::telemetry::StatementOrigin::from_sql(Some(t), culprit_core::Engine::Mysql)
                        .0
                        .is_culprit()
                }),
            ) {
                out.entry(id).or_insert(text);
            }
        }
    }
    out
}

pub(crate) async fn recent(conn: &mut impl crate::read::QueryRead, since_hours: f64) -> Result<DeadlockReport> {
    let mut report = DeadlockReport { source: "InnoDB status".into(), since_hours, ..Default::default() };
    let status = query_one(conn, LATEST_SQL)
        .await
        .ok()
        .flatten()
        .and_then(|row| row.text("Status").or_else(|| row.text("status")))
        .filter(|status| !status.trim().is_empty());
    let Some(status) = status else {
        report.note = Some(needs_process_privilege());
        return Ok(report);
    };
    report.scope = Some(MeasurementScope::Server);
    // InnoDB prints the time in the server's own (system) time zone; the server converts it to UTC, daylight saving
    // included. When it can't, the deadlock stays undated: never guessed, and never counted in a window.
    let at = match local_time(&status) {
        Some(local) => query_with(
            conn,
            "SELECT DATE_FORMAT(CONVERT_TZ(?, 'SYSTEM', '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS at",
            vec![Value::from(local)],
        )
        .await
        .ok()
        .and_then(|rows| rows.first().and_then(|r| r.text("at"))),
        None => None,
    };
    // RFC 3339 UTC times compare as text.
    let cutoff = query_one(
        conn,
        &format!(
            "SELECT DATE_FORMAT(UTC_TIMESTAMP() - INTERVAL {} SECOND, '%Y-%m-%dT%H:%i:%sZ') AS cutoff",
            (since_hours * 3600.0).round() as i64
        ),
    )
    .await
    .ok()
    .flatten()
    .and_then(|r| r.text("cutoff"));
    let mut found = parse_at(&status, at);
    found.retain(|d| match (d.at.as_deref(), cutoff.as_deref()) {
        (Some(at), Some(cutoff)) => at >= cutoff,
        _ => true,
    });
    let undated = found.iter().any(|d| d.at.is_none());
    report.deadlocks = found;
    report.note = Some(only_the_latest(undated));
    report.recent_only = true;
    Ok(report)
}

fn metadata_instrument_off() -> String {
    t!("provider.mysql.locks.metadata_instrument_off")
}

fn needs_process_privilege() -> String {
    t!("provider.mysql.deadlocks.needs_process")
}

/// InnoDB keeps one deadlock; `undated`: its time couldn't be converted from the server's time zone.
fn only_the_latest(undated: bool) -> String {
    if undated {
        t!("provider.mysql.deadlocks.only_the_latest_undated")
    } else {
        t!("provider.mysql.deadlocks.only_the_latest")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Rec;

    #[tokio::test]
    async fn failed_or_missing_native_status_has_no_measurement_scope_or_followup_read() {
        use crate::read::test_support::Read;
        for reply in [
            Err(culprit_core::Error::invalid("native status is unavailable")),
            Ok(vec![]),
            Ok(vec![Rec::from_values(vec![("Status", Value::NULL)])]),
            Ok(vec![Rec::from_values(vec![("Status", Value::from(" "))])]),
        ] {
            let mut io = Read::with(vec![reply]);
            let report = recent(&mut io, 24.0).await.unwrap();
            assert_eq!(report.scope, None);
            assert!(report.deadlocks.is_empty());
            assert!(report.note.is_some());
            assert_eq!(io.queries.len(), 1);
            assert_eq!(io.closed, 0, "the caller retains lifecycle ownership");
        }
    }

    #[tokio::test]
    async fn successful_native_status_keeps_server_scope_even_without_a_retained_graph() {
        use crate::read::test_support::Read;
        let mut io = Read::with(vec![
            Ok(vec![Rec::from_values(vec![("Status", Value::from("INNODB MONITOR OUTPUT"))])]),
            Ok(vec![]),
        ]);
        let report = recent(&mut io, 24.0).await.unwrap();
        assert_eq!(report.scope, Some(MeasurementScope::Server));
        assert!(!report.has_database_scope());
        assert!(report.deadlocks.is_empty());
        assert!(report.recent_only);
        assert_eq!(io.queries.len(), 2);
        assert_eq!(io.closed, 0);
    }

    #[test]
    fn deadlock_notes_and_lock_modes_say_what_they_are_in_english() {
        assert_eq!(
            only_the_latest(false),
            "InnoDB keeps only its most recent deadlock. Turn on innodb_print_all_deadlocks to write every one to the \
             error log."
        );
        assert_eq!(
            only_the_latest(true),
            "InnoDB keeps only its most recent deadlock, and Culprit couldn't convert its time from the server's time \
             zone, so when it happened is unknown (it may be from long ago). Turn on innodb_print_all_deadlocks to \
             write every one to the error log."
        );
        assert_eq!(
            needs_process_privilege(),
            "Reading InnoDB's deadlock report needs the PROCESS privilege. Paste the LATEST DETECTED DEADLOCK section of \
             SHOW ENGINE INNODB STATUS to have it explained."
        );
        assert_eq!(
            metadata_instrument_off(),
            "Metadata-lock instrumentation is disabled; absence of a chain does not establish absence of metadata \
             blocking."
        );
    }

    #[test]
    fn queued_waiters_point_at_the_session_ahead_of_them() {
        let pairs: Vec<(String, String)> =
            [("79", "78"), ("79", "77"), ("78", "77")].iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
        let direct = direct_blockers(&pairs);
        assert_eq!(direct.get("79").map(String::as_str), Some("78"), "79 waits behind 78, which waits behind 77");
        assert_eq!(direct.get("78").map(String::as_str), Some("77"));
    }
}
