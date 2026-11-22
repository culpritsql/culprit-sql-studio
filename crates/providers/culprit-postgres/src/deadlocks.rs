//! What blocked sessions wait for, and deadlocks. Postgres counts deadlocks per database but writes the details only to
//! the server log, which SQL can't read; Culprit shows the counter and explains pasted log lines.

use culprit_core::format::digits;
use culprit_core::{DeadlockReport, LockWait, MeasurementScope, Result, t};
use tokio_postgres::Client;

use crate::rows::{iso, query};

/// The lock each waiting backend asked for. A row-lock wait is a wait on another transaction's id; the table comes from
/// the tuple lock the waiter holds (or queues on), or else the table it is changing.
const LOCK_WAITS_SQL: &str = "
SELECT w.pid::text AS session, w.mode, w.locktype,
       coalesce(rel.name, tup.name, mod.name) AS object
  FROM pg_locks w
  LEFT JOIN LATERAL (SELECT n.nspname || '.' || c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE c.oid = w.relation) rel ON true
  LEFT JOIN LATERAL (SELECT n.nspname || '.' || c.relname AS name FROM pg_locks t
                       JOIN pg_class c ON c.oid = t.relation JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE t.pid = w.pid AND t.locktype = 'tuple' LIMIT 1) tup ON true
  LEFT JOIN LATERAL (SELECT n.nspname || '.' || c.relname AS name FROM pg_locks t
                       JOIN pg_class c ON c.oid = t.relation JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE t.pid = w.pid AND t.locktype = 'relation' AND t.mode = 'RowExclusiveLock' AND c.relkind IN ('r', 'p')
                        AND n.nspname NOT IN ('pg_catalog', 'information_schema') LIMIT 1) mod ON true
 WHERE NOT w.granted AND w.pid <> pg_backend_pid()";

pub(crate) async fn lock_waits(client: &Client) -> Result<Vec<LockWait>> {
    let mut out: Vec<LockWait> = Vec::new();
    for r in query(client, LOCK_WAITS_SQL).await? {
        let session = r.string("session");
        if out.iter().any(|l| l.session == session) {
            continue;
        }
        out.push(LockWait {
            session,
            mode: r.text("mode"),
            resource: r.text("locktype"),
            object: r.text("object"),
            index: None,
            native: None,
        });
    }
    Ok(out)
}

pub(crate) async fn recent(client: &mut impl crate::read::QueryRead, since_hours: f64) -> Result<DeadlockReport> {
    let row = crate::read::query_one(
        client,
        &format!(
            "SELECT deadlocks, {} AS since, current_setting('log_lock_waits') AS log_lock_waits
               FROM pg_stat_database WHERE datname = current_database()",
            iso("stats_reset")
        ),
    )
    .await?;
    let counted = row.as_ref().and_then(|r| r.number("deadlocks"));
    let since = row.as_ref().and_then(|r| r.text("since"));
    let log_waits = row.as_ref().and_then(|r| r.text("log_lock_waits")).is_some_and(|v| v == "on");
    let note = counted_note(counted, since.as_deref(), log_waits);
    Ok(DeadlockReport {
        scope: counted.map(|_| MeasurementScope::Database),
        deadlocks: Vec::new(),
        source: "pg_stat_database".into(),
        since_hours,
        counted,
        counted_since: since,
        note: Some(note),
        recent_only: false,
        lock_waits_logged: Some(log_waits),
    })
}

/// What the deadlock counter says, where the details are, and (with `log_lock_waits` off) how to log waits too.
fn counted_note(counted: Option<f64>, since: Option<&str>, log_waits: bool) -> String {
    let date = since.map(culprit_core::format::friendly_date_today);
    let count = |n: f64| digits(format!("{n:.0}"));
    let mut note = vec![
        match (counted, date) {
            (Some(n), None) => t!("provider.postgres.deadlocks.counted", n = n, count = count(n)),
            (Some(n), Some(date)) => {
                t!("provider.postgres.deadlocks.counted_since", n = n, count = count(n), date = date)
            }
            (None, None) => t!("provider.postgres.deadlocks.not_counted"),
            (None, Some(date)) => t!("provider.postgres.deadlocks.not_counted_since", date = date),
        },
        t!("provider.postgres.deadlocks.details"),
    ];
    if !log_waits {
        note.push(t!("provider.postgres.deadlocks.log_lock_waits"));
    }
    note.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // What the counter's note says, word for word.
    #[test]
    fn the_counter_note_in_english() {
        let details = "It writes the details (who waited for whom, and the statements) only to the server log, which \
                       SQL can't read: paste those log lines here to have them explained.";
        let log = "Turning on log_lock_waits also logs any lock wait longer than deadlock_timeout.";
        assert_eq!(
            counted_note(Some(3.0), None, true),
            format!("PostgreSQL counted 3 deadlocks in this database. {details}")
        );
        assert_eq!(
            counted_note(Some(1.0), Some("2024-10-01T00:00:00Z"), false),
            format!(
                "PostgreSQL counted 1 deadlock in this database since statistics were reset on Oct 1, 2024. {details} {log}"
            )
        );
        assert_eq!(
            counted_note(Some(0.0), Some("2024-10-01T00:00:00Z"), true),
            format!(
                "PostgreSQL counted 0 deadlocks in this database since statistics were reset on Oct 1, 2024. {details}"
            )
        );
        assert_eq!(
            counted_note(None, None, false),
            format!("PostgreSQL counted no deadlocks in this database. {details} {log}")
        );
        assert_eq!(
            counted_note(None, Some("2024-10-01T00:00:00Z"), true),
            format!(
                "PostgreSQL counted no deadlocks in this database since statistics were reset on Oct 1, 2024. {details}"
            )
        );
    }
}
