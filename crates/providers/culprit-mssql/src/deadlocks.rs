//! What blocked sessions wait for, and recent deadlocks: from configured system_health event files, or on Azure SQL
//! Database from an explicitly configured database-scoped event session. Server target-data views are never read. The parser also
//! reads saved .xdl files; XML with a DTD is refused, so entities never resolve.

use culprit_core::{DeadlockReport, LockWait, MeasurementScope, Result, t};

use crate::read::rows as query;
use crate::read::{NativeRead, ReadError, integer, invalid_data, text};
mod database;
pub(crate) mod native;
pub(crate) mod system_health;
#[cfg(test)]
pub(crate) use database::tests as database_test_support;

/// The lock each waiting task asked for. Objects in other databases are named only for whole-object locks; row and
/// page locks resolve through this database's partitions.
const LOCK_WAITS_SQL: &str = r#"
SELECT
  CAST(wt.session_id AS varchar(10)) AS session_id,
  l.request_mode AS mode,
  l.resource_type AS resource,
  CASE
    WHEN l.resource_type = 'OBJECT'
      THEN OBJECT_SCHEMA_NAME(l.resource_associated_entity_id, l.resource_database_id) + '.'
         + OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id)
    WHEN p.object_id IS NOT NULL THEN OBJECT_SCHEMA_NAME(p.object_id) + '.' + OBJECT_NAME(p.object_id)
  END AS object_name,
  i.name AS index_name
FROM sys.dm_os_waiting_tasks wt
JOIN sys.dm_tran_locks l ON l.lock_owner_address = wt.resource_address
LEFT JOIN sys.partitions p
  ON l.resource_type IN ('KEY', 'PAGE', 'RID', 'HOBT') AND l.resource_database_id = DB_ID()
 AND p.hobt_id = l.resource_associated_entity_id
LEFT JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE wt.session_id IS NOT NULL AND wt.session_id <> @@SPID
"#;

pub(crate) async fn lock_waits(client: &mut impl NativeRead) -> Result<Vec<LockWait>> {
    let rows = query(client, LOCK_WAITS_SQL).await?;
    let mut out: Vec<LockWait> = Vec::new();
    for r in rows {
        let session = r.string("session_id");
        // A parallel request has one waiting task per thread; one lock per session is enough to explain it.
        if out.iter().any(|l| l.session == session) {
            continue;
        }
        out.push(LockWait {
            session,
            mode: r.str("mode"),
            resource: r.str("resource"),
            object: r.str("object_name"),
            index: r.str("index_name"),
            native: None,
        });
    }
    Ok(out)
}

const READ_LIMIT: usize = 50;

/// The explicitly configured, database-scoped source, as a person reads it.
pub(crate) fn database_session() -> String {
    t!("provider.mssql.deadlocks.database_session")
}

pub(crate) fn database_session_unreadable(error: &dyn std::fmt::Display) -> String {
    t!("provider.mssql.deadlocks.database_session_unreadable", error = error.to_string())
}

/// The supported server-scoped configured-file source, named independently of its availability.
pub(crate) fn system_health_files() -> String {
    t!("provider.mssql.deadlocks.origin")
}

/// Retained deadlocks in the last `since_hours`. SQL Server needs an absolute configured system_health file path;
/// Azure SQL Database uses its explicit database event session. Unavailable configuration remains a note and unknown
/// scope, without a runtime-target or server ring fallback. Neither source proves a complete retention horizon.
pub(crate) async fn recent<R: NativeRead>(io: &mut R, since_hours: f64) -> Result<DeadlockReport> {
    let hours = since_hours.clamp(1.0, 24.0 * 30.0).round() as u32;
    let mut report = DeadlockReport { since_hours: hours as f64, ..Default::default() };
    let server = io.query(native::SERVER, vec![], 1).await.map_err(ReadError::into_error)?;
    let server = server.first().ok_or_else(invalid_data)?;
    let database = text(server, "db")?;
    let until = culprit_core::telemetry::canonical_time(&text(server, "observed_at")?)?;
    let now = chrono::DateTime::parse_from_rfc3339(&until).map_err(|_| invalid_data())?;
    let cutoff = now - chrono::Duration::hours(i64::from(hours));
    let from = cutoff.to_rfc3339();
    if integer(server, "edition")? == 5 {
        report.source = database_session();
        match native::read(io, native::Origin::DatabaseSession, &database, &from, &until, READ_LIMIT, true).await {
            Ok(events) => {
                report.scope = Some(MeasurementScope::Database);
                report.deadlocks.extend(events.into_iter().flat_map(|event| event.graphs));
                report.note = Some(t!("provider.mssql.deadlocks.database_session_recent"));
            }
            Err(error) => {
                report.note = Some(database_session_unreadable(&error));
            }
        }
        report.deadlocks.sort_by(|a, b| b.at.cmp(&a.at));
        report.recent_only = true;
        return Ok(report);
    }
    report.source = system_health_files();
    match native::read(io, native::Origin::SystemHealth, &database, &from, &until, READ_LIMIT, true).await {
        Ok(events) => {
            report.scope = Some(MeasurementScope::Server);
            report.deadlocks.extend(events.into_iter().flat_map(|event| event.graphs));
            report.deadlocks.sort_by(|a, b| b.at.cmp(&a.at));
            report.deadlocks.truncate(READ_LIMIT);
            report.note = Some(t!("provider.mssql.deadlocks.configured_file_recent"));
            report.recent_only = true;
        }
        Err(_) => {
            report.note = Some(t!("provider.mssql.deadlocks.configured_file_unavailable"));
        }
    }
    Ok(report)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        read::ReadResult,
        resources::test_support::FakeRead,
        value::{Rec, Val},
    };

    use culprit_plans::mssql::test_support::deadlocks::event;

    pub(crate) fn file_row(at: &str, xml: &str) -> Rec {
        Rec::of(&[
            ("file_name", Val::Text("/var/opt/mssql/log/system_health_0_134388425128000000.xel".into())),
            ("file_offset", Val::Int(512)),
            ("at", Val::Text(at.into())),
            ("event_xml", Val::Text(xml.into())),
        ])
    }

    fn scan_configuration() -> Rec {
        crate::resources::test_support::replace(
            &system_health::tests::configuration(),
            &[("observed_at", Val::Text("2026-10-11T12:10:00Z".into()))],
        )
    }

    fn recent_reader(file_events: ReadResult<Vec<Rec>>, final_configuration: Rec) -> FakeRead {
        FakeRead::new(vec![
            Ok(vec![Rec::of(&[
                ("edition", Val::Int(3)),
                ("db", Val::Text("shop".into())),
                ("observed_at", Val::Text("2026-10-11T12:10:00Z".into())),
            ])]),
            Ok(vec![scan_configuration()]),
            file_events,
            Ok(vec![final_configuration]),
        ])
    }

    #[tokio::test]
    async fn configured_file_history_keeps_fractional_events_and_unresolved_membership_without_database_grading() {
        let a = "2026-10-11T12:05:01.1234567Z";
        let b = "2026-10-11T12:05:01.1234568Z";
        let unnamed = event(b).replace(r#" currentdbname="shop""#, "");
        let mut io = recent_reader(Ok(vec![file_row(a, &event(a)), file_row(b, &unnamed)]), scan_configuration());
        let report = recent(&mut io, 1.0).await.unwrap();
        assert_eq!(report.scope, Some(MeasurementScope::Server));
        assert_eq!(report.deadlocks.len(), 2);
        assert_ne!(report.deadlocks[0].id, report.deadlocks[1].id);
        assert!(
            report.deadlocks.iter().any(|graph| { graph.processes.iter().all(|process| process.database.is_none()) })
        );
        assert!(report.recent_only && report.note.is_some());
        assert_eq!(io.calls.len(), 4);
        assert!(
            io.calls
                .iter()
                .all(|call| !call.sql.contains("dm_xe_session_targets") && !call.sql.contains("target_data"))
        );
        assert_eq!(io.closed, 0, "scan caller retains close ownership");
    }

    #[tokio::test]
    async fn database_scoped_session_owns_report_scope_even_when_graph_database_names_are_missing() {
        use super::database::tests::{configuration, event, target};
        let server = Rec::of(&[
            ("edition", Val::Int(5)),
            ("db", Val::Text("shop".into())),
            ("observed_at", Val::Text("2020-01-01T07:00:00Z".into())),
        ]);
        let mut io =
            FakeRead::new(vec![Ok(vec![server.clone()]), Ok(vec![configuration()]), Ok(vec![target(&[event(1)])])]);
        let report = recent(&mut io, 1.0).await.unwrap();
        assert_eq!(report.scope, Some(MeasurementScope::Database));
        assert_eq!(report.deadlocks.len(), 1);
        assert!(report.deadlocks[0].processes.iter().all(|process| process.database.is_none()));
        assert_eq!(io.calls.len(), 3);
        assert_eq!(io.closed, 0, "the caller still owns close");

        let mut io = FakeRead::new(vec![Ok(vec![server]), Err(invalid_data().into())]);
        let unavailable = recent(&mut io, 1.0).await.unwrap();
        assert_eq!(unavailable.scope, None);
        assert!(unavailable.deadlocks.is_empty());
        assert!(unavailable.note.is_some());
        assert_eq!(io.calls.len(), 2, "an unavailable database source has no server-wide fallback");
    }

    #[tokio::test]
    async fn unavailable_relative_or_changed_file_configuration_does_not_claim_empty_server_history() {
        use crate::resources::test_support::replace;
        for configuration in
            [vec![], vec![replace(&scan_configuration(), &[("file_path", Val::Text("system_health.xel".into()))])]]
        {
            let mut io = FakeRead::new(vec![
                Ok(vec![Rec::of(&[
                    ("edition", Val::Int(3)),
                    ("db", Val::Text("shop".into())),
                    ("observed_at", Val::Text("2026-10-11T12:10:00Z".into())),
                ])]),
                Ok(configuration),
            ]);
            let report = recent(&mut io, 1.0).await.unwrap();
            assert_eq!(report.scope, None);
            assert!(report.deadlocks.is_empty() && report.note.is_some());
            assert_eq!(io.calls.len(), 2, "no ring/runtime path fallback");
        }
        let mut io = recent_reader(Ok(vec![]), replace(&scan_configuration(), &[("event_session_id", Val::Int(2))]));
        let changed = recent(&mut io, 1.0).await.unwrap();
        assert_eq!(changed.scope, None);
        assert!(changed.deadlocks.is_empty() && changed.note.is_some());
        assert_eq!(io.calls.len(), 4);
    }

    #[tokio::test]
    async fn successful_empty_file_history_has_observed_server_scope_without_proving_retention() {
        let mut io = recent_reader(Ok(vec![]), scan_configuration());
        let report = recent(&mut io, 1.0).await.unwrap();
        assert_eq!(report.scope, Some(MeasurementScope::Server));
        assert!(report.deadlocks.is_empty() && report.recent_only && report.note.is_some());
        assert_eq!(io.calls.len(), 4);
    }
}

#[cfg(test)]
mod native_tests;
