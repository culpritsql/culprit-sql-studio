use super::*;
use crate::rows::Row;

fn identity_row(user: &str, since: &str, started: &str, own: bool) -> Row {
    let fields = [
        ("id", format!("17/{user}/9/true/{since}/{started}")),
        ("queryid", "17".into()),
        ("userid", user.into()),
        ("dbid", "9".into()),
        ("toplevel", "true".into()),
        ("stats_since", since.into()),
        ("postmaster_started_at", started.into()),
        ("query", "SELECT original_entry FROM native_table".into()),
        ("calls", "4".into()),
        ("total_ms", "20".into()),
    ];
    let mut fields = fields.into_iter().map(|(key, value)| (key.into(), Some(value))).collect::<Vec<_>>();
    fields.push(("own_query".into(), own.then(|| "SELECT original_entry FROM native_table".into())));
    Row::from_fields(fields)
}

#[test]
fn same_queryid_under_another_user_reset_or_process_remains_a_separate_native_query() {
    let since = "2026-01-01T10:00:00Z";
    let started = "2026-01-01T00:00:00Z";
    let original = query_stat(&identity_row("41", since, started, true)).unwrap();
    for row in [
        identity_row("42", since, started, false),
        identity_row("41", "2026-01-01T11:00:00Z", started, true),
        identity_row("41", since, "2026-01-01T01:00:00Z", true),
    ] {
        let other = query_stat(&row).unwrap();
        assert_ne!(original.id, other.id);
        assert_ne!(original.native_identity, other.native_identity);
        assert_eq!(original.text, other.text);
        assert_eq!(original.executions, 4.0);
        assert_eq!(other.executions, 4.0);
    }
}

#[test]
fn counter_start_is_not_the_first_execution_and_own_text_does_not_rebind_another_role() {
    let own = identity_row("41", "2026-01-01T10:00:00Z", "2026-01-01T00:00:00Z", true);
    let other = identity_row("42", "2026-01-01T10:00:00Z", "2026-01-01T00:00:00Z", false);
    let rows = [own, other];
    let own_texts = rows
        .iter()
        .filter_map(|row| Some((row.string("id"), row.text("own_query")?)))
        .collect::<std::collections::HashMap<_, _>>();
    let original = query_stat(&rows[0]).unwrap();
    let foreign = query_stat(&rows[1]).unwrap();
    assert_eq!(original.first_seen, None);
    assert_eq!(original.last_seen, None);
    assert!(own_texts.contains_key(&original.id));
    assert!(!own_texts.contains_key(&foreign.id));
    assert_eq!(original.native_identity.as_ref().unwrap().qualifiers["stats_since"], "2026-01-01T10:00:00.000000000Z");
}

#[test]
fn source_versions_keep_only_dimensions_the_native_layout_really_exposes() {
    let old = statements_base("public.pg_stat_statements", 120_000);
    assert!(old.contains("GROUP BY queryid, userid, dbid"));
    assert!(!old.contains("GROUP BY queryid, userid, dbid, toplevel"));
    assert!(!old.contains("minmax_stats_since"));
    assert!(old.contains("NULL::text AS stats_since"));
    let modern = statements_base("public.pg_stat_statements", 180_000);
    assert!(modern.contains("GROUP BY queryid, userid, dbid, toplevel, stats_since"));
    assert!(modern.contains("bool_and(minmax_stats_since = stats_since)"));
    assert!(modern.contains("pg_postmaster_start_time()"));
    assert!(!modern.contains("AS first_seen"));
    // Family members use the same per-entry id as workload/own-plan rows, never the bare numeric queryid.
    assert!(families_sql(&modern).contains("array_agg(id::text ORDER BY total_ms DESC)"));
}
