use super::*;
use culprit_core::{ErrorCode, telemetry::CollectionStop};
use std::collections::{BTreeMap, VecDeque};

fn identity() -> NativeQueryIdentity {
    NativeQueryIdentity {
        query_id: "-777".into(),
        qualifiers: BTreeMap::from([
            ("userid".into(), "10".into()),
            ("dbid".into(), "42".into()),
            ("postmaster_started_at".into(), "2026-11-21T00:00:00.000001Z".into()),
            ("toplevel".into(), "true".into()),
            ("stats_since".into(), "2026-11-21T00:00:00.000002Z".into()),
        ]),
    }
}
fn expected(identity: &NativeQueryIdentity) -> ExpectedEntry {
    ExpectedEntry::from_statement(RecordedStatement::new(
        "SELECT untrusted_input()",
        "opaque/composite/id",
        Some(identity),
    ))
    .unwrap()
    .unwrap()
}
fn current_fields(identity: &NativeQueryIdentity) -> Vec<(String, Option<String>)> {
    let mut fields: Vec<_> =
        identity.qualifiers.iter().map(|(key, value)| (key.clone(), Some(value.clone()))).collect();
    fields.extend([
        ("queryid".into(), Some(identity.query_id.clone())),
        ("query".into(), Some("SELECT native_own_copy($1)".into())),
        ("own_query".into(), Some("t".into())),
    ]);
    fields
}
fn current_row(identity: &NativeQueryIdentity) -> Row {
    Row::from_fields(current_fields(identity))
}
fn catalog(top: bool, since: bool) -> Row {
    Row::from_fields([
        ("schema".into(), Some("actual\"extension".into())),
        ("has_toplevel".into(), Some(if top { "t" } else { "f" }.into())),
        ("has_stats_since".into(), Some(if since { "t" } else { "f" }.into())),
    ])
}

#[test]
fn full_native_entry_returns_only_the_native_text_and_uses_bound_values() {
    let identity = identity();
    let expected = expected(&identity);
    let row = current_row(&identity);
    assert_eq!(expected.text(&row, true).unwrap(), "SELECT native_own_copy($1)");
    let columns = NativeColumns::from_row(&catalog(true, true), &expected).unwrap();
    let sql = columns.query();
    for predicate in [
        "s.queryid=(($1::text[])[1])::bigint",
        "s.userid=(($1::text[])[2])::oid",
        "s.dbid=(($1::text[])[3])::oid",
        "pg_catalog.pg_postmaster_start_time()=(($1::text[])[4])::timestamptz",
        "s.toplevel=(($1::text[])[5])::boolean",
        "s.stats_since=(($1::text[])[6])::timestamptz",
        "datname=pg_catalog.current_database()",
        "current_user=session_user",
        "LIMIT 2",
    ] {
        assert!(sql.contains(predicate), "{predicate}");
    }
    assert!(sql.contains("\"actual\"\"extension\".pg_stat_statements"));
    assert!(!sql.contains("untrusted_input") && !sql.contains("opaque/composite") && !sql.contains("-777"));
    assert_eq!(expected.parameters()[0], identity.query_id);
}
#[test]
fn changed_query_user_database_top_reset_or_process_cannot_rebind_a_recorded_entry() {
    let original = identity();
    let expected = expected(&original);
    for field in ["queryid", "userid", "dbid", "toplevel", "stats_since", "postmaster_started_at"] {
        let mut changed = original.clone();
        match field {
            "queryid" => changed.query_id = "-778".into(),
            "userid" => {
                changed.qualifiers.insert(field.into(), "11".into());
            }
            "dbid" => {
                changed.qualifiers.insert(field.into(), "43".into());
            }
            "toplevel" => {
                changed.qualifiers.insert(field.into(), "false".into());
            }
            _ => {
                changed.qualifiers.insert(field.into(), "2026-11-21T00:00:00.000003Z".into());
            }
        }
        assert!(expected.text(&current_row(&changed), false).is_err(), "{field}");
    }
}
#[test]
fn every_available_native_dimension_is_required_without_inventing_old_catalog_fields() {
    let original = identity();
    for field in ["toplevel", "stats_since"] {
        let mut missing = original.clone();
        missing.qualifiers.remove(field);
        assert!(NativeColumns::from_row(&catalog(true, true), &expected(&missing)).is_err(), "{field}");
    }
    assert!(NativeColumns::from_row(&catalog(false, false), &expected(&original)).is_err());
    let mut old = original;
    old.qualifiers.remove("toplevel");
    old.qualifiers.remove("stats_since");
    let expected = expected(&old);
    assert!(NativeColumns::from_row(&catalog(false, false), &expected).is_ok());
    assert!(expected.text(&current_row(&old), false).is_ok());
    assert!(expected.text(&current_row(&old), true).is_err(), "unknown top level cannot authorize an elevated planner");
}
#[test]
fn elevated_planning_requires_the_authenticated_top_level_copy_and_visible_text() {
    let original = identity();
    let expected = expected(&original);
    let foreign = Row::from_fields(current_fields(&original).into_iter().map(|(key, value)| {
        let value = if key == "own_query" { Some("f".into()) } else { value };
        (key, value)
    }));
    assert!(expected.text(&foreign, true).is_err());
    assert!(expected.text(&foreign, false).is_ok());
    for text in [None, Some(""), Some("<insufficient privilege>")] {
        let unavailable = Row::from_fields(current_fields(&original).into_iter().map(|(key, value)| {
            let value = if key == "query" { text.map(str::to_owned) } else { value };
            (key, value)
        }));
        assert!(expected.text(&unavailable, true).is_err());
    }
}
#[test]
fn null_native_fields_and_row_shape_errors_never_become_unknown_scope_substitutes() {
    let original = identity();
    let expected = expected(&original);
    let values: Vec<_> = ENTRY_FIELDS
        .iter()
        .map(|name| {
            if *name == "queryid" {
                Some(original.query_id.clone())
            } else if *name == "query" {
                Some("SELECT 1".into())
            } else if *name == "own_query" {
                Some("t".into())
            } else {
                original.qualifiers.get(*name).cloned()
            }
        })
        .collect();
    for index in [1, 2, 3, 4, 5] {
        let mut missing = values.clone();
        missing[index] = None;
        assert!(expected.text(&entry_row(missing).unwrap(), true).is_err());
    }
    assert!(entry_row(vec![]).is_err());
    let missing_catalog = Row::from_fields([("schema".into(), Some("extension".into()))]);
    assert!(NativeColumns::from_row(&missing_catalog, &expected).is_err());
}

struct Reader {
    results: VecDeque<Result<Vec<Row>>>,
    calls: Vec<String>,
    entry_calls: usize,
}
impl Reader {
    fn new(results: Vec<Result<Vec<Row>>>) -> Self {
        Self { results: results.into(), calls: Vec::new(), entry_calls: 0 }
    }
}
#[async_trait::async_trait]
impl QueryRead for Reader {
    async fn query(&mut self, sql: &str, _limit: usize) -> Result<Vec<Row>> {
        self.calls.push(sql.into());
        self.results.pop_front().expect("only the declared catalog/cleanup phases")
    }
    async fn query_text_array(
        &mut self,
        _sql: &str,
        _values: Vec<String>,
        _limit: usize,
    ) -> Result<Vec<tokio_postgres::Row>> {
        self.entry_calls += 1;
        Err(CollectionStop::Cancelled.error())
    }
}
fn privilege(elevated: bool) -> Row {
    Row::from_fields([("elevated".into(), Some(if elevated { "t" } else { "f" }.into()))])
}
#[tokio::test]
async fn missing_or_malformed_native_identity_refuses_before_any_dispatch_without_numeric_id_fallback() {
    let mut reader = Reader::new(vec![]);
    assert!(resolve(&mut reader, RecordedStatement::new("SELECT untrusted_input()", "-777", None)).await.is_err());
    for key in ["userid", "dbid", "postmaster_started_at"] {
        let mut missing = identity();
        missing.qualifiers.remove(key);
        assert!(resolve(&mut reader, RecordedStatement::new("SELECT 1", "opaque/id", Some(&missing))).await.is_err());
    }
    for (key, value) in [
        ("queryid", "not-a-number"),
        ("userid", "0"),
        ("dbid", "42;SELECT 1"),
        ("postmaster_started_at", "native-without-offset"),
        ("toplevel", "unknown"),
        ("stats_since", ""),
        ("foreign-authority", "true"),
    ] {
        let mut invalid = identity();
        if key == "queryid" {
            invalid.query_id = value.into();
        } else {
            invalid.qualifiers.insert(key.into(), value.into());
        }
        assert!(
            resolve(&mut reader, RecordedStatement::new("SELECT 1", "opaque/id", Some(&invalid))).await.is_err(),
            "{key}"
        );
    }
    assert!(reader.calls.is_empty());
    assert_eq!(reader.entry_calls, 0);
}
#[tokio::test]
async fn unscoped_logs_preserve_least_privilege_and_elevated_refusal() {
    for elevated in [false, true] {
        let mut reader = Reader::new(vec![Ok(vec![privilege(elevated)])]);
        let result = resolve(&mut reader, RecordedStatement::unscoped("SELECT logged_statement($1)")).await;
        if elevated {
            assert_eq!(result.unwrap_err().code, ErrorCode::PermissionDenied);
        } else {
            assert_eq!(result.unwrap(), "SELECT logged_statement($1)");
        }
        assert_eq!(reader.calls.len(), 1);
        assert_eq!(reader.entry_calls, 0);
    }
}
#[tokio::test]
async fn an_interrupted_exact_lookup_rolls_back_the_same_reader_and_preserves_the_original_error() {
    let native = identity();
    let mut reader = Reader::new(vec![
        Ok(vec![privilege(true)]),
        Ok(vec![]),
        Ok(vec![catalog(true, true)]),
        Err(CollectionStop::SourceChanged.error()),
    ]);
    let error = resolve(&mut reader, RecordedStatement::new("SELECT untrusted_input()", "opaque/id", Some(&native)))
        .await
        .unwrap_err();
    assert!(matches!(error.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::Cancelled))));
    assert_eq!(reader.entry_calls, 1);
    assert_eq!(reader.calls.last().map(String::as_str), Some("ROLLBACK"));
    assert!(reader.results.is_empty());
}
