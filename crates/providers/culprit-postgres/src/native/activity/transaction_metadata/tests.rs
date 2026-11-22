use super::*;
use crate::progress::test_support::{self as fixture, AT, BACKEND, REQUEST, Reader};

pub(crate) fn slrus(version: u32) -> String {
    serde_json::to_string(&slru_names(version).map(|name|serde_json::json!({"native_name":name,"observed_at":AT,"stats_reset":"2026-11-15T07:00:00Z",
        "blocks_zeroed":"0","blocks_hit":"9223372036854775807","blocks_read":"1","blocks_written":"2","blocks_exists":"0","flushes":"1","truncates":"0"}))).unwrap()
}
pub(crate) fn cache_row(count: Option<&str>, overflow: Option<&str>) -> Row {
    let native = slrus(180_000);
    fixture::row(&[
        ("observed_at", Some(AT)),
        ("pid", Some("42")),
        ("backend_type", Some("client backend")),
        ("backend_start", Some(BACKEND)),
        ("query_start", Some(REQUEST)),
        ("transaction_start", Some(REQUEST)),
        ("database_oid", Some("16384")),
        ("database", Some("shop")),
        ("state", Some("active")),
        ("elapsed_ms", Some("1000")),
        ("transaction_ms", Some("1000")),
        ("wait_event_type", Some("LWLock")),
        ("wait_event", Some("SubtransSLRU")),
        ("native_backend_id", Some("7")),
        ("native_cached_subxids", count),
        ("native_overflowed", overflow),
        ("native_slrus", Some(&native)),
    ])
}
async fn meta() -> Metadata {
    let mut io = Reader {
        replies: [Ok(vec![fixture::metadata_with("f", "2026-11-14T00:00:00Z")])].into(),
        ..Default::default()
    };
    crate::native::metadata::activity(&mut io, "fixture.invalid", 9).await.unwrap()
}
#[test]
fn one_public_backend_status_and_slru_pass_uses_version_owned_names_without_lock_or_application_reads() {
    for (version, names) in [
        (160_000, ["Subtrans", "MultiXactMember", "MultiXactOffset"]),
        (170_000, ["subtransaction", "multixact_member", "multixact_offset"]),
        (180_000, ["subtransaction", "multixact_member", "multixact_offset"]),
    ] {
        let sql = query(version, 42, 33).unwrap();
        for name in names {
            assert!(sql.contains(&format!("'{name}'")));
        }
        assert_eq!(sql.matches("pg_stat_get_backend_idset()").count(), 1);
        assert_eq!(sql.matches("pg_stat_get_backend_subxact(").count(), 1);
        assert_eq!(sql.matches("pg_catalog.pg_stat_slru").count(), 1);
        assert!(
            sql.contains("c.native_backend_start=a.backend_start")
                && sql.contains("c.native_transaction_start IS NOT DISTINCT FROM a.xact_start")
        );
        assert!(
            sql.contains("cache(subxact_count,subxact_overflowed)")
                && sql.contains("c.native_overflowed IS TRUE")
                && sql.contains("LIMIT 33")
        );
        assert!(
            !sql.contains("pg_locks")
                && !sql.contains("SAVEPOINT")
                && !sql.contains("orders")
                && !sql.contains("pg_stat_reset")
        );
    }
    assert!(query(150_000, 42, 33).is_err());
    assert!(query(180_000, 0, 33).is_err());
    assert!(query(180_000, 42, 34).is_err());
}
#[tokio::test]
async fn direct_native_overflow_remains_true_below_sixty_four_and_backend_index_is_never_a_pid() {
    let meta = meta().await;
    for (count, overflow, expected) in [
        (Some("64"), Some("true"), Some(true)),
        (Some("3"), Some("true"), Some(true)),
        (Some("0"), Some("false"), Some(false)),
        (None, None, None),
    ] {
        let mut io = Reader { replies: [Ok(vec![cache_row(count, overflow)])].into(), ..Default::default() };
        let read = read(&mut io, &meta, 42, 32).await.unwrap();
        assert_eq!(io.calls.len(), 1);
        assert_eq!(read.caches[0].backend_id, 7);
        assert_eq!(read.caches[0].actor.key.session.id, "42");
        assert_eq!(read.caches[0].overflowed, expected);
        assert_eq!(read.caches[0].cached_subxids, count.map(|count| count.parse().unwrap()));
        assert_eq!(read.slrus[0].blocks_hit.as_deref(), Some("9223372036854775807"));
        assert!(
            read.frame.observations()[0].wait.as_ref().unwrap().class
                == Some(ActivityWaitClass::SubtransactionMetadata)
        );
    }
}
#[tokio::test]
async fn malformed_status_duplicate_backend_and_recycled_database_refuse_while_native_reset_clock_is_retained() {
    let meta = meta().await;
    for rows in [
        vec![cache_row(Some("64"), None)],
        vec![cache_row(Some("-1"), Some("true"))],
        vec![cache_row(Some("64"), Some("maybe"))],
        vec![cache_row(Some("64"), Some("true")), cache_row(Some("64"), Some("true"))],
    ] {
        let mut io = Reader { replies: [Ok(rows)].into(), ..Default::default() };
        assert!(read(&mut io, &meta, 42, 32).await.is_err());
        assert_eq!(io.calls.len(), 1);
    }
    for (field, value) in [("database_oid", "7"), ("database", "foreign"), ("observed_at", "2026-11-13T00:00:00Z")] {
        let original = cache_row(Some("64"), Some("true"));
        let names = [
            "observed_at",
            "pid",
            "backend_type",
            "backend_start",
            "query_start",
            "transaction_start",
            "database_oid",
            "database",
            "state",
            "elapsed_ms",
            "transaction_ms",
            "wait_event_type",
            "wait_event",
            "native_backend_id",
            "native_cached_subxids",
            "native_overflowed",
            "native_slrus",
        ];
        let row = Row::from_fields(
            names.map(|name| (name.into(), if name == field { Some(value.into()) } else { original.text(name) })),
        );
        let mut io = Reader { replies: [Ok(vec![row])].into(), ..Default::default() };
        assert!(read(&mut io, &meta, 42, 32).await.is_err());
        assert_eq!(io.calls.len(), 1);
    }
    let raw = slrus(180_000).replace("2026-11-15T07:00:00Z", "2026-11-16T07:00:00Z");
    assert!(decode_slrus(&raw, 180_000).is_ok());
    assert!(decode_slrus(&slrus(160_000), 180_000).is_err());
}
#[test]
fn generic_slru_io_extension_or_lock_counts_do_not_select_a_transaction_metadata_question() {
    for (kind, event, expected) in [
        ("LWLock", "SubtransSLRU", Some(ActivityWaitClass::SubtransactionMetadata)),
        ("LWLock", "MultiXactGen", Some(ActivityWaitClass::MultiTransactionMetadata)),
        ("IO", "SLRURead", None),
        ("IO", "SLRUWrite", None),
        ("LWLock", "ProcArray", None),
        ("Extension", "SubtransSLRU", None),
    ] {
        let base = cache_row(Some("0"), Some("false"));
        let mut record = decode_row(base, "16384").unwrap();
        record.facts.as_mut().unwrap().wait.as_mut().unwrap().native_type = Some(format!("{kind}: {event}"));
        let mut frame = ActivityFrame { at: AT.into(), records: vec![record], locks: vec![] };
        classify(&mut frame);
        assert_eq!(frame.observations()[0].wait.as_ref().unwrap().class, expected, "{kind}:{event}");
    }
}
