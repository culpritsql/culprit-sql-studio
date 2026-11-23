use super::*;
use crate::read::test_support::Read;
pub(crate) fn ticket(address: u64, session: u64, mode: &str, status: &str) -> Rec {
    Rec::from_values(vec![
        ("kind", "ticket".into()),
        ("ticket_address", address.into()),
        ("object_type", "TABLE".into()),
        ("object_schema", "shop".into()),
        ("object_name", "items".into()),
        ("lock_type", mode.into()),
        ("lock_duration", "TRANSACTION".into()),
        ("lock_status", status.into()),
        ("owner_thread_id", (session + 100).into()),
        ("owner_process_id", session.into()),
        ("owner_event_id", 8u64.into()),
        ("transaction_event_id", 2u64.into()),
        ("transaction_timer_start", 9000u64.into()),
        ("transaction_end_event_id", Value::NULL),
        ("transaction_state", "ACTIVE".into()),
    ])
}
pub(crate) fn endpoint(at: &str) -> Vec<Rec> {
    vec![
        Rec::from_values(vec![("kind", "guard".into()), ("observed_at", at.into())]),
        ticket(1, 1, "SHARED_READ", "PENDING"),
        ticket(2, 3, "EXCLUSIVE", "PENDING"),
        ticket(3, 2, "SHARED_READ", "GRANTED"),
    ]
}
#[tokio::test]
async fn one_selected_endpoint_keeps_requesting_event_distinct_and_preserves_native_nulls_and_unknowns() {
    let mut rows = endpoint("2020-01-01T06:00:01.150000Z");
    rows[3] = rows[3].with_fields(&[]).with_fields(&[
        ("owner_event_id", Value::NULL),
        ("transaction_timer_start", Value::NULL),
        ("lock_status", "CONVERT".into()),
    ]);
    let mut reader = Read::with(vec![Ok(rows)]).with_native_phases();
    let result = read(&mut reader, "shop", 101, 63).await.unwrap();
    assert_eq!(result.rows, 3);
    assert!(result.complete());
    assert_eq!(result.tickets[0].owner_event_id.as_deref(), Some("8"));
    assert_eq!(result.tickets[2].owner_event_id, None);
    assert_eq!(result.tickets[2].transaction.as_ref().unwrap().timer_start, None);
    assert_eq!(result.tickets[2].status, "CONVERT");
    assert_eq!(reader.queries[0].1, vec![Value::UInt(101), Value::from("shop")]);
    assert_eq!(reader.queries[0].2, 65);
}
#[tokio::test]
async fn duplicate_tickets_missing_guard_and_excess_prefix_cannot_be_accepted_as_a_complete_endpoint() {
    for defect in ["duplicate", "guard", "excess"] {
        let mut rows = endpoint("2020-01-01T06:00:01.150000Z");
        match defect {
            "duplicate" => rows[2] = rows[1].with_fields(&[]),
            "guard" => {
                rows.remove(0);
            }
            _ => rows.push(ticket(4, 4, "SHARED_READ", "GRANTED")),
        }
        let mut reader = Read::with(vec![Ok(rows)]);
        assert_eq!(
            CollectionStop::from_error(&read(&mut reader, "shop", 101, 2).await.err().unwrap()),
            CollectionStop::InvalidPage,
            "{defect}"
        );
    }
    let mut reader = Read::with(vec![Ok(endpoint("2020-01-01T06:00:01.150000Z"))]);
    let prefix = read(&mut reader, "shop", 101, 2).await.unwrap();
    assert!(!prefix.complete());
    assert_eq!(prefix.rows, 3);
    assert_eq!(prefix.tickets.len(), 2);
}
#[test]
fn selected_query_never_joins_an_owner_event_to_current_sql_or_invents_a_pending_blocker() {
    let sql = query(63);
    assert_eq!(sql.matches("performance_schema.metadata_locks").count(), 2);
    assert_eq!(sql.matches("events_transactions_current").count(), 1);
    assert!(sql.contains("p.OWNER_THREAD_ID=?"));
    assert!(sql.contains("p.OBJECT_SCHEMA=?"));
    assert!(sql.contains("LIMIT 64"));
    assert!(!sql.contains("events_statements_history"));
    assert!(!sql.contains("OWNER_EVENT_ID="));
    assert!(!sql.contains("sys.schema_table_lock_waits"));
    assert!(!sql.contains("TRX_ID"));
    assert!(!sql.contains("SQL_TEXT"));
}
