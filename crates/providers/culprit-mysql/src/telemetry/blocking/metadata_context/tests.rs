use super::*;
use crate::native::locks::metadata_context::tests::{endpoint, ticket};
use crate::read::test_support::Read;
use crate::telemetry::blocking::tests::{holder, member, metadata_request_on as request, reader as blocking_reader};
fn payload(row: Rec, sql: Option<&str>) -> Rec {
    row.with_fields(&[
        ("info", Value::NULL),
        ("sql_text", sql.map(Value::from).unwrap_or(Value::NULL)),
        ("sql_text_bytes", sql.map(|text| Value::UInt(text.len() as u64)).unwrap_or(Value::NULL)),
    ])
}
fn gate(at: &str) -> Rec {
    let row = crate::native::activity::test_support::row_with(
        3,
        "Query",
        true,
        &[("state", "Waiting for table metadata lock".into()), ("seconds", 40u64.into()), ("observed_at", at.into())],
    );
    payload(row, Some("ALTER TABLE shop.items ADD COLUMN owned_value INT NULL"))
}
fn context_holder(at: &str) -> Rec {
    payload(holder(at, 102), None)
}
fn current_rows(at: &str) -> Vec<Rec> {
    vec![crate::telemetry::blocking::tests::waiter(at, true), context_holder(at), gate(at)]
}
fn reader(version: &str) -> Reader<Read> {
    let mut reader = blocking_reader();
    reader.detail = Detail::MetadataLocks;
    reader.io.replies.insert(8, Ok(endpoint("2020-01-01T06:00:01.150000Z")));
    reader.io.replies.insert(12, Ok(endpoint("2020-01-01T06:00:01.250000Z")));
    reader.io.replies[9] = Ok(current_rows("2020-01-01T06:00:01.200000Z"));
    reader.io.replies[13] = Ok(current_rows("2020-01-01T06:00:01.300000Z"));
    for index in [0, 14] {
        let row = reader.io.replies[index].as_mut().unwrap().remove(0);
        reader.io.replies[index] = Ok(vec![row.with_fields(&[("version", version.into())])]);
    }
    for index in [2, 16] {
        let row = reader.io.replies[index].as_mut().unwrap().remove(0);
        reader.io.replies[index] = Ok(vec![row.with_fields(&[("native_member", member(version).into())])]);
    }
    reader.io = reader.io.with_query_budget(METADATA_MAX_QUERIES, 7);
    reader
}
fn evidence(observed: &FrozenObservation) -> culprit_core::blocking::MetadataQueueEvidence {
    let rows = observed
        .records
        .iter()
        .map(|record| {
            let TelemetryRecord::Activity(actor) = record else { panic!() };
            ActivityFrameRow { observation: actor, reference: None }
        })
        .collect::<Vec<_>>();
    culprit_core::blocking::metadata_queue(&ActivityFrameEvidence::new(&observed.description, &rows, true).unwrap())
        .unwrap()
}
#[tokio::test]
async fn deep_both_flavor_frame_pays_thirty_seven_phases_and_never_invents_a_pending_edge() {
    for version in ["8.4.6", "11.8.9-MariaDB-ubu2404"] {
        let request = request(version).await;
        let mut reader = reader(version);
        let observed = reader.observe(&request).await.unwrap();
        let proof = evidence(&observed);
        assert_eq!(reader.query_count(), 37);
        assert_eq!(reader.io.queries.len(), 17);
        assert_eq!(observed.records.len(), 3);
        let root = observed
            .records
            .iter()
            .find_map(|record| match record {
                TelemetryRecord::Activity(actor) if actor.key.session.id == "1" => Some(actor),
                _ => None,
            })
            .unwrap();
        assert!(
            root.statement.is_available(),
            "the narrower cohort SQL cap cannot erase the independently retained root payload"
        );

        assert_eq!(proof.candidates.len(), 1);
        assert!(proof.cascades.is_empty());
        assert_eq!(proof.status, MetadataQueueStatus::Unavailable);
        assert!(proof.issues.contains(&MetadataQueueIssue::PendingBlockerNotExposed));
        assert_eq!(proof.candidates[0].pending_exclusive[0].actor.session.id, "3");
        assert_eq!(proof.candidates[0].granted_holders[0].actor.session.id, "2");
        for index in [8, 12] {
            assert_eq!(reader.io.queries[index].1, vec![Value::UInt(101), Value::from("shop")]);
            assert_eq!(reader.io.queries[index].2, 65);
        }
        assert_eq!(reader.io.queries[9].1[1..], vec![Value::UInt(1), Value::UInt(2), Value::UInt(3)]);
        for index in [9, 13] {
            assert!(reader.io.queries[index].0.contains("e.END_EVENT_ID IS NULL AND t.PROCESSLIST_COMMAND<>'Sleep'"));
            assert_eq!(reader.io.queries[index].0.matches("ELSE NULL END").count(), 4);
        }

        assert!(reader.verify(&observed.description).await.unwrap());
        assert_eq!(reader.query_count(), 37);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn unknown_event_changed_ticket_ended_request_prefix_and_conversion_preserve_context_without_a_sandwich() {
    for defect in ["event", "ticket", "ended", "prefix", "convert", "resource"] {
        let request = request("8.4.6").await;
        let mut reader = reader("8.4.6");
        match defect {
            "event" => {
                for index in [8, 12] {
                    let row = reader.io.replies[index].as_mut().unwrap()[2].with_fields(&[]);
                    reader.io.replies[index].as_mut().unwrap()[2] = row.with_fields(&[("owner_event_id", Value::NULL)]);
                }
            }
            "ticket" => {
                let row = reader.io.replies[12].as_mut().unwrap()[2].with_fields(&[]);
                reader.io.replies[12].as_mut().unwrap()[2] = row.with_fields(&[("ticket_address", 999u64.into())]);
            }
            "ended" => {
                reader.io.replies[13] =
                    Ok(vec![context_holder("2020-01-01T06:00:01.300000Z"), gate("2020-01-01T06:00:01.300000Z")])
            }
            "prefix" => {
                for index in [8, 12] {
                    for address in 4..=64 {
                        reader.io.replies[index].as_mut().unwrap().push(ticket(address, 3, "EXCLUSIVE", "PENDING"));
                    }
                }
            }
            "convert" => {
                for index in [8, 12] {
                    let row = reader.io.replies[index].as_mut().unwrap()[2].with_fields(&[]);
                    reader.io.replies[index].as_mut().unwrap()[2] =
                        row.with_fields(&[("lock_status", "CONVERT".into())]);
                }
            }
            _ => {
                for index in [8, 12] {
                    let row = reader.io.replies[index].as_mut().unwrap()[2].with_fields(&[]);
                    reader.io.replies[index].as_mut().unwrap()[2] =
                        row.with_fields(&[("object_name", "other_items".into())]);
                }
            }
        }
        let observed = reader.observe(&request).await.unwrap();
        let proof = evidence(&observed);
        assert!(proof.cascades.is_empty());
        assert!(proof.candidates.is_empty(), "{defect}");
        assert!(proof.issues.contains(&MetadataQueueIssue::PendingBlockerNotExposed));
        assert_eq!(reader.query_count(), 37);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn stricter_query_rows_and_final_native_owner_cannot_be_repaired_by_the_deep_profile() {
    {
        let mut request = request("8.4.6").await;
        request.budget.max_queries = 36;
        let mut reader = reader("8.4.6");
        reader.io = reader.io.with_query_budget(36, 7);
        assert_eq!(
            CollectionStop::from_error(&reader.observe(&request).await.err().unwrap()),
            CollectionStop::QueryLimit
        );
        assert!(reader.verified.is_none());
        assert_eq!(reader.query_count(), 36);
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
    for defect in ["member", "role", "process", "transport", "permission", "rows"] {
        let mut request = request("8.4.6").await;
        let mut reader = reader("8.4.6");
        let (index, field, value) = match defect {
            "member" => (16, "native_member", Value::from("f54a461c-0001-4000-8000-000000000002")),
            "role" => (14, "read_only", Value::UInt(1)),
            "process" => (15, "uptime", Value::UInt(999)),
            "permission" => (16, "process_wide", Value::UInt(0)),
            _ => (0, "version", Value::from("8.4.6")),
        };
        if defect == "transport" {
            reader.io.change_generation_after = Some(9);
        } else if defect == "rows" {
            request.budget.max_snapshot_rows = 1;
        } else {
            let row = reader.io.replies[index].as_mut().unwrap()[0].with_fields(&[]);
            reader.io.replies[index] = Ok(vec![row.with_fields(&[(field, value)])]);
        }
        assert!(reader.observe(&request).await.is_err(), "{defect}");
        assert!(reader.verified.is_none());
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    }
}
#[tokio::test]
async fn every_deep_phase_stops_on_the_original_failure_and_never_dispatches_after_retirement() {
    for index in 0..17 {
        for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
            let request = request("8.4.6").await;
            let mut reader = reader("8.4.6");
            reader.io.replies[index] = Err(stop.error());
            reader.io.change_generation_after = Some(index + 1);
            reader.io.shutdown = Some(SourceShutdown::Unconfirmed);
            assert_eq!(CollectionStop::from_error(&reader.observe(&request).await.err().unwrap()), stop, "{index}");
            assert_eq!(reader.io.queries.len(), index + 1);
            assert!(reader.verified.is_none());
            let paid = reader.query_count();
            assert_eq!(reader.close().await, SourceShutdown::Unconfirmed);
            assert_eq!(reader.io.closed, 1);
            assert_eq!(reader.query_count(), paid);
        }
    }
}

#[tokio::test]
async fn a_completed_or_idle_native_event_cannot_donate_previous_sql_even_if_a_transport_supplies_it() {
    let request = request("8.4.6").await;
    let mut reader = reader("8.4.6");
    for index in [9, 13] {
        let rows = reader.io.replies[index].as_mut().unwrap();
        rows[1] = payload(
            holder(if index == 9 { "2020-01-01T06:00:01.200000Z" } else { "2020-01-01T06:00:01.300000Z" }, 102),
            Some("SELECT secret FROM completed_private_query"),
        );
    }
    let result = reader.observe(&request).await.unwrap();
    let holder = result
        .records
        .iter()
        .find_map(|record| match record {
            TelemetryRecord::Activity(actor) if actor.key.session.id == "2" => Some(actor),
            _ => None,
        })
        .unwrap();
    assert!(holder.idle && holder.key.request.is_none() && holder.statement_identity.is_none());
    assert!(!holder.statement.is_available());
    assert!(!serde_json::to_string(&result.records).unwrap().contains("completed_private_query"));
    assert_eq!(reader.query_count(), 37);
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
}
