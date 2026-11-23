//! Native-shaped replies exercise the production scope/pager. They are not native cost or grant qualification.
use super::*;
use crate::{
    read::ReadResult,
    resources::test_support::{FakeRead, inventory_row, replace, row},
    value::{Rec, Val},
};
const LOGICAL: &str = "11111111-1111-1111-1111-111111111111";
const PHYSICAL: &str = "22222222-2222-2222-2222-222222222222";
const REPLICA: &str = "91487244-730d-4b83-8e49-d482241c7288";
fn request() -> CollectionRequest {
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::ResourceHistory {
            from: "2020-01-01T06:00:00Z".into(),
            until: "2020-01-01T06:01:00Z".into(),
        },
        budget: CollectionBudget::default(),
    }
}
fn governance() -> Rec {
    Rec::of(&[
        ("database_name", Val::Text("shop".into())),
        ("incarnation", Val::Text(LOGICAL.into())),
        ("physical_incarnation", Val::Text(PHYSICAL.into())),
        ("updateability", Val::Text("READ_ONLY".into())),
    ])
}
fn authority(at: &str) -> Rec {
    replace(
        &crate::activity::database_context::tests::row(5),
        &[
            ("incarnation", Val::Text(LOGICAL.into())),
            ("physical_incarnation", Val::Text(PHYSICAL.into())),
            ("updateability", Val::Text("READ_ONLY".into())),
            ("observed_at", Val::Text(at.into())),
        ],
    )
}
fn inventory(role: i64) -> Rec {
    replace(&inventory_row(), &[("replica_role", Val::Int(role))])
}
fn sample(at: &str, role: i64) -> Rec {
    replace(
        &row(at),
        &[
            ("database_edition", Val::Text("Hyperscale".into())),
            ("replica_id", Val::Text(REPLICA.into())),
            ("replica_role", Val::Int(role)),
        ],
    )
}
fn replies(role: i64) -> Vec<ReadResult<Vec<Rec>>> {
    vec![
        Ok(vec![Rec::of(&[("edition", Val::Int(5))])]),
        Ok(vec![governance()]),
        Ok(vec![authority("2020-01-01T06:02:00Z")]),
        Ok(vec![inventory(role)]),
        Ok(vec![authority("2020-01-01T06:02:01Z")]),
        Ok(vec![sample("2020-01-01T06:00:15Z", role), sample("2020-01-01T06:00:30Z", role)]),
        Ok(vec![authority("2020-01-01T06:02:02Z")]),
        Ok(vec![authority("2020-01-01T06:02:03Z")]),
        Ok(vec![inventory(role)]),
    ]
}
fn scripted_source(replies: Vec<ReadResult<Vec<Rec>>>) -> ResourceSource<FakeRead> {
    ResourceSource::with_io(FakeRead::new(replies).with_generation(1), &request(), "fixture.invalid".into(), 1433)
        .unwrap()
}
async fn page(source: &mut ResourceSource<FakeRead>) -> Result<Vec<TelemetryRecord>> {
    let CollectionRead::Page { records, next_cursor } =
        source.read_page(TelemetryStream::ResourceSamples, None).await?
    else {
        return Err(decode::malformed());
    };
    assert!(next_cursor.is_none());
    Ok(records)
}
#[tokio::test]
async fn each_native_readonly_role_retains_history_without_rebinding_its_samples_to_the_current_member() {
    for role in [1, 2, 3] {
        let mut source = scripted_source(replies(role));
        let description = source.describe().await.unwrap();
        assert_eq!(description.source, MSSQL_READABLE_RESOURCE_HISTORY_SOURCE);
        assert_eq!(description.partition, SourcePartition::Target);
        assert_eq!(description.continuity, SourceContinuity::Unverified);
        let SourceObservation::MssqlReadableResources(history) = &description.observation else { panic!() };
        assert!(history.is_valid() && history.current_reader.has_qualified_local_owner());
        assert!(history.limitations().contains(&MssqlReadableResourceLimitation::HistoricalMemberUnknown));
        let records = page(&mut source).await.unwrap();
        for record in records {
            let TelemetryRecord::ResourceSample(sample) = &record else { panic!() };
            assert!(sample.replica_id.is_none());
            assert_eq!(
                sample.replica_role,
                Some(NativeResourceReplicaRole::MssqlDbResourceStats(
                    MssqlResourceReplicaRole::from_native_code(role).unwrap()
                ))
            );
            assert_eq!(sample.data_io_accounting, DataIoAccounting::ComputeLocalSsd);
            let cpu = sample
                .metrics
                .iter()
                .find(|metric| metric.key == "cpu" && metric.scope == culprit_core::MeasurementScope::Database)
                .unwrap();
            assert_eq!(cpu.value, Some(0.0));
            assert_eq!(cpu.limit_basis, ResourceLimitBasis::DatabaseLimit);
            ExportedRecord::new(
                &description,
                &CaptureIdentity { observation_id: "readable-history-scripted".into(), revision: None },
                record,
            )
            .unwrap();
        }
        assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
        assert_eq!(source.query_count(), 9);
        assert_eq!(source.session.calls.iter().filter(|call| call.sql.contains("sys.dm_db_resource_stats")).count(), 3);
        assert!(
            !source
                .session
                .calls
                .iter()
                .any(|call| call.sql.contains("sys.resource_stats")
                    || call.sql.contains("sys.query_store_runtime_stats"))
        );
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
        assert_eq!(source.session.closed, 1);
    }
}
#[tokio::test]
async fn non_hyperscale_readers_keep_current_member_unknown_and_never_turn_pid_or_role_into_replica_identity() {
    let mut data = replies(1);
    for index in [2, 4, 6, 7] {
        data[index].as_mut().unwrap()[0] = replace(&data[index].as_ref().unwrap()[0], &[("replica_id", Val::Null)]);
    }
    for row in data[5].as_mut().unwrap() {
        *row = replace(row, &[("database_edition", Val::Text("Business Critical".into())), ("replica_id", Val::Null)]);
    }
    let mut source = scripted_source(data);
    let description = source.describe().await.unwrap();
    let SourceObservation::MssqlReadableResources(history) = &description.observation else { panic!() };
    assert!(history.limitations().contains(&MssqlReadableResourceLimitation::CurrentMemberUnknown));
    assert!(matches!(
        history.current_reader.partition,
        SourcePartition::InstanceLocal { member: None, epoch: Some(_) }
    ));
    let rows = page(&mut source).await.unwrap();
    assert!(rows.iter().all(|row| matches!(row, TelemetryRecord::ResourceSample(sample) if sample.replica_id.is_none() && sample.data_io_accounting==DataIoAccounting::DataFiles)));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    source.close().await;
}
#[tokio::test]
async fn initial_and_paired_native_role_process_member_and_database_changes_refuse_the_same_held_read() {
    for index in [2, 4, 6, 7] {
        for (name, value) in [
            ("physical_incarnation", Val::Text("33333333-3333-3333-3333-333333333333".into())),
            ("incarnation", Val::Text("44444444-4444-4444-4444-444444444444".into())),
            ("replica_id", Val::Text("55555555-5555-5555-5555-555555555555".into())),
            ("local_process_id", Val::Int(99)),
            ("local_started_ticks", Val::Int(101)),
            ("selected_database_id", Val::Int(8)),
            ("updateability", Val::Text("READ_WRITE".into())),
        ] {
            // Initial process/member observations establish the original, so those changes are meaningful on subsequent guards.
            if index == 2 && !matches!(name, "physical_incarnation" | "incarnation" | "updateability") {
                continue;
            }
            let mut data = replies(1);
            data[index].as_mut().unwrap()[0] = replace(&data[index].as_ref().unwrap()[0], &[(name, value)]);
            let mut source = scripted_source(data);
            if index == 2 {
                assert!(source.describe().await.is_err());
            } else {
                source.describe().await.unwrap();
                if index == 7 {
                    page(&mut source).await.unwrap();
                    assert!(source.finish().await.is_err());
                } else {
                    assert!(page(&mut source).await.is_err());
                    assert!(source.finish().await.is_err());
                }
            }
            source.close().await;
        }
    }
}
#[tokio::test]
async fn unknown_epoch_role_or_foreign_page_profile_does_not_gain_history_authority() {
    for changes in [
        vec![
            ("local_epoch_visible", Val::Bool(false)),
            ("local_started_ticks", Val::Null),
            ("local_now_ticks", Val::Null),
            ("local_started_without_offset", Val::Null),
        ],
        vec![("updateability", Val::Null)],
    ] {
        let mut data = replies(1);
        data[2].as_mut().unwrap()[0] = replace(&data[2].as_ref().unwrap()[0], &changes);
        let mut source = scripted_source(data);
        assert!(source.describe().await.is_err());
        assert_eq!(source.query_count(), 3);
        source.close().await;
    }
    for (field, value) in [
        ("replica_id", Val::Text("66666666-6666-6666-6666-666666666666".into())),
        ("replica_role", Val::Null),
        ("replica_role", Val::Int(0)),
        ("engine_edition", Val::Int(8)),
    ] {
        let mut data = replies(1);
        for row in data[5].as_mut().unwrap() {
            *row = replace(row, &[(field, value.clone())]);
        }
        let mut source = scripted_source(data);
        source.describe().await.unwrap();
        assert!(page(&mut source).await.is_err());
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), 6);
        source.close().await;
    }
}
#[tokio::test]
async fn role_changes_in_inventory_page_or_final_selection_and_transport_changes_never_publish_a_complete_read() {
    for index in [3, 5, 8] {
        let mut data = replies(1);
        for row in data[index].as_mut().unwrap() {
            *row = replace(row, &[("replica_role", Val::Int(if index == 3 { 0 } else { 2 }))]);
        }
        let mut source = scripted_source(data);
        if index == 3 {
            assert!(source.describe().await.is_err());
        } else {
            source.describe().await.unwrap();
            if index == 5 {
                assert!(page(&mut source).await.is_err());
            } else {
                page(&mut source).await.unwrap();
                assert_eq!(source.finish().await.unwrap(), SourceCompletion::Changed);
            }
        }
        source.close().await;
    }
    let mut source = scripted_source(replies(1));
    source.session = source.session.change_generation_at(4);
    source.describe().await.unwrap();
    assert!(page(&mut source).await.is_err());
    assert_eq!(source.query_count(), 4);
    source.close().await;
}
#[tokio::test]
async fn empty_partial_and_missing_suffix_history_are_distinct_from_a_healthy_empty_or_completed_window() {
    let empty = replace(
        &inventory(1),
        &[
            ("available_from", Val::Null),
            ("available_until", Val::Null),
            ("selected_count", Val::Int(0)),
            ("retained_count", Val::Int(0)),
            ("selected_first", Val::Null),
            ("selected_last", Val::Null),
            ("replica_role", Val::Null),
        ],
    );
    let partial = replace(&inventory(1), &[("available_from", Val::Text("2020-01-01T06:00:15Z".into()))]);
    for (inventory, stop) in [(empty, CollectionStop::SourceNotReady), (partial, CollectionStop::RetentionGap)] {
        let mut data = replies(1);
        data[3] = Ok(vec![inventory]);
        let mut source = scripted_source(data);
        let description = source.describe().await.unwrap();
        assert!(!description.observation.covers_request());
        assert!(!description.observation.can_advance_history_window());
        assert!(
            matches!(source.read_page(TelemetryStream::ResourceSamples, None).await.unwrap(), CollectionRead::Stopped(actual) if actual == stop)
        );
        assert_eq!(source.query_count(), 4);
        source.close().await;
    }
    let mut data = replies(1);
    data[5] = Ok(vec![]);
    let mut source = scripted_source(data);
    source.describe().await.unwrap();
    assert!(page(&mut source).await.unwrap().is_empty());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Changed);
    source.close().await;
}
#[tokio::test]
async fn native_failures_cancellation_and_tighter_query_caps_stop_without_a_post_error_guard_or_reopen() {
    for stop in [
        CollectionStop::Cancelled,
        CollectionStop::TimedOut,
        CollectionStop::CancellationUnconfirmed,
        CollectionStop::ByteLimit,
    ] {
        let mut data = replies(1);
        data[5] = Err(ReadError::Stopped(stop));
        let mut source = scripted_source(data);
        source.describe().await.unwrap();
        assert!(
            matches!(source.read_page(TelemetryStream::ResourceSamples, None).await.unwrap(), CollectionRead::Stopped(actual) if actual == stop)
        );
        assert_eq!(source.query_count(), 6);
        assert!(source.finish().await.is_err());
        assert_eq!(source.query_count(), 6);
        source.close().await;
        assert_eq!(source.session.closed, 1);
    }
    let mut data = replies(1);
    data[5] = Err(culprit_core::Error::new(
        culprit_core::ErrorCode::PermissionDenied,
        "scripted resource permission failure",
    )
    .into());
    let mut source = scripted_source(data);
    source.session = source.session.change_generation_at(6);
    source.describe().await.unwrap();
    assert_eq!(page(&mut source).await.unwrap_err().code, culprit_core::ErrorCode::PermissionDenied);
    assert_eq!(source.query_count(), 6);
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 6);
    source.close().await;
    let mut source = scripted_source(replies(1));
    source.session = source.session.with_query_limit(8);
    source.describe().await.unwrap();
    page(&mut source).await.unwrap();
    assert_eq!(CollectionStop::from_error(&source.finish().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 8);
    source.close().await;
    let mut source = scripted_source(replies(1));
    source.session = source.session.pause_on_query(3);
    assert!(tokio::time::timeout(std::time::Duration::from_millis(2), source.describe()).await.is_err());
    assert!(source.describe().await.is_err());
    assert!(source.finish().await.is_err());
    assert_eq!(source.query_count(), 3);
    source.session.shutdown = SourceShutdown::Unconfirmed;
    assert_eq!(source.close().await, SourceShutdown::Unconfirmed);
}

mod original;

#[tokio::test]
async fn paged_readable_history_keeps_native_lookahead_and_one_cursor_owner_across_both_pages() {
    let mut data = replies(1);
    data.splice(
        7..7,
        [
            Ok(vec![authority("2020-01-01T06:02:02Z")]),
            Ok(vec![sample("2020-01-01T06:00:30Z", 1)]),
            Ok(vec![authority("2020-01-01T06:02:03Z")]),
        ],
    );
    let mut source = scripted_source(data);
    source.request.budget.page_rows = 1;
    source.describe().await.unwrap();
    let CollectionRead::Page { records: first, next_cursor: Some(cursor) } =
        source.read_page(TelemetryStream::ResourceSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(first.len(), 1);
    let CollectionRead::Page { records: last, next_cursor: None } =
        source.read_page(TelemetryStream::ResourceSamples, Some(&cursor)).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(last.len(), 1);
    assert_ne!(first[0].native_identity(), last[0].native_identity());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 12);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[test]
fn expanded_readable_history_contract_remains_unqualified_without_changing_primary_ids_or_default_admission() {
    let recipe = crate::telemetry::recipes().into_iter().find(|recipe| recipe.id == "mssql.resource-history").unwrap();
    assert_eq!(recipe.version, 4);
    assert!(matches!(recipe.admission, RecipeAdmission::Unqualified));
    assert_eq!(recipe.purpose, RecipePurpose::Baseline);
}
