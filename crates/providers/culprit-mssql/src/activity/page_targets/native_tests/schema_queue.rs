//! Native same-object sandwich plus an independently pending same-short-name operation. Unqualified.
use super::{
    fixture::{self, Fixture, actors},
    workload::CompletedWorker,
};
use crate::{
    conn::MsClient,
    read::integer,
    test_support::{
        owned_captures::{Captured, OwnedCaptures},
        owned_case, receipt_directory, save_cost_receipt,
    },
};
use culprit_core::{Error, Result, blocking::*, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use std::time::Duration;
const CASE_TIMEOUT: Duration = Duration::from_secs(60);
const FIXTURE_DISPATCH_CAP: u32 = 22;
const SOURCE_TOTAL_CAP: u32 = 28; // Activity8 +selectedBlocking12 +endedBlocking8, each including SET.
const GATE_APP: &str = "schema-queue-pending-main";
const OTHER_APP: &str = "schema-queue-pending-other";
const VICTIM_APP: &str = "schema-queue-affected-main";
const SETUP: &str = "EXEC(N'CREATE SCHEMA queue_other');CREATE TABLE queue_other.page_fixture_select(id int NOT NULL PRIMARY KEY);INSERT queue_other.page_fixture_select VALUES(0)";
const HOLD: &str = "SET LOCK_TIMEOUT 1000;BEGIN TRANSACTION;UPDATE dbo.page_fixture_select WITH(ROWLOCK) SET id=-1 WHERE id=0;UPDATE queue_other.page_fixture_select WITH(ROWLOCK) SET id=-1 WHERE id=0";
const GATE: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;BEGIN TRANSACTION;ALTER TABLE dbo.page_fixture_select ADD queue_column int NULL;ROLLBACK TRANSACTION;SELECT CONVERT(bigint,1) AS completed";
const OTHER: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;BEGIN TRANSACTION;ALTER TABLE queue_other.page_fixture_select ADD queue_column int NULL;ROLLBACK TRANSACTION;SELECT CONVERT(bigint,1) AS completed";
// Delay arranges the fixture. Only the actual native pointers/resources/modes can prove the queue.
const VICTIM: &str = "SET NOCOUNT ON;SET LOCK_TIMEOUT 20000;WAITFOR DELAY '00:00:01';BEGIN TRANSACTION;DECLARE @n bigint;SELECT @n=COUNT_BIG(*) FROM dbo.page_fixture_select WITH(READCOMMITTEDLOCK);ROLLBACK TRANSACTION;SELECT @n AS completed";
#[tokio::test]
#[ignore = "requires nonce-owned SQL16 edition3 loopback supervisor, seeded guard database and explicit private receipt directory; correctness only, no automatic/cost admission"]
async fn same_owned_schema_queue_excludes_the_other_object_and_ended_request() {
    Box::pin(fixture::run(|fixture| {
        Box::pin(async move {
            let cancellation = CollectionCancellation::default();
            let (expired, result) = owned_case::finish_case(
                tokio::time::Instant::now() + CASE_TIMEOUT,
                &cancellation,
                Box::pin(prove(fixture, &cancellation)),
            )
            .await;
            result?;
            if expired {
                return Err(CollectionStop::TimedOut.error());
            }
            Ok(())
        })
    }))
    .await;
}
async fn prove(fixture: &mut Fixture, cancellation: &CollectionCancellation) -> Result<()> {
    owned_case::require_active(cancellation)?;
    let (object, other) = setup(fixture, cancellation).await?;
    start_actors(fixture, cancellation).await?;
    tokio::select! {_ = cancellation.cancelled()=>return Err(CollectionStop::Cancelled.error()),
    _ = tokio::time::sleep(Duration::from_millis(1500))=>{}}
    owned_case::require_active(cancellation)?;
    if fixture.workers.iter().flatten().any(|worker| worker.is_finished()) {
        return Err(invalid());
    }
    let mut captures = OwnedCaptures::from_pool(fixture.pool.as_ref().ok_or_else(invalid)?.clone(), fixture.port)?;
    let original = Box::new(captures.capture_with_cancellation(CollectionSelection::Activity, cancellation).await?);
    save_observation("original", &original)?;
    let focus = focus(&original)?;
    let current = Box::new(
        captures
            .capture_with_cancellation(
                CollectionSelection::FocusedBlocking { focus: Box::new(focus.clone()) },
                cancellation,
            )
            .await?,
    );
    save_observation("focused", &current)?;
    let report = current.with_activity_frame(|frame| {
        let rows = frame.rows().collect::<Vec<_>>();
        culprit_checks::focused_blocking::explain(current.source()?, &rows, true)
    })?;
    save_cost_receipt(
        &receipt_directory(),
        "mssql-schema-queue-report-observation",
        &serde_json::json!({"assertions_passed": false, "native_cost_qualified": false, "report": report}),
    )?;
    if report.metadata_queue.evidence.status != MetadataQueueStatus::Observed
        || report.metadata_queue.evidence.cascades.len() != 1
    {
        return Err(invalid());
    }
    let case = &report.metadata_queue.evidence.cascades[0];
    let MetadataQueueResource::MssqlObject(resource) = &case.resource else {
        return Err(invalid());
    };
    if resource.associated_entity_id != object.to_string()
        || resource.associated_entity_id == other.to_string()
        || case.requested_mode != "Sch-M"
        || case.affected.len() != 1
        || case.holders.len() != 1
        || case.affected[0].actor != focus.original.key
        || case.holders[0].actor.request.is_some()
        || case.holders[0].actor.transaction.is_none()
    {
        return Err(invalid());
    }
    let mut applications = Vec::new();
    for row in &current.records {
        let TelemetryRecord::Activity(point) = &row.record else {
            return Err(invalid());
        };
        applications.push(point.client.as_ref().and_then(|client| client.application.as_deref()).unwrap_or_default());
    }
    if !applications.contains(&GATE_APP) || !applications.contains(&VICTIM_APP) || applications.contains(&OTHER_APP) {
        return Err(invalid());
    }
    if current.source()?.target.database().ok_or_else(invalid)?.incarnation.is_some() {
        return Err(invalid());
    }
    fixture.release_holder_gate().await?;
    let completed = fixture.finish_workload().await?;
    if completed.len() != 3 || completed.iter().any(|actor| actor.inserts != if actor.pattern == 1 { 100 } else { 1 }) {
        return Err(invalid());
    }
    owned_case::require_active(cancellation)?;
    let ended = Box::new(
        captures
            .capture_with_cancellation(CollectionSelection::FocusedBlocking { focus: Box::new(focus) }, cancellation)
            .await?,
    );
    let SourceObservation::FocusedBlocking { result, .. } = &ended.source()?.observation else {
        return Err(invalid());
    };
    if !matches!(result.binding, FocusedRequestBinding::ParticipantMissing | FocusedRequestBinding::ParticipantChanged)
        || result.membership != FocusedRequestMembership::NotObserved
        || !ended.records.is_empty()
    {
        return Err(invalid());
    }
    for captured in [&original, &current, &ended] {
        captures.verify_retained(captured).await?;
        if captured.manifest.status != CollectionStatus::Complete {
            return Err(invalid());
        }
        let metrics = captured.manifest.read_metrics.as_ref().ok_or_else(invalid)?;
        if metrics.has_unconfirmed_work() || metrics.active_tasks != 0 || metrics.untracked_tasks != 0 {
            return Err(invalid());
        }
    }
    if original.manifest.source_queries > 7 || current.manifest.source_queries > 11 || ended.manifest.source_queries > 7
    {
        return Err(invalid());
    }
    save_cost_receipt(
        &receipt_directory(),
        "mssql-schema-queue-correctness",
        &serde_json::json!({
        "original":original.manifest,"focused":current.manifest,"ended":ended.manifest,"report":report,
        "independent_same_name_object_ids":[object,other],"actor_completions":completed.iter().map(|actor|(actor.pattern,actor.inserts)).collect::<Vec<_>>(),
        "fixture_dispatch_cap_including_setup_actor_oracle_cleanup":FIXTURE_DISPATCH_CAP,
        "source_allowance_including_SET":SOURCE_TOTAL_CAP,"combined_dispatch_cap":SOURCE_TOTAL_CAP+FIXTURE_DISPATCH_CAP,
        "body_deadline_ms":CASE_TIMEOUT.as_millis(),"native_lock_timeout_ms":20000,"actor_population":3,
        "proof":"actual reciprocal native pointer/resource/mode/transaction/actor endpoints from the production held source; canonical reopened",
        "qualification":"unqualified","native_cost_qualified":false,"automatic_admission":false,
        "limitations":"not Azure/MI/AG/readable/version/load acceptance; endpoints do not establish continuous holding or acquiring SQL"}),
    )
}
async fn setup(fixture: &Fixture, cancellation: &CollectionCancellation) -> Result<(i64, i64)> {
    owned_case::require_active(cancellation)?;
    let mut setup = fixture.client("schema-queue-setup").await?;
    let result=async {
        owned_case::require_active(cancellation)?;fixture::execute(&mut setup,SETUP).await?;
        owned_case::require_active(cancellation)?;
        let rows=crate::conn::query(&mut setup,"SELECT /* culprit fixture */ OBJECT_ID(N'dbo.page_fixture_select') AS object_id,OBJECT_ID(N'queue_other.page_fixture_select') AS other_object_id").await.map_err(crate::conn::map_error)?;
        let [row]=rows.as_slice() else{return Err(invalid());};
        let object=integer(row,"object_id")?;let other=integer(row,"other_object_id")?;
        if object<=0 || other<=0 || object==other{return Err(invalid());}Ok((object,other))
    }.await;
    let retired = owned_case::retire(setup).await;
    let result = result?;
    retired?;
    Ok(result)
}
async fn start_actors(fixture: &mut Fixture, cancellation: &CollectionCancellation) -> Result<()> {
    fixture.require_idle_workload()?;
    let mut clients: Vec<(MsClient, u8, String)> = Vec::new();
    for (application, kind, statement) in [(GATE_APP, 0, GATE), (VICTIM_APP, 1, VICTIM), (OTHER_APP, 2, OTHER)] {
        let connected = async {
            owned_case::require_active(cancellation)?;
            fixture.client(application).await
        }
        .await;
        match connected {
            Ok(client) => clients.push((client, kind, statement.into())),
            Err(original) => {
                for (client, _, _) in clients {
                    let _ = owned_case::retire(client).await;
                }
                return Err(original);
            }
        }
    }
    if let Err(original) = owned_case::require_active(cancellation) {
        for (client, _, _) in clients {
            let _ = owned_case::retire(client).await;
        }
        return Err(original);
    }
    let prepared = async {
        owned_case::require_active(cancellation)?;
        let (victim, _, _) = clients.get_mut(1).ok_or_else(invalid)?;
        // Compile/drain the identical bounded victim batch on the same actor before any fixture lock exists.
        // Actual native locator/resource/transaction witnesses remain mandatory after it starts again.
        fixture::execute(victim, VICTIM).await?;
        owned_case::require_active(cancellation)?;
        fixture.start_holder_gate_with_statement(HOLD).await?;
        owned_case::require_active(cancellation)
    }
    .await;
    if let Err(original) = prepared {
        for (client, _, _) in clients {
            let _ = owned_case::retire(client).await;
        }
        return Err(original);
    }
    actors::start(&mut fixture.workers, clients, |pattern, row| {
        Ok(CompletedWorker { pattern, inserts: actors::completion_count(row, "completed")? })
    })
    .await?;
    Ok(())
}
fn focus(original: &Captured) -> Result<ActivityRequestFocus> {
    let source = original.source()?;
    let native = source.observation.activity().ok_or_else(invalid)?;
    if !native.coverage.complete_scope || !original.activity_context()?.has_qualified_local_owner() {
        return Err(invalid());
    }
    for (ordinal, row) in original.records.iter().enumerate() {
        let TelemetryRecord::Activity(actor) = &row.record else {
            return Err(invalid());
        };
        if actor.client.as_ref().and_then(|client| client.application.as_deref()) != Some(VICTIM_APP)
            || actor.waiting_on_lock != Some(true)
        {
            continue;
        }
        let focus = ActivityRequestFocus {
            parent: original.reference(ordinal)?,
            target: source.target.clone(),
            partition: source.partition.clone(),
            database_writable: native.database_writable,
            source: source.source.clone(),
            epoch: native.epoch.clone().ok_or_else(invalid)?,
            original_observed_at: native.at.clone(),
            statement_identity: actor.statement_identity.clone().ok_or_else(invalid)?,
            original: actor.as_ref().clone(),
        };
        if focus.is_blocking_focus() {
            return Ok(focus);
        }
    }
    Err(invalid())
}
fn save_observation(phase: &str, captured: &Captured) -> Result<()> {
    save_cost_receipt(
        &receipt_directory(),
        &format!("mssql-schema-queue-{phase}-observation"),
        &serde_json::json!({
            "assertions_passed": false,
            "native_cost_qualified": false,
            "phase": phase,
            "source": captured.source()?,
            "manifest": captured.manifest,
            "records": captured.records,
        }),
    )
}
#[track_caller]
fn invalid() -> Error {
    eprintln!("owned schema queue fixture premise failed at {}", std::panic::Location::caller());
    CollectionStop::SourceUnavailable.error()
}
#[test]
fn actual_schema_queue_body_future_has_bounded_inline_storage() {
    fn storage<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> (usize, usize) {
        (std::mem::size_of::<F>(), std::mem::size_of::<F::Output>())
    }
    let (bytes, output) =
        storage(|(fixture, cancel): (&'static mut Fixture, &'static CollectionCancellation)| prove(fixture, cancel));
    assert!(bytes <= 64 * 1024, "actual schema queue body {bytes}");
    assert!(output <= 256, "actual schema queue output {output}");
}
