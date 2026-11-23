//! The oracle is a positive observation of the unchanged waiter's grant, never a disappeared row or accepted drain.
use super::{
    FIXTURE_ROWS,
    fixture::Fixture,
    reservations::{self, HolderRequirement},
};
use crate::{read::invalid_data, test_support::save_cost_receipt};
use culprit_core::{Result, telemetry::*};

pub(super) async fn prove(fixture: &mut Fixture) -> Result<()> {
    let pair = reservations::establish(fixture, HolderRequirement::ClientOutputWait).await?;
    const FIRST_TEMPDB_GOVERNANCE_MAJOR: u32 = 17;
    if pair.point.native_major < FIRST_TEMPDB_GOVERNANCE_MAJOR
        && pair.point.observations.iter().any(|grant| grant.workload_group_tempdb.is_some())
    {
        return Err(invalid_data());
    }
    if !fixture.can_observe() {
        return Err(fixture.observation_stop());
    }
    save_cost_receipt(
        &fixture.directory,
        "drain-before",
        &serde_json::json!({
            "point": pair.point.snapshot(),
            "original_holder": pair.holder,
            "original_waiter": pair.waiter,
            "waiter_actor_index": pair.waiter_index,
            "intervention": "only the original holder's unchanged result stream will be drained",
        }),
    )?;
    require_original_resource_shortfall(&pair.point.report, &pair.waiter)?;
    let holder_rows = fixture.drain_original_holder().await?;
    if holder_rows != FIXTURE_ROWS {
        return Err(invalid_data());
    }
    while fixture.can_observe() {
        fixture.pause().await?;
        let point = fixture.observe().await?;
        if !point.context.matches_earlier_scope(&pair.point.context) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let Some(waiter) = point.actor(fixture.session(pair.waiter_index)?)? else { continue };
        if !same_original_waiter(&pair.waiter, waiter) {
            return Err(CollectionStop::SourceChanged.error());
        }
        if waiter.state != ExecutionGrantState::Granted {
            continue;
        }
        if !waiter.granted_kibibytes.as_deref().is_some_and(|value| value.parse::<u64>().is_ok_and(|amount| amount > 0))
            || waiter.granted_without_offset.is_none()
        {
            return Err(invalid_data());
        }
        return save_cost_receipt(
            &fixture.directory,
            "drain-acceptance",
            &serde_json::json!({
                "original_point": pair.point.snapshot(),
                "later_point": point.snapshot(),
                "original_holder": pair.holder,
                "original_waiter": pair.waiter,
                "same_waiter_granted": waiter,
                "holder_rows_drained_and_native_stream_completed": holder_rows,
                "intervention_count": 1,
                "waiter_sql_or_input_replaced": false,
                "disappearance_used_as_recovery": false,
                "original_resource_condition_retained": true,
                "causal_scope": "owned comparable-load control; production observations alone do not establish an exclusive cause",
                "native_cost_approval": false,
            }),
        );
    }
    Err(fixture.observation_stop())
}

fn require_original_resource_shortfall(
    report: &culprit_core::MemoryParticipantsReport,
    waiter: &ExecutionGrantObservation,
) -> Result<()> {
    let resource = waiter.resource_observation.as_ref().ok_or_else(invalid_data)?;
    let available =
        resource.available_kibibytes.as_deref().ok_or_else(invalid_data)?.parse::<i64>().map_err(|_| invalid_data())?;
    let required =
        waiter.required_kibibytes.as_deref().ok_or_else(invalid_data)?.parse::<u64>().map_err(|_| invalid_data())?;
    let requested =
        waiter.requested_kibibytes.as_deref().ok_or_else(invalid_data)?.parse::<u64>().map_err(|_| invalid_data())?;
    if waiter.queue.next_candidate != Some(true)
        || required == 0
        || required > requested
        || i128::from(available) >= i128::from(requested)
    {
        return Err(invalid_data());
    }
    let key = waiter.key()?;
    let entry = report.entries.iter().find(|entry| entry.observation_key == key).ok_or_else(invalid_data)?;
    if !entry.investigation.contains(&culprit_core::t!("view.request_memory.resource.investigation"))
        || !entry.explanation.contains(&culprit_core::t!("view.request_memory.resource.limit"))
    {
        return Err(invalid_data());
    }
    Ok(())
}

fn same_original_waiter(before: &ExecutionGrantObservation, after: &ExecutionGrantObservation) -> bool {
    let (Some(original), Some(current)) = (&before.request, &after.request) else { return false };
    before.is_valid()
        && after.is_valid()
        && before.source == after.source
        && before.identity == after.identity
        && before.requested_without_offset == after.requested_without_offset
        && before.resource.is_some()
        && before.resource == after.resource
        && before.scope == after.scope
        && before.request_association == RequestGrantAssociation::GenerationChecked
        && after.request_association == RequestGrantAssociation::GenerationChecked
        && original.key == current.key
        && original.statement_identity.is_some()
        && original.statement_identity == current.statement_identity
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn original() -> ExecutionGrantObservation {
        let mut reader = crate::activity::memory::tests::successful();
        let captured = reader.capture_memory().await.unwrap();
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        captured.observations.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn a_positive_transition_keeps_original_native_request_and_grant_time_instead_of_a_reused_session_number() {
        let granted = original().await;
        let mut waiting = granted.clone();
        waiting.state = ExecutionGrantState::Waiting;
        waiting.granted_without_offset = None;
        waiting.granted_kibibytes = Some("0".into());
        assert!(waiting.is_valid() && granted.is_valid());
        assert!(same_original_waiter(&waiting, &granted));
        #[derive(Debug)]
        enum Change {
            SessionGeneration,
            RequestGeneration,
            GrantTime,
            Resource,
            Statement,
        }
        for change in [
            Change::SessionGeneration,
            Change::RequestGeneration,
            Change::GrantTime,
            Change::Resource,
            Change::Statement,
        ] {
            let mut changed = granted.clone();
            match change {
                Change::SessionGeneration => {
                    changed.request.as_mut().unwrap().key.session.generation = "2020-01-01T07:57:01".into()
                }
                Change::RequestGeneration => {
                    changed.request.as_mut().unwrap().key.request.as_mut().unwrap().generation =
                        "2020-01-01T07:58:00.003".into()
                }
                Change::GrantTime => {
                    changed.requested_without_offset = "2020-01-01T07:58:01.001".into();
                    changed
                        .identity
                        .qualifiers
                        .insert("requested_without_offset".into(), changed.requested_without_offset.clone());
                }
                Change::Resource => changed.resource.as_mut().unwrap().id = "3".into(),
                Change::Statement => {
                    changed.request.as_mut().unwrap().statement_identity.as_mut().unwrap().id.push('1')
                }
            }
            assert!(changed.is_valid(), "{change:?} remains independently valid native-shaped evidence");
            assert!(!same_original_waiter(&waiting, &changed), "{change:?}");
        }
        let mut missing = granted;
        missing.request = None;
        assert!(!same_original_waiter(&waiting, &missing), "a missing request is not a positive transition");
    }

    #[tokio::test]
    async fn native_shortfall_accepts_composed_reservation_advice_without_losing_its_original_limits() {
        use crate::{activity::memory::tests, resources::test_support::replace, value::Val};
        use culprit_core::{SourceOutcome, SourceState};
        let holder = replace(
            &tests::row(),
            &[
                ("edition", Val::Int(3)),
                ("grant_requested_memory_kb", Val::Int(181_304)),
                ("grant_required_memory_kb", Val::Int(512)),
                ("grant_granted_memory_kb", Val::Int(181_304)),
                ("resource_pool_id", Val::Int(2)),
                ("resource_resource_semaphore_id", Val::Int(0)),
                ("resource_available_memory_kb", Val::Int(76_456)),
                ("resource_granted_memory_kb", Val::Int(181_304)),
                ("resource_grantee_count", Val::Int(1)),
                ("resource_waiter_count", Val::Int(1)),
            ],
        );
        let waiter = replace(
            &holder,
            &[
                ("session_id", Val::Int(43)),
                ("grant_session_id", Val::Int(43)),
                ("grant_granted_without_offset", Val::Null),
                ("grant_granted_memory_kb", Val::Int(0)),
                ("grant_is_next_candidate", Val::Int(1)),
            ],
        );
        let mut reader = tests::reader(vec![
            Ok(vec![crate::instance::test_support::metadata()]),
            Ok(vec![crate::instance::test_support::epoch(100)]),
            Ok(vec![crate::activity::database_context::tests::row(3)]),
            Ok(tests::columns()),
            Ok(vec![holder, waiter]),
        ]);
        let frame = reader.observe_memory().await.unwrap();
        let context = frame.owner.completed(&frame.owner).unwrap();
        let observations = frame.into_observations().unwrap();
        assert_eq!(reader.close().await, SourceShutdown::Quiescent);
        let waiter = observations.iter().find(|row| row.state == ExecutionGrantState::Waiting).unwrap();
        let rows = observations.iter().collect::<Vec<_>>();
        let report = culprit_checks::request_memory::explain(
            culprit_checks::request_memory::RequestMemoryEvidence { context: &context, observations: &rows },
            Some(&SourceOutcome { source: waiter.source.clone(), state: SourceState::Collected, reason: None }),
        )
        .unwrap();
        let entry = report.entries.iter().find(|entry| entry.observation_key == waiter.key().unwrap()).unwrap();
        assert!(entry.investigation.contains(&culprit_core::t!("view.request_memory.demand.larger_investigation")));
        assert!(entry.investigation.contains(&culprit_core::t!("view.request_memory.resource.investigation")));
        require_original_resource_shortfall(&report, waiter).unwrap();
        for (available, required, next) in [("181304", "512", true), ("76456", "0", true), ("76456", "512", false)] {
            let mut changed = waiter.clone();
            changed.resource_observation.as_mut().unwrap().available_kibibytes = Some(available.into());
            changed.required_kibibytes = Some(required.into());
            changed.queue.next_candidate = Some(next);
            assert!(require_original_resource_shortfall(&report, &changed).is_err());
        }
        let mut incomplete_advice = report;
        for entry in &mut incomplete_advice.entries {
            entry.investigation.clear();
        }
        assert!(require_original_resource_shortfall(&incomplete_advice, waiter).is_err());
    }
}
