use super::*;
use crate::native::readable_secondary_recovery::tests::{actor_row, endpoint_rows, request_and_metadata};
use crate::progress::test_support as fixture;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Scripted {
    native: fixture::Reader,
    generation: u64,
    change_after: Option<usize>,
    closes: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl QueryRead for Scripted {
    fn connection_generation(&self) -> Option<u64> {
        Some(self.generation)
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        let result = self.native.query(sql, limit).await;
        if self.change_after == Some(self.native.calls.len()) {
            self.generation += 1;
        }
        result
    }
}
#[async_trait::async_trait]
impl NativeRead for Scripted {
    fn queries(&self) -> u32 {
        self.native.queries()
    }
    fn metrics(&self) -> Option<ReadWorkMetrics> {
        self.native.metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.native.close().await
    }
}
async fn request() -> CollectionRequest {
    let (original, _) = request_and_metadata().await;
    let context = NativeReadContext {
        target: original.target.clone(),
        partition: original.partition.clone(),
        database_writable: original.database_writable,
        observed_from: original.original_observed_at.clone(),
        observed_until: original.original_observed_at.clone(),
    };
    let focus = SourceReadFocus {
        basis: Box::new(original.parent.clone()),
        target: original.target.clone(),
        question: SourceReadQuestion::ReadableSecondaryRecovery,
        prerequisite: SourceReadPrerequisite::ObservedReadableSecondaryReader { request: Box::new(original) },
    };
    let execution = SourceReadExecutionScope::from_context(focus.question, &context).unwrap();
    CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::SourceRead { focus: Box::new(focus), execution: Box::new(execution) },
        budget: bounded_budget(&CollectionBudget::default()),
    }
}
fn scripted(request: &CollectionRequest, startup: bool) -> Scripted {
    let CollectionSelection::SourceRead { focus, .. } = &request.selection else { panic!() };
    let original = focus.readable_secondary_request().unwrap();
    Scripted {
        native: fixture::Reader {
            replies: [
                Ok(vec![fixture::metadata_with("t", "2026-11-14T00:00:00Z")]),
                Ok(vec![fixture::member("127.0.0.1")]),
                Ok(vec![actor_row()]),
                Ok(endpoint_rows(original, startup)),
                Ok(endpoint_rows(original, startup)),
                Ok(vec![actor_row()]),
                Ok(vec![fixture::metadata_with("t", "2026-11-14T00:00:00Z")]),
                Ok(vec![fixture::member("127.0.0.1")]),
            ]
            .into(),
            query_limit: Some(8),
            ..Default::default()
        },
        generation: 1,
        change_after: None,
        closes: Arc::default(),
    }
}
fn frozen(io: Scripted, request: &CollectionRequest) -> FrozenSource<Reader<Scripted>> {
    FrozenSource::new(
        Reader { io, host: "fixture.invalid".into(), port: 9, profile: None, verified: None, generation: None },
        request,
    )
    .unwrap()
}
#[tokio::test]
async fn actual_readonly_native_owner_survives_both_endpoints_eof_and_final_guard_with_one_held_reader() {
    let request = request().await;
    let mut source = frozen(scripted(&request, true), &request);
    let description = source.describe().await.unwrap();
    assert!(description.target.database().unwrap().incarnation.is_none());
    assert!(description.matches_request(&request));
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    let [
        TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::ReadableSecondaryRecovery(point),
            ..
        }),
    ] = records.as_slice()
    else {
        panic!()
    };
    assert_eq!(point.native_blocking_readers().len(), 1);
    assert!(point.conflict_changes().unwrap().iter().all(|change| matches!(
        change,
        RecoveryConflictChange::Unavailable { reason: RecoveryConflictDeltaGap::UnorderedClocks, .. }
    )));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.query_count(), 8, "plus the production owner's separately reserved SET makes nine");
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn empty_startup_is_partial_unknown_and_never_a_healthy_or_culprit_verdict() {
    let request = request().await;
    let mut source = frozen(scripted(&request, false), &request);
    let description = source.describe().await.unwrap();
    assert_eq!(
        description.observation.snapshot().unwrap().coverage_state().unwrap(),
        culprit_core::SourceState::Partial
    );
    let CollectionRead::Page { records, .. } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    let TelemetryRecord::Operational(OperationalObservation {
        fact: OperationalFact::ReadableSecondaryRecovery(point),
        ..
    }) = &records[0]
    else {
        panic!()
    };
    assert!(point.native_blocking_readers().is_empty());
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn cancellation_error_and_transport_change_stop_at_each_new_native_phase_without_replacement() {
    for index in 0..8 {
        let request = request().await;
        for stop in [CollectionStop::Cancelled, CollectionStop::TimedOut, CollectionStop::CancellationUnconfirmed] {
            let mut io = scripted(&request, true);
            io.native.replies[index] = Err(stop.error());
            let closes = io.closes.clone();
            let mut source = frozen(io, &request);
            assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), stop, "{index}");
            assert_eq!(source.query_count(), index as u32 + 1);
            assert_eq!(source.close().await, SourceShutdown::Quiescent);
            assert_eq!(closes.load(Ordering::SeqCst), 1);
            assert!(source.finish().await.is_err());
            assert_eq!(source.query_count(), index as u32 + 1);
        }
    }
    let request = request().await;
    let mut io = scripted(&request, true);
    io.change_after = Some(4);
    let mut source = frozen(io, &request);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 4);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
#[tokio::test]
async fn caller_eight_cannot_replace_the_reserved_setup_with_missing_final_native_work() {
    let mut request = request().await;
    request.budget.max_queries = 8;
    let mut io = scripted(&request, true);
    io.native.query_limit = Some(7);
    let mut source = frozen(io, &request);
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::QueryLimit);
    assert_eq!(source.query_count(), 7);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let caps = bounded_budget(&CollectionBudget::upper_limits());
    assert_eq!(
        (caps.max_queries, caps.max_snapshot_rows, caps.statement_timeout_ms, caps.total_timeout_ms),
        (9, 1, 1000, 5000)
    );
}

#[tokio::test]
async fn supported_native_profile_role_member_and_visibility_remain_mandatory_before_publication() {
    for defect in ["version", "primary", "permission", "member", "restart"] {
        let request = request().await;
        let mut io = scripted(&request, true);
        match defect {
            "version" => {
                io.native.replies[0] = Ok(vec![fixture::metadata_on_version(170000, "t", "2026-11-14T00:00:00Z")])
            }
            "primary" => io.native.replies[0] = Ok(vec![fixture::metadata_with("f", "2026-11-14T00:00:00Z")]),
            "permission" => {
                io.native.replies[0] =
                    Ok(vec![fixture::metadata_with_visibility(180000, "t", "2026-11-14T00:00:00Z", false)])
            }
            "member" => io.native.replies[7] = Ok(vec![fixture::member("127.0.0.2")]),
            _ => io.native.replies[6] = Ok(vec![fixture::metadata_with("t", "2026-11-14T01:00:00Z")]),
        }
        let mut source = frozen(io, &request);
        assert!(source.describe().await.is_err(), "{defect}");
        assert_eq!(
            source.query_count(),
            match defect {
                "member" => 8,
                "restart" => 7,
                _ => 1,
            }
        );
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}

#[tokio::test]
async fn source_owned_builtin_verification_refuses_forged_policy_foreign_scope_and_retired_authority() {
    use std::sync::atomic::AtomicBool;
    let request = request().await;
    let recipe = crate::telemetry::declarations()
        .into_iter()
        .find(|recipe| recipe.id == "postgres.readable-secondary-recovery")
        .unwrap();
    let RecipeAdmission::BuiltinSafety(policy) = recipe.admission else { panic!() };
    assert!(crate::builtin_safety::verify(&policy).unwrap());
    let CollectionSelection::SourceRead { execution, .. } = &request.selection else { panic!() };
    let scope = RecipeCostScope::NativeRead(execution.as_ref().clone());
    let profile = SourceProfileAdmission::for_builtin(
        digest(b"controlled-native-route"),
        policy.as_ref().clone(),
        Some(scope.clone()),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    let mut source = FrozenSource::new(
        Reader {
            io: scripted(&request, true),
            host: "fixture.invalid".into(),
            port: 9,
            profile: Some(SourceProfileGuard::new(&request, &profile).unwrap()),
            verified: None,
            generation: None,
        },
        &request,
    )
    .unwrap();
    let description = source.describe().await.unwrap();
    let CollectionRead::Page { records, next_cursor: None } =
        source.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!("the actual frozen result must reach EOF")
    };
    assert_eq!(records.len(), description.observation.snapshot().unwrap().records as usize);
    let [
        TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::ReadableSecondaryRecovery(point),
            ..
        }),
    ] = records.as_slice()
    else {
        panic!("one native recovery point")
    };
    assert!(point.fits_source(&description));
    assert_eq!(source.finish().await.unwrap(), SourceCompletion::Verified);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut forged = policy.as_ref().clone();
    forged.templates_sha256 = digest(b"foreign-template");
    assert!(!crate::builtin_safety::verify(&forged).unwrap());
    let profile = SourceProfileAdmission::for_builtin(
        digest(b"controlled-native-route"),
        forged,
        Some(scope.clone()),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    let mut source = FrozenSource::new(
        Reader {
            io: scripted(&request, true),
            host: "fixture.invalid".into(),
            port: 9,
            profile: Some(SourceProfileGuard::new(&request, &profile).unwrap()),
            verified: None,
            generation: None,
        },
        &request,
    )
    .unwrap();
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(
        source.query_count(),
        1,
        "direct native guard stops after the paid platform check; normal registration also verifies offline"
    );
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let RecipeCostScope::NativeRead(mut foreign) = scope.clone() else { panic!() };
    let SourceReadExecutionScope::Local(local) = &mut foreign else { panic!() };
    let SourcePartition::InstanceLocal { epoch, .. } = &mut local.partition else { panic!() };
    *epoch = Some("foreign-native-recovery-epoch".into());
    let profile = SourceProfileAdmission::for_builtin(
        digest(b"controlled-native-route"),
        policy.as_ref().clone(),
        Some(RecipeCostScope::NativeRead(foreign)),
        request.budget.clone(),
        || true,
    )
    .unwrap();
    assert!(SourceProfileGuard::new(&request, &profile).is_err());
    let active = Arc::new(AtomicBool::new(true));
    let owner = active.clone();
    let profile = SourceProfileAdmission::for_builtin(
        digest(b"controlled-native-route"),
        policy.as_ref().clone(),
        Some(scope),
        request.budget.clone(),
        move || owner.load(Ordering::SeqCst),
    )
    .unwrap();
    let guard = SourceProfileGuard::new(&request, &profile).unwrap();
    active.store(false, Ordering::SeqCst);
    let mut source = FrozenSource::new(
        Reader {
            io: scripted(&request, true),
            host: "fixture.invalid".into(),
            port: 9,
            profile: Some(guard),
            verified: None,
            generation: None,
        },
        &request,
    )
    .unwrap();
    assert_eq!(CollectionStop::from_error(&source.describe().await.unwrap_err()), CollectionStop::SourceChanged);
    assert_eq!(source.query_count(), 0);
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
}
