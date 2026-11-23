use super::*;
use culprit_collector::{
    capture::{SourceFactory, collect},
    outbox::Outbox,
};
use culprit_read::admission::SourceGate;

pub(super) struct OriginalAttempt {
    pub receipt: DiagnosticAttemptReceipt,
    pub manifest: Box<CollectionManifest>,
}

struct Factory {
    pool: Arc<Pool>,
    port: u16,
}
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        crate::telemetry::source(self.pool.clone(), request, "127.0.0.1".into(), self.port)
    }
}

pub(super) async fn attempt(
    outbox: &mut Outbox,
    root: &Path,
    pool: Arc<Pool>,
    port: u16,
    cancellation: &CollectionCancellation,
) -> Result<OriginalAttempt> {
    let request = request()?;
    let started = Instant::now();
    let mut lease = outbox
        .reserve_connection(
            "owned-refused-route",
            &request,
            &digest(b"owned-refused-route-revision"),
            culprit_core::Engine::Mssql,
        )
        .await?;
    let identity = lease.identity().clone();
    let directory = lease.directory().to_owned();
    let result = Box::pin(collect(
        &SourceGate::default(),
        &Factory { pool, port },
        request,
        &directory,
        identity.clone(),
        cancellation,
        Some(&mut lease),
    ))
    .await;
    let outcome = match result {
        Ok(_) => CollectorCaptureOutcome::Returned,
        Err(error) => CollectorCaptureOutcome::Failed { code: error.code },
    };
    let expected_failure =
        outcome == (CollectorCaptureOutcome::Failed { code: culprit_core::ErrorCode::ConnectionFailed });
    lease.finish_observed(outcome, started.elapsed().as_secs_f64() * 1000.0).await?;
    let receipt = outbox.attempt_receipt(&identity, None).await?;
    let manifest = Box::new(
        Outbox::inspect_manifest(&root.join("outbox"), &identity.observation_id).await?.ok_or_else(invalid_data)?,
    );
    let status = outbox.status(2, None).await?;
    save_terminal_facts(root, &receipt, &manifest, &status, &outcome);
    if !expected_failure {
        return Err(invalid_data());
    }
    validate_manifest(&manifest)?;
    if receipt.manifest_sha256.is_none()
        || status.captures != 1
        || status.source_debts != 0
        || status.connection_controls_pending != 1
        || status.entries.len() != 1
        || status.entries[0].capture != identity
        || status.entries[0].state != OutboxCaptureState::Incomplete
        || status.entries[0].source_work != SourceWorkState::Quiescent
    {
        return Err(invalid_data());
    }
    Ok(OriginalAttempt { receipt, manifest })
}

fn save_terminal_facts(
    root: &Path,
    original: &DiagnosticAttemptReceipt,
    manifest: &CollectionManifest,
    status: &OutboxStatus,
    outcome: &CollectorCaptureOutcome,
) {
    // The existing public metric projection removes SQL/native proof bodies. Diagnostic I/O cannot replace
    // the first native/assertion error; the sealed original and its recorded ownership remain unchanged.
    let metrics =
        manifest.read_metrics.as_ref().map(|metrics| metrics.summary(manifest.description.as_ref())).transpose();
    let (metrics, metric_error) = match metrics {
        Ok(metrics) => (metrics, None),
        Err(error) => (None, Some(error.code)),
    };
    let saved = save_cost_receipt(
        root,
        &format!("source-{}-terminal", original.capture.observation_id),
        &serde_json::json!({
            "stage":"sealed-original-before-native-fact-assertions","original":original,
            "collector_api_outcome":outcome,"original_status":manifest.status,
            "description_present":manifest.description.is_some(),"source_queries":manifest.source_queries,
            "read_metrics":metrics,"metrics_projection_error":metric_error,
            "files":manifest.files.iter().map(|file|serde_json::json!({
                "stream":file.stream,"status":file.status,"rows":file.rows,"pages":file.pages,"bytes":file.bytes
            })).collect::<Vec<_>>(),
            "captures":status.captures,"source_debts":status.source_debts,
            "connection_controls_pending":status.connection_controls_pending,
            "owners":status.entries.iter().map(|entry|serde_json::json!({
                "capture":entry.capture,"state":entry.state,"source_work":entry.source_work
            })).collect::<Vec<_>>(),
            "cost_qualified":false
        }),
    );
    if let Err(error) = saved {
        eprintln!(
            "owned refused-connection diagnostics unavailable: {}",
            serde_json::json!({"stage":"sealed-original-diagnostics","code":error.code})
        );
    }
}

fn request() -> Result<CollectionRequest> {
    let provisional = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Activity,
        budget: CollectionBudget::default(),
    };
    let recipe =
        crate::telemetry::recipes().into_iter().find(|recipe| recipe.accepts(&provisional)).ok_or_else(invalid_data)?;
    let request = CollectionRequest { budget: provisional.budget.intersection(&recipe.budget), ..provisional };
    request.validate()?;
    Ok(request)
}

fn validate_manifest(manifest: &CollectionManifest) -> Result<()> {
    let metrics = manifest.read_metrics.as_ref().ok_or_else(invalid_data)?;
    let work = metrics.connection_work.as_ref().ok_or_else(invalid_data)?;
    if manifest.finished_at.is_none()
        || !matches!(manifest.status, CollectionStatus::Incomplete(_))
        || manifest.description.is_some()
        || manifest.source_queries != 1
        || !metrics.templates.is_empty()
        || !metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Quiescent)
        || metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Active)
        || metrics.proves_pre_dispatch_connection_failure(SourceWorkState::Unconfirmed)
        || !work.has_complete_observation_roster()
        || work.attempts.len() != 1
        || work.attempts[0].reader_generation.is_some()
        || !work.attempts[0].phases.iter().any(|phase| {
            phase.stage == ReadConnectionStage::Transport
                && matches!(phase.outcome, ReadConnectionOutcome::Failed { .. })
        })
        || work.attempts[0].phases.iter().any(|phase| {
            matches!(
                phase.stage,
                ReadConnectionStage::HandshakeAndLogin
                    | ReadConnectionStage::SessionSetup
                    | ReadConnectionStage::FirstQuery
            )
        })
        || manifest.files.iter().any(|file| file.rows != 0 || file.pages != 0 || file.bytes != 0)
    {
        return Err(invalid_data());
    }
    Ok(())
}
