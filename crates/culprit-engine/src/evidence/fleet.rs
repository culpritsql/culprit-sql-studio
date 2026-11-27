//! Source-free estate triage over authenticated archive publications and verified derived diagnoses.
use super::{
    EvidenceReadPermit, EvidenceService, EvidenceSession,
    session::{MetadataAccounting, ReadFence, SessionUsage, add_metrics, deduct},
};
use culprit_core::{collected_analysis::EvidenceGapReason, serialization::JsonSizeError, telemetry::*, *};
use culprit_evidence::{
    analytical::evidence::{
        EvidenceFamily, EvidenceReadBudget, FleetResourceQuery, PublishedResourceHint, resource_namespace_id,
    },
    delivery::{
        object_store::ObjectRecoveryReader,
        recovery::{CommittedRange, RecoveryRead, reopen},
    },
};
use culprit_read::cancellation::CollectionCancellation;
use std::collections::BTreeMap;

impl EvidenceService {
    pub fn fleet_budget() -> EvidenceReadBudget {
        EvidenceReadBudget { max_queries: 256, max_rows: 50_000, ..EvidenceReadBudget::default() }
    }

    pub async fn fleet(
        &self,
        request: &WarehouseFleetRequest,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<WarehouseFleetReport> {
        self.read_fleet(request, budget, cancellation, None).await
    }

    pub async fn fleet_with_permit(
        &self,
        request: &WarehouseFleetRequest,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
        permit: EvidenceReadPermit,
    ) -> Result<WarehouseFleetReport> {
        self.read_fleet(request, budget, cancellation, Some(permit)).await
    }

    async fn read_fleet(
        &self,
        request: &WarehouseFleetRequest,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
        permit: Option<EvidenceReadPermit>,
    ) -> Result<WarehouseFleetReport> {
        let mut session = self.session(budget, cancellation)?.retaining(permit);
        session.fleet(request).await
    }
}

impl EvidenceSession<'_> {
    pub async fn fleet(&mut self, request: &WarehouseFleetRequest) -> Result<WarehouseFleetReport> {
        self.perform(
            |budget| {
                request.validate()?;
                budget.validate()
            },
            |service, budget, fence| async move { evaluate_fleet(service, request, budget, fence).await },
        )
        .await
    }
}

impl crate::WarehouseFleetReader for EvidenceService {
    fn fleet<'a>(
        &'a self,
        request: &'a WarehouseFleetRequest,
        cancellation: &'a CollectionCancellation,
    ) -> crate::WarehouseFleetTask<'a> {
        Box::pin(self.fleet(request, Self::fleet_budget(), cancellation))
    }
}

async fn evaluate_fleet(
    service: &EvidenceService,
    selection: &WarehouseFleetRequest,
    budget: EvidenceReadBudget,
    fence: ReadFence,
) -> Result<(WarehouseFleetReport, SessionUsage)> {
    let accounting =
        MetadataAccounting::new(service.recovery.clone(), budget.max_queries, budget.max_proof_bytes, fence.clone());
    let recovery = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
    let namespace_id = resource_namespace_id(&service.contract, recovery.id())?;
    let query = FleetResourceQuery { selection: selection.clone(), recovery_store_id: recovery.id().into() };
    fence.check()?;
    let page = service.reader.fleet_candidates(&service.contract, &query, budget).await?;
    let mut remaining = deduct(budget, &page.metrics)?;
    let mut metrics = page.metrics;
    let mut verified_rows = Vec::new();
    let mut gaps = Vec::new();

    for hint in &page.candidates {
        fence.check()?;
        let range = match reopen(&recovery, &hint.position.range_id).await {
            Ok(range) => range,
            Err(error) if error.detail == Some(ErrorDetail::Delivery(DeliveryProblem::NotReady)) => {
                gaps.push(WarehouseFleetGap::OriginalUnavailable);
                continue;
            }
            Err(error) => return Err(error),
        };
        if !hint.matches_original(&range)? {
            return Err(DeliveryProblem::Integrity.error());
        }
        if !selection.accepts_acquisition(&range.manifest().collected_from, &range.manifest().collected_until) {
            gaps.push(WarehouseFleetGap::AcquisitionClockBoundary);
            continue;
        }
        let mut step_budget = remaining;
        step_budget.max_proof_bytes = step_budget
            .max_proof_bytes
            .checked_sub(accounting.work()?.bytes)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(capacity)?;
        fence.check()?;
        if matches!(&range.manifest().source.observation, SourceObservation::DerivedDiagnosis(_)) {
            let families =
                [EvidenceFamily::DerivedIncidents, EvidenceFamily::DerivedFindings, EvidenceFamily::DerivedChanges];
            let capture = service.reader.read_capture(&range, &families, step_budget, None).await?;
            remaining = deduct(remaining, capture.metrics())?;
            add_metrics(&mut metrics, capture.metrics())?;
            if capture.gaps().iter().any(|gap| gap.reason == EvidenceGapReason::NotPublished) {
                gaps.push(WarehouseFleetGap::NotPublished);
                continue;
            }
            if !capture.is_read_complete() {
                gaps.push(WarehouseFleetGap::IncompleteDerivedAnalysis);
            }
            verified_rows.push(project_derived(hint, &range, &capture, &namespace_id, selection)?);
        } else {
            let metadata = service.reader.read_metadata(&range, step_budget).await?;
            remaining = deduct(remaining, metadata.metrics())?;
            add_metrics(&mut metrics, metadata.metrics())?;
            if !metadata.is_read_complete() {
                gaps.push(WarehouseFleetGap::NotPublished);
                continue;
            }
            verified_rows.push(project_raw(hint, &metadata, &namespace_id, selection)?);
        }
    }

    if page.next.is_some() {
        gaps.push(WarehouseFleetGap::MoreCandidates);
    }
    let metadata_work = accounting.work()?;
    metrics.proof_bytes = metrics.proof_bytes.checked_add(metadata_work.bytes).ok_or_else(capacity)?;

    let summary = summarize(&verified_rows);
    let mut rows = verified_rows.into_iter().filter(|row| matches_filter(row, &selection.filter)).collect::<Vec<_>>();
    sort_rows(&mut rows, selection.sort);

    let mut qualifications = vec![t!("fleet.reader.scope")];
    if !gaps.is_empty() {
        qualifications.push(t!("fleet.reader.partial"));
    }
    let report = WarehouseFleetReport {
        request: selection.clone(),
        summary,
        rows,
        next: page.next,
        discovery_complete: gaps.is_empty(),
        gaps,
        candidate_rows: page.charged_candidate_rows,
        archive_work: crate::collected_context::read_metrics(&metrics),
        qualifications,
        collector_targets: None,
    };
    culprit_core::serialization::json_size(&report, MAX_DERIVED_OUTPUT_BYTES).map_err(|error| match error {
        JsonSizeError::LimitExceeded => capacity(),
        JsonSizeError::Serialization => DeliveryProblem::Integrity.error(),
    })?;
    fence.check()?;
    Ok((report, SessionUsage { metrics, candidate_rows: page.charged_candidate_rows }))
}

fn project_derived(
    hint: &PublishedResourceHint,
    range: &CommittedRange,
    capture: &culprit_evidence::analytical::evidence::EvidenceCapture,
    namespace_id: &str,
    selection: &WarehouseFleetRequest,
) -> Result<WarehouseFleetRow> {
    let source = capture.source();
    let database = source.target.database().ok_or_else(invalid)?;
    let description = capture.derived_description().ok_or_else(invalid)?;
    if range.manifest().source != *source {
        return Err(invalid());
    }

    let mut episode_heads: BTreeMap<(&str, u32), (&IncidentHistoryEntry, u64)> = BTreeMap::new();
    for incident in capture.derived_incidents() {
        let ordinal = incident.entry.revision.parse::<u64>().unwrap_or(0);
        let key = (incident.entry.episode_id.as_str(), incident.entry.controller_version);
        match episode_heads.get(&key) {
            Some((_, prior)) if *prior >= ordinal => {}
            _ => {
                episode_heads.insert(key, (&incident.entry, ordinal));
            }
        }
    }
    let mut active_incidents = 0u32;
    let mut uncertain_incidents = 0u32;
    for (entry, _) in episode_heads.values() {
        match entry.lifecycle {
            DiagnosticLifecycle::Active => active_incidents = active_incidents.saturating_add(1),
            DiagnosticLifecycle::Uncertain => uncertain_incidents = uncertain_incidents.saturating_add(1),
            DiagnosticLifecycle::Resolved => {}
        }
    }

    let mut high_findings = 0u32;
    let mut actionable_findings = 0u32;
    for row in capture.derived_findings() {
        let finding = &row.finding;
        if !finding.inactive && finding.actionability.is_actionable() {
            actionable_findings = actionable_findings.saturating_add(1);
            if finding.severity == Severity::High {
                high_findings = high_findings.saturating_add(1);
            }
        }
    }

    let mut recent_changes = 0u32;
    let mut recent_regressions = 0u32;
    for row in capture.derived_changes() {
        recent_changes = recent_changes.saturating_add(1);
        if !row.change.minor {
            recent_regressions = recent_regressions.saturating_add(1);
        }
    }

    let shared_group = partition_shared_group(&source.partition)?.or_else(|| {
        capture.derived_incidents().find_map(|incident| {
            incident
                .culprit_queries
                .iter()
                .chain(incident.affected_queries.iter())
                .chain(incident.candidate_queries.iter())
                .find_map(|query| match &query.identity {
                    Some(DerivedQueryIdentity::SharedActivity { member_id, member_kind, .. }) => {
                        Some(WarehouseFleetSharedGroup {
                            member_id: member_id.clone(),
                            member_kind: *member_kind,
                            label: shared_group_label(*member_kind, &member_id[..member_id.len().min(12)]),
                        })
                    }
                    _ => None,
                })
        })
    });

    let freshness = derived_freshness(
        &description.observed_until,
        &selection.as_of,
        selection.max_age_seconds,
        capture.is_read_complete(),
        description,
    );
    let mut limitations = description.limitations();
    if !capture.is_read_complete() {
        limitations.push(t!("fleet.reader.partial"));
    }

    Ok(WarehouseFleetRow {
        resource_id: hint.position.resource_id.clone(),
        selector: PublishedResourceSelector {
            namespace_id: namespace_id.into(),
            database: IncidentDatabaseSelector::from_route(database)?,
        },
        engine: database.engine,
        database_name: description.display.database_name.clone().unwrap_or_else(|| database.database.clone()),
        connection_alias: Some(description.display.connection_alias.clone()),
        environment: description.display.environment.clone(),
        shared_group,
        freshness,
        latest_collected_from: capture.collected_from().into(),
        latest_collected_until: capture.collected_until().into(),
        latest_observed_from: Some(description.observed_from.clone()),
        latest_observed_until: Some(description.observed_until.clone()),
        active_incidents,
        uncertain_incidents,
        high_findings,
        actionable_findings,
        recent_regressions,
        recent_changes,
        input_read_complete: description.input_read_complete && capture.is_read_complete(),
        limitations,
        telemetry: None,
    })
}

fn project_raw(
    hint: &PublishedResourceHint,
    metadata: &culprit_evidence::analytical::evidence::EvidenceCaptureMetadata,
    namespace_id: &str,
    selection: &WarehouseFleetRequest,
) -> Result<WarehouseFleetRow> {
    let source = metadata.source();
    let database = source.target.database().ok_or_else(invalid)?;
    let (observed_from, observed_until) = native_bounds(source);
    let freshness = match (utc_time(&selection.as_of), utc_time(metadata.collected_until())) {
        (Some(as_of), Some(until)) if until > as_of => WarehouseFleetFreshness::ClockBoundary,
        (Some(as_of), Some(until))
            if until < as_of - chrono::Duration::seconds(i64::from(selection.max_age_seconds)) =>
        {
            WarehouseFleetFreshness::Stale
        }
        (Some(_), Some(_)) => WarehouseFleetFreshness::NoDerivedDiagnosis,
        _ => WarehouseFleetFreshness::UnknownClock,
    };
    Ok(WarehouseFleetRow {
        resource_id: hint.position.resource_id.clone(),
        selector: PublishedResourceSelector {
            namespace_id: namespace_id.into(),
            database: IncidentDatabaseSelector::from_route(database)?,
        },
        engine: database.engine,
        database_name: database.database.clone(),
        connection_alias: None,
        environment: None,
        shared_group: partition_shared_group(&source.partition)?,
        freshness,
        latest_collected_from: metadata.collected_from().into(),
        latest_collected_until: metadata.collected_until().into(),
        latest_observed_from: observed_from,
        latest_observed_until: observed_until,
        active_incidents: 0,
        uncertain_incidents: 0,
        high_findings: 0,
        actionable_findings: 0,
        recent_regressions: 0,
        recent_changes: 0,
        input_read_complete: metadata.is_read_complete(),
        limitations: vec![t!("fleet.reader.no_derived")],
        telemetry: None,
    })
}

fn partition_shared_group(partition: &SourcePartition) -> Result<Option<WarehouseFleetSharedGroup>> {
    let SourcePartition::InstanceLocal { member: Some(member), .. } = partition else {
        return Ok(None);
    };
    let member_id = member.telemetry_id()?;
    let short_name = serde_json::to_value(&member.native)
        .ok()
        .and_then(|value| value.get("id").and_then(serde_json::Value::as_str).map(str::to_owned))
        .unwrap_or_else(|| member_id[..member_id.len().min(12)].to_string());
    Ok(Some(WarehouseFleetSharedGroup {
        member_id,
        member_kind: member.kind,
        label: shared_group_label(member.kind, &short_name),
    }))
}

fn shared_group_label(kind: SharedResourceKind, member: &str) -> String {
    let kind_label = match kind {
        SharedResourceKind::Pool => "Elastic Pool",
        SharedResourceKind::Instance => "Instance",
        SharedResourceKind::Cluster => "Cluster",
        SharedResourceKind::Node => "Node",
    };
    t!("fleet.reader.shared_group", kind = kind_label, member = member)
}

fn derived_freshness(
    observed_until: &str,
    as_of: &str,
    max_age_seconds: u32,
    capture_complete: bool,
    description: &DerivedDiagnosisDescription,
) -> WarehouseFleetFreshness {
    let (Some(observed), Some(cutoff)) = (utc_time(observed_until), utc_time(as_of)) else {
        return WarehouseFleetFreshness::UnknownClock;
    };
    if observed > cutoff {
        WarehouseFleetFreshness::ClockBoundary
    } else if observed < cutoff - chrono::Duration::seconds(i64::from(max_age_seconds)) {
        WarehouseFleetFreshness::Stale
    } else if !capture_complete
        || !description.input_read_complete
        || !description.context_issues.is_empty()
        || description.output_coverage.incidents != DerivedOutputCoverage::Evaluated
        || description.output_coverage.findings != DerivedOutputCoverage::Evaluated
        || description.output_coverage.changes != DerivedOutputCoverage::Evaluated
    {
        WarehouseFleetFreshness::Partial
    } else {
        WarehouseFleetFreshness::Observed
    }
}

fn native_bounds(source: &CollectionDescription) -> (Option<String>, Option<String>) {
    if let Some(snapshot) = source.observation.snapshot() {
        return (Some(snapshot.observed_from.clone()), Some(snapshot.observed_until.clone()));
    }
    if let Some(activity) = source.observation.activity() {
        return (Some(activity.observed_from.clone()), Some(activity.observed_until.clone()));
    }
    if let Ok(range) = source.captured_range() {
        match range.range {
            TelemetryRange::NativeInterval { interval } => return (Some(interval.from), Some(interval.until)),
            TelemetryRange::TimeWindow { from, until } => return (Some(from), Some(until)),
            _ => {}
        }
    }
    (None, None)
}

fn summarize(rows: &[WarehouseFleetRow]) -> WarehouseFleetSummary {
    let mut summary = WarehouseFleetSummary { evaluated_resources: rows.len() as u32, ..Default::default() };
    for row in rows {
        if row.needs_attention() {
            summary.attention_resources = summary.attention_resources.saturating_add(1);
        }
        summary.active_incidents = summary.active_incidents.saturating_add(row.active_incidents);
        if row.high_findings > 0 {
            summary.high_finding_resources = summary.high_finding_resources.saturating_add(1);
        }
        if row.recent_regressions > 0 {
            summary.regression_resources = summary.regression_resources.saturating_add(1);
        }
        if !matches!(row.freshness, WarehouseFleetFreshness::Observed) {
            summary.stale_or_partial_resources = summary.stale_or_partial_resources.saturating_add(1);
        }
    }
    summary
}

fn matches_filter(row: &WarehouseFleetRow, filter: &WarehouseFleetFilter) -> bool {
    if filter.attention_only && !row.needs_attention() {
        return false;
    }
    if let Some(engine) = filter.engine
        && row.engine != engine
    {
        return false;
    }
    if let Some(environment) = &filter.environment
        && row.environment.as_deref() != Some(environment.as_str())
    {
        return false;
    }
    if let Some(group) = &filter.shared_group
        && row.shared_group.as_ref().is_none_or(|shared| shared.member_id != *group && shared.label != *group)
    {
        return false;
    }
    true
}

fn sort_rows(rows: &mut [WarehouseFleetRow], sort: WarehouseFleetSort) {
    match sort {
        WarehouseFleetSort::Resource => rows.sort_by(|left, right| {
            left.database_name.cmp(&right.database_name).then_with(|| left.resource_id.cmp(&right.resource_id))
        }),
        WarehouseFleetSort::Attention => rows.sort_by(|left, right| {
            right
                .needs_attention()
                .cmp(&left.needs_attention())
                .then_with(|| right.active_incidents.cmp(&left.active_incidents))
                .then_with(|| right.high_findings.cmp(&left.high_findings))
                .then_with(|| right.uncertain_incidents.cmp(&left.uncertain_incidents))
                .then_with(|| right.recent_regressions.cmp(&left.recent_regressions))
                .then_with(|| freshness_rank(right.freshness).cmp(&freshness_rank(left.freshness)))
                .then_with(|| left.database_name.cmp(&right.database_name))
                .then_with(|| left.resource_id.cmp(&right.resource_id))
        }),
    }
}

fn freshness_rank(freshness: WarehouseFleetFreshness) -> u8 {
    match freshness {
        WarehouseFleetFreshness::ClockBoundary | WarehouseFleetFreshness::UnknownClock => 4,
        WarehouseFleetFreshness::Stale => 3,
        WarehouseFleetFreshness::Partial => 2,
        WarehouseFleetFreshness::NoDerivedDiagnosis => 1,
        WarehouseFleetFreshness::Observed => 0,
    }
}

fn utc_time(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if value.len() > 64 {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .filter(|at| at.offset().local_minus_utc() == 0)
        .map(|at| at.with_timezone(&chrono::Utc))
}

fn invalid() -> Error {
    DeliveryProblem::Integrity.error()
}

fn capacity() -> Error {
    DeliveryProblem::Capacity.error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_evidence::analytical::{
        evidence::EvidenceReader,
        indexes::ProjectionIndexes,
        test_support::{ReadOnlyFixture, derived_diagnosis_evidence_cases, waits},
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn fleet_authenticates_derived_diagnoses_and_detects_staleness() {
        let (source, records) = derived_diagnosis_evidence_cases().remove(0);
        let fixture =
            ReadOnlyFixture::capture_at(source, records, 880_001, "2020-01-01T12:05:00Z", "2020-01-01T12:05:05Z").await;
        let service = EvidenceService {
            recovery: fixture.transport(),
            contract: fixture.contract.clone(),
            reader: EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport()))),
        };
        let cancellation = CollectionCancellation::default();

        let fresh = service
            .fleet(
                &WarehouseFleetRequest {
                    as_of: "2020-01-01T12:10:00Z".into(),
                    max_age_seconds: 3600,
                    ..Default::default()
                },
                EvidenceService::fleet_budget(),
                &cancellation,
            )
            .await
            .unwrap();
        assert!(fresh.discovery_complete);
        assert_eq!(fresh.rows.len(), 1);
        let row = &fresh.rows[0];
        assert!(row.active_incidents > 0 || row.actionable_findings > 0 || row.recent_changes > 0);
        assert!(fresh.summary.evaluated_resources == 1);

        let stale = service
            .fleet(
                &WarehouseFleetRequest {
                    as_of: "2020-01-02T12:10:00Z".into(),
                    max_age_seconds: 1800,
                    ..Default::default()
                },
                EvidenceService::fleet_budget(),
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(stale.rows[0].freshness, WarehouseFleetFreshness::Stale);
        assert!(stale.rows[0].needs_attention());
    }

    #[tokio::test]
    async fn fleet_reports_raw_only_when_database_lacks_derived_diagnosis() {
        let raw = waits("boundary:warehouse", &"a".repeat(64), 880_002, true, true);
        let fixture = ReadOnlyFixture::capture_at(
            raw.manifest.source,
            vec![],
            880_002,
            "2020-01-01T12:00:00Z",
            "2020-01-01T12:00:01Z",
        )
        .await;
        let service = EvidenceService {
            recovery: fixture.transport(),
            contract: fixture.contract.clone(),
            reader: EvidenceReader::new(fixture.store.clone(), Arc::new(ProjectionIndexes::new(fixture.transport()))),
        };
        let report = service
            .fleet(
                &WarehouseFleetRequest {
                    as_of: "2020-01-01T12:05:00Z".into(),
                    max_age_seconds: 1800,
                    ..Default::default()
                },
                EvidenceService::fleet_budget(),
                &CollectionCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].freshness, WarehouseFleetFreshness::NoDerivedDiagnosis);
    }
}
