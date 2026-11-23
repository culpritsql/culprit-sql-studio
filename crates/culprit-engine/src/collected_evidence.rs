//! A verified historical query capture can feed the existing pure checks without a database provider or credentials.
use culprit_core::{
    CollectorNote, Environment, MissingCapability, Result, SNAPSHOT_FORMAT, ServerInfo, Snapshot, SourceOutcome,
    SourceState, Workload,
    collected_analysis::*,
    serialization::{JsonSizeError, json_size},
    source_id,
    telemetry::DeliveryProblem,
};
use culprit_evidence::analytical::evidence::EvidenceCapture;
use culprit_evidence::original_projection::OriginalProjection;

pub(crate) mod catalogs;
mod groups;
mod populations;
mod settings;
pub(crate) mod statement_counters;
mod validation;
mod waits;
pub(crate) use settings::{RetainedSettingsAnalysis, analyze_settings_original, settings_read};
mod window;
pub use window::analyze_window;
pub(crate) use window::analyze_window_with;
#[cfg(all(test, feature = "store-adx", feature = "recovery-azure"))]
pub(crate) use window::tests::case as window_test_original;

const MAX_RETAINED_ANALYSIS_BYTES: usize = 256 * 1024 * 1024;

/// Analyze one explicitly selected capture. This cannot merge overlapping captures, source epochs or databases.
pub fn analyze(
    capture: &EvidenceCapture,
    population: Option<&culprit_core::telemetry::NativeIdentity>,
) -> Result<CollectedQueryAnalysis> {
    analyze_with_limit(capture, population, MAX_RETAINED_ANALYSIS_BYTES)
}

fn analyze_with_limit(
    capture: &EvidenceCapture,
    population: Option<&culprit_core::telemetry::NativeIdentity>,
    limit: usize,
) -> Result<CollectedQueryAnalysis> {
    validation::warehouse_capture(capture)?;
    let original = capture.verified_original()?;
    let mut analysis = CollectedQueryAnalysis {
        context: crate::collected_context::from_capture(capture),
        requested_population: population.cloned(),
        analyses: Vec::new(),
    };
    let retained = json_size(&analysis, limit).map_err(output_size_error)?;
    analysis.analyses = analyze_original_with_limit(
        &original,
        population,
        capture.range_id(),
        limit - retained,
        &culprit_checks::Pipeline::builtin(),
        culprit_core::AnalysisOrigin::ArchivedTelemetry,
        &catalogs::CatalogContext::default(),
    )?;
    Ok(analysis)
}

pub(crate) fn analyze_original_with_catalogs(
    capture: &dyn OriginalProjection,
    analysis_key: &str,
    pipeline: &culprit_checks::Pipeline,
    catalogs: &catalogs::CatalogContext,
) -> Result<Vec<CollectedQueryAnalysisResult>> {
    analyze_original_with_limit(
        capture,
        None,
        analysis_key,
        MAX_RETAINED_ANALYSIS_BYTES - 2,
        pipeline,
        culprit_core::AnalysisOrigin::RetainedInput,
        catalogs,
    )
}
/// Comparison uses the same empty historical Snapshot owner, with only the admitted settings populated.
pub(crate) fn settings_snapshot(
    original: &dyn OriginalProjection,
    settings: Vec<culprit_core::Setting>,
    taken_at: &str,
    pipeline: &culprit_checks::Pipeline,
) -> Result<Snapshot> {
    let database = original.source().target.database().ok_or_else(invalid)?.clone();
    let analysis_key = original.original().key()?;
    let catalogs = catalogs::CatalogContext::default();
    let policy = AnalysisPolicy {
        analysis_key: &analysis_key,
        pipeline,
        origin: culprit_core::AnalysisOrigin::RetainedInput,
        catalogs: &catalogs,
    };
    let mut snapshot = snapshot(original, database, None, false, &policy);
    snapshot.taken_at = taken_at.into();
    snapshot.settings = settings;
    Ok(snapshot)
}
fn analyze_original_with_limit(
    capture: &dyn OriginalProjection,
    population: Option<&culprit_core::telemetry::NativeIdentity>,
    analysis_key: &str,
    limit: usize,
    pipeline: &culprit_checks::Pipeline,
    origin: culprit_core::AnalysisOrigin,
    catalogs: &catalogs::CatalogContext,
) -> Result<Vec<CollectedQueryAnalysisResult>> {
    let policy = AnalysisPolicy { analysis_key, pipeline, origin, catalogs };
    let database = validation::capture(capture)?;
    let selected = populations::PopulationIndex::new(capture, population)?;
    let definitions = validation::Definitions::new(capture)?;
    let mut results = Vec::new();
    let mut retained = 0usize;
    for population in selected.selected() {
        let result = analyze_population(
            capture,
            &definitions,
            &database,
            population.clone(),
            selected.rows(population)?,
            &policy,
        )?;
        let comma = usize::from(!results.is_empty());
        let remaining = limit
            .checked_sub(retained)
            .and_then(|remaining| remaining.checked_sub(comma))
            .ok_or_else(|| output_size_error(JsonSizeError::LimitExceeded))?;
        let bytes = json_size(&result, remaining).map_err(output_size_error)?;
        retained += comma + bytes;
        results.push(result);
    }
    Ok(results)
}

fn output_size_error(error: JsonSizeError) -> culprit_core::Error {
    match error {
        JsonSizeError::LimitExceeded => {
            DeliveryProblem::Capacity.error().with_hint(culprit_core::t!("engine.collected_analysis.output_limit_hint"))
        }
        JsonSizeError::Serialization => DeliveryProblem::Integrity.error(),
    }
}

fn analyze_population(
    capture: &dyn OriginalProjection,
    definitions: &validation::Definitions<'_>,
    database: &culprit_core::DatabaseIdentity,
    population: CollectedQueryPopulation,
    rows: &populations::NativeRows<'_>,
    policy: &AnalysisPolicy<'_>,
) -> Result<CollectedQueryAnalysisResult> {
    let prepared = prepare_population(capture, definitions, population, rows)?;
    finish_population(capture, database, prepared, policy)
}

struct PreparedPopulation {
    population: CollectedQueryPopulation,
    totals: Option<groups::CapturedTotals>,
    query_inputs: Vec<CollectedQueryInput>,
    workload: Option<Workload>,
    partial: bool,
    limitations: Vec<CollectedAnalysisLimitation>,
}
fn prepare_population(
    capture: &dyn OriginalProjection,
    definitions: &validation::Definitions<'_>,
    population: CollectedQueryPopulation,
    rows: &populations::NativeRows<'_>,
) -> Result<PreparedPopulation> {
    let mut limitations = vec![
        limit(None, CollectedAnalysisLimit::MissingCurrentServerMetadata),
        limit(None, CollectedAnalysisLimit::MissingSchema),
        limit(None, CollectedAnalysisLimit::MissingSettings),
        limit(None, CollectedAnalysisLimit::MissingResourceMeasurements),
    ];
    let totals = if population == CollectedQueryPopulation::Unknown {
        None
    } else {
        groups::captured_totals(capture, definitions, rows)?
    };
    let (workload, query_inputs, partial) = if population == CollectedQueryPopulation::Unknown {
        limitations.push(limit(None, CollectedAnalysisLimit::ExecutionPopulationUnknown));
        (None, vec![], true)
    } else {
        let (queries, inputs, partial) = groups::project(capture, definitions, &population, rows, &mut limitations)?;
        (workload(capture, &population, totals.as_ref(), queries, &mut limitations)?, inputs, partial)
    };
    let membership_incomplete = capture
        .source()
        .query_history()
        .and_then(|history| history.population_catalog.as_ref())
        .is_some_and(|catalog| !catalog.has_complete_membership());
    Ok(PreparedPopulation {
        population,
        totals,
        query_inputs,
        workload,
        partial: partial || membership_incomplete,
        limitations,
    })
}
fn finish_population(
    capture: &dyn OriginalProjection,
    database: &culprit_core::DatabaseIdentity,
    prepared: PreparedPopulation,
    policy: &AnalysisPolicy<'_>,
) -> Result<CollectedQueryAnalysisResult> {
    let PreparedPopulation { population, totals, query_inputs, workload, partial, mut limitations } = prepared;
    let runtime_totals = totals.as_ref().map(groups::CapturedTotals::report);
    let mut snapshot = snapshot(capture, database.clone(), workload, partial, policy);
    policy.catalogs.apply(&mut snapshot);
    if policy.catalogs.settings_complete() {
        limitations.retain(|limitation| limitation.reason != CollectedAnalysisLimit::MissingSettings);
    }
    let report = run_retained_snapshot(snapshot, policy.pipeline, policy.origin, &mut limitations);
    Ok(CollectedQueryAnalysisResult {
        runtime_totals,
        population,
        query_inputs,
        workload: report.snapshot.workload,
        findings: report.findings,
        assessments: report.assessments,
        locked: report.locked,
        limitations,
        provenance: report.provenance,
    })
}

fn workload(
    capture: &dyn OriginalProjection,
    population: &CollectedQueryPopulation,
    totals: Option<&groups::CapturedTotals>,
    queries: Vec<culprit_core::QueryStat>,
    limitations: &mut Vec<CollectedAnalysisLimitation>,
) -> Result<Option<Workload>> {
    if queries.is_empty() {
        return Ok(None);
    }
    let history = capture.source().query_history().ok_or_else(invalid)?;
    let Some(totals) = totals else {
        limitations.push(limit(None, CollectedAnalysisLimit::RuntimeDenominatorUnavailable));
        return Ok(None);
    };
    let Some(time_ms) = totals.time_ms.filter(|_| totals.executions <= u128::from(groups::MAX_EXACT_COUNT)) else {
        limitations.push(limit(None, CollectedAnalysisLimit::RuntimeDenominatorUnavailable));
        return Ok(None);
    };
    let hours = validation::hours(&history.interval.from, &history.interval.until)?;
    Ok(Some(Workload {
        population: match population {
            CollectedQueryPopulation::Population { identity } => Some(identity.clone()),
            _ => None,
        },
        population_catalog: None,
        source: capture.source().source.clone(),
        kind: None,
        windowed: true,
        log: None,
        window_start: Some(history.interval.from.clone()),
        window_end: Some(history.interval.until.clone()),
        window_hours: hours,
        total_time_ms: time_ms,
        total_cpu_ms: totals.cpu_ms,
        total_executions: totals.executions as f64,
        query_count: totals.queries.len() as f64,
        queries,
        timeline: None,
        untracked_time_ms: None,
        hourly_time_ms: None,
        min_logged_ms: None,
        history_inputs: vec![],
        counter_set: None,
        source_label: None,
    }))
}

fn snapshot(
    capture: &dyn OriginalProjection,
    database: culprit_core::DatabaseIdentity,
    workload: Option<Workload>,
    partial: bool,
    policy: &AnalysisPolicy<'_>,
) -> Snapshot {
    let have_plans =
        workload.as_ref().is_some_and(|workload| workload.queries.iter().any(|query| query.plan.is_some()));
    let reason = culprit_core::message!("engine.collected_analysis.not_captured");
    let mut sources = source_id::ALL
        .iter()
        .map(|source| SourceOutcome {
            source: (*source).into(),
            state: SourceState::Unavailable,
            reason: Some(culprit_core::LocalizedText::new(reason.clone())),
        })
        .collect::<Vec<_>>();
    if workload.is_some()
        && let Some(source) = sources.iter_mut().find(|source| source.source == source_id::WORKLOAD)
    {
        source.state = if partial { SourceState::Partial } else { SourceState::Collected };
        source.reason = partial.then(|| {
            culprit_core::LocalizedText::new(culprit_core::message!("engine.collected_analysis.partial_workload"))
        });
    }
    if let Some(workload) = &workload
        && workload.queries.iter().any(|query| query.waits_coverage.is_some())
        && let Some(source) = sources.iter_mut().find(|source| source.source == source_id::QUERY_WAITS)
    {
        let complete = workload.queries.iter().all(|query| {
            query.waits_coverage.as_ref().is_some_and(|coverage| coverage.complete && coverage.is_valid())
        });
        source.state = if complete { SourceState::Collected } else { SourceState::Partial };
        source.reason = (!complete).then(|| {
            culprit_core::LocalizedText::new(culprit_core::message!("engine.collected_analysis.partial_workload"))
        });
    }
    if have_plans && let Some(source) = sources.iter_mut().find(|source| source.source == source_id::RETAINED_PLANS) {
        source.state = SourceState::Partial;
        source.reason = Some(culprit_core::message!("engine.collected_analysis.partial_workload").into());
    }
    let capabilities = policy
        .pipeline
        .checks()
        .iter()
        .flat_map(|check| check.info().requires)
        .collect::<std::collections::BTreeSet<_>>();
    let server = ServerInfo {
        engine: database.engine,
        database: database.database.clone(),
        product: String::new(),
        version: String::new(),
        edition: None,
        cloud: None,
        platform: None,
        database_size_bytes: None,
        started_at: None,
        read_only_session: false,
        capabilities: if have_plans { vec![culprit_core::Capability::QueryPlans] } else { vec![] },
        missing: capabilities
            .into_iter()
            .filter(|capability| !have_plans || *capability != culprit_core::Capability::QueryPlans)
            .map(|capability| MissingCapability::new(capability, reason.clone()))
            .collect(),
    };
    Snapshot {
        format: SNAPSHOT_FORMAT,
        id: policy.analysis_key.into(),
        connection: String::new(),
        // No connection is opened and no environment/session claim is exposed by CollectedQueryAnalysis.
        environment: Environment::Production,
        taken_at: capture.collected_until().into(),
        duration_ms: 0.0,
        server,
        runtime_populations: None,
        workload,
        tables: vec![],
        indexes: vec![],
        columns: vec![],
        foreign_keys: vec![],
        missing_indexes: vec![],
        statistics: vec![],
        waits: vec![],
        resources: vec![],
        settings: vec![],
        notes: vec![CollectorNote::recorded("collected-evidence", reason)],
        pressure: None,
        options: None,
        query_families: vec![],
        plan_cache: None,
        tuning: None,
        deadlocks: None,
        constraints: vec![],
        histograms: None,
        escalations_before: None,
        compilations_before: None,
        contention_before: None,
        evictions_before: None,
        sizes_before: None,
        operations: Default::default(),
        sources,
        identity: Some(database),
    }
}

pub(super) fn limit(record_id: Option<String>, reason: CollectedAnalysisLimit) -> CollectedAnalysisLimitation {
    CollectedAnalysisLimitation { record_id, reason }
}

fn run_retained_snapshot(
    snapshot: Snapshot,
    pipeline: &culprit_checks::Pipeline,
    origin: culprit_core::AnalysisOrigin,
    limitations: &mut Vec<CollectedAnalysisLimitation>,
) -> culprit_core::Report {
    let mut report = pipeline.run(snapshot);
    if let Some(provenance) = &mut report.provenance {
        provenance.origin = origin;
    }
    apply_executable_fix_policy(&mut report.findings, report.snapshot.workload.as_mut(), limitations);
    report
}

fn apply_executable_fix_policy(
    findings: &mut [culprit_core::Finding],
    workload: Option<&mut Workload>,
    limitations: &mut Vec<CollectedAnalysisLimitation>,
) {
    let prerequisites_missing = limitations.iter().any(|limitation| {
        matches!(
            limitation.reason,
            CollectedAnalysisLimit::MissingCurrentServerMetadata
                | CollectedAnalysisLimit::MissingSchema
                | CollectedAnalysisLimit::MissingSettings
        )
    });
    if !prerequisites_missing {
        return;
    }
    // This decision follows missing native prerequisites, not the age or storage location of the input.
    // Preserve the check's plain-language advice and verification while withholding unestablished executable SQL.
    for finding in findings {
        if let Some(fix) = &mut finding.fix
            && fix.sql.take().is_some()
        {
            limitations
                .push(limit(Some(finding.id.clone()), CollectedAnalysisLimit::ExecutableFixPrerequisitesUnknown));
        }
    }
    for query in workload.into_iter().flat_map(|workload| &mut workload.queries) {
        let mut changed = false;
        for plan in query
            .plan
            .iter_mut()
            .chain(query.plan_history.iter_mut().filter_map(|history| history.captured_plan.as_mut()))
        {
            changed |= crate::archived_advice::withhold_plan_sql(plan);
        }
        if changed {
            limitations.push(limit(Some(query.id.clone()), CollectedAnalysisLimit::ExecutableFixPrerequisitesUnknown));
        }
    }
}

#[cfg(test)]
#[path = "collected_evidence/advice_tests.rs"]
mod advice_tests;

#[cfg(test)]
#[path = "collected_evidence/retention_tests.rs"]
mod retention_tests;

pub(super) fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

/// Reuse the same exact native definition join for a developer-facing original query.
pub(crate) fn original_query<'a>(
    capture: &'a dyn OriginalProjection,
    references: &culprit_core::telemetry::StatementReferences,
) -> Result<Option<&'a culprit_core::telemetry::QueryDefinition>> {
    Ok(validation::Definitions::new(capture)?.join(references)?.query)
}

struct AnalysisPolicy<'a> {
    analysis_key: &'a str,
    pipeline: &'a culprit_checks::Pipeline,
    origin: culprit_core::AnalysisOrigin,
    catalogs: &'a catalogs::CatalogContext,
}

#[cfg(test)]
pub(crate) use retention_tests::fixture::Fixture as RetainedEvidenceFixture;
