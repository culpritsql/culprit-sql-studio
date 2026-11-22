use super::*;
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::BTreeSet;

pub(super) fn project(
    originals: &[&dyn OriginalProjection],
    description: &DerivedDiagnosisDescription,
    display: &DerivedDisplayContext,
    budget: &mut OutputBudget,
) -> Result<Vec<DerivedIncident>> {
    let primary = *originals.first().ok_or_else(invalid)?;
    let rows = primary
        .records()
        .iter()
        .filter(|row| matches!(row.row().record, TelemetryRecord::Operational(_)))
        .map(|row| (row.row(), row.row_ordinal()))
        .collect::<Vec<_>>();
    let history = crate::incident_history::from_original(
        primary.source(),
        primary.original(),
        &rows,
        primary.is_read_complete(),
    )?;
    let database = primary.source().target.database().map(IncidentDatabaseSelector::from_route).transpose()?;
    let mut output = Vec::with_capacity(history.entries.len());
    for mut entry in history.entries {
        for issue in &description.context_issues {
            let reason = issue.explanation();
            if !entry.reasons.contains(&reason) {
                entry.reasons.push(reason);
            }
        }
        let position=primary.records().iter().position(|row|matches!(&row.row().record,
            TelemetryRecord::Operational(OperationalObservation {fact:OperationalFact::DiagnosticRevision(revision),..}) if revision.id==entry.revision_id)).ok_or_else(invalid)?;
        let TelemetryRecord::Operational(OperationalObservation {
            fact: OperationalFact::DiagnosticRevision(revision),
            ..
        }) = &primary.records()[position].row().record
        else {
            return Err(invalid());
        };
        let episode = &revision.episode;
        let affected_queries = affected(originals, episode)?;
        let explanation = explanations::for_episode(originals, episode)?;
        let original_interval = match (&episode.signal, &episode.scope.population) {
            (
                DiagnosticSignal::OriginalObservation {
                    condition: DiagnosticOriginalCondition::MssqlQueryHistory { .. },
                    ..
                },
                DiagnosticPopulation::Source {
                    source: DiagnosticSourceKind::QueryHistory,
                    range: TelemetryRange::NativeInterval { interval },
                },
            ) => Some(interval.clone()),
            _ => None,
        };
        let caller = if affected_queries.len() == 1 { affected_queries[0].caller.clone() } else { None };
        for reason in &explanation.limitations {
            if !entry.reasons.contains(reason) {
                entry.reasons.push(reason.clone());
            }
        }
        let attribution_status = explanation.attribution_status();
        let candidates = explanation.candidates;
        let culprits = explanation.culprits;
        let mut incident = DerivedIncident {
            analysis_id: description.analysis_id.clone(),
            database: database.clone(),
            display: display.clone(),
            revision_reference: primary.reference(position)?,
            victim_summary: entry.title.clone(),
            original_interval,
            affected_queries,
            culprit_queries: culprits,
            attribution_status,
            candidate_queries: candidates,
            mechanism: explanation.mechanism,
            developer_action: explanation.action.unwrap_or_else(|| entry.next_step.clone()),
            verification: explanation.verify.unwrap_or_else(|| t!("view.synthesis.verify")),
            caller,
            entry,
        };
        incident.redact(primary.source().target.engine());
        budget.admit(&incident)?;
        output.push(incident);
    }
    Ok(output)
}
fn affected(originals: &[&dyn OriginalProjection], episode: &DiagnosticEpisode) -> Result<Vec<DerivedIncidentQuery>> {
    // A task-point episode starts at the page receipt. Its explicit Activity parent owns the affected
    // historical statement; no later request or SQL-by-name join can substitute for that parent.
    let reference = match &episode.signal {
        DiagnosticSignal::PageTaskWait { parent, .. } => parent.as_ref(),
        _ => &episode.first,
    };
    let Some((original, position)) = resolve(originals, reference)? else { return Ok(vec![]) };
    let row = &original.records()[position].row().record;
    if let TelemetryRecord::Activity(observation) = row
        && (episode.signal.activity_key() != Some(&observation.key)
            || matches!(&episode.signal, DiagnosticSignal::PageTaskWait { statement_identity, .. }
                if observation.statement_identity.as_ref() != Some(statement_identity)))
    {
        return Err(invalid());
    }
    Ok(queries::from_record(original, position)?.into_iter().collect())
}

pub(super) fn resolve<'a>(
    originals: &[&'a dyn OriginalProjection],
    reference: &DiagnosticEvidenceRef,
) -> Result<Option<(&'a dyn OriginalProjection, usize)>> {
    if !reference.is_valid() {
        return Err(invalid());
    }
    let Some(original) = originals.iter().copied().find(|original| original.capture() == &reference.capture) else {
        return Ok(None);
    };
    if original.original() != &reference.capture_ref() {
        return Err(invalid());
    }
    let Some((position, row)) = original
        .records()
        .iter()
        .enumerate()
        .find(|(_, row)| row.row_ordinal() == reference.row_ordinal && row.batch_id() == reference.batch_id)
    else {
        return Ok(None);
    };
    if row.row().record_id.as_ref() != Some(&reference.record_id)
        || row.native_payload_sha256() != reference.payload_sha256
        || row.row().representation_version != reference.representation_version
        || row.row().record.native_identity() != reference.native_key
    {
        return Err(invalid());
    }
    Ok(Some((original, position)))
}

pub(super) fn distinct(queries: Vec<DerivedIncidentQuery>) -> Result<Vec<DerivedIncidentQuery>> {
    let mut keys = BTreeSet::new();
    let mut output = Vec::new();
    for query in queries {
        let encoded = serde_json::to_vec(&query).map_err(|_| invalid())?;
        if keys.insert(digest(&encoded)) {
            output.push(query)
        }
        if output.len() > MAX_DERIVED_QUERIES {
            return Err(capacity());
        }
    }
    Ok(output)
}
