//! Resolve only original rows from the same bounded node/database capture.
use super::*;

const MAX_CONTEXT_REFERENCES: usize = 6;

pub(super) fn explain(original: &dyn OriginalProjection, position: usize) -> Result<Explanation> {
    let record = &original.records().get(position).ok_or_else(invalid)?.row().record;
    let Some(DiagnosticOriginalCondition::ClickHouse { incident }) =
        DiagnosticOriginalCondition::from_record(original.source(), record)
    else {
        return Err(invalid());
    };
    let point = observation(record).ok_or_else(invalid)?;
    let identity = point.identity();
    let node = identity.qualifiers.get("node").ok_or_else(invalid)?;
    let mut words =
        Explanation::words(&point.summary(), &incident.followup(), &t!("provider.clickhouse.verify", node = node));
    words.victim_summary = Some(incident.title());
    for (sibling, row) in original.records().iter().enumerate() {
        let Some(context) = observation(&row.row().record) else {
            continue;
        };
        if position == sibling || !context.fits_source(original.source()) || !context.is_valid() {
            continue;
        }
        if let Some(label) = context_label(point, context) {
            let reference = original.reference(sibling)?;
            words.context_evidence.push(IncidentHistoryEvidence {
                label,
                capture: reference.capture_ref(),
                reference: Some(reference),
                availability: t!("view.incident_history.evidence.referenced"),
            });
            if words.context_evidence.len() == MAX_CONTEXT_REFERENCES {
                break;
            }
        }
    }
    Ok(words)
}

fn observation(record: &TelemetryRecord) -> Option<&ClickHouseObservation> {
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::ClickHouse(point), .. }) = record
    else {
        return None;
    };
    Some(point)
}

fn context_label(point: &ClickHouseObservation, context: &ClickHouseObservation) -> Option<String> {
    if let (ClickHouseObservation::Failure(failure), ClickHouseObservation::QueryFamily(family)) = (point, context) {
        return (failure.family == family.family).then(|| {
            t!(
                "provider.clickhouse.context_family",
                family = &family.family,
                runs = culprit_core::format::count(family.executions as f64),
                from = &family.from,
                until = &family.until
            )
        });
    }
    let context_id = context.identity();
    let table = context_id.qualifiers.get("table")?;
    let shares_table = match point {
        ClickHouseObservation::Failure(failure) => failure.tables.contains(&format!("{}.{}", failure.database, table)),
        _ => point.identity().qualifiers.get("table") == Some(table),
    };
    if !shares_table {
        return None;
    }
    // Co-occurrence on a table does not identify the query or actor responsible for a failure.
    let record = match context {
        ClickHouseObservation::Parts(row) => t!(
            "provider.clickhouse.context_parts",
            partition = &row.partition_id,
            parts = culprit_core::format::count(row.active_parts as f64)
        ),
        ClickHouseObservation::Merge(row) => t!("provider.clickhouse.context_merge", part = &row.result_part_name),
        ClickHouseObservation::Replica(row) => t!(
            "provider.clickhouse.context_replica",
            replica = &row.replica_name,
            tasks = culprit_core::format::count(row.queue_size as f64)
        ),
        ClickHouseObservation::Mutation(row) => t!(
            "provider.clickhouse.context_mutation",
            mutation = &row.mutation_id,
            parts = culprit_core::format::count(row.parts_to_do as f64)
        ),
        _ => return None,
    };
    Some(t!("provider.clickhouse.context_table", table = table, observation = record))
}
