use super::*;
use culprit_core::{DatabaseIdentity, telemetry::*};
use culprit_evidence::analytical::evidence::EvidenceCapture;
use culprit_evidence::original_projection::OriginalProjection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn warehouse_capture(capture: &EvidenceCapture) -> Result<DatabaseIdentity> {
    let source = capture.source();
    let CollectionTarget::Database(database) = &source.target else {
        return Err(invalid());
    };
    if !source.target.is_valid()
        || !valid_digest(capture.range_id())
        || !valid_digest(capture.recovery_manifest_sha256())
        || !valid_digest(capture.capture_manifest_sha256())
    {
        return Err(invalid());
    }
    let history = source.query_history().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    if !history.is_valid() || hours(&history.interval.from, &history.interval.until)? <= 0.0 {
        return Err(invalid());
    }
    // Native history bounds and collector acquisition bounds belong to independent clocks. Validate each
    // interval internally; a native window cannot be contained by the collector's wall clock.
    if ObservationBounds::new(capture.collected_from(), capture.collected_until()).is_none() {
        return Err(invalid());
    }
    let mut rows = BTreeSet::new();
    for record in capture.records() {
        let row = record.row();
        if row.capture != *capture.capture()
            || record.row_ordinal() == 0
            || !valid_digest(record.native_payload_sha256())
            || !rows.insert((record.batch_id(), record.row_ordinal()))
            || culprit_evidence::delivery::evidence::RowReceipt::from_export(row, record.row_ordinal())?.payload_digest
                != record.native_payload_sha256()
            || ExportedRecord::new(source, capture.capture(), row.record.clone())? != *row
        {
            return Err(invalid());
        }
    }
    Ok(database.clone())
}

pub(super) fn capture(capture: &dyn OriginalProjection) -> Result<DatabaseIdentity> {
    let CollectionTarget::Database(database) = &capture.source().target else { return Err(invalid()) };
    let history = capture.source().query_history().ok_or_else(|| DeliveryProblem::UnsupportedSchema.error())?;
    if !capture.original().is_valid()
        || !capture.source().partition_is_consistent()
        || !history.is_valid()
        || hours(&history.interval.from, &history.interval.until)? <= 0.0
        || ObservationBounds::new(capture.collected_from(), capture.collected_until()).is_none()
    {
        return Err(invalid());
    }
    Ok(database.clone())
}

pub(super) fn hours(from: &str, until: &str) -> Result<f64> {
    let elapsed = time(until)?.signed_duration_since(time(from)?);
    let elapsed = elapsed.to_std().map_err(|_| invalid())?;
    let hours = elapsed.as_secs_f64() / 3600.0;
    if hours <= 0.0 {
        return Err(invalid());
    }
    Ok(hours)
}

pub(super) fn time(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid())
}

pub(super) struct Definitions<'a> {
    source: &'a CollectionDescription,
    queries: BTreeMap<&'a str, &'a QueryDefinition>,
    plans: BTreeMap<&'a str, &'a PlanDefinition>,
}
pub(super) struct Joined<'a> {
    pub query: Option<&'a QueryDefinition>,
    pub plan: Option<&'a PlanDefinition>,
}
impl<'a> Definitions<'a> {
    pub fn new(capture: &'a dyn OriginalProjection) -> Result<Self> {
        let mut definitions = Self { source: capture.source(), queries: BTreeMap::new(), plans: BTreeMap::new() };
        for record in capture.records() {
            match &record.row().record {
                TelemetryRecord::QueryDefinition(query) => {
                    insert_unique(&mut definitions.queries, &query.id, query)?;
                }
                TelemetryRecord::PlanDefinition(plan) => {
                    insert_unique(&mut definitions.plans, &plan.id, plan)?;
                }
                _ => (),
            }
        }
        Ok(definitions)
    }

    pub fn join(&self, references: &StatementReferences) -> Result<Joined<'a>> {
        let query_id = references.query.as_ref().and_then(|reference| reference.definition_id.as_deref());
        let plan_id = references.plan.as_ref().and_then(|reference| reference.definition_id.as_deref());
        let query = query_id.and_then(|id| self.queries.get(id).copied());
        let plan = plan_id.and_then(|id| self.plans.get(id).copied());
        let query = references.bind_query_definition(self.source, query).map_err(|_| invalid())?;
        let plan = references.bind_plan_definition(self.source, plan).map_err(|_| invalid())?;
        Ok(Joined { query, plan })
    }
}

fn insert_unique<'a, T>(map: &mut BTreeMap<&'a str, &'a T>, id: &'a str, record: &'a T) -> Result<()> {
    if map.insert(id, record).is_some() {
        return Err(invalid());
    }
    Ok(())
}
