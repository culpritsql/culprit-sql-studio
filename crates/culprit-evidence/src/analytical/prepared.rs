//! Target representation only: native recovery records remain unchanged and own every full payload.
use culprit_core::{Result, telemetry::*};
use serde_json::{Value, json};

pub const PREPARATION_VERSION: u32 = 64;
const MAX_PREVIEW_BYTES: usize = 1024;
const MAX_INLINE_DETAIL_BYTES: usize = 128 * 1024;

pub fn prepare(input: &mut Value, record: &TelemetryRecord, target: &CollectionTarget) -> Result<()> {
    input["_projection"] = json!({"payloads":[]});
    for field in record.definition_payloads() {
        let pointer = field.pointer;
        let payload = input.pointer_mut(pointer.as_ref()).ok_or_else(invalid)?;
        let mut reference = payload_reference(payload)?;
        reference["pointer"] = json!(pointer);
        *payload = reference.clone();
        input["_projection"]["payloads"].as_array_mut().ok_or_else(invalid)?.push(reference);
    }
    match record {
        TelemetryRecord::CollectorConnection(row) => {
            let (status, stop) = match &row.original_status {
                Some(CollectionStatus::Complete) => (json!("complete"), Value::Null),
                Some(CollectionStatus::Incomplete(stop)) => (json!("incomplete"), json!(stop)),
                None => (Value::Null, Value::Null),
            };
            input["_projection"]["original_status"] = status;
            input["_projection"]["original_stop"] = stop;
            input["_projection"]["connection_outcome"] = json!(row.connection_outcome());
            input["_projection"]["ordered_collector_clocks"] = json!(row.ordered_collector_clocks());
        }
        TelemetryRecord::DerivedIncident(_)
        | TelemetryRecord::DerivedFinding(_)
        | TelemetryRecord::DerivedChange(_) => {
            let details = input["record"]["data"].clone();
            let bytes = serde_json::to_vec(&details).map_err(|_| invalid())?;
            if bytes.len() > culprit_core::MAX_DERIVED_RECORD_BYTES {
                return Err(DeliveryProblem::Capacity.error());
            }
            let description: culprit_core::DerivedDiagnosisDescription =
                serde_json::from_value(input.pointer("/_culprit/derived").ok_or_else(invalid)?.clone())
                    .map_err(|_| invalid())?;
            input["_projection"]["output_limitations"] =
                json!(culprit_core::i18n::scope("en", || description.limitations()));
            input["_projection"]["detail_state"] = json!("retained");
            input["_projection"]["detail_sha256"] = json!(digest(&bytes));
            input["_projection"]["details"] = details;
        }
        TelemetryRecord::ResourceSample(row) => {
            let metrics = row
                .metrics
                .iter()
                .map(|metric| {
                    let mut value = serde_json::to_value(metric).map_err(|_| invalid())?;
                    value["describes_subject"] = json!(metric.describes_subject(target));
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;
            input["_projection"]["resource_metrics"] = json!(metrics);
        }
        TelemetryRecord::Activity(_) => input["_projection"]["activity"] = input["record"]["data"].clone(),
        TelemetryRecord::Operational(row) => operational(input, row)?,
        TelemetryRecord::Replication(row) => fact(input, &row.fact.key())?,
        TelemetryRecord::Tuning(row) => fact(input, &row.fact.key())?,
        TelemetryRecord::ObjectDefinition(row) => fact(input, &row.key)?,
        TelemetryRecord::Deadlock(row) => deadlock(input, row)?,
        _ => (),
    }
    Ok(())
}

fn payload_reference(payload: &mut Value) -> Result<Value> {
    let reference = payload.as_object_mut().ok_or_else(invalid)?;
    if let Some(text) = reference.remove("text") {
        let text = text.as_str().ok_or_else(invalid)?;
        reference.insert("bytes".into(), json!(text.len()));
        reference.insert("preview".into(), json!(preview(text)));
        reference.insert("preview_truncated".into(), json!(text.len() > MAX_PREVIEW_BYTES));
    }
    Ok(Value::Object(std::mem::take(reference)))
}
fn preview(text: &str) -> &str {
    let mut end = text.len().min(MAX_PREVIEW_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
fn fact(input: &mut Value, key: &str) -> Result<()> {
    input["_projection"]["fact_key"] = json!(key);
    let details = input["record"]["data"]["fact"]["data"].clone();
    detail(input, details)
}
fn operational(input: &mut Value, row: &OperationalObservation) -> Result<()> {
    if let OperationalFact::ReadableSecondaryRecovery(point) = &row.fact {
        super::worker_contexts::project_recovery(input, point)?;
    }
    if let OperationalFact::MssqlCurrentExecutionProfile(point) = &row.fact {
        super::worker_contexts::project_execution_profile(input, point)?;
    }
    if let OperationalFact::MssqlWorkerStarvation(point) = &row.fact {
        super::worker_contexts::project(input.pointer_mut("/record/data/fact/data").ok_or_else(invalid)?, point)?;
    }
    if let OperationalFact::CounterSet(sample) = &row.fact
        && database_performance_candidate(sample)
    {
        *input.pointer_mut("/record/data/fact/data/identity").ok_or_else(invalid)? =
            super::database_counter_contexts::project(&sample.identity)?;
    }
    if let OperationalFact::PageWait(page) = &row.fact
        && let Some(target) = &page.target
        && let PageTargetScope::SharedResource { context } = &target.scope
    {
        *input.pointer_mut("/record/data/fact/data/target/scope/context").ok_or_else(invalid)? =
            super::page_target_contexts::project(context)?;
    }
    if let OperationalFact::DiagnosticRevision(revision) = &row.fact {
        // The authenticated recovery description owns every original native scope. This opaque reference also
        // preserves older replica epochs without duplicating private database or compute-member identity.
        *input.pointer_mut("/record/data/fact/data/episode/scope").ok_or_else(invalid)? =
            json!({"key": revision.episode.scope.key()?});
        let stages = revision
            .episode
            .source_reads
            .iter()
            .map(|stage| super::investigations::source_stage(&revision.episode, stage))
            .collect::<Result<Vec<_>>>()?;
        *input.pointer_mut("/record/data/fact/data/episode/source_reads").ok_or_else(invalid)? = Value::Array(stages);
        if let Some(investigation) = &revision.investigation {
            *input.pointer_mut("/record/data/fact/data/investigation").ok_or_else(invalid)? =
                super::investigations::event(investigation)?;
        }
    }
    fact(input, &row.key)
}
fn deadlock(input: &mut Value, row: &DeadlockEvent) -> Result<()> {
    input["_projection"]["process_count"] = json!(row.processes.len());
    input["_projection"]["resource_count"] = json!(row.resources.len());
    input["_projection"]["victim_count"] = json!(row.processes.iter().filter(|process| process.victim).count());
    let mut processes = serde_json::to_value(&row.processes).map_err(|_| invalid())?;
    for (process, native) in processes.as_array_mut().ok_or_else(invalid)?.iter_mut().zip(&row.processes) {
        process["statement"] = match &native.statement {
            Some(text) => json!({"sha256":digest(text.as_bytes()), "bytes":text.len(), "preview":preview(text),
                "preview_truncated":text.len()>MAX_PREVIEW_BYTES, "redaction_version":REDACTION_VERSION}),
            None => Value::Null,
        };
    }
    detail(input, json!({"processes":processes,"resources":row.resources}))
}
fn detail(input: &mut Value, details: Value) -> Result<()> {
    if !(details.is_object() || details.is_array()) {
        return Err(invalid());
    }
    let bytes = serde_json::to_vec(&details).map_err(|_| invalid())?;
    let inline = bytes.len() <= MAX_INLINE_DETAIL_BYTES;
    input["_projection"]["detail_state"] = json!(if inline { "inline" } else { "recovery-object" });
    input["_projection"]["detail_sha256"] = json!(digest(&bytes));
    input["_projection"]["details"] = if inline { details } else { Value::Null };
    Ok(())
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

#[cfg(test)]
mod tests;
