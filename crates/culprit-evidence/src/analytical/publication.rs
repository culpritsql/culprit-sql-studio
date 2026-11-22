//! Canonical publication projection shared by writers and proof readers.
mod context;
use super::{ProjectionCatalog, envelope::capture_context};
use crate::delivery::recovery::CommittedRange;
use culprit_core::{
    Result,
    telemetry::{DeliveryProblem, digest},
};
use serde_json::{Value, json};

pub fn input(catalog: &dyn ProjectionCatalog, range: &CommittedRange) -> Result<Value> {
    let manifest = range.manifest();
    let receipts = manifest.receipts_digest()?;
    let publication_id =
        digest(&serde_json::to_vec(&(range.store_id(), range.manifest_digest(), &receipts)).map_err(|_| invalid())?);
    let batches=manifest.batches.iter().map(|batch|{
        let table=if batch.delivery_required {Some(catalog.schema(batch.stream)?.table())} else {None};
        Ok(json!({"batch_id":batch.batch_id,"stream":batch.stream,"table":table,"rows":batch.rows.len(),
            "native_object":batch.object,"native_sha256":batch.sha256,"bytes":batch.bytes,"delivery_required":batch.delivery_required}))
    }).collect::<Result<Vec<_>>>()?;
    let observation = context::observation(&manifest.source.observation)?;
    let read_metrics =
        manifest.read_metrics.as_ref().map(|metrics| metrics.summary(Some(&manifest.source))).transpose()?;
    // Coverage is public analytical evidence; raw target identities remain in the private recovery manifest.
    Ok(json!({"_culprit":capture_context(manifest,range.manifest_digest())?,"recovery_store_id":range.store_id(),
        "receipts_sha256":receipts,"publication_id":publication_id,"capture":manifest.capture,
        "source":{"source":manifest.source.source,"continuity":manifest.source.continuity,
            "observation":observation},
        "reference_coverage":manifest.reference_coverage,
        "source_queries":manifest.source_queries,"read_metrics":read_metrics,"batches":batches}))
}

fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
