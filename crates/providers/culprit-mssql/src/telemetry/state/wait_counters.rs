//! The pool and instance wait profiles share one existing response envelope, never an inferred counter reset.
use super::*;

// Instance: initial identity2 + clocks2 + native waits1 + final identity2 + owned setup1.
// Pool: existing platform/membership/admission/native/final7 + owned setup1.
const MAX_TOTAL_QUERIES: u32 = 8;
const MAX_CURRENT_ROWS: u32 = 500;
const MAX_PAGE_BYTES: u32 = 256 * 1024;
const MAX_TOTAL_BYTES: u32 = 1024 * 1024;
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 5000;

pub(in crate::telemetry) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        max_queries: caller.max_queries.min(MAX_TOTAL_QUERIES),
        max_snapshot_rows: caller.max_snapshot_rows.min(MAX_CURRENT_ROWS),
        page_rows: caller.page_rows.min(MAX_CURRENT_ROWS),
        page_bytes: caller.page_bytes.min(MAX_PAGE_BYTES),
        total_bytes: caller.total_bytes.min(MAX_TOTAL_BYTES),
        statement_timeout_ms: caller.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS),
        total_timeout_ms: caller.total_timeout_ms.min(TOTAL_TIMEOUT_MS),
        ..caller.clone()
    }
}

pub(super) fn instance_selected(request: &CollectionRequest) -> bool {
    request.selection == (CollectionSelection::Snapshot { dataset: SnapshotDataset::WaitCounters })
        && matches!(&request.subject, CollectionSubject::Shared { resource }
            if resource.engine == culprit_core::Engine::Mssql && resource.kind == SharedResourceKind::Instance)
}

pub(super) fn check_transport(original: Option<u64>, current: Option<u64>) -> Result<()> {
    if original.is_none_or(|generation| generation == 0 || current != Some(generation)) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
