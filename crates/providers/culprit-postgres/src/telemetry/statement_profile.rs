//! Text-disabled point counters use a fixed native entry bound, independently of the returned row cap.
use super::*;

// PG18's documented startup default. This bounds entries visited/materialized by the native SRF;
// an outer database filter/LIMIT does not. It is not a measured CPU, latency or cost claim.
// https://www.postgresql.org/docs/18/pgstatstatements.html#PGSTATSTATEMENTS-CONFIG-PARAMS
pub(super) const MAX_NATIVE_ENTRIES: u32 = 5_000;
const MAX_PAGE_ROWS: u32 = 500;
const MAX_PAGE_BYTES: u32 = 512 * 1024;
const MAX_CAPTURE_BYTES: u32 = 8 * 1024 * 1024;
const MAX_DEFINITION_CAPTURE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_DEFINITION_SQL_BYTES: u32 = 16 * 1024;
const MAX_QUERIES: u32 = 6; // SET1 +metadata/member2 +text-disabled counters1 +metadata/member2.
const MAX_STATEMENT_MS: u32 = 1_000;
const MAX_DURATION_MS: u32 = 5_000;

pub(super) fn bounded_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_NATIVE_ENTRIES);
    budget.page_rows = budget.page_rows.min(MAX_PAGE_ROWS).min(budget.max_snapshot_rows);
    budget.page_bytes = budget.page_bytes.min(MAX_PAGE_BYTES);
    budget.total_bytes = budget.total_bytes.min(MAX_CAPTURE_BYTES);
    budget.max_queries = budget.max_queries.min(MAX_QUERIES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(MAX_STATEMENT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(MAX_DURATION_MS);
    budget
}

/// External query-text file materialization in `pg_stat_statements(true)` is not bounded by SQL `LIMIT`
/// or a file-size preflight, so this route remains `Unqualified` while bounding its held read and output.
pub(super) fn definitions_budget(requested: &CollectionBudget) -> CollectionBudget {
    let mut budget = requested.clone();
    budget.max_snapshot_rows = budget.max_snapshot_rows.min(MAX_NATIVE_ENTRIES);
    budget.page_rows = budget.page_rows.min(MAX_PAGE_ROWS).min(budget.max_snapshot_rows);
    budget.page_bytes = budget.page_bytes.min(MAX_PAGE_BYTES);
    budget.total_bytes = budget.total_bytes.min(MAX_DEFINITION_CAPTURE_BYTES);
    budget.max_sql_bytes = budget.max_sql_bytes.min(MAX_DEFINITION_SQL_BYTES);
    budget.max_queries = budget.max_queries.min(MAX_QUERIES);
    budget.statement_timeout_ms = budget.statement_timeout_ms.min(MAX_STATEMENT_MS);
    budget.total_timeout_ms = budget.total_timeout_ms.min(MAX_DURATION_MS);
    budget
}

#[cfg(test)]
mod native_tests;
