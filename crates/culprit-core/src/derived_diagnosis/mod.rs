//! Culprit synthesis is derived evidence, never a native DMV population or authority for fresh actions.
use crate::telemetry::*;
use crate::{AnalysisOrigin, ChangeEvent, ComponentVersion, Finding, IncidentHistoryEntry, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ts_rs::TS;
mod comparison;
mod coverage;
mod description;
mod evaluation;
mod inputs;
mod records;
mod redaction;
mod selector;
mod validation;
pub use comparison::*;
pub use coverage::*;
pub use description::*;
pub use evaluation::*;
pub use inputs::*;
pub use records::*;
pub use selector::*;
pub const MAX_DERIVED_REFERENCES: usize = 256;
pub const MAX_DERIVED_CAPTURES: usize = 256;
pub const MAX_DERIVED_ROWS: usize = 4096;
fn invalid() -> crate::Error {
    DeliveryProblem::InvalidContract.error()
}
fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}
#[cfg(test)]
mod tests;

/// Bounds one redacted analytical row before the canonical destination envelope is added.
pub const MAX_DERIVED_RECORD_BYTES: usize = 256 * 1024;
pub const MAX_DERIVED_SQL_BYTES: usize = 64 * 1024;
pub const MAX_DERIVED_QUERIES: usize = 32;

/// A local synthesis owner, never a native acquisition source.
pub const DERIVED_DIAGNOSIS_SOURCE: &str = "culprit.derived-diagnosis";

fn utc(value: &str) -> bool {
    canonical_time(value).is_ok()
        && chrono::DateTime::parse_from_rfc3339(value).is_ok_and(|time| time.offset().local_minus_utc() == 0)
}

/// One complete derived synthesis output, independently of native/read/delivery budgets.
pub const MAX_DERIVED_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
