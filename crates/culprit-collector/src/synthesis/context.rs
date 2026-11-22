//! Explicit selection gaps do not change the EOF proof of available selected originals.
use super::*;
use serde::{Deserialize, Serialize};

pub const MAX_SYNTHESIS_CONTEXT_ISSUES: usize = 20;

/// Selection gaps are independent of canonical EOF completion of the available, selected originals.
/// Captures in these private diagnostics never become native authority or user-facing identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SynthesisContextIssue {
    MissingLocalOriginal { capture: DiagnosticCaptureRef },
    UnavailableOriginal { capture: DiagnosticCaptureRef },
    ContextLimit,
    UnqualifiedCatalogScope,
    UnknownCatalogClock,
    CatalogNotRetainedAsOf,
}

impl SynthesisContextIssue {
    pub fn derived_issue(&self) -> culprit_core::DerivedContextIssue {
        use culprit_core::DerivedContextIssue;
        match self {
            Self::MissingLocalOriginal { .. } => DerivedContextIssue::MissingOriginal,
            Self::UnavailableOriginal { .. } => DerivedContextIssue::UnavailableOriginal,
            Self::ContextLimit => DerivedContextIssue::ContextLimit,
            Self::UnqualifiedCatalogScope => DerivedContextIssue::UnqualifiedCatalogScope,
            Self::UnknownCatalogClock => DerivedContextIssue::UnknownCatalogClock,
            Self::CatalogNotRetainedAsOf => DerivedContextIssue::CatalogNotRetainedAsOf,
        }
    }
}
