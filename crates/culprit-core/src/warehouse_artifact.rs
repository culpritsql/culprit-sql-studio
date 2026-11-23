//! Explicit archive detail from returned original references, never a native login or arbitrary path.
use crate::{
    Engine, IncidentDatabaseSelector, Plan, PublishedResourceSelector, Result, WindowNativeClock,
    collected_analysis::{CollectedEvidenceMetrics, EvidenceGap},
    telemetry::*,
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;
pub const MAX_WAREHOUSE_ARTIFACT_REFERENCES: usize = 16;
pub const MAX_WAREHOUSE_ARTIFACT_RECORDS: usize = MAX_WAREHOUSE_ARTIFACT_REFERENCES * 3;
pub const MAX_WAREHOUSE_ARTIFACT_PAYLOADS: usize = 128;
pub const MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS: u32 = 2048;
#[derive(Clone, Debug, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseArtifactQuery {
    pub resource: PublishedResourceSelector,
    /// Copied from the selected query/finding/incident. These are locators until original proof succeeds.
    pub references: Vec<DiagnosticEvidenceRef>,
    /// Explicit whole immutable recovery-body opt-in; the existing independent payload budget still applies.
    pub full_payloads: bool,
    pub text: WarehouseArtifactTextRead,
    /// Known content only. A matching SQL digest never establishes an actor, caller or causal role.
    pub expected_sql_sha256: Option<String>,
}
impl WarehouseArtifactQuery {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        self.text.validate()?;
        let mut unique = std::collections::BTreeSet::new();
        for reference in &self.references {
            if !unique.insert(reference.key()?) {
                return Err(DeliveryProblem::InvalidContract.error());
            }
        }
        if matches!(&self.text, WarehouseArtifactTextRead::Chunk {cursor}
            if !self.references.iter().any(|reference| reference.capture_ref().key().ok().as_ref()==Some(&cursor.capture_key)))
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        if self.references.is_empty()
            || self.references.len() > MAX_WAREHOUSE_ARTIFACT_REFERENCES
            || self.references.iter().any(|reference| !reference.is_valid())
            || self.expected_sql_sha256.as_deref().is_some_and(|value| !valid_digest(value))
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum WarehouseArtifactTextRead {
    Complete,
    /// Enumerate typed SQL/plan content metadata and return source-generated text continuations.
    Inventory {
        chunk_chars: u32,
    },
    Chunk {
        cursor: WarehouseArtifactTextCursor,
    },
}
impl WarehouseArtifactTextRead {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Complete => Ok(()),
            Self::Inventory { chunk_chars } if (1..=MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS).contains(chunk_chars) => Ok(()),
            Self::Chunk { cursor } => cursor.validate(),
            _ => Err(DeliveryProblem::InvalidContract.error()),
        }
    }
}
/// Consistency binding only. The same original/manifest/publication/body proof is required for every action.
#[derive(Clone, Debug, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WarehouseArtifactTextCursor {
    pub binding: String,
    pub reference_key: String,
    pub capture_key: String,
    pub location: String,
    pub payload_sha256: String,
    pub chunk_chars: u32,
    /// None starts at character0. A continuation must prove strictly positive progress within this payload.
    pub previous_character: Option<u32>,
    pub next_character: u32,
}
impl WarehouseArtifactTextCursor {
    pub fn validate(&self) -> Result<()> {
        let progress = match self.previous_character {
            None => self.next_character == 0,
            Some(previous) => previous < self.next_character && self.next_character - previous == self.chunk_chars,
        };
        if !valid_digest(&self.binding)
            || !valid_digest(&self.reference_key)
            || !valid_digest(&self.capture_key)
            || !valid_digest(&self.payload_sha256)
            || self.location.is_empty()
            || self.location.len() > 1024
            || !self.location.starts_with("/record/")
            || self.location.chars().any(char::is_control)
            || !(1..=MAX_WAREHOUSE_ARTIFACT_CHUNK_CHARS).contains(&self.chunk_chars)
            || !progress
        {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseArtifactText {
    /// The complete validated payload, preserving source restriction/encryption/unavailable state.
    Complete { payload: DefinitionPayload },
    Metadata {
        sha256: String,
        redaction_version: u32,
        bytes: u64,
        characters: u32,
        next: Option<WarehouseArtifactTextCursor>,
    },
    /// A safe Unicode scalar slice of the exact SHA-bound full body. Never a complete DefinitionPayload/plan.
    Chunk {
        sha256: String,
        redaction_version: u32,
        from_character: u32,
        until_character: u32,
        total_characters: u32,
        text: String,
        next: Option<WarehouseArtifactTextCursor>,
    },
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseArtifactScope {
    Database {
        database: IncidentDatabaseSelector,
        label: String,
    },
    /// Exact original shared member, without relabeling its SQL as the selected database's population.
    Shared {
        member_id: String,
        member_kind: SharedResourceKind,
    },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseDefinitionArtifact {
    /// Existing inventory location within this original row. Never an OS path, URL or user-supplied lookup.
    pub location: String,
    pub kind: DefinitionContentKind,
    pub label: String,
    pub payload: WarehouseArtifactText,
    pub matches_requested_sql: bool,
    pub plan: Option<Box<Plan>>,
    pub limitations: Vec<String>,
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseArtifactRecord {
    pub reference: DiagnosticEvidenceRef,
    pub engine: Engine,
    pub scope: WarehouseArtifactScope,
    pub source: String,
    pub native_clock: WindowNativeClock,
    pub collected_from: String,
    pub collected_until: String,
    /// Every artifact belongs to this original record; no census participant is guessed from its position.
    pub artifacts: Vec<WarehouseDefinitionArtifact>,
    /// Exact source-owned native definition references, not a time/name/handle join.
    pub linked_from: Vec<DiagnosticEvidenceRef>,
}
#[derive(Clone, Debug, Serialize, TS)]
#[serde(tag = "kind", rename_all = "kebab-case")]
#[ts(export)]
pub enum WarehouseArtifactGap {
    OriginalNotCommitted { capture: DiagnosticCaptureRef },
    OriginalUnavailable { reference: Box<DiagnosticEvidenceRef>, gaps: Vec<EvidenceGap> },
    DefinitionUnavailable { from: Box<DiagnosticEvidenceRef>, content_kind: DefinitionContentKind },
    NoDefinitionPayload { reference: Box<DiagnosticEvidenceRef> },
}
#[derive(Clone, Debug, Serialize, TS)]
#[ts(export)]
pub struct WarehouseArtifactReport {
    pub query: WarehouseArtifactQuery,
    pub records: Vec<WarehouseArtifactRecord>,
    pub gaps: Vec<WarehouseArtifactGap>,
    pub qualifications: Vec<String>,
    pub archive_work: CollectedEvidenceMetrics,
}
