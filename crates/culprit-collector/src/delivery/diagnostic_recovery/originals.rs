//! Compact index pointers never replace authentication of immutable original revision bodies.
use super::*;

pub const MAX_ORIGINAL_HISTORY_LOOKUP_GROUPS: usize = MAX_DIAGNOSTIC_EPISODES;
pub(crate) const MAX_POINTER_BYTES: usize = 16 * 1024;
pub(crate) const MAX_ORIGINAL_INDEX_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn validate_original_group_keys(keys: &[String]) -> Result<()> {
    if keys.len() > MAX_ORIGINAL_HISTORY_LOOKUP_GROUPS {
        return Err(DeliveryProblem::Capacity.error());
    }
    let mut unique = BTreeSet::new();
    if keys.iter().any(|key| !valid_digest(key) || !unique.insert(key)) {
        return Err(invalid());
    }
    Ok(())
}

/// Exact selected membership from one committed root. Missing entries cannot masquerade as proven absence.
pub struct OriginalHistoryLookup {
    entries: BTreeMap<String, Option<OriginalHistoryPointer>>,
}
impl OriginalHistoryLookup {
    pub fn new(keys: &[String], entries: BTreeMap<String, Option<OriginalHistoryPointer>>) -> Result<Self> {
        let lookup = Self { entries };
        lookup.validate_for(keys)?;
        for (key, pointer) in &lookup.entries {
            if let Some(pointer) = pointer {
                let _ = pointer.bytes()?;
                if pointer.group_key != *key {
                    return Err(invalid());
                }
            }
        }
        Ok(lookup)
    }
    pub(crate) fn validate_for(&self, keys: &[String]) -> Result<()> {
        validate_original_group_keys(keys)?;
        if self.entries.len() != keys.len() || keys.iter().any(|key| !self.entries.contains_key(key)) {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn into_one(mut self, key: &str) -> Result<Option<OriginalHistoryPointer>> {
        if self.entries.len() != 1 {
            return Err(invalid());
        }
        self.entries.remove(key).ok_or_else(invalid)
    }
    pub(crate) fn entries(&self) -> &BTreeMap<String, Option<OriginalHistoryPointer>> {
        &self.entries
    }
    /// A capture is read sequentially once. Conflicting expected manifests for the same range refuse before I/O.
    pub(crate) fn captures(&self) -> Result<Vec<Vec<&OriginalHistoryPointer>>> {
        let mut captures = BTreeMap::<&str, Vec<&OriginalHistoryPointer>>::new();
        for pointer in self.entries.values().flatten() {
            let group = captures.entry(&pointer.range_id).or_default();
            if group.first().is_some_and(|first| {
                first.manifest_digest != pointer.manifest_digest
                    || first.reference.capture_ref() != pointer.reference.capture_ref()
            }) {
                return Err(invalid());
            }
            group.push(pointer);
        }
        Ok(captures.into_values().collect())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OriginalHistoryPointer {
    pub group_key: String,
    pub range_id: String,
    pub manifest_digest: String,
    pub reference: DiagnosticEvidenceRef,
}
impl OriginalHistoryPointer {
    pub fn from_record(manifest: &RangeManifest, row: &ExportedRecord, ordinal: u32) -> Result<Option<Self>> {
        let Some(revision) = original_revision(&row.record) else { return Ok(None) };
        let Some(group_key) = revision.episode.original_group_key()? else { return Ok(None) };
        if ExportedRecord::new(&manifest.source, &manifest.capture, row.record.clone())? != *row {
            return Err(invalid());
        }
        let pointer = Self {
            group_key,
            range_id: manifest.range_id.clone(),
            manifest_digest: manifest.digest()?,
            reference: DiagnosticEvidenceRef::from_export(
                &manifest.source,
                &manifest.capture_manifest_sha256,
                row,
                ordinal,
            )?,
        };
        pointer.validate()?;
        Ok(Some(pointer))
    }
    pub fn validate(&self) -> Result<()> {
        if !valid_digest(&self.group_key)
            || !valid_digest(&self.range_id)
            || !valid_digest(&self.manifest_digest)
            || !self.reference.is_valid()
            || self.reference.stream != TelemetryStream::OperationalSamples
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > MAX_POINTER_BYTES {
            return Err(DeliveryProblem::Capacity.error());
        }
        Ok(bytes)
    }
    pub fn from_bytes(bytes: &[u8], expected: &str) -> Result<Self> {
        if bytes.len() > MAX_POINTER_BYTES || !valid_digest(expected) || digest(bytes) != expected {
            return Err(invalid());
        }
        let pointer: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        pointer.validate()?;
        Ok(pointer)
    }
}

/// The existing committed checkpoint owns the complete key roster. Object digests are hints; callers reopen
/// their original manifests and verify EOF/receipts before using any first identity or completed answer.
#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct OriginalGroupHeads(BTreeMap<String, String>);
impl OriginalGroupHeads {
    pub fn validate(&self) -> Result<()> {
        if self.0.iter().any(|(key, pointer)| !valid_digest(key) || !valid_digest(pointer)) {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn selected(&self, keys: &[String]) -> Result<BTreeMap<String, Option<String>>> {
        self.validate()?;
        validate_original_group_keys(keys)?;
        Ok(keys.iter().map(|key| (key.clone(), self.0.get(key).cloned())).collect())
    }
    pub fn observe(&mut self, pointer: &OriginalHistoryPointer) -> Result<String> {
        let hash = digest(&pointer.bytes()?);
        self.0.insert(pointer.group_key.clone(), hash.clone());
        Ok(hash)
    }
}

pub fn original_revision(record: &TelemetryRecord) -> Option<&DiagnosticRevision> {
    let TelemetryRecord::Operational(OperationalObservation {
        source,
        key,
        fact: OperationalFact::DiagnosticRevision(revision),
    }) = record
    else {
        return None;
    };
    (source == DIAGNOSTICS_SOURCE
        && key == &revision.id
        && revision.is_valid()
        && matches!(revision.episode.signal, DiagnosticSignal::OriginalObservation { .. }))
    .then_some(revision.as_ref())
}

/// Authentication reuses the current recovery reader, complete original row receipt and native scope checks.
/// This does not authorize source work and never labels an absent/partial lookup healthy or recovered.
pub async fn authenticate_original_history(
    store: &dyn RecoveryStore,
    key: &str,
    pointer: &OriginalHistoryPointer,
) -> Result<Box<RecoveredDiagnosticEvidence>> {
    if pointer.group_key != key {
        return Err(invalid());
    }
    let input = Box::pin(authenticate_original_capture(store, &[pointer])).await?;
    let record = input.record(&pointer.reference)?.clone();
    let RecoveredDiagnosticInput { range, rows } = *input;
    Ok(Box::new(RecoveredDiagnosticEvidence { range, record, cached_rows: Some(rows) }))
}

/// The current reader verifies every byte/receipt to EOF once, then each selected original row is checked against
/// that immutable body. This proof is owned by this call only, never a cross-call validation cache.
pub(crate) async fn authenticate_original_capture(
    store: &dyn RecoveryStore,
    pointers: &[&OriginalHistoryPointer],
) -> Result<Box<RecoveredDiagnosticInput>> {
    let first = pointers.first().ok_or_else(invalid)?;
    validate_original_group_keys(&pointers.iter().map(|pointer| pointer.group_key.clone()).collect::<Vec<_>>())?;
    for pointer in pointers {
        pointer.validate()?;
        if pointer.range_id != first.range_id
            || pointer.manifest_digest != first.manifest_digest
            || pointer.reference.capture_ref() != first.reference.capture_ref()
        {
            return Err(invalid());
        }
    }
    // Diagnostic revisions are evidence-only prerequisites, not native input obligations. Preserve the existing
    // evidence reader/cache admission rather than routing them through pending-input eligibility.
    let evidence = Box::pin(load_diagnostic_evidence(store, &first.reference)).await?;
    let input = Box::new(evidence.into_cached_input()?);
    if input.range.manifest_digest() != first.manifest_digest
        || capture_reference(input.range.manifest())? != first.reference.capture_ref()
    {
        return Err(invalid());
    }
    for pointer in pointers {
        let revision = original_revision(&input.record(&pointer.reference)?.record).ok_or_else(invalid)?;
        if revision.episode.original_group_key()?.as_deref() != Some(pointer.group_key.as_str()) {
            return Err(invalid());
        }
    }
    Ok(input)
}
