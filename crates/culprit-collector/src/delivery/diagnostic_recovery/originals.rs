//! Authentication remains with the original local canonical recovery owner.
use super::*;
use culprit_evidence::delivery::recovery::RecoveryRead;
/// Authentication reuses the current recovery reader, complete original row receipt and native scope checks.
/// This does not authorize source work and never labels an absent/partial lookup healthy or recovered.
pub async fn authenticate_original_history(
    store: &dyn RecoveryRead,
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
    store: &dyn RecoveryRead,
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
