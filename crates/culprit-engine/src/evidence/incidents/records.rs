//! Whole captures and exact selected rows retain their DISTINCT proof owners. Only immutable receipts add rows.
use super::*;

type RecordKey = (String, String, u32);
fn key(reference: &DiagnosticEvidenceRef) -> Result<RecordKey> {
    Ok((reference.capture_ref().key()?, reference.batch_id.clone(), reference.row_ordinal))
}

#[derive(Default)]
pub(super) struct RecordSelections {
    rows: BTreeMap<RecordKey, Arc<SelectedEvidenceRecords>>,
}
impl RecordSelections {
    fn known(&self, reference: &DiagnosticEvidenceRef) -> Result<Option<OriginalRows>> {
        let Some(selected) = self.rows.get(&key(reference)?) else { return Ok(None) };
        if !selected.contains_reference(reference)? {
            return Err(invalid());
        }
        Ok(Some(OriginalRows::Selected(selected.clone())))
    }
    fn remember(&mut self, selected: Arc<SelectedEvidenceRecords>) -> Result<()> {
        for reference in selected.references() {
            let key = key(reference)?;
            if let Some(known) = self.rows.get(&key) {
                if !known.contains_reference(reference)? {
                    return Err(invalid());
                }
            } else {
                if self.rows.len() >= MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                    return Err(capacity());
                }
                self.rows.insert(key, selected.clone());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum OriginalRows {
    Capture(Arc<EvidenceCapture>),
    Selected(Arc<SelectedEvidenceRecords>),
}
impl OriginalRows {
    fn selected(&self, reference: &DiagnosticEvidenceRef) -> Result<Option<&VerifiedEvidenceRecord>> {
        match self {
            Self::Capture(capture) => references::selected_record(capture, reference),
            Self::Selected(selected) => selected.selected(reference),
        }
    }
    fn source(&self) -> &CollectionDescription {
        match self {
            Self::Capture(capture) => capture.source(),
            Self::Selected(selected) => selected.metadata().source(),
        }
    }
    fn gaps(&self) -> &[EvidenceGap] {
        match self {
            Self::Capture(capture) => capture.gaps(),
            Self::Selected(selected) => selected.gaps(),
        }
    }
}
#[derive(Default)]
pub(super) struct RecordLookup {
    rows: BTreeMap<RecordKey, OriginalRows>,
}
impl RecordLookup {
    pub(super) fn selected(&self, reference: &DiagnosticEvidenceRef) -> Result<Option<&VerifiedEvidenceRecord>> {
        self.rows.get(&key(reference)?).map(|rows| rows.selected(reference)).transpose().map(Option::flatten)
    }
    pub(super) fn source(&self, reference: &DiagnosticEvidenceRef) -> Result<Option<&CollectionDescription>> {
        Ok(self.rows.get(&key(reference)?).map(OriginalRows::source))
    }
    pub(super) fn gaps(&self, reference: &DiagnosticEvidenceRef) -> Result<Vec<EvidenceGap>> {
        Ok(self.rows.get(&key(reference)?).map(|rows| ancestry::row_gaps(rows.gaps(), reference)).unwrap_or_default())
    }
}

impl Assembly<'_> {
    pub(super) async fn record_lookup(&mut self, references: &[DiagnosticEvidenceRef]) -> Result<RecordLookup> {
        if references.len() > MAX_DIAGNOSTIC_ANCESTRY_LINKS {
            return Err(capacity());
        }
        let mut distinct = BTreeMap::new();
        for reference in references {
            if !reference.is_valid() {
                return Err(invalid());
            }
            let key = key(reference)?;
            if distinct.get(&key).is_some_and(|known| *known != reference) {
                return Err(invalid());
            }
            distinct.insert(key, reference);
        }
        let mut groups = BTreeMap::<String, Vec<&DiagnosticEvidenceRef>>::new();
        for reference in distinct.into_values() {
            groups.entry(reference.capture_ref().key()?).or_default().push(reference);
        }
        let mut lookup = RecordLookup::default();
        for group in groups.into_values() {
            let range = range_id_for(self.contract, &group[0].capture)?;
            let mut missing = Vec::new();
            for reference in group {
                if let Some(capture) = self
                    .captures
                    .get(&range)
                    .filter(|capture| capture.families().iter().any(|family| family.stream() == reference.stream))
                {
                    references::validate_capture_reference(capture, &reference.capture_ref())?;
                    lookup.rows.insert(key(reference)?, OriginalRows::Capture(capture.clone()));
                } else if let Some(known) = self.selected_records.known(reference)? {
                    lookup.rows.insert(key(reference)?, known);
                } else {
                    missing.push(reference.clone());
                }
            }
            if missing.is_empty() {
                continue;
            }
            if self
                .selected_records
                .rows
                .len()
                .checked_add(missing.len())
                .is_none_or(|count| count > MAX_DIAGNOSTIC_ANCESTRY_LINKS)
            {
                return Err(capacity());
            }
            let expected = missing[0].capture_ref();
            let availability = self.authenticate_header(&range, &expected).await?;
            if availability != IncidentEvidenceAvailability::Authenticated {
                if let IncidentEvidenceAvailability::Unavailable { gaps } = availability {
                    self.gaps.push(IncidentReadGap::CaptureUnavailable { capture: expected, gaps });
                }
                continue;
            }
            let selected = Arc::new(self.session.read_records(&missing).await?);
            self.require_selected_header(&range, selected.metadata())?;
            self.selected_records.remember(selected.clone())?;
            for reference in missing {
                lookup.rows.insert(key(&reference)?, OriginalRows::Selected(selected.clone()));
            }
        }
        Ok(lookup)
    }
    fn require_selected_header(&self, range: &str, actual: &EvidenceCaptureMetadata) -> Result<()> {
        let (capture, manifest, source, from, until) = if let Some(known) = self.captures.get(range) {
            (
                known.capture(),
                known.capture_manifest_sha256(),
                known.source(),
                known.collected_from(),
                known.collected_until(),
            )
        } else if let Some(known) = self.metadata.get(range) {
            (
                known.capture(),
                known.capture_manifest_sha256(),
                known.source(),
                known.collected_from(),
                known.collected_until(),
            )
        } else {
            return Err(invalid());
        };
        if capture != actual.capture()
            || manifest != actual.capture_manifest_sha256()
            || source != actual.source()
            || from != actual.collected_from()
            || until != actual.collected_until()
        {
            return Err(invalid());
        }
        Ok(())
    }
}
