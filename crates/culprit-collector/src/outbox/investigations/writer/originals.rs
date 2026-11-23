//! Exact originals live only through one serialized writer validation; no trust survives the call.
use super::super::input::DiagnosticReferenceRows;
use super::*;
use std::rc::Rc;

pub(super) struct CommitOriginals {
    current: Rc<DiagnosticInput>,
    current_rows: BTreeMap<String, usize>,
    foreign: Option<CaptureOriginals>,
}

struct CaptureOriginals {
    original: DiagnosticReferenceRows,
    positions: BTreeMap<String, usize>,
    input: Option<Rc<DiagnosticInput>>,
}

fn row_positions(rows: &[DiagnosticInputRow]) -> Result<BTreeMap<String, usize>> {
    let mut positions = BTreeMap::new();
    for (position, row) in rows.iter().enumerate() {
        if positions.insert(row.reference.key()?, position).is_some() {
            return Err(integrity());
        }
    }
    Ok(positions)
}

impl CommitOriginals {
    pub(super) fn new(current: DiagnosticInput) -> Result<Self> {
        let current_rows = row_positions(&current.rows)?;
        Ok(Self { current: Rc::new(current), current_rows, foreign: None })
    }

    pub(super) fn current(&self) -> Rc<DiagnosticInput> {
        Rc::clone(&self.current)
    }

    pub(super) fn reference(
        &mut self,
        files: &mut Files,
        reference: &DiagnosticEvidenceRef,
    ) -> Result<(CollectionDescription, ExportedRecord)> {
        let key = reference.key()?;
        let parent = reference.capture_ref();
        if parent.capture.observation_id == self.current.parent.capture.observation_id {
            if parent != self.current.parent {
                return Err(integrity());
            }
            if let Some(position) = self.current_rows.get(&key) {
                let row = &self.current.rows[*position];
                if row.reference != *reference {
                    return Err(integrity());
                }
                return Ok((self.current.source.clone(), row.row.clone()));
            }
        }
        // A SourceRead input may intentionally retain only its closed answer roster. Resolve a reference
        // outside that roster through the ordinary complete-body owner, never through an invented ordinal.
        let original = self.capture(files, &parent)?;
        let position = original.positions.get(&key).ok_or_else(integrity)?;
        let row = &original.original.rows[*position];
        if row.reference != *reference {
            return Err(integrity());
        }
        Ok((original.original.source.clone(), row.row.clone()))
    }

    pub(super) fn input(
        &mut self,
        files: &mut Files,
        parent: &DiagnosticCaptureRef,
        controller_version: u32,
    ) -> Result<Rc<DiagnosticInput>> {
        if parent.capture.observation_id == self.current.parent.capture.observation_id {
            if *parent != self.current.parent || controller_version != self.current.controller_version {
                return Err(integrity());
            }
            return Ok(Rc::clone(&self.current));
        }
        let original = self.capture(files, parent)?;
        if let Some(input) = &original.input
            && input.controller_version == controller_version
        {
            return Ok(Rc::clone(input));
        }
        let input = Rc::new(files.diagnostic_input_from_reference_rows(&original.original, controller_version)?);
        original.input = Some(Rc::clone(&input));
        Ok(input)
    }

    fn capture(&mut self, files: &mut Files, parent: &DiagnosticCaptureRef) -> Result<&mut CaptureOriginals> {
        if !parent.is_valid() {
            return Err(integrity());
        }
        if let Some(original) = &self.foreign
            && original.original.parent.capture.observation_id == parent.capture.observation_id
            && original.original.parent != *parent
        {
            return Err(integrity());
        }
        if self.foreign.as_ref().is_none_or(|original| original.original.parent != *parent) {
            // Retain at most one complete foreign body plus its selected input. Release it BEFORE
            // opening another capture; interleaved foreign reports may pay another full verification.
            self.foreign = None;
            let original = files.diagnostic_reference_rows(&parent.capture.observation_id)?;
            if original.parent != *parent {
                return Err(integrity());
            }
            let positions = row_positions(&original.rows)?;
            self.foreign = Some(CaptureOriginals { original, positions, input: None });
        }
        self.foreign.as_mut().ok_or_else(integrity)
    }
}

#[cfg(test)]
mod tests;
