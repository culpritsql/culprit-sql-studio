//! One output budget and strict original/projected duplicate policy serves every retained finding input.
use super::*;
use std::collections::BTreeMap;

pub(super) struct FindingRows<'a> {
    rows: Vec<DerivedFinding>,
    identities: BTreeMap<&'a str, (&'a Finding, usize)>,
}
impl<'a> FindingRows<'a> {
    pub fn new() -> Self {
        Self { rows: vec![], identities: BTreeMap::new() }
    }
    pub fn admit(
        &mut self,
        original: &'a Finding,
        mut projected: DerivedFinding,
        engine: Engine,
        budget: &mut OutputBudget,
    ) -> Result<()> {
        projected.redact(engine);
        if let Some((previous, position)) = self.identities.get(original.id.as_str()) {
            if *previous != original || self.rows[*position] != projected {
                return Err(invalid());
            }
            return Ok(());
        }
        budget.admit(&projected)?;
        self.identities.insert(&original.id, (original, self.rows.len()));
        self.rows.push(projected);
        Ok(())
    }
    pub fn finish(self) -> Vec<DerivedFinding> {
        self.rows
    }
}
