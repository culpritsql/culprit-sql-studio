//! Charge each output before retaining it; native read allowances and transport verification remain independent.
use super::*;
use culprit_core::serialization::json_size;
use serde::Serialize;

pub(super) struct OutputBudget {
    remaining: usize,
    rows: usize,
}
impl OutputBudget {
    pub fn new(empty: &DerivedDiagnosis) -> Result<Self> {
        let used = json_size(empty, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| capacity())?;
        // Counts start at zero. Each u32 can grow by at most nine decimal bytes; coverage moves from a
        // longer ContextUnavailable form to Evaluated. Later evaluation detail is admitted once through admit_header_growth.
        let count_growth = 3 * (u32::MAX.to_string().len() - 1);
        let remaining = MAX_DERIVED_OUTPUT_BYTES
            .checked_sub(used)
            .and_then(|bytes| bytes.checked_sub(count_growth))
            .ok_or_else(capacity)?;
        Ok(Self { remaining, rows: 0 })
    }
    pub fn admit_header_growth(
        &mut self,
        previous: &DerivedDiagnosisDescription,
        current: &DerivedDiagnosisDescription,
    ) -> Result<()> {
        let before = json_size(previous, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| capacity())?;
        let after = json_size(current, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| capacity())?;
        self.remaining = self.remaining.checked_sub(after.saturating_sub(before)).ok_or_else(capacity)?;
        Ok(())
    }
    pub fn admit(&mut self, row: &impl Serialize) -> Result<()> {
        if self.rows >= MAX_DERIVED_ROWS {
            return Err(capacity());
        }
        // One separator is conservatively charged even for the first item of each array.
        let available = self.remaining.checked_sub(1).ok_or_else(capacity)?;
        let bytes = json_size(row, available.min(MAX_DERIVED_RECORD_BYTES)).map_err(|_| capacity())?;
        self.remaining = available.checked_sub(bytes).ok_or_else(capacity)?;
        self.rows += 1;
        Ok(())
    }
}
