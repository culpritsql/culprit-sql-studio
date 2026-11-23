//! One bounded native-row index per compatible window/population; no per-query scans of every capture.
use super::*;

pub(super) struct NativeInputs<'a> {
    runtime: BTreeMap<String, Vec<&'a RuntimeMeasurement>>,
    plans: BTreeMap<&'a str, &'a PlanDefinition>,
}
impl<'a> NativeInputs<'a> {
    pub fn new(originals: &[&'a dyn OriginalProjection], population: &CollectedQueryPopulation) -> Result<Self> {
        let mut index = Self { runtime: BTreeMap::new(), plans: BTreeMap::new() };
        let requested = match population {
            CollectedQueryPopulation::Population { identity } => Some(identity),
            _ => None,
        };
        for &original in originals {
            let native = populations::PopulationIndex::new(original, requested)?;
            for &(_, row) in &native.rows(population)?.runtime {
                let key = native_query_key(
                    original.source(),
                    population,
                    &row.key.references,
                    row.key.cohort.as_ref(),
                    &row.key.native.qualifiers,
                )?;
                index.runtime.entry(key).or_default().push(row);
            }
            for row in original.records() {
                if let TelemetryRecord::PlanDefinition(plan) = &row.row().record {
                    index
                        .plans
                        .entry(&plan.id)
                        .and_modify(|known| {
                            if !known.plan.is_available() && plan.plan.is_available() {
                                *known = plan;
                            }
                        })
                        .or_insert(plan);
                }
            }
        }
        Ok(index)
    }
    pub fn runtime(&self, key: &str) -> &[&'a RuntimeMeasurement] {
        self.runtime.get(key).map(Vec::as_slice).unwrap_or_default()
    }
    pub fn plan(&self, id: &str) -> Option<&'a PlanDefinition> {
        self.plans.get(id).copied()
    }
}
