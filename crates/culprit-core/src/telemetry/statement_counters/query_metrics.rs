//! Numeric projection does not establish a definition, application origin, observation window or denominator.
use super::*;
use crate::{QueryStat, Spread};
use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

/// The fields a QueryStat can represent without rounding exact native counts or changing their units.
/// This transient projection is not a replacement for the original counters and their independent reset markers.
#[derive(Clone, Debug, PartialEq)]
pub struct StatementQueryMetrics {
    pub executions: f64,
    pub total_time_ms: f64,
    pub mean_time_ms: f64,
    pub max_time_ms: Option<f64>,
    pub stddev_time_ms: Option<f64>,
    pub min_time_ms: Option<f64>,
    pub cpu_time_ms: Option<f64>,
    pub rows: Option<f64>,
    pub logical_reads: Option<f64>,
    pub physical_reads: Option<f64>,
}

impl StatementQueryMetrics {
    /// A point may have unknown lifetime and still contain a measured total. That supplies no rate, share,
    /// first execution time, execution population or application classification.
    pub fn from_sample(sample: &StatementCounterSample) -> crate::Result<Option<Self>> {
        if !sample.is_valid() {
            return Err(invalid());
        }
        let distribution = sample
            .distributions
            .iter()
            .find(|distribution| distribution.statistics.key == RuntimeMetricKey::Duration)
            .filter(|distribution| same_reset(&sample.reset, &distribution.reset))
            .map(|distribution| {
                let mut statistics = distribution.statistics.clone();
                if !same_reset(&sample.reset, &distribution.extrema_reset) {
                    statistics.minimum = None;
                    statistics.maximum = None;
                }
                statistics
            });
        Self::from_counters(&sample.totals, distribution.as_ref())
    }

    /// Project numeric values whose native population the caller has already qualified. A supplied duration
    /// distribution must describe those same totals. This method never joins rows, authenticates SQL or resets,
    /// differences observations, or asserts that the values describe the whole database.
    pub fn from_counters(
        counters: &[StatementCounter],
        duration_distribution: Option<&RuntimeMetric>,
    ) -> crate::Result<Option<Self>> {
        let mut keys = BTreeSet::new();
        if counters.len() > MAX_STATEMENT_METRICS
            || counters.iter().any(|counter| !counter.is_valid() || !keys.insert(&counter.key))
            || duration_distribution
                .is_some_and(|distribution| distribution.key != RuntimeMetricKey::Duration || !distribution.is_valid())
        {
            return Err(invalid());
        }
        let total = |key: RuntimeMetricKey| counters.iter().find(|counter| counter.key == key);
        let Some(executions) = total(RuntimeMetricKey::Executions).and_then(count) else { return Ok(None) };
        let Some(total_time_ms) = total(RuntimeMetricKey::Duration).and_then(milliseconds) else { return Ok(None) };
        if executions == 0.0 && total_time_ms != 0.0 {
            return Ok(None);
        }
        let mean_time_ms = if executions == 0.0 { 0.0 } else { total_time_ms / executions };
        if !mean_time_ms.is_finite() {
            return Ok(None);
        }
        let extremum = |value: Option<&MeasurementValue>| {
            duration_distribution.and_then(|distribution| {
                value.and_then(MeasurementValue::exact_real).and_then(|value| distribution.unit.milliseconds(value))
            })
        };
        Ok(Some(Self {
            executions,
            total_time_ms,
            mean_time_ms,
            max_time_ms: extremum(duration_distribution.and_then(|distribution| distribution.maximum.as_ref())),
            stddev_time_ms: duration_distribution.and_then(|distribution| {
                distribution.standard_deviation.and_then(|value| distribution.unit.milliseconds(value))
            }),
            min_time_ms: extremum(duration_distribution.and_then(|distribution| distribution.minimum.as_ref())),
            cpu_time_ms: total(RuntimeMetricKey::CpuTime).and_then(milliseconds),
            rows: total(RuntimeMetricKey::Rows).and_then(count),
            logical_reads: total(RuntimeMetricKey::LogicalReads).and_then(reads),
            physical_reads: total(RuntimeMetricKey::PhysicalReads).and_then(reads),
        }))
    }

    /// Assemble the shared QueryStat shape once. The caller remains responsible for the exact original identity
    /// and SQL definition. Counter reset times are deliberately not first_seen execution timestamps.
    pub fn into_query(self, id: String, text: String) -> QueryStat {
        QueryStat {
            native_identity: None,
            native_plan_identity: None,
            plan_history: None,
            variant_family: None,
            resolved_write_target: None,
            requests: None,
            id,
            query_hash: None,
            text,
            parameters: None,
            object_name: None,
            executions: self.executions,
            total_time_ms: self.total_time_ms,
            mean_time_ms: self.mean_time_ms,
            max_time_ms: self.max_time_ms,
            stddev_time_ms: self.stddev_time_ms,
            cpu_time_ms: self.cpu_time_ms,
            rows: self.rows,
            logical_reads: self.logical_reads,
            physical_reads: self.physical_reads,
            temporary_work: None,
            first_seen: None,
            last_seen: None,
            plans: vec![],
            plan: None,
            waits: vec![],
            waits_coverage: None,
            trend: None,
            spread: self.min_time_ms.map(|min_ms| Spread { min_ms: Some(min_ms), ..Default::default() }),
            costs: None,
            rows_affected: None,
            plan_id: None,
        }
    }
}

fn same_reset(left: &crate::ResetMarker, right: &crate::ResetMarker) -> bool {
    !matches!(left, crate::ResetMarker::Unknown) && left == right
}

fn count(counter: &StatementCounter) -> Option<f64> {
    (counter.unit == RuntimeMetricUnit::Count).then(|| counter.total.exact_real()).flatten()
}

fn milliseconds(counter: &StatementCounter) -> Option<f64> {
    counter.total.exact_real().and_then(|value| counter.unit.milliseconds(value))
}

fn reads(counter: &StatementCounter) -> Option<f64> {
    matches!(counter.unit, RuntimeMetricUnit::Count | RuntimeMetricUnit::Pages { .. })
        .then(|| counter.total.exact_real())
        .flatten()
}

fn invalid() -> crate::Error {
    CollectionStop::InvalidPage.error()
}
