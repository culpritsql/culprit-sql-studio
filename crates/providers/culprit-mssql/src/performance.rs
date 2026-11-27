//! One native performance-counter read supplies exact inputs, pressure and connection-churn projections.
use crate::read::{NativeRead, ReadError, invalid_data, optional_text, signed_integer, text};
use culprit_core::{
    MeasurementScope, ResetMarker, Result,
    telemetry::{
        CollectionStop, CounterNature, MAX_NATIVE_COUNTERS, NativeCounterSample, OperationalObservation,
        PerformanceCounterSpec, digest,
    },
};
use std::collections::{BTreeSet, HashMap};
mod database;
#[cfg(test)]
pub(crate) use database::tests as test_support;
mod ratios;
mod resource_governor;
mod row;
mod selectors;
pub(crate) use database::read_current;
pub(crate) use ratios::PerformanceRead;
pub(crate) use resource_governor::{read as read_for_instance, read_pool as read_for_pool};
pub(crate) const MAX_COUNTERS: usize = MAX_NATIVE_COUNTERS;
const COUNTER_COLUMNS: &str = "RTRIM(object_name) AS native_object, RTRIM(counter_name) AS native_name,\nRTRIM(instance_name) AS native_instance, cntr_value AS value, cntr_type AS native_type";
#[derive(Clone, Copy)]
enum Population<'a> {
    Shared,
    InstanceInvestigation,
    Database(&'a crate::local_source::Context),
}
impl Population<'_> {
    fn scope(self) -> MeasurementScope {
        match self {
            Self::Shared | Self::InstanceInvestigation => MeasurementScope::Server,
            Self::Database(_) => MeasurementScope::Database,
        }
    }
    fn field(self, object: &str, name: &str, instance: Option<&str>) -> Option<&'static PerformanceCounterSpec> {
        match self {
            Self::Shared => specs().iter().find(|spec| spec.matches(object, name, instance)),
            Self::InstanceInvestigation => {
                specs().iter().find(|spec| spec.matches(object, name, instance)).or_else(|| {
                    let profile = culprit_core::Engine::Mssql.spec().performance_counters()?;
                    profile
                        .common_field(object, name, instance)
                        .or_else(|| profile.detailed_field(object, name, instance))
                })
            }
            Self::Database(owner) => {
                if instance != Some(owner.activity.database.database.as_str()) {
                    return None;
                }
                culprit_core::Engine::Mssql.spec().performance_counters()?.database_field(object, name)
            }
        }
    }
    fn selection(self) -> &'static str {
        match self {
            Self::InstanceInvestigation => culprit_core::telemetry::MSSQL_COMMON_COUNTER_PROFILE,
            Self::Shared | Self::Database(_) => culprit_core::telemetry::PERFORMANCE_COUNTER_PROFILE,
        }
    }
}

fn specs() -> &'static [PerformanceCounterSpec] {
    culprit_core::Engine::Mssql.spec().performance_counters().map(|profile| profile.fields).unwrap_or_default()
}
fn sql() -> String {
    query(false)
}
fn query(governed: bool) -> String {
    query_with_context(governed, "")
}
fn pool_query() -> String {
    query_with_context(true, ",CONVERT(nvarchar(60),DATABASEPROPERTYEX(DB_NAME(),N'Updateability')) AS updateability")
}
fn query_with_context(governed: bool, native_context: &str) -> String {
    let (relation, extra) = if governed {
        (selectors::governed_relation(), resource_governor::columns())
    } else {
        (selectors::shared_relation(), String::new())
    };
    format!(
        "DECLARE @counter_observed_at datetime2(7)=SYSUTCDATETIME();
/* culprit */ SELECT {COUNTER_COLUMNS},
CONVERT(nvarchar(40),@counter_observed_at,127)+N'Z' AS counter_observed_at,
CONVERT(int,SERVERPROPERTY(N'EngineEdition')) AS engine_edition,
CONVERT(nvarchar(128),DATABASEPROPERTYEX(DB_NAME(),N'Edition')) AS database_edition,
CONVERT(nvarchar(128),DATABASEPROPERTYEX(DB_NAME(),N'ReplicaID')) AS replica_id{native_context}{extra}
{relation} OPTION(MAXDOP 1)"
    )
}
pub(crate) async fn read<R: NativeRead>(reader: &mut R, limit: usize) -> Result<PerformanceRead> {
    if specs().is_empty() {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    let limit = limit.min(MAX_COUNTERS);
    let rows = reader.query(&sql(), vec![], limit + 1).await.map_err(ReadError::into_error)?;
    decode(&rows, limit, Population::Shared)
}
fn decode(rows: &[crate::value::Rec], limit: usize, population: Population<'_>) -> Result<PerformanceRead> {
    if rows.len() > limit {
        return Err(CollectionStop::RowLimit.error());
    }
    if rows.is_empty() {
        return Err(CollectionStop::SourceUnavailable.error());
    }
    let mut identities = BTreeSet::new();
    let mut counters = Vec::new();
    let mut rejected = Vec::new();
    for row in rows {
        let input = row::CounterInput::decode(row)?;
        let spec = population.field(input.object(), input.name(), input.instance()).ok_or_else(invalid_data)?;
        if !identities.insert(input.identity().to_owned()) {
            return Err(invalid_data());
        }
        match input.measurement(spec, population.scope())? {
            row::CounterMeasurement::Value(counter) => counters.push(counter),
            row::CounterMeasurement::Rejected(gap) => rejected.push(gap),
        }
    }
    match population {
        Population::Shared | Population::InstanceInvestigation => {
            ratios::captured(rows, counters, rejected, population)
        }
        Population::Database(owner) => database::captured(rows, counters, owner),
    }
}
pub(crate) fn application(rows: &[NativeCounterSample]) -> HashMap<String, f64> {
    let mut values = HashMap::new();
    for spec in specs().iter().filter(|spec| spec.application) {
        if let Some(value) = global(rows, spec).and_then(|row| row.value.integer()) {
            values.insert(spec.native_name.into(), value as f64);
        }
    }
    values
}
fn global<'a>(rows: &'a [NativeCounterSample], spec: &PerformanceCounterSpec) -> Option<&'a NativeCounterSample> {
    let mut matching = rows.iter().filter(|row| {
        spec.object_family.is_some_and(|family| row.native_object.contains(family))
            && row.native_name == spec.native_name
            && row.nature == spec.nature
            && row.unit == spec.unit
            && row.native_instance.is_none()
    });
    let selected = matching.next()?;
    matching.next().is_none().then_some(selected)
}
pub(crate) fn sampled_rates(
    before: &[NativeCounterSample],
    after: &[NativeCounterSample],
    elapsed_seconds: f64,
) -> HashMap<String, f64> {
    if !elapsed_seconds.is_finite() || elapsed_seconds <= 0.0 {
        return HashMap::new();
    }
    specs()
        .iter()
        .filter(|spec| spec.application && spec.nature == CounterNature::Cumulative)
        .filter_map(|spec| {
            let previous = global(before, spec)?;
            let current = global(after, spec)?;
            if previous.scope != current.scope
                || previous.reset != current.reset
                || previous.native_type != current.native_type
                || previous.native_object != current.native_object
            {
                return None;
            }
            let previous = previous.value.integer()?;
            let current = current.value.integer()?;
            let delta = current.checked_sub(previous)?;
            Some((spec.native_name.into(), delta as f64 / elapsed_seconds))
        })
        .collect()
}
pub(crate) fn observations(read: PerformanceRead) -> Result<Vec<OperationalObservation>> {
    std::iter::once(read.sample)
        .chain(read.resource_governor)
        .map(|sample| OperationalObservation::from_counters(culprit_core::source_id::PERFORMANCE_COUNTERS, sample))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod native_tests;
