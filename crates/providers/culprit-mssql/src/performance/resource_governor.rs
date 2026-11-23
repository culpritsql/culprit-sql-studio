//! An explicitly requested instance population shares the existing query/decoder and original source owner.
use super::*;
use crate::{
    instance,
    read::{optional_counter, optional_text},
    value::Rec,
};
use culprit_core::telemetry::*;
use std::collections::BTreeMap;

pub(super) const FROM: &str = r#"FROM sys.dm_os_performance_counters AS c
 CROSS JOIN sys.dm_os_sys_info AS i
 LEFT JOIN sys.dm_resource_governor_workload_groups AS w
  ON RIGHT(RTRIM(c.object_name),LEN(N':Workload Group Stats'))=N':Workload Group Stats'
   AND CONVERT(varbinary(256),RTRIM(c.instance_name))=CONVERT(varbinary(256),w.name)
 LEFT JOIN sys.dm_resource_governor_resource_pools AS p
  ON RIGHT(RTRIM(c.object_name),LEN(N':Resource Pool Stats'))=N':Resource Pool Stats'
   AND CONVERT(varbinary(256),RTRIM(c.instance_name))=CONVERT(varbinary(256),p.name)"#;

pub(super) fn columns() -> String {
    format!(
        r#",
 {},w.group_id AS rg_group_id,
 CASE WHEN RIGHT(RTRIM(c.object_name),LEN(N':Workload Group Stats'))=N':Workload Group Stats'
  THEN w.pool_id ELSE p.pool_id END AS rg_pool_id,
 CASE WHEN RIGHT(RTRIM(c.object_name),LEN(N':Workload Group Stats'))=N':Workload Group Stats'
  THEN w.name ELSE p.name END AS rg_name,
 CASE WHEN RIGHT(RTRIM(c.object_name),LEN(N':Workload Group Stats'))=N':Workload Group Stats'
  THEN CONVERT(varchar(40),CAST(w.statistics_start_time AS datetime2),126)
  ELSE CONVERT(varchar(40),CAST(p.statistics_start_time AS datetime2),126) END AS rg_statistics_started_without_offset"#,
        instance::point_columns(instance::PointClock::PerformanceRead)
    )
}

pub(crate) async fn read<R: NativeRead>(
    reader: &mut R,
    limit: usize,
    expected: &instance::Context,
) -> Result<PerformanceRead> {
    let limit = limit.min(MAX_COUNTERS);
    if limit == 0 {
        return Err(CollectionStop::RowLimit.error());
    }
    let profile = culprit_core::Engine::Mssql.spec().performance_counters().ok_or_else(invalid_data)?;
    let rows = reader.query(&query(true), vec![], limit + 1).await.map_err(ReadError::into_error)?;
    if rows.len() > limit {
        return Err(CollectionStop::RowLimit.error());
    }
    let (owner, at) = observed(&rows, expected)?;
    let (standard, governed) = partition(&rows, profile)?;
    let mut read = decode(&standard, limit, Population::InstanceInvestigation)?;
    let epoch = owner.capture_epoch()?;
    let mut sets = group(&governed, Observer::Instance { owner: &owner, epoch: &epoch }, &at, profile)?;
    read.resource_governor_complete = !sets.is_empty();
    for sample in sets.values_mut() {
        sample.properties.insert(PERFORMANCE_COUNTER_ROWS_PROPERTY.into(), sample.counters.len().to_string());
        profile.validate(sample)?;
        let metadata = MssqlResourceGovernorCounterSet::from_sample(sample)?;
        read.resource_governor_complete &= metadata.has_native_ids();
    }
    read.sample.properties.insert(MSSQL_RESOURCE_GOVERNOR_REQUEST_PROPERTY.into(), "true".into());
    read.sample.properties.insert("resource_governor_native_rows".into(), governed.len().to_string());
    read.sample.properties.insert("resource_governor_instance_id".into(), owner.identity.telemetry_id()?);
    read.sample.properties.insert("resource_governor_instance_epoch".into(), epoch);
    profile.validate(&read.sample)?;
    read.native_rows = rows.len();
    read.resource_governor = sets.into_values().collect();
    Ok(read)
}

#[derive(Clone, Copy)]
enum Observer<'a> {
    Instance { owner: &'a instance::Context, epoch: &'a str },
    Pool(&'a crate::resources::pool::native::Context),
}
impl Observer<'_> {
    fn fields(self) -> Result<BTreeMap<String, String>> {
        Ok(match self {
            Self::Instance { owner, epoch } => BTreeMap::from([
                ("resource_governor_instance_id".into(), owner.identity.telemetry_id()?),
                ("resource_governor_instance_epoch".into(), epoch.into()),
            ]),
            Self::Pool(owner) => {
                let (epoch, _) = owner.scope.observed().ok_or_else(invalid_data)?;
                BTreeMap::from([
                    ("resource_governor_elastic_pool_id".into(), owner.current.point.pool.telemetry_id()?),
                    ("resource_governor_anchor_id".into(), owner.current.point.member.key()?),
                    ("resource_governor_pool_process_epoch".into(), epoch.into()),
                ])
            }
        })
    }
    fn bind(self, mut identity: NativeIdentity) -> Result<NativeIdentity> {
        for (name, value) in self.fields()? {
            identity = identity.qualified(name, value);
        }
        Ok(identity)
    }
}

fn partition<'a>(rows: &'a [Rec], profile: &PerformanceCounterProfile) -> Result<(Vec<Rec>, Vec<&'a Rec>)> {
    let mut standard = Vec::new();
    let mut governed = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        let input = row::CounterInput::decode(row)?;
        if !seen.insert(input.identity().to_owned()) {
            return Err(invalid_data());
        }
        if profile.investigation_field(input.object(), input.name(), input.instance()).is_some() {
            governed.push(row);
        } else {
            standard.push(row.clone());
        }
    }
    Ok((standard, governed))
}
fn group(
    rows: &[&Rec],
    observer: Observer<'_>,
    at: &str,
    profile: &PerformanceCounterProfile,
) -> Result<BTreeMap<(MssqlResourceGovernorCounterKind, String), NativeCounterSet>> {
    let mut sets = BTreeMap::new();
    for row in rows {
        let input = row::CounterInput::decode(row)?;
        let spec =
            profile.investigation_field(input.object(), input.name(), input.instance()).ok_or_else(invalid_data)?;
        let kind = match spec.family {
            PerformanceCounterFamily::WorkloadGroup => MssqlResourceGovernorCounterKind::WorkloadGroup,
            PerformanceCounterFamily::ResourcePool => MssqlResourceGovernorCounterKind::ResourcePool,
            _ => return Err(invalid_data()),
        };
        let name = input.instance().ok_or_else(invalid_data)?.to_owned();
        let mut sample = header(row, observer, at, kind, &name)?;
        let row::CounterMeasurement::Value(counter) = input.measurement(spec, MeasurementScope::Server)? else {
            return Err(invalid_data());
        };
        sample.counters.push(counter);
        let target = sets.entry((kind, name)).or_insert_with(|| sample.clone());
        if target.identity != sample.identity || target.properties != sample.properties {
            return Err(CollectionStop::SourceChanged.error());
        }
        if target.counters[0].key != sample.counters[0].key {
            target.counters.push(sample.counters.remove(0));
        }
    }
    Ok(sets)
}

pub(crate) async fn read_pool<R: NativeRead>(
    reader: &mut R,
    limit: usize,
    expected: &crate::resources::pool::native::Context,
) -> Result<PerformanceRead> {
    let limit = limit.min(MAX_COUNTERS);
    if limit == 0 {
        return Err(CollectionStop::RowLimit.error());
    }
    let rows = reader.query(&pool_query(), vec![], limit + 1).await.map_err(ReadError::into_error)?;
    if rows.len() > limit {
        return Err(CollectionStop::RowLimit.error());
    }
    let profile = culprit_core::Engine::Mssql.spec().performance_counters().ok_or_else(invalid_data)?;
    let mut at = None;
    for row in &rows {
        let time = canonical_time(&text(row, "counter_observed_at")?)?;
        if !expected.accepts_counter_point(row)?
            || ObservationBounds::new(&expected.current.point.observed_at, &time).is_none()
            || at.as_ref().is_some_and(|before| before != &time)
        {
            return Err(CollectionStop::SourceChanged.error());
        }
        at = Some(time);
    }
    let at = at.ok_or_else(|| CollectionStop::SourceUnavailable.error())?;
    let (standard, governed) = partition(&rows, profile)?;
    let mut read = decode(&standard, limit, Population::InstanceInvestigation)?;
    let mut sets = group(&governed, Observer::Pool(expected), &at, profile)?;
    read.resource_governor_complete = !sets.is_empty();
    for sample in sets.values_mut() {
        sample.properties.insert(PERFORMANCE_COUNTER_ROWS_PROPERTY.into(), sample.counters.len().to_string());
        profile.validate(sample)?;
        read.resource_governor_complete &= MssqlResourceGovernorCounterSet::from_sample(sample)?.has_native_ids();
    }
    read.resource_governor_complete &= pool_population_complete(&sets, expected, profile)?;
    read.sample.properties.insert(MSSQL_RESOURCE_GOVERNOR_REQUEST_PROPERTY.into(), "true".into());
    read.sample.properties.insert("resource_governor_native_rows".into(), governed.len().to_string());
    read.sample.properties.extend(Observer::Pool(expected).fields()?);
    profile.validate(&read.sample)?;
    read.native_rows = rows.len();
    read.resource_governor = sets.into_values().collect();
    Ok(read)
}

fn pool_population_complete(
    sets: &BTreeMap<(MssqlResourceGovernorCounterKind, String), NativeCounterSet>,
    owner: &crate::resources::pool::native::Context,
    profile: &PerformanceCounterProfile,
) -> Result<bool> {
    let (_, members) = owner.scope.observed().ok_or_else(invalid_data)?;
    let mut groups = BTreeMap::new();
    let mut pools = BTreeMap::new();
    for sample in sets.values() {
        let context = MssqlResourceGovernorCounterSet::from_sample(sample)?.context();
        match context.kind {
            MssqlResourceGovernorCounterKind::WorkloadGroup => {
                if let Some(id) = context.native_group_id
                    && groups.insert(id, (sample, context.native_pool_id)).is_some()
                {
                    return Err(CollectionStop::SourceChanged.error());
                }
            }
            MssqlResourceGovernorCounterKind::ResourcePool => {
                if let Some(id) = context.native_pool_id
                    && pools.insert(id, sample).is_some()
                {
                    return Err(CollectionStop::SourceChanged.error());
                }
            }
        }
    }
    for member in members {
        let Some((group, Some(pool_id))) = groups.get(&member.workload_group_id.to_string()) else {
            return Ok(false);
        };
        let Some(pool) = pools.get(pool_id) else {
            return Ok(false);
        };
        if !complete_fields(group, PerformanceCounterFamily::WorkloadGroup, profile)
            || !complete_fields(pool, PerformanceCounterFamily::ResourcePool, profile)
        {
            return Ok(false);
        }
    }
    Ok(true)
}
fn complete_fields(
    sample: &NativeCounterSet,
    family: PerformanceCounterFamily,
    profile: &PerformanceCounterProfile,
) -> bool {
    profile.investigation_fields.iter().filter(|field| field.family == family).all(|field| {
        sample.counters.iter().any(|counter| {
            field.matches(&counter.native_object, &counter.native_name, counter.native_instance.as_deref())
        })
    })
}

fn observed(rows: &[Rec], expected: &instance::Context) -> Result<(instance::Context, String)> {
    let mut first = None;
    for row in rows {
        let owner = instance::observation(row)?
            .context_from_epoch(row)?
            .ok_or_else(|| CollectionStop::SourceChanged.error())?;
        let at = canonical_time(&text(row, "counter_observed_at")?)?;
        if canonical_time(&text(row, "observed_at")?)? != at {
            return Err(CollectionStop::SourceChanged.error());
        }
        if &owner != expected || first.as_ref().is_some_and(|before| before != &(owner.clone(), at.clone())) {
            return Err(CollectionStop::SourceChanged.error());
        }
        first = Some((owner, at));
    }
    first.ok_or_else(|| CollectionStop::SourceUnavailable.error())
}
fn header(
    row: &Rec,
    observer: Observer<'_>,
    at: &str,
    kind: MssqlResourceGovernorCounterKind,
    name: &str,
) -> Result<NativeCounterSet> {
    let mut properties = BTreeMap::from([
        (PERFORMANCE_COUNTER_PROFILE_PROPERTY.into(), MSSQL_RESOURCE_GOVERNOR_COUNTER_PROFILE.into()),
        (PERFORMANCE_COUNTER_CLOCK_PROPERTY.into(), at.into()),
        ("resource_name".into(), name.into()),
    ]);
    for (column, key) in [("rg_group_id", "group_id"), ("rg_pool_id", "pool_id")] {
        if let Some(id) = optional_counter(row, column)? {
            if !id.parse::<i32>().is_ok_and(|id| id > 0) {
                return Err(invalid_data());
            }
            properties.insert(key.into(), id);
        }
    }
    let native_name = optional_text(row, "rg_name")?;
    if properties.contains_key("pool_id") && native_name.as_deref() != Some(name) {
        return Err(CollectionStop::SourceChanged.error());
    }
    if let Some(started) = optional_text(row, "rg_statistics_started_without_offset")? {
        properties.insert("statistics_started_without_offset".into(), crate::sql_time::native_time(&started)?);
    }
    Ok(NativeCounterSet {
        identity: observer.bind(
            NativeIdentity::new(digest(name.as_bytes()))
                .qualified("kind", MSSQL_RESOURCE_GOVERNOR_COUNTER_KIND)
                .qualified("native_object", "sys.dm_os_performance_counters")
                .qualified("resource_kind", kind.key()),
        )?,
        scope: MeasurementScope::Server,
        counters: Vec::new(),
        ratios: Vec::new(),
        ratio_gaps: Vec::new(),
        properties,
    })
}

#[cfg(test)]
mod tests;
