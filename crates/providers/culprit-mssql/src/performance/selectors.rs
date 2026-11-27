use super::{resource_governor, specs};
use culprit_core::telemetry::{PerformanceCounterInstances, PerformanceCounterSpec};

// Match the native counter name once per profile entry. The family and instance rules qualify that match.
pub(super) fn shared_relation() -> String {
    let mut groups = Vec::new();
    let mut rows = Vec::new();
    for spec in specs() {
        let rule = (spec.object_family, instance_condition(spec.instances));
        let group = groups.iter().position(|existing| existing == &rule).unwrap_or_else(|| {
            groups.push(rule);
            groups.len() - 1
        });
        let pattern = spec
            .object_family
            .map_or_else(|| "CONVERT(nvarchar(64),NULL)".to_owned(), |family| format!("N'%{family}%'"));
        rows.push(format!("(N'{}',{pattern},{group})", spec.native_name));
    }
    let instances = groups
        .iter()
        .enumerate()
        .map(|(group, (_, condition))| format!("(sel.instance_group={group}{condition})"))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "FROM sys.dm_os_performance_counters INNER JOIN (VALUES {}) sel(wanted,pattern,instance_group) ON counter_name=sel.wanted WHERE (sel.pattern IS NULL OR object_name LIKE sel.pattern) AND ({instances})",
        rows.join(",")
    )
}

pub(super) fn governed_relation() -> String {
    // The selector grammar comes only from these fixed specifications, never from a database value or user input.
    let Some(profile) = culprit_core::Engine::Mssql.spec().performance_counters() else {
        return format!("{} WHERE 1=0", resource_governor::FROM);
    };
    let selected = profile
        .fields
        .iter()
        .chain(profile.common_fields)
        .chain(profile.investigation_fields)
        .map(|spec| (spec, false))
        .chain(profile.detailed_fields.iter().map(|spec| (spec, true)));
    let filter =
        selected.map(|(spec, exact_object)| counter_predicate(spec, exact_object)).collect::<Vec<_>>().join(" OR ");
    format!("{} WHERE {filter}", resource_governor::FROM)
}

fn counter_predicate(spec: &PerformanceCounterSpec, exact_object: bool) -> String {
    let family = spec
        .object_family
        .map(|family| {
            if exact_object {
                format!("RIGHT(RTRIM(object_name),LEN(N':{family}'))=N':{family}' AND ")
            } else {
                format!("object_name LIKE N'%{family}%' AND ")
            }
        })
        .unwrap_or_default();
    let instance = instance_condition(spec.instances);
    format!("({family}counter_name=N'{}'{instance})", spec.native_name)
}

fn instance_condition(instances: PerformanceCounterInstances) -> String {
    match instances {
        PerformanceCounterInstances::Uninstanced => " AND RTRIM(instance_name)=N''".into(),
        PerformanceCounterInstances::Named(names) => format!(
            " AND RTRIM(instance_name) IN ({})",
            names.iter().map(|name| format!("N'{name}'")).collect::<Vec<_>>().join(",")
        ),
        PerformanceCounterInstances::Any => String::new(),
    }
}
