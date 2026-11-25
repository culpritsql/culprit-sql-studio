//! Offline namespace-bound helpers consume published Gold; no KQL detector or native current-state inference.
use crate::{catalog::SchemaCatalog, eligibility};
use culprit_core::Result;
use culprit_evidence::analytical::{
    query_helpers::{QueryExample, QueryHelperAssets, QueryHelperBinding, render_friendly_query},
    semantics::{EligibilityScope, NAMESPACE_QUERY_STREAMS},
};
pub fn configured(binding: QueryHelperBinding) -> Result<QueryHelperAssets> {
    binding.validate()?;
    let catalog = SchemaCatalog::new()?;
    if binding.schema_digest != catalog.digest() {
        return Err(culprit_core::telemetry::DeliveryProblem::UnsupportedSchema.error());
    }
    let mut commands = vec![eligibility::published(&catalog, EligibilityScope::Namespace)];
    for stream in NAMESPACE_QUERY_STREAMS {
        let schema = catalog.get(stream)?;
        commands.push(eligibility::eligible(schema, EligibilityScope::Namespace));
        commands.push(eligibility::rows(schema, EligibilityScope::Namespace, false));
    }
    let deadlocks = catalog.get(culprit_core::telemetry::TelemetryStream::DeadlockEvents)?;
    commands.push(eligibility::eligible(deadlocks, EligibilityScope::Namespace));
    commands.push(eligibility::rows(deadlocks, EligibilityScope::Namespace, true));
    let string = |value: &str| {
        serde_json::to_string(value).map_err(|_| culprit_core::telemetry::DeliveryProblem::InvalidContract.error())
    };
    let analyzer = |field: &str| -> Result<String> {
        let revision = match &binding.policy.analyzer.revision {
            Some(value) => format!("tostring({field}.revision) == {}", string(value)?),
            None => format!("isnull({field}.revision)"),
        };
        Ok(format!(
            "tostring({field}.component) == {} and tostring({field}.version) == {} and {revision}",
            string(&binding.policy.analyzer.component)?,
            string(binding.policy.analyzer.version.as_deref().unwrap_or_default())?
        ))
    };
    let text = render_friendly_query(
        include_str!("kql/friendly.kql"),
        &[
            ("$N", binding.policy.name.clone()),
            ("$CONTRACT", string(&binding.contract_id)?),
            ("$STORE", string(&binding.recovery_store_id)?),
            ("$FROM", format!("datetime({})", binding.policy.history_from)),
            ("$HEADER_ANALYZER_MATCH", analyzer("Header.analyzer")?),
            ("$ANALYZER_MATCH", analyzer("Analyzer")?),
            ("$RULES", string(&binding.policy.rules_version)?),
        ],
    )?;
    commands.extend(
        text.split("\n\n.create")
            .enumerate()
            .map(|(i, part)| if i == 0 { part.to_string() } else { format!(".create{part}") }),
    );
    let name = &binding.policy.name;
    let mut examples = vec![
        QueryExample { name: "resources", query: format!("{name}Resources(now())") },
        QueryExample { name: "status", query: format!("{name}Status(\"orders\",now(),30m)") },
        QueryExample { name: "incidents", query: format!("{name}Incidents(\"orders\",ago(1h),now(),now())") },
        QueryExample { name: "history", query: format!("{name}IncidentHistory(\"orders\",ago(1d),now(),now())") },
        QueryExample { name: "findings", query: format!("{name}Findings(\"orders\",ago(1h),now(),now())") },
        QueryExample { name: "changes", query: format!("{name}Changes(\"orders\",ago(1h),now(),now())") },
        QueryExample {
            name: "new-recorded-incident-alert",
            query: format!("{name}NewIncidents(\"orders\",ago(5m),now(),now())"),
        },
        QueryExample {
            name: "stale-or-unknown-evidence-alert",
            query: format!(
                "{name}Status(\"orders\",now(),30m) | where State != \"observed\" or not(InputReadComplete) or array_length(ContextIssues)>0"
            ),
        },
    ];
    let window = "declare query_parameters(resource:string,window_from:datetime,window_until:datetime);\n";
    for (label, function) in [
        ("incident-window", "IncidentsDuring"),
        ("finding-window", "FindingsDuring"),
        ("change-window", "ChangesDuring"),
        ("recorded-incident-alert-window", "RecordedIncidentAlertsDuring"),
    ] {
        examples.push(QueryExample {
            name: label,
            query: format!("{window}{name}{function}(resource,window_from,window_until)"),
        });
    }
    examples.push(QueryExample {
        name: "evidence-availability-alert",
        query: format!("declare query_parameters(resource:string,max_age:timespan);\n{name}StatusNow(resource,max_age) | where State != \"observed\" or not(InputReadComplete) or array_length(ContextIssues)>0"),
    });
    examples.push(QueryExample {
        name: "native-deadlock-window",
        query: format!("{window}{name}DeadlocksDuring(resource,window_from,window_until)"),
    });
    examples.push(QueryExample { name: "high-severity-findings", query: format!("{window}{name}FindingsDuring(resource,window_from,window_until) | where Severity == \"high\" and Actionability in (\"needs-attention\",\"optional-improvement\") and not(Inactive)") });
    examples.push(QueryExample {
        name: "published-collection-status",
        query: format!("{name}CollectionStatus(now(),30m)"),
    });
    examples
        .push(QueryExample { name: "fleet-summary", query: format!("{name}FleetSummary(ago(24h),now(),now(),30m)") });
    examples.push(QueryExample {
        name: "opt-in-adx-queued-batching",
        query: include_str!("kql/operator_batching_opt_in.kql").into(),
    });
    let script = format!(".execute database script with (ContinueOnErrors=false) <|\n{}\n", commands.join("\n\n"));
    let operator = operators::assets(&binding)?;
    Ok(QueryHelperAssets { binding, commands, script, examples, operator })
}
mod operators;
#[cfg(test)]
mod tests;
