//! Parameterized read helpers above the shared eligibility templates. No native detector is implemented in SQL.
use crate::{catalog::SchemaCatalog, eligibility};
use culprit_core::{Result, telemetry::DeliveryProblem};
use culprit_evidence::analytical::{
    query_helpers::{QueryExample, QueryHelperAssets, QueryHelperBinding, render_friendly_query},
    semantics::{EligibilityScope, NAMESPACE_QUERY_STREAMS},
};
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
pub fn configured(binding: QueryHelperBinding) -> Result<QueryHelperAssets> {
    binding.validate()?;
    let catalog = SchemaCatalog::new()?;
    if binding.schema_digest != catalog.digest() {
        return Err(DeliveryProblem::UnsupportedSchema.error());
    }
    let mut commands = vec![eligibility::published(&catalog, EligibilityScope::Namespace)];
    for stream in NAMESPACE_QUERY_STREAMS {
        let schema = catalog.get(stream)?;
        commands.push(eligibility::eligible(schema, EligibilityScope::Namespace));
        commands.push(eligibility::rows(schema, EligibilityScope::Namespace, false));
    }
    let analyzer = |field: &str| {
        format!(
            "JSONExtractString({field},'component')={} AND JSONExtractString({field},'version')={} AND JSONExtractString({field},'revision')={}",
            literal(&binding.policy.analyzer.component),
            literal(binding.policy.analyzer.version.as_deref().unwrap_or_default()),
            literal(binding.policy.analyzer.revision.as_deref().unwrap_or_default())
        )
    };
    let history = chrono::DateTime::parse_from_rfc3339(&binding.policy.history_from)
        .ok()
        .and_then(|at| at.timestamp_nanos_opt())
        .ok_or_else(|| DeliveryProblem::InvalidContract.error())?;
    let text = render_friendly_query(
        include_str!("sql/friendly.sql"),
        &[
            ("$N", binding.policy.name.clone()),
            ("$CONTRACT", literal(&binding.contract_id)),
            ("$STORE", literal(&binding.recovery_store_id)),
            ("$FROM", format!("fromUnixTimestamp64Nano({history})")),
            ("$HEADER_ANALYZER_MATCH", analyzer("JSONExtractRaw(Header,'analyzer')")),
            ("$ANALYZER_MATCH", analyzer("Analyzer")),
            ("$RULES", literal(&binding.policy.rules_version)),
        ],
    )?;
    commands.extend(
        text.split(";\n\n").filter(|part| !part.trim().is_empty()).map(|part| part.trim_end_matches(';').to_string()),
    );
    let n = &binding.policy.name;
    let args = "resource='orders',from=now64(9)-INTERVAL 1 HOUR,until=now64(9),asof=now64(9)";
    let examples = vec![
        QueryExample { name: "resources", query: format!("SELECT * FROM {n}Resources(asof=now64(9))") },
        QueryExample {
            name: "status",
            query: format!("SELECT * FROM {n}Status(resource='orders',asof=now64(9),max_age_seconds=1800)"),
        },
        QueryExample { name: "incidents", query: format!("SELECT * FROM {n}Incidents({args})") },
        QueryExample { name: "history", query: format!("SELECT * FROM {n}IncidentHistory({args})") },
        QueryExample { name: "findings", query: format!("SELECT * FROM {n}Findings({args})") },
        QueryExample { name: "changes", query: format!("SELECT * FROM {n}Changes({args})") },
        QueryExample {
            name: "new-recorded-incident-alert",
            query: format!(
                "SELECT * FROM {n}NewIncidents(resource='orders',from=now64(9)-INTERVAL 5 MINUTE,until=now64(9),asof=now64(9))"
            ),
        },
        QueryExample {
            name: "stale-or-unknown-evidence-alert",
            query: format!(
                "SELECT * FROM {n}Status(resource='orders',asof=now64(9),max_age_seconds=1800) WHERE State!='observed' OR NOT InputReadComplete OR length(JSONExtractArrayRaw(ContextIssues))>0"
            ),
        },
    ];
    let script = format!("{};\n", commands.join(";\n\n"));
    Ok(QueryHelperAssets { binding, commands, script, examples })
}
#[cfg(test)]
mod tests;
