//! Human output reads the same closed derived fields as JSON; it never promotes a victim or candidate.
use super::*;
use culprit_core::{DerivedCaller, DerivedIncidentQuery, IncidentReadReport};

pub(crate) fn incidents(report: &IncidentReadReport) -> String {
    let mut output = incident_history(&report.history);
    output.push('\n');
    output.push_str(&t!(
        "incident.cli.scope",
        from = preview(&report.from),
        until = preview(&report.until),
        cutoff = preview(&report.recorded_before)
    ));
    for analysis in &report.analyses {
        for limitation in analysis.description.limitations() {
            append(&mut output, preview(&limitation));
        }
    }
    if report.analyses.iter().any(|analysis| !analysis.description.input_read_complete) {
        append(&mut output, t!("incident.cli.inputs_partial"));
    }
    append(&mut output, t!("incident.cli.caller_qualification"));
    for incident in &report.incidents {
        output.push_str("\n\n");
        output.push_str(&t!("incident.cli.victim", victim = preview(&incident.victim_summary)));
        if let Some(caller) = &incident.caller
            && !incident.affected_queries.iter().any(|query| query.caller.as_ref() == Some(caller))
        {
            append_caller(&mut output, Some(caller));
        }
        for query in &incident.affected_queries {
            append_query(&mut output, t!("incident.cli.affected", query = query_text(query)), query);
        }
        for query in &incident.culprit_queries {
            append_query(&mut output, t!("incident.cli.culprit", query = query_text(query)), query);
        }
        if incident.culprit_queries.is_empty() {
            append(&mut output, t!("incident.cli.culprit_unknown"));
        }
        for query in &incident.candidate_queries {
            append_query(&mut output, t!("incident.cli.candidate", query = query_text(query)), query);
        }
        append(
            &mut output,
            match &incident.mechanism {
                Some(mechanism) => t!("incident.cli.mechanism", mechanism = preview(mechanism)),
                None => t!("incident.cli.mechanism_unknown"),
            },
        );
        append(&mut output, t!("incident.cli.action", action = preview(&incident.developer_action)));
        append(&mut output, t!("incident.cli.verification", verification = preview(&incident.verification)));
    }
    if !report.discovery_complete {
        append(&mut output, t!("evidence.cli.read_incomplete"));
    }
    output.push('\n');
    output
}
fn append(output: &mut String, line: String) {
    output.push('\n');
    output.push_str(&line);
}
fn query_text(query: &DerivedIncidentQuery) -> String {
    match &query.sql {
        culprit_core::telemetry::DefinitionPayload::Available { text, .. } => preview(text),
        culprit_core::telemetry::DefinitionPayload::Unavailable { .. } => statement::sql(Some(&query.sql)),
    }
}

fn append_query(output: &mut String, label: String, query: &DerivedIncidentQuery) {
    append(output, label);
    if let Some(role) = &query.causal_role {
        append(output, t!("incident.cli.causal_role", explanation = preview(&role.explanation.message().render())));
    }
    append_caller(output, query.caller.as_ref());
}
fn append_caller(output: &mut String, caller: Option<&DerivedCaller>) {
    let Some(caller) = caller else {
        append(output, t!("incident.cli.caller_unavailable"));
        return;
    };
    append(output, request_labels::client(caller.client.as_ref()));
    let session = caller.session_context.as_deref();
    append(
        output,
        t!(
            "incident.cli.caller_session",
            library = request_labels::optional(session.and_then(|session| session.client_library.as_deref())),
            process = request_labels::optional(session.and_then(|session| session.client_process_id.as_deref()))
        ),
    );
}
fn preview(text: &str) -> String {
    culprit_core::format::escaped_preview(text, OBSERVATION_DETAIL_CHARS).text
}
