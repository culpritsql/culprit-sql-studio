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
    if let Some(scope) = query.identity.as_ref().and_then(culprit_core::DerivedQueryIdentity::shared_scope_label) {
        append(output, preview(&scope));
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_core::telemetry::*;
    #[test]
    fn shared_member_query_labels_are_bounded_escaped_and_never_replaced_by_an_incident_database() {
        let mut query = DerivedIncidentQuery {
            causal_role: None,
            identity: Some(culprit_core::DerivedQueryIdentity::SharedActivity {
                member_id: digest(b"native-instance"),
                member_kind: SharedResourceKind::Instance,
                actor: ActivityKey {
                    session: NativeActivityIdentity { id: "42".into(), generation: "native-login".into() },
                    request: Some(NativeActivityIdentity { id: "0".into(), generation: "native-request".into() }),
                    transaction: None,
                },
                statement: None,
                database: Some(format!("foreign\n\u{1b}[31m{}", "x".repeat(2000))),
            }),
            sql: DefinitionPayload::Unavailable { reason: PayloadUnavailable::Missing },
            caller: None,
            evidence: vec![],
        };
        let mut output = String::new();
        append_query(&mut output, "query".into(), &query);
        assert!(output.contains("foreign"));
        assert!(!output.contains('\u{1b}'));
        assert!(output.len() < 1400);
        let Some(culprit_core::DerivedQueryIdentity::SharedActivity { database, .. }) = &mut query.identity else {
            panic!()
        };
        *database = None;
        let mut output = String::new();
        append_query(&mut output, "query".into(), &query);
        assert!(output.contains(&t!("view.synthesis.shared_database_unknown")));
    }
}
