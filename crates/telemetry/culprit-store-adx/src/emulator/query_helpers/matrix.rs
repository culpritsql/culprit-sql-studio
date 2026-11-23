//! One controlled stored-function matrix over the same unpublished fixture and unchanged public proof graph.
use super::*;

struct Case {
    label: &'static str,
    call: String,
    definitions: Vec<String>,
}
pub(super) async fn unpublished(emulator: &Emulator, commands: &[String], arguments: &str) {
    let mut outcomes = Vec::new();
    for case in cases(commands, arguments) {
        eprintln!("Owned emulator matrix begin: {}", case.label);
        let started = std::time::Instant::now();
        let installed =
            case.definitions.iter().try_for_each(|command| emulator.management_result(true, command).map(|_| ()));
        let setup_ok = installed.is_ok();
        let outcome = match installed {
            Ok(()) => query_count(emulator, &case.call).await,
            Err(error) => Err(error),
        };
        eprintln!(
            "Owned emulator matrix outcome: {}",
            json!({
                "case":case.label,"setup_ok":setup_ok,"count":outcome.as_ref().ok(),
                "error":outcome.as_ref().err().map(|error|error.code),"client_elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            })
        );
        outcomes.push((case.label, outcome));
    }
    // Keep the actual public production failure after the complete diagnostic matrix. Neither partial
    // HTTP200 nor an unavailable result can stand in for a proved zero unpublished incidents.
    let production = outcomes.iter().find(|(label, _)| *label == "production-materialize-extend").unwrap();
    assert_eq!(
        *production.1.as_ref().expect("the actual public IncidentsDuring must complete without a partial failure"),
        0
    );
    for (label, outcome) in outcomes {
        assert_eq!(outcome.expect("every controlled equivalent must return its real count"), 0, "{label}");
    }
}
async fn query_count(emulator: &Emulator, call: &str) -> Result<u64> {
    let table = emulator
        .query(QueryRequest {
            text: format!("{call} | summarize N=count()"),
            parameters: BTreeMap::new(),
            columns: vec![Column { column_name: "N".into(), column_type: "long".into() }],
            row_limit: 10_000,
            byte_limit: crate::response::MAX_RESPONSE_BYTES,
        })
        .await?;
    if table.rows.len() != 1 {
        return Err(DeliveryProblem::Integrity.error());
    }
    table.rows[0].first().and_then(Value::as_u64).ok_or_else(|| DeliveryProblem::Integrity.error())
}
fn alias(name: &str, target: &str, operator: &str) -> String {
    format!(
        ".create-or-alter function {name}(resource:string,window_from:datetime,window_until:datetime) {{
{target}(resource,window_from,window_until,now()){operator}
}}"
    )
}
fn copied(commands: &[String], original: &str, replacement: &str) -> String {
    let prefix = format!(".create-or-alter function {original}(");
    let command =
        commands.iter().find(|command| command.starts_with(&prefix)).expect("already verified generated function");
    assert_eq!(command.matches(&format!("{original}(")).count(), 1);
    command.replacen(&format!("{original}("), &format!("{replacement}("), 1)
}
fn projected(commands: &[String]) -> Vec<String> {
    let window = copied(commands, "FriendlySmokeIncidentWindow", "FriendlySmokeMatrixProjectedWindow");
    assert_eq!(window.matches("| project ResourceId,").count(), 1);
    let window = window.replacen("| project ResourceId,", "| project PublicationTimeKnown=false,ResourceId,", 1);
    let core = copied(commands, "FriendlySmokeIncidents", "FriendlySmokeMatrixProjectedCore");
    assert_eq!(core.matches("FriendlySmokeIncidentWindow(").count(), 1);
    let core = core.replacen("FriendlySmokeIncidentWindow(", "FriendlySmokeMatrixProjectedWindow(", 1);
    vec![window, core, alias("FriendlySmokeMatrixProjected", "FriendlySmokeMatrixProjectedCore", "")]
}
fn cases(commands: &[String], arguments: &str) -> Vec<Case> {
    vec![
        Case {
            label: "public-explicit-asof",
            call: format!("FriendlySmokeIncidents({arguments},now())"),
            definitions: vec![],
        },
        Case {
            label: "stored-alias-no-operator",
            call: format!("FriendlySmokeMatrixAlias({arguments})"),
            definitions: vec![alias("FriendlySmokeMatrixAlias", "FriendlySmokeIncidents", "")],
        },
        Case {
            label: "stored-alias-extend-false",
            call: format!("FriendlySmokeMatrixExtend({arguments})"),
            definitions: vec![alias(
                "FriendlySmokeMatrixExtend",
                "FriendlySmokeIncidents",
                " | extend PublicationTimeKnown=false",
            )],
        },
        Case {
            label: "production-materialize-extend",
            call: format!("FriendlySmokeIncidentsDuring({arguments})"),
            definitions: vec![],
        },
        Case {
            label: "flag-in-existing-window-project",
            call: format!("FriendlySmokeMatrixProjected({arguments})"),
            definitions: projected(commands),
        },
    ]
}
