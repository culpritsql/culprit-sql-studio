//! Compare the actual published/nonempty boundary through the unchanged public proof graph.
use super::*;

struct Case {
    label: &'static str,
    call: String,
    definitions: Vec<String>,
}
pub(super) struct Window<'a> {
    pub resource: &'a str,
    pub from: &'a str,
    pub until: &'a str,
}
impl Window<'_> {
    fn arguments(&self) -> String {
        format!("{},datetime({}),datetime({})", self.resource, self.from, self.until)
    }
}
pub(super) async fn published(emulator: &Emulator, commands: &[String], window: Window<'_>) {
    let mut outcomes = collect(emulator, ladder(&window)).await;
    outcomes.extend(collect(emulator, cases(commands, &window.arguments())).await);
    // Keep the actual public production failure after the entire published/nonempty matrix. Neither
    // partial HTTP200 nor an unavailable result can stand in for the one genuinely published incident.
    let production = outcomes.iter().find(|(label, _)| *label == "production-materialize-extend").unwrap();
    assert_eq!(
        *production.1.as_ref().expect("the actual public IncidentsDuring must complete without a partial failure"),
        1
    );
    for (label, outcome) in outcomes {
        assert_eq!(
            outcome.expect("every published stage and controlled equivalent must return its real count"),
            1,
            "{label}"
        );
    }
}
async fn collect(emulator: &Emulator, cases: Vec<Case>) -> Vec<(&'static str, Result<u64>)> {
    let mut outcomes = Vec::new();
    for case in cases {
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
                "case":case.label,"fixture_state":"published","setup_ok":setup_ok,"count":outcome.as_ref().ok(),
                "error":outcome.as_ref().err().map(|error|error.code),"client_elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            })
        );
        outcomes.push((case.label, outcome));
    }
    outcomes
}
fn ladder(window: &Window<'_>) -> Vec<Case> {
    [
        ("stage-published", "FriendlySmokePublished(now())".into()),
        ("stage-incident-rows", "FriendlySmokeIncidentRows(now())".into()),
        ("stage-incident-completeness", "FriendlySmokeIncidentCompleteness(now())".into()),
        ("stage-head-basis", "FriendlySmokeHeadBasis(now())".into()),
        ("stage-heads", "FriendlySmokeHeads(now())".into()),
        ("stage-resource-selection", format!("FriendlySmokeResourceSelection({},now())", window.resource)),
        (
            "stage-incident-window",
            format!("FriendlySmokeIncidentWindow(datetime({}),datetime({}),now())", window.from, window.until),
        ),
    ]
    .into_iter()
    .map(|(label, call)| Case { label, call, definitions: vec![] })
    .collect()
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
pub(super) fn copied(commands: &[String], original: &str, replacement: &str) -> String {
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
