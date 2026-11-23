//! Exercise the generated status census with a real published resource and controlled latest-header ties.
use super::*;

pub(super) async fn assert_census(emulator: &Emulator, commands: &[String], label: &str, asof: &str) {
    let columns = vec![
        Column { column_name: "State".into(), column_type: "string".into() },
        Column { column_name: "UnselectedIncidentHeads".into(), column_type: "long".into() },
        Column { column_name: "UnavailableFirstObservations".into(), column_type: "long".into() },
    ];
    let row = vec![json!("observed"), json!(0), json!(0)];
    let status = emulator.values(
        format!("FriendlySmokeStatus({label},datetime({asof}),3650d) | project State,UnselectedIncidentHeads,UnavailableFirstObservations"),
        BTreeMap::new(), columns.clone(),
    ).await;
    assert_eq!(
        status,
        vec![row.clone()],
        "one complete published head with its first original has no missing status census"
    );

    let status = matrix::copied(commands, "FriendlySmokeStatusRows", "FriendlySmokeStatusTiedHeaders");
    let input = "let Headers=materialize(FriendlySmokeCompleteness(asof));";
    assert_eq!(status.matches(input).count(), 1);
    // Duplicate only the already-proved header input. The generated Times/Latest and census owners
    // must preserve both latest ties rather than replacing them with an arbitrary header.
    let status = status.replacen(input,
        "let Headers=materialize(FriendlySmokeCompleteness(asof) | mv-expand CulpritHeaderTie=dynamic([0,1]) | project-away CulpritHeaderTie);", 1);
    emulator.management(true, &status);
    let tied = emulator.values(
        format!("FriendlySmokeStatusTiedHeaders(datetime({asof}),3650d) | project State,UnselectedIncidentHeads,UnavailableFirstObservations"),
        BTreeMap::new(), columns,
    ).await;
    assert_eq!(tied, vec![row.clone(), row], "both latest headers keep the same exact selected/missing-first census");
}
