//! Controlled post-proof interpretation rows exercise the same generated head selector, not a second algorithm.
use super::*;

pub(super) async fn assert_selection(emulator: &Emulator, commands: &[String]) {
    assert_eq!(
        columns(emulator, "FriendlySmokeHeads").await,
        columns(emulator, "FriendlySmokeHeadBasis").await,
        "head selection preserves every original output column and type"
    );
    let different_detail =
        format!("iff(DetailSha256 == \"{}\",\"{}\",\"{}\")", "f".repeat(64), "e".repeat(64), "f".repeat(64));
    for (name, input, expected) in [
        (
            "FriendlySmokeHeadDuplicate",
            " | mv-expand CulpritHeadCase=dynamic([0,1]) | project-away CulpritHeadCase".to_owned(),
            1,
        ),
        (
            "FriendlySmokeHeadLatestConflict",
            format!(
                " | mv-expand CulpritHeadCase=dynamic([0,1]) to typeof(long) | extend DetailSha256=iff(CulpritHeadCase == 1,{different_detail},DetailSha256) | project-away CulpritHeadCase"
            ),
            0,
        ),
        (
            "FriendlySmokeHeadNoOlderFallback",
            format!(
                " | mv-expand CulpritHeadCase=dynamic([0,1,2]) to typeof(long) | extend RecordedAt=iff(CulpritHeadCase == 0,RecordedAt,RecordedAt+1s),DetailSha256=iff(CulpritHeadCase == 2,{different_detail},DetailSha256) | project-away CulpritHeadCase"
            ),
            0,
        ),
        ("FriendlySmokeHeadUnknownClock", " | extend RecordedAt=datetime(null)".to_owned(), 0),
    ] {
        let head = head(commands, name, &format!("(FriendlySmokeHeadBasis(asof){input})"));
        emulator.management(true, &head);
        assert_eq!(count(emulator, &format!("{name}(now())")).await, expected, "{name}");
    }
    newer_unpinned_revision(emulator, commands).await;
}
async fn columns(emulator: &Emulator, name: &str) -> Vec<Vec<Value>> {
    emulator
        .values(
            format!("{name}(now()) | getschema | project ColumnName,ColumnType | sort by ColumnName asc"),
            BTreeMap::new(),
            vec![
                Column { column_name: "ColumnName".into(), column_type: "string".into() },
                Column { column_name: "ColumnType".into(), column_type: "string".into() },
            ],
        )
        .await
}
fn head(commands: &[String], name: &str, input: &str) -> String {
    let head = matrix::copied(commands, "FriendlySmokeHeads", name);
    assert_eq!(head.matches("FriendlySmokeHeadBasis(asof)").count(), 1);
    head.replacen("FriendlySmokeHeadBasis(asof)", input, 1)
}
async fn newer_unpinned_revision(emulator: &Emulator, commands: &[String]) {
    let mut basis = matrix::copied(commands, "FriendlySmokeHeadBasis", "FriendlySmokeHeadRawRevisionBasis");
    let original = "let Rows=materialize(OriginalRows | join kind=leftsemi CompleteResources on ResourceId);";
    assert_eq!(basis.matches(original).count(), 1);
    // Keep the real eligibility/completeness proof. Only this synthetic census's rows gain a newer raw
    // revision, whose foreign rules cannot be accepted by the configured interpretation policy.
    basis = basis.replacen(original,
        "let Rows=materialize(OriginalRows | join kind=leftsemi CompleteResources on ResourceId | mv-expand CulpritHeadRevision=dynamic([0,1]) to typeof(long) | extend Revision=Revision+todecimal(CulpritHeadRevision),RulesVersion=iff(CulpritHeadRevision == 1,\"not-the-configured-rules\",RulesVersion) | project-away CulpritHeadRevision);", 1);
    emulator.management(true, &basis);
    let head = head(commands, "FriendlySmokeHeadUnpinnedRawRevision", "FriendlySmokeHeadRawRevisionBasis(asof)");
    emulator.management(true, &head);
    assert_eq!(
        count(emulator, "FriendlySmokeHeadUnpinnedRawRevision(now())").await,
        0,
        "a newer raw revision without a configured interpretation must not revive the older head"
    );
}
