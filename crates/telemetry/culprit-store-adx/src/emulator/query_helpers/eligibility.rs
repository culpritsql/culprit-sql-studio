//! Mutate only the synthetic native row input; the installed production membership/count guards stay unchanged.
use super::*;

pub(super) async fn assert_checked_cohort(emulator: &Emulator, original: &CommittedRange, asof: &str) {
    let catalog = SchemaCatalog::new().unwrap();
    let schema = catalog.get(TelemetryStream::DerivedIncidents).unwrap();
    let eligible =
        crate::eligibility::eligible(schema, culprit_evidence::analytical::semantics::EligibilityScope::Namespace);
    let native_input = "    CulpritIncidentsV1\n";
    assert_eq!(eligible.matches(native_input).count(), 1);
    let contract = serde_json::to_string(&original.manifest().contract.id().unwrap()).unwrap();
    let store = serde_json::to_string(original.store_id()).unwrap();
    for (name, input, expected) in [
        (
            "FriendlySmokeExactDuplicate",
            "    CulpritIncidentsV1 | mv-expand CulpritFixtureDuplicate=dynamic([0,1]) | project-away CulpritFixtureDuplicate\n",
            1,
        ),
        (
            "FriendlySmokeUnpublishedSiblingBatch",
            "    CulpritIncidentsV1 | mv-expand BatchId=pack_array(BatchId,\"unpublished-fixture-batch\") to typeof(string)\n",
            0,
        ),
        ("FriendlySmokeExtremeOrdinal", "    CulpritIncidentsV1 | extend RowOrdinal=long(9223372036854775807)\n", 0),
    ] {
        let command = eligible.replacen("CulpritIncidentsV1NamespaceEligible(", &format!("{name}("), 1).replacen(
            native_input,
            input,
            1,
        );
        emulator.management(true, &command);
        let call = format!("{name}({contract},{store},datetime(2019),datetime({asof}))");
        assert_eq!(count(emulator, &call).await, expected, "{name}");
    }
}
