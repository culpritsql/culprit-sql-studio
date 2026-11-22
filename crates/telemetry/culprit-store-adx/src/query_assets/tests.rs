use super::*;
use culprit_core::ComponentVersion;
use culprit_evidence::delivery::contract::Contract;
fn binding() -> QueryHelperBinding {
    let catalog = SchemaCatalog::new().unwrap();
    let contract =
        Contract::new("pipeline".into(), "shard".into(), "warehouse".into(), catalog.digest().into()).unwrap();
    QueryHelperBinding::new(
        culprit_evidence::analytical::query_helpers::QueryHelperPolicy {
            name: "Pilot".into(),
            analyzer: ComponentVersion::package("culprit-checks", "1", Some("build-1")),
            rules_version: "rules-2".into(),
            history_from: "2019-01-01T00:00:00Z".into(),
        },
        &contract,
        &"b".repeat(64),
    )
    .unwrap()
}
#[test]
fn friendly_helpers_keep_publication_membership_and_closed_namespace_binding_for_all_three_gold_families() {
    let b = binding();
    let assets = configured(b.clone()).unwrap();
    let text = assets.commands.join("\n");
    assert!(text.contains(&b.contract_id) && text.contains(&b.recovery_store_id) && text.contains(&b.schema_digest));
    for table in ["CulpritIncidentsV1", "CulpritFindingsV1", "CulpritChangesV1"] {
        assert!(text.contains(&format!("{table}NamespaceEligible")));
        assert!(text.contains(&format!("{table}NamespaceRows")));
    }
    for guard in [
        "PublishedManifest",
        "PublishedSchema",
        "PublishedEpoch",
        "PublishedSequence",
        "PublishedReferencesComplete",
        "ExpectedRows",
        "RowOrdinal",
    ] {
        assert!(text.contains(guard), "{guard}");
    }
    for forbidden in [
        "skipvalidation",
        "ingestion_time()",
        "COUNT(DISTINCT Query",
        "arg_max(",
        "argMax(",
        "CREATE TABLE",
        ".create table",
    ] {
        assert!(!text.contains(forbidden), "{forbidden}");
    }
    assert!(!text.contains('$'));
    assert!(assets.script.starts_with(".execute database script with (ContinueOnErrors=false) <|\n"));
    assert!(assets.script.ends_with('\n'));
    assert!(assets.commands.iter().all(|command| assets.script.contains(command)));
    assert_eq!(assets.examples.len(), 8);
    assert!(
        assets
            .examples
            .iter()
            .all(|example| !example.query.contains(&b.contract_id) && !example.query.contains(&b.recovery_store_id))
    );
    let mut foreign = b;
    foreign.schema_digest = "f".repeat(64);
    assert!(configured(foreign).is_err());
}
#[test]
fn episode_heads_precede_window_and_rules_selection_and_alerts_join_first_original_observation_to_latest_head() {
    let assets = configured(binding()).unwrap();
    let text = assets.commands.join("\n");
    let basis = text.find("PilotHeadBasis").unwrap();
    let heads = text.find("PilotHeads").unwrap();
    let incidents = text.find("PilotIncidents").unwrap();
    assert!(basis < heads && heads < incidents);
    assert!(text[basis..heads].contains("max(Revision)"));
    assert!(
        !text[basis..heads].contains("rules-2"),
        "newer resolved revisions cannot be hidden by pinning an older interpreter first"
    );
    let alert = text.split("PilotNewIncidents").nth(1).unwrap().split("PilotStatus").next().unwrap();
    assert!(
        alert.contains("FirstRecordedObservation")
            && alert.contains("OriginalRecordedAt")
            && alert.contains("PilotHeads")
    );
    assert!(alert.contains("active") && alert.contains("InputReadComplete"));
    assert!(!alert.contains("NativeFirstSeenAt") && !alert.contains("NativeLastSeenAt"));
    assert!(
        text.contains("OutputRowsComplete")
            && text.contains("IncidentRowsComplete")
            && text.contains("incomplete-output")
    );
    assert!(
        text.contains("ambiguous-label")
            && text.contains("unknown-label")
            && text.contains("head-interpretation-unavailable")
            && text.contains("stale")
    );
    for field in [
        "VictimSummary",
        "AffectedQueries",
        "CulpritQueries",
        "CandidateQueries",
        "Mechanism",
        "DeveloperAction",
        "Verification",
        "Caller",
    ] {
        assert!(text.contains(field));
    }
    assert!(text.contains("Header") && text.contains("display") && text.contains("connection_alias"));
    assert!(!text.contains("target.identity.database"));
}
#[test]
fn configured_rules_are_literal_values_and_never_identifiers_or_a_lexical_latest_policy() {
    let mut b = binding();
    b.policy.rules_version = "rules-2'\"\\value".into();
    let assets = configured(b).unwrap();
    let text = assets.commands.join("\n");
    assert!(!text.contains("max(RulesVersion)") && !text.contains("max(Analyzer)"));
    assert!(text.contains("rules-2"));
    let mut b = binding();
    b.policy.name = "Pilot);DROP".into();
    assert!(configured(b).is_err());
}

#[test]
fn namespace_membership_retains_the_published_resource_for_every_gold_family_while_scalar_scope_stays_local() {
    let assets = configured(binding()).unwrap();
    let catalog = SchemaCatalog::new().unwrap();
    for stream in NAMESPACE_QUERY_STREAMS {
        let schema = catalog.get(stream).unwrap();
        let namespace = assets
            .commands
            .iter()
            .find(|command| {
                command.starts_with(&format!(".create-or-alter function {}NamespaceEligible(", schema.table()))
            })
            .unwrap();
        assert!(namespace.contains("PublishedResource=ResourceId"));
        assert!(namespace.contains("or ResourceId != PublishedResource"));
        let scalar = eligibility::eligible(schema, EligibilityScope::Resource);
        assert!(scalar.contains("ResourceId == resource"));
        assert!(!scalar.contains("PublishedResource"));
        assert!(!scalar.contains('$'));
    }
}

#[test]
fn first_observation_consistency_precedes_its_window_and_is_shared_by_heads_alerts_and_status() {
    let assets = configured(binding()).unwrap();
    let definitions = &assets.commands;
    let original = definitions
        .iter()
        .find(|command| {
            command.contains("VIEW PilotConsistentOriginals ") || command.contains("function PilotConsistentOriginals(")
        })
        .unwrap();
    for field in ["RevisionId", "OriginalRecordedAt", "Lifecycle", "OriginalReference"] {
        assert!(original.contains(field), "{field}");
    }
    assert!(!original.contains("rules-2") && !original.contains("window_from") && !original.contains("{from:"));
    let head = definitions
        .iter()
        .find(|command| command.contains("VIEW PilotHeadBasis ") || command.contains("function PilotHeadBasis("))
        .unwrap();
    assert!(head.contains("max(Revision)") && head.contains("PilotConsistentOriginals("));
    let alert = definitions
        .iter()
        .find(|command| command.contains("VIEW PilotNewIncidents ") || command.contains("function PilotNewIncidents("))
        .unwrap();
    assert!(alert.contains("PilotConsistentOriginals("));
    let status = definitions
        .iter()
        .find(|command| command.contains("VIEW PilotStatus ") || command.contains("function PilotStatus("))
        .unwrap();
    assert!(status.contains("PilotConsistentOriginals("));
    assert!(status.contains("first-observation-unavailable") && status.contains("UnavailableFirstObservations"));
}
