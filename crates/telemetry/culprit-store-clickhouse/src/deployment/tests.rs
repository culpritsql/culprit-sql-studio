use super::*;
#[test]
fn physical_storage_and_canonical_views_share_one_complete_contract() {
    let assets = DeploymentAssets::new().unwrap();
    let script = assets.script();
    assert_eq!(
        script.matches("CREATE TABLE IF NOT EXISTS").count(),
        SchemaCatalog::new().unwrap().schemas().count() + 1
    );
    assert!(script.contains("Nullable(String)"));
    assert!(!script.contains("HasDataset"));
    assert!(script.contains("DateTime64(9, 'UTC')"));
    assert!(script.contains("Decimal(38, 0)"));
    assert!(script.contains("ORDER BY (RangeId, BatchId, RowOrdinal)"));
    assert!(script.contains("PROJECTION ByResourceTime"));
    for name in [
        "CulpritPublishedV1",
        "CulpritActivityHeadsV1",
        "CulpritSnapshotHeadsV1",
        "CulpritQueryRuntimeV1Rows",
        "CulpritActivityV1Current",
    ] {
        assert!(script.contains(name), "{name}");
    }
    assert!(script.contains(&assets.schema_digest));
    assert!(script.contains("R.ManifestSha256!=M.PublishedManifest"));
    assert!(script.contains("HAVING count()!=1"));
    assert!(script.contains("JSONExtractBool(Coverage,'complete_scope')"));
    assert!(script.contains("JSONExtractBool(SourceDescription,'observation','data','coverage','complete_scope')"));
    assert!(script.contains(
        "JSONExtractString(SourceDescription,'observation','data','coverage','state') IN ('collected','empty')"
    ));
    assert!(!script.contains("JSONExtractBool(SourceDescription,'observation','data','complete_scope')"));
    assert!(!script.contains("$TABLE"));
    assert!(!script.contains("$ARGS"));
    assert!(!script.contains("DROP TABLE"));
    assert!(
        DeploymentAssets::with_layout(TableLayout::Replicated)
            .unwrap()
            .script()
            .contains("ENGINE = ReplicatedMergeTree")
    );
}

#[test]
fn gold_outputs_are_deployed_through_the_same_catalog_without_native_current_or_detector_rules() {
    let assets = DeploymentAssets::new().unwrap();
    let script = assets.script();
    let catalog = SchemaCatalog::new().unwrap();
    for (stream, name) in [
        (culprit_core::telemetry::TelemetryStream::DerivedIncidents, "CulpritIncidentsV1"),
        (culprit_core::telemetry::TelemetryStream::DerivedFindings, "CulpritFindingsV1"),
        (culprit_core::telemetry::TelemetryStream::DerivedChanges, "CulpritChangesV1"),
    ] {
        let schema = catalog.get(stream).unwrap();
        assert_eq!(schema.table(), name);
        assert!(script.contains(&format!("{name}Rows")));
        assert!(script.contains(&format!("{name}Eligible")));
        assert!(
            !script.contains(&format!("{name}Current")),
            "native lifecycle is in the row, not synthesized by KQL/SQL"
        );
    }
    for field in [
        "VictimSummary",
        "AffectedQueries",
        "CulpritQueries",
        "CandidateQueries",
        "AttributionStatus",
        "Mechanism",
        "DeveloperAction",
        "Verification",
        "Caller",
        "OriginalReference",
        "InputReferences",
    ] {
        assert!(script.contains(field), "{field}");
    }
}

#[test]
fn collector_connections_use_one_catalog_table_and_canonical_append_only_eligibility_without_native_heads() {
    let stream = culprit_core::telemetry::TelemetryStream::CollectorConnections;
    let catalog = SchemaCatalog::new().unwrap();
    let schema = catalog.get(stream).unwrap();
    let assets = DeploymentAssets::new().unwrap();
    let semantics = stream_semantics(stream).unwrap();
    assert_eq!(schema.table(), "CulpritCollectorConnectionsV1");
    assert!(!semantics.latest_revision);
    assert!(semantics.current.is_none());
    assert!(assets.commands.contains(&eligibility::eligible(schema, EligibilityScope::Resource)));
    assert!(assets.commands.contains(&eligibility::rows(schema, EligibilityScope::Resource, false)));
    let script = assets.script();
    assert!(script.contains("CulpritCollectorConnectionsV1Eligible"));
    assert!(script.contains("CulpritCollectorConnectionsV1Rows"));
    assert!(!script.contains("CulpritCollectorConnectionsV1Current"));
    for name in [
        "ConnectionId",
        "RouteRevision",
        "ConnectionName",
        "OriginalAttempt",
        "OriginalStatus",
        "OriginalStop",
        "OriginalEvidence",
        "CollectorOutcome",
        "ConnectionOutcome",
        "SourceWork",
        "CollectorStartedAt",
        "CollectorFinishedAt",
        "CollectorElapsedMs",
        "OrderedCollectorClocks",
        "ConnectionPhases",
        "OriginalSourceQueries",
        "NativeTargetLink",
        "Details",
    ] {
        assert!(schema.columns().iter().any(|column| column.name == name), "{name}");
    }
}
