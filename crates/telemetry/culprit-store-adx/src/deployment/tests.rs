use super::*;
#[test]
fn deployment_contains_one_route_and_the_exact_runtime_mapping_for_every_analytical_table() {
    let catalog = SchemaCatalog::new().unwrap();
    let assets = DeploymentAssets::new().unwrap();
    assert_eq!(assets.schema_digest, catalog.digest());
    assert_eq!(assets.routes.len(), catalog.schemas().count() + 1);
    for schema in catalog.schemas().map(|(_, schema)| schema).chain(std::iter::once(catalog.publication())) {
        assert!(assets.commands.contains(&schema.create_table()));
        assert!(assets.commands.contains(&schema.create_mapping().unwrap()));
        let route = assets.routes.iter().find(|route| route.table == schema.table()).unwrap();
        assert_eq!(route.mapping, schema.mapping_name());
        assert_eq!(route.blob_prefix, format!("ready/{}/", schema.table()));
        assert_eq!(route.blob_suffix, ".ndjson");
        assert_eq!(route.format, "json");
    }
    assert!(assets.commands.iter().all(|command| {
        ![
            "$TABLE",
            "$PROJECTION",
            "$COLUMNS",
            "$SCHEMA",
            "$RESTORE",
            "$FUNCTION",
            "$COMPLETE",
            "$SELECTION",
            "$GROUP_KEYS",
        ]
        .iter()
        .any(|placeholder| command.contains(placeholder))
    }));
    assert!(!assets.script().contains("skipvalidation"));
    assert!(assets.script().starts_with(".execute database script with (ContinueOnErrors=false)"));
}
#[test]
fn empty_set_heads_and_revision_rules_are_deliberate_per_family_and_never_use_arrival_order() {
    let assets = DeploymentAssets::new().unwrap();
    let script = assets.script();
    assert!(script.contains("CulpritQueryRuntimeV1Rows"));
    assert!(script.contains("CulpritDeadlocksV1Rows"));
    assert!(script.contains("CulpritActivityV1Current"));
    assert!(script.contains("CulpritObjectsV1Current"));
    assert!(script.contains("tobool(SourceDescription.observation.data.coverage.complete_scope)"));
    assert!(
        script.contains("tostring(SourceDescription.observation.data.coverage.state) in (\"collected\", \"empty\")")
    );
    assert!(!script.contains("SourceDescription.observation.data.complete_scope"));
    assert!(script.contains("max(CaptureSequence)"));
    assert!(script.contains("PresentRows == ExpectedRows"));
    assert!(script.contains("Variants != 1"));
    assert!(!script.contains("ingestion_time"));
    assert!(!script.contains("arg_max"));
    // These are contract structure checks. Functional Kusto execution is a separate emulator integration gate.
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
