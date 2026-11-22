use super::*;
use culprit_core::{
    IncidentDatabaseSelector, PublishedDatabaseResource, PublishedResourcePosition, PublishedResourceSelector,
    collected_analysis::CollectedEvidenceMetrics, telemetry::*,
};
fn report(n: usize) -> PublishedResources {
    PublishedResources {
        as_of: "2026-11-22T12:00:00Z".into(),
        window: None,
        resources: (0..n)
            .map(|i| PublishedDatabaseResource {
                position: PublishedResourcePosition {
                    resource_id: format!("{i:064x}"),
                    source_id: "b".repeat(64),
                    range_id: "c".repeat(64),
                },
                logical_source_id: "d".repeat(64),
                selector: PublishedResourceSelector {
                    namespace_id: "e".repeat(64),
                    database: IncidentDatabaseSelector::from_key(&"f".repeat(64)).unwrap(),
                },
                engine: culprit_core::Engine::Postgres,
                database_name: "<script>db\u{1b}[2J</script>".into(),
                connection_alias: None,
                source_name: "native\nsource".into(),
                collection: TelemetryCollection::Activity,
                continuity: SourceContinuity::Unverified,
                collected_from: "2026-11-22T10:00:00Z".into(),
                collected_until: "2026-11-22T10:00:01Z".into(),
                native_time: PublishedResourceTime::Unknown,
                coverage: PublishedResourceCoverage::Activity {
                    coverage: StateSourceCoverage::observed("native".into(), 1, false),
                },
            })
            .collect(),
        next: None,
        discovery_complete: true,
        gaps: vec![],
        candidate_rows: n as u32,
        recovery_metadata_reads: 1,
        archive_work: CollectedEvidenceMetrics {
            selected_rows: 0,
            queries: 1,
            pages: 1,
            returned_rows: 0,
            response_bytes: 2,
            proof_bytes: 3,
            recovery_objects: 0,
            recovery_bytes: 0,
        },
    }
}
#[test]
fn ordinary_resources_command_needs_only_archive_config_and_keeps_window_pairs_explicit() {
    use clap::Parser;
    assert!(crate::Cli::try_parse_from(["culprit", "resources", "--config", "archive.json"]).is_ok());
    assert!(
        crate::Cli::try_parse_from([
            "culprit",
            "resources",
            "--config",
            "archive.json",
            "--from",
            "2026-11-22T10:00:00Z"
        ])
        .is_err()
    );
    assert!(
        crate::Cli::try_parse_from(["culprit", "resources", "--config", "archive.json", "--page-rows", "33"]).is_err()
    );
    assert!(
        parse_cursor(
            r#"{"selection_key":"label","position":{"resource_id":"name","source_id":"name","range_id":"name"}}"#
        )
        .is_err()
    );
}
#[test]
fn resources_human_output_escapes_and_bounds_labels_and_never_displays_private_proof_keys() {
    let mut report = report(1);
    report.resources[0].database_name.push_str(&"x".repeat(10_000));
    let text = human(&report);
    assert!(!text.contains('\u{1b}'));
    assert!(!text.contains("native\nsource"));
    assert!(text.len() < 2000);
    for secret in [
        &report.resources[0].position.resource_id,
        &report.resources[0].position.source_id,
        &report.resources[0].selector.namespace_id,
    ] {
        assert!(!text.contains(secret));
    }
    assert!(text.contains(&t!("resources.cli.native_partial")) && text.contains(&t!("resources.cli.clock_unknown")));
    let json = bounded_output::json(&report, 256 * 1024).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert!(value["resources"][0]["connection_alias"].is_null());
    assert_eq!(value["resources"][0]["selector"]["namespace_id"], report.resources[0].selector.namespace_id);
}
#[test]
fn resource_summary_plural_and_unavailable_state_use_catalogs_for_zero_one_many() {
    culprit_core::i18n::scope("en", || {
        assert!(human(&report(0)).contains("0 authenticated observations"));
        assert!(human(&report(1)).contains("1 authenticated observation"));
        assert!(human(&report(2)).contains("2 authenticated observations"));
    });
    let mut missing = report(0);
    missing.discovery_complete = false;
    missing.gaps.push(culprit_core::PublishedResourceGap::OriginalUnavailable);
    assert!(human(&missing).contains(&t!("resources.cli.unavailable")));
}
