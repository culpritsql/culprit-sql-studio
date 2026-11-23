use super::*;
use crate::{Cli, Command};
use clap::Parser;
#[test]
fn ordinary_archive_window_accepts_only_resource_time_and_config_and_makes_full_workload_explicit() {
    let parse = |extra: &[&str]| {
        let mut args = vec![
            "culprit",
            "window",
            "orders",
            "--from",
            "2020-01-01T06:00:00Z",
            "--until",
            "2020-01-01T08:00:00Z",
            "--config",
            "archive.json",
        ];
        args.extend(extra);
        Cli::try_parse_from(args)
    };
    let Command::Window(args) = parse(&[]).unwrap().command else { panic!() };
    assert_eq!(args.resource, "orders");
    assert!(!args.workload);
    assert!(args.cursor.is_none());
    let Command::Window(args) = parse(&["--workload"]).unwrap().command else { panic!() };
    assert!(args.workload);
    for extra in [&["--range", "manual-hash"][..], &["--connection", "production"][..], &["--password", "private"][..]]
    {
        assert!(parse(extra).is_err());
    }
    assert!(parse(&["--cursor", "{}", "--workload"]).is_err());
}
#[test]
fn copied_continuation_requires_fixed_cutoff_and_never_accepts_an_unvalidated_locator() {
    assert!(parse_cursor("{}").is_err());
    assert!(parse_cursor(&"x".repeat(4097)).is_err());
    let cursor = PublishedResourceCursor {
        selection_key: culprit_core::telemetry::digest(b"selection"),
        position: culprit_core::PublishedResourcePosition {
            resource_id: culprit_core::telemetry::digest(b"resource"),
            source_id: culprit_core::telemetry::digest(b"source"),
            range_id: culprit_core::telemetry::digest(b"range"),
        },
    };
    assert_eq!(parse_cursor(&serde_json::to_string(&cursor).unwrap()).unwrap(), cursor);
}

#[test]
fn human_window_preserves_unknown_coverage_qualified_roles_escaping_and_the_bound_continuation() {
    use culprit_core::*;
    let incidents = crate::incident_cmd::tests::incident_report();
    let mut report = WarehouseWindowReport {
        request: WarehouseWindowQuery {
            resource: PublishedResourceSelector {
                namespace_id: telemetry::digest(b"namespace"),
                database: incidents.database.clone(),
            },
            window: PublishedResourceWindow { from: incidents.from.clone(), until: incidents.until.clone() },
            as_of: incidents.recorded_before.clone(),
            view: WarehouseWindowView::PublishedAnswers,
            page_rows: 1,
            after: None,
        },
        discovery_complete: false,
        originals: vec![],
        workload: WindowWorkload::NotRequested,
        next: Some(PublishedResourceCursor {
            selection_key: telemetry::digest(b"selection"),
            position: PublishedResourcePosition {
                resource_id: telemetry::digest(b"resource"),
                source_id: telemetry::digest(b"source"),
                range_id: telemetry::digest(b"range"),
            },
        }),
        archive_work: incidents.archive_work.clone(),
        incidents,
        incident_limitations: vec![],
        published: vec![],
        gaps: vec![WarehouseWindowGap::MorePublishedAnalyses, WarehouseWindowGap::NoPublishedAnalyses],
        qualifications: vec![],
    };
    report.prepare_for_reader();
    let human = observations::window(&report).unwrap();
    for reason in [
        t!("window.reader.no_answers"),
        t!("window.reader.more"),
        t!("window.reader.workload_not_requested"),
        t!("incident.cli.culprit_unknown"),
    ] {
        assert!(human.contains(&reason), "{reason}");
    }
    assert!(!human.contains('\u{1b}'));
    assert!(human.contains("\\u{1b}"));
    assert!(human.contains("--as-of"));
    assert!(human.contains("--cursor"));
    assert!(
        !human.contains(&report.incidents.incidents[0].revision_reference.manifest_sha256),
        "private receipt is not human identity"
    );
}

#[test]
fn artifact_details_require_explicit_whole_recovery_opt_in_and_no_native_route_or_manual_range() {
    let parse = |extra: &[&str]| {
        let mut args = vec![
            "culprit",
            "window",
            "orders",
            "--from",
            "2020-01-01T06:00:00Z",
            "--until",
            "2020-01-01T08:00:00Z",
            "--config",
            "archive.json",
        ];
        args.extend(extra);
        Cli::try_parse_from(args)
    };
    assert!(parse(&["--detail", "1"]).is_err());
    assert!(parse(&["--full-payloads"]).is_err());
    let Command::Window(args) = parse(&["--detail", "1", "--full-payloads"]).unwrap().command else { panic!() };
    assert_eq!(args.detail, Some(1));
    assert!(args.full_payloads);
    assert!(parse(&["--detail", "1", "--full-payloads", "--range", "manual"]).is_err());
}
