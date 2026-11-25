use culprit_core::{Category, CheckStatus, Environment, PostureSnapshot, Severity, Snapshot};

fn demo_mssql_snapshot() -> Snapshot {
    serde_json::from_str(include_str!("../../../../../fixtures/mssql-demo.json")).unwrap()
}

#[test]
fn unobserved_posture_marks_checks_insufficient_without_false_clean_verdicts() {
    let mut snap = demo_mssql_snapshot();
    snap.operations.posture = None;
    let report = crate::analyze(snap);

    for check_id in [
        "public-endpoint",
        "unencrypted-connections",
        "password-auth-enabled",
        "audit-disabled",
        "backup-retention-short",
        "integrity-check-overdue",
    ] {
        assert!(!report.findings.iter().any(|f| f.check == check_id));
        let assessment = report.assessments.iter().find(|a| a.check == check_id).expect(check_id);
        assert_eq!(
            assessment.status,
            CheckStatus::InsufficientEvidence,
            "{check_id} must not claim Assessed when unobserved"
        );
    }
}

#[test]
fn posture_checks_emit_actionable_posture_findings_with_zero_db_time_share() {
    let mut snap = demo_mssql_snapshot();
    snap.environment = Environment::Production;
    snap.operations.posture = Some(Box::new(PostureSnapshot {
        public_network_access: Some(true),
        private_endpoint_only: Some(false),
        tls_enforced: Some(false),
        min_tls_version: Some("1.0".into()),
        unencrypted_sessions: Some(4),
        password_auth_enabled: Some(true),
        auditing_enabled: Some(false),
        backup_retention_days: Some(0),
        last_full_backup_days: Some(12.0),
        last_integrity_check_days: Some(45.0),
        coverage_notes: Vec::new(),
    }));

    let report = crate::analyze(snap);
    let posture_findings: Vec<_> = report.findings.iter().filter(|f| f.category == Category::Posture).collect();
    assert_eq!(posture_findings.len(), 6);

    for f in &posture_findings {
        assert_eq!(f.impact.db_time_share, None, "{} must never claim a share of DB time", f.id);
        assert_eq!(f.impact.time_ms_per_day, None);
        assert!(f.fix.is_some(), "{} needs a fix", f.id);
        assert!(!f.verify.is_empty(), "{} needs a verification step", f.id);
    }

    let pub_ep = posture_findings.iter().find(|f| f.check == "public-endpoint").unwrap();
    assert_eq!(pub_ep.severity, Severity::High);
    let backup = posture_findings.iter().find(|f| f.check == "backup-retention-short").unwrap();
    assert_eq!(backup.severity, Severity::High);
    let integrity = posture_findings.iter().find(|f| f.check == "integrity-check-overdue").unwrap();
    assert_eq!(integrity.severity, Severity::High);
}
