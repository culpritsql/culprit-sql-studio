//! Autonomous warehouse triage and `lint-sql` safety gate (`culprit agent triage-warehouse`).

use crate::{
    SeverityArg,
    output::{Format, Out},
};
use clap::Args;
use culprit_core::{
    Error, PublishedResourceWindow, Result, Severity, WarehouseFleetFilter, WarehouseFleetRequest,
    WarehouseWindowQuery, WarehouseWindowView, format, redact, t,
};
use culprit_engine::{
    WarehouseFleetReader, WarehouseIncidentReader, Workspace,
    evidence::{EvidenceConfig, EvidenceRuntime},
};
use culprit_read::cancellation::CollectionCancellation;
use serde::Serialize;
use std::path::{Path, PathBuf};

const EXIT_LINT_BLOCKED: u8 = 3;

#[derive(Args)]
pub struct TriageWarehouseArgs {
    #[arg(long, value_name = "FILE", help = culprit_core::t!("agent.triage.warehouse_config"))]
    pub warehouse_evidence: PathBuf,
    #[arg(long, value_name = "NAME", help = culprit_core::t!("agent.triage.resource"))]
    pub resource: Option<String>,
    #[arg(long, value_name = "ENV", help = culprit_core::t!("agent.triage.environment"))]
    pub environment: Option<String>,
    #[arg(long, value_parser = crate::parse_window_arg, default_value = "24h", help = culprit_core::t!("agent.triage.window"))]
    pub window: u32,
    #[arg(long, value_name = "FILE", help = culprit_core::t!("agent.triage.report"))]
    pub report: Option<PathBuf>,
    #[arg(long, value_name = "FILE", help = culprit_core::t!("agent.triage.proposed_sql"))]
    pub proposed_sql: Option<PathBuf>,
    #[arg(long, value_enum, default_value = "high", help = culprit_core::t!("agent.triage.fail_on"))]
    pub fail_on: SeverityArg,
}

#[derive(Clone, Debug, Serialize)]
pub struct TriageFindingBrief {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub summary: String,
    pub source_locations: Vec<String>,
    pub fix_sql: Option<String>,
    pub verify_command: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TriageLintGate {
    pub passed: bool,
    pub threshold: Severity,
    pub findings_count: usize,
    pub blocking_titles: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WarehouseTriageReport {
    pub resource_alias: String,
    pub database_name: String,
    pub engine: culprit_core::Engine,
    pub window: PublishedResourceWindow,
    pub active_incidents: u32,
    pub high_findings: u32,
    pub recent_regressions: u32,
    pub findings: Vec<TriageFindingBrief>,
    pub lint_gate: TriageLintGate,
    pub pr_markdown: String,
}

/// Extract Marginalia / SqlCommenter `file='...'` or `line='...'` or `file:line` comment provenance tags.
pub fn extract_source_locations(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    for segment in sql.split("/*").skip(1) {
        let Some((comment, _)) = segment.split_once("*/") else { continue };
        let mut file_val: Option<String> = None;
        let mut line_val: Option<String> = None;
        for token in comment.split([',', ' ', '\t', '\n']) {
            let token = token.trim();
            if let Some((k, v)) = token.split_once(['=', ':']) {
                let key = k.trim().to_ascii_lowercase();
                let val = v.trim_matches(['\'', '"', '`']).trim();
                if val.is_empty() || val.len() > 256 || val.chars().any(char::is_control) {
                    continue;
                }
                match key.as_str() {
                    "file" | "source" | "filepath" => file_val = Some(val.to_string()),
                    "line" => line_val = Some(val.to_string()),
                    _ => {}
                }
            }
        }
        if let Some(f) = file_val {
            let loc = match line_val {
                Some(l) => format!("{f}:{l}"),
                None => f,
            };
            if !found.contains(&loc) {
                found.push(loc);
            }
        }
    }
    found
}

pub struct TriageEvaluationInput<'a> {
    pub alias: &'a str,
    pub database_name: &'a str,
    pub engine: culprit_core::Engine,
    pub window: PublishedResourceWindow,
    pub active_incidents: u32,
    pub high_findings: u32,
    pub recent_regressions: u32,
    pub findings: Vec<(culprit_core::Finding, Option<String>)>,
    pub snapshot: &'a culprit_core::Snapshot,
    pub proposed_sql: Option<&'a str>,
    pub fail_on: Severity,
}

pub fn evaluate_triage_brief(input: TriageEvaluationInput<'_>) -> WarehouseTriageReport {
    let TriageEvaluationInput {
        alias,
        database_name,
        engine,
        window,
        active_incidents,
        high_findings,
        recent_regressions,
        findings,
        snapshot,
        proposed_sql,
        fail_on,
    } = input;
    let mut briefs = Vec::new();
    let mut sql_candidates = Vec::new();

    if let Some(custom_sql) = proposed_sql {
        sql_candidates.push(custom_sql.to_string());
    }

    for (finding, raw_query) in findings {
        if !finding.is_actionable() || finding.inactive {
            continue;
        }
        let source_locations = raw_query.as_deref().map(extract_source_locations).unwrap_or_default();
        let fix_sql = finding.fix.as_ref().and_then(|fix| fix.sql.clone());
        if proposed_sql.is_none()
            && let Some(sql) = &fix_sql
        {
            sql_candidates.push(sql.clone());
        }
        let verify_command = format!("culprit verify {alias} --finding '{}'", finding.id);
        briefs.push(TriageFindingBrief {
            id: finding.id,
            severity: finding.severity,
            title: finding.title,
            summary: finding.summary,
            source_locations,
            fix_sql,
            verify_command,
        });
    }

    let mut lint_findings = Vec::new();
    for candidate in &sql_candidates {
        lint_findings.extend(culprit_engine::lint_sql(snapshot, candidate));
    }
    let blocking_titles: Vec<String> =
        lint_findings.iter().filter(|f| f.severity >= fail_on).map(|f| f.title.clone()).collect();
    let lint_gate = TriageLintGate {
        passed: blocking_titles.is_empty(),
        threshold: fail_on,
        findings_count: lint_findings.len(),
        blocking_titles,
    };

    let pr_markdown = render_pr_markdown(alias, database_name, engine, &window, &briefs, &lint_gate, proposed_sql);

    WarehouseTriageReport {
        resource_alias: alias.into(),
        database_name: database_name.into(),
        engine,
        window,
        active_incidents,
        high_findings,
        recent_regressions,
        findings: briefs,
        lint_gate,
        pr_markdown,
    }
}

fn render_pr_markdown(
    alias: &str,
    database_name: &str,
    engine: culprit_core::Engine,
    window: &PublishedResourceWindow,
    findings: &[TriageFindingBrief],
    gate: &TriageLintGate,
    proposed_sql: Option<&str>,
) -> String {
    let mut lines = vec![
        format!("## Culprit Warehouse Triage: `{alias}` ({})", engine.label()),
        String::new(),
        format!("- **Database:** `{database_name}`"),
        format!("- **Evaluated window:** `{}` → `{}`", window.from, window.until),
        format!(
            "- **Migration `lint-sql` gate (`>= {:?}`):** {}",
            gate.threshold,
            if gate.passed { "PASSED" } else { "BLOCKED" }
        ),
        String::new(),
    ];
    for f in findings {
        lines.push(format!("### `{}` — {}", f.id, f.title));
        lines.push(f.summary.clone());
        if !f.source_locations.is_empty() {
            lines.push(format!("- **Source provenance:** `{}`", f.source_locations.join("`, `")));
        }
        if let Some(sql) = &f.fix_sql {
            let redacted = redact::redact_sql(sql, engine);
            lines.push(format!("```sql\n{redacted}\n```"));
        }
        lines.push(format!("- **Post-deploy verification:** `{}`", f.verify_command));
        lines.push(String::new());
    }
    if let Some(sql) = proposed_sql {
        let redacted = redact::redact_sql(sql, engine);
        lines.push("### Proposed Migration SQL".into());
        lines.push(format!("```sql\n{redacted}\n```"));
        lines.push(String::new());
    }
    if !gate.blocking_titles.is_empty() {
        lines.push("### Blocked DDL Hazards (`culprit lint-sql`)".into());
        for hazard in &gate.blocking_titles {
            lines.push(format!("- {hazard}"));
        }
    }
    lines.join("\n")
}

fn load_or_empty_snapshot(
    ws: &Workspace,
    path: Option<&Path>,
    engine: culprit_core::Engine,
    db: &str,
) -> Result<culprit_core::Snapshot> {
    if let Some(p) = path {
        return Ok(ws.analyze_file(p)?.snapshot);
    }
    Ok(culprit_core::Snapshot {
        operations: Default::default(),
        format: culprit_core::SNAPSHOT_FORMAT,
        id: "offline-triage".into(),
        connection: db.into(),
        environment: culprit_core::Environment::Production,
        taken_at: chrono::Utc::now().to_rfc3339(),
        duration_ms: 0.0,
        server: culprit_core::ServerInfo {
            engine,
            product: engine.label().into(),
            version: String::new(),
            edition: None,
            cloud: None,
            platform: None,
            database: db.into(),
            database_size_bytes: None,
            started_at: None,
            read_only_session: true,
            capabilities: Vec::new(),
            missing: Vec::new(),
        },
        runtime_populations: None,
        workload: None,
        tables: Vec::new(),
        indexes: Vec::new(),
        columns: Vec::new(),
        foreign_keys: Vec::new(),
        missing_indexes: Vec::new(),
        statistics: Vec::new(),
        waits: Vec::new(),
        resources: Vec::new(),
        settings: Vec::new(),
        notes: Vec::new(),
        pressure: None,
        options: None,
        query_families: Vec::new(),
        plan_cache: None,
        tuning: None,
        deadlocks: None,
        constraints: Vec::new(),
        histograms: None,
        escalations_before: None,
        compilations_before: None,
        contention_before: None,
        evictions_before: None,
        sizes_before: None,
        sources: Vec::new(),
        identity: None,
    })
}

pub async fn run(ws: &Workspace, args: TriageWarehouseArgs, out: &Out) -> Result<u8> {
    let config = EvidenceConfig::from_file(&args.warehouse_evidence)?;
    let service = EvidenceRuntime::default().evidence(&config)?;
    let now = chrono::Utc::now();
    let window = PublishedResourceWindow {
        from: (now - chrono::Duration::minutes(i64::from(args.window))).to_rfc3339(),
        until: now.to_rfc3339(),
    };
    let as_of = now.to_rfc3339();
    let cancellation = CollectionCancellation::default();

    let proposed_text = match &args.proposed_sql {
        Some(path) => {
            Some(std::fs::read_to_string(path).map_err(|_| Error::invalid(t!("agent.triage.invalid_sql_file")))?)
        }
        None => None,
    };

    let fleet_req = WarehouseFleetRequest {
        as_of: as_of.clone(),
        window: Some(window.clone()),
        filter: WarehouseFleetFilter {
            environment: args.environment.clone(),
            attention_only: args.resource.is_none(),
            ..Default::default()
        },
        ..Default::default()
    };
    let fleet_report = crate::collection_shutdown::run(
        &cancellation,
        WarehouseFleetReader::fleet(&service, &fleet_req, &cancellation),
    )
    .await?;
    let selected = fleet_report
        .rows
        .iter()
        .find(|row| match &args.resource {
            Some(target) => row.connection_alias.as_deref() == Some(target.as_str()) || &row.database_name == target,
            None => row.needs_attention(),
        })
        .ok_or_else(|| Error::not_found(t!("agent.triage.nothing_needs_attention")))?;

    let win_req = WarehouseWindowQuery {
        resource: selected.selector.clone(),
        window: window.clone(),
        as_of,
        view: WarehouseWindowView::PublishedAnswers,
        page_rows: 4,
        after: None,
    };
    let win_report = crate::collection_shutdown::run(
        &cancellation,
        WarehouseIncidentReader::window(&service, &win_req, &cancellation),
    )
    .await?;

    let mut extracted = Vec::new();
    for analysis in &win_report.published {
        for published_finding in &analysis.findings {
            let sql = published_finding.finding.query.as_ref().and_then(|q| match &q.sql {
                culprit_core::telemetry::DefinitionPayload::Available { text, .. } => Some(text.clone()),
                _ => None,
            });
            extracted.push((published_finding.finding.finding.clone(), sql));
        }
    }

    let alias = selected.connection_alias.as_deref().unwrap_or(&selected.database_name);
    let snapshot = load_or_empty_snapshot(ws, args.report.as_deref(), selected.engine, &selected.database_name)?;
    let triage = evaluate_triage_brief(TriageEvaluationInput {
        alias,
        database_name: &selected.database_name,
        engine: selected.engine,
        window,
        active_incidents: selected.active_incidents,
        high_findings: selected.high_findings,
        recent_regressions: selected.recent_regressions,
        findings: extracted,
        snapshot: &snapshot,
        proposed_sql: proposed_text.as_deref(),
        fail_on: Severity::from(args.fail_on),
    });

    match out.format() {
        Format::Json => out.value(&serde_json::to_value(&triage).map_err(|e| Error::internal(e.to_string()))?),
        Format::Md | Format::Text => println!("{}", triage.pr_markdown),
        Format::Sarif => return Err(Error::invalid(t!("cli.no_sarif"))),
    }
    if !triage.lint_gate.passed {
        eprintln!(
            "{}",
            t!("agent.triage.lint_blocked", count = format::count(triage.lint_gate.blocking_titles.len() as f64))
        );
        return Ok(EXIT_LINT_BLOCKED);
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triage_brief_extracts_sqlcommenter_locations_and_blocks_unsafe_offline_ddl() {
        let sql_with_tag = "/*application='orders',file='app/repositories/order_repo.rs',line='142'*/ SELECT * FROM orders WHERE customer_id = 42";
        assert_eq!(extract_source_locations(sql_with_tag), vec!["app/repositories/order_repo.rs:142"]);

        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/pg-demo.report.json");
        let dir = std::env::temp_dir().join(format!("culprit-triage-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ws = Workspace::open_in(&dir);
        let report = ws.analyze_file(&fixture).unwrap();
        let table = &report.snapshot.tables[0].name;
        let window =
            PublishedResourceWindow { from: "2026-11-25T11:00:00Z".into(), until: "2026-11-25T12:00:00Z".into() };

        // 1. Unsafe non-concurrent index build on a populated Postgres table is BLOCKED by lint-sql!
        let unsafe_ddl = format!("CREATE INDEX ix_unsafe_triage ON {table} (id);");
        let blocked = evaluate_triage_brief(TriageEvaluationInput {
            alias: "orders-prod",
            database_name: "orders",
            engine: culprit_core::Engine::Postgres,
            window: window.clone(),
            active_incidents: 0,
            high_findings: 1,
            recent_regressions: 1,
            findings: vec![(report.findings[0].clone(), Some(sql_with_tag.into()))],
            snapshot: &report.snapshot,
            proposed_sql: Some(&unsafe_ddl),
            fail_on: Severity::High,
        });
        assert!(!blocked.lint_gate.passed);
        assert!(!blocked.lint_gate.blocking_titles.is_empty());
        assert!(blocked.pr_markdown.contains("app/repositories/order_repo.rs:142"));
        assert!(blocked.pr_markdown.contains("culprit verify orders-prod"));

        // 2. Safe CONCURRENTLY index build passes the deterministic lint-sql gate!
        let safe_ddl = format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_safe_triage ON {table} (id);");
        let passed = evaluate_triage_brief(TriageEvaluationInput {
            alias: "orders-prod",
            database_name: "orders",
            engine: culprit_core::Engine::Postgres,
            window,
            active_incidents: 0,
            high_findings: 1,
            recent_regressions: 0,
            findings: vec![(report.findings[0].clone(), Some(sql_with_tag.into()))],
            snapshot: &report.snapshot,
            proposed_sql: Some(&safe_ddl),
            fail_on: Severity::High,
        });
        assert!(passed.lint_gate.passed);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
