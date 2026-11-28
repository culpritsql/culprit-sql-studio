//! ClickHouse describes logged executions and MergeTree parts. Conventional B-tree index advice does not apply.
use super::*;
use crate::dialect::NewIndex;

pub(super) struct ClickHouse;

const PRODUCTS: &[ProductSpec] =
    &[ProductSpec::primary(DatabaseProduct::Clickhouse, "ClickHouse", key!("core.engines.clickhouse.plan_format"))];

impl EngineSpec for ClickHouse {
    fn default_port(&self) -> Option<u16> {
        Some(8443)
    }
    fn products(&self) -> &'static [ProductSpec] {
        PRODUCTS
    }
    fn connect(&self) -> Connect {
        Connect::server(
            "ClickHouse",
            "clickhouse://reader@db.example.com:8443/analytics",
            key!("core.engines.clickhouse.accepts"),
        )
        .without_ssh()
    }
    fn connection_security(&self) -> Option<crate::ConnectionSecuritySpec> {
        use crate::ConnectionSecurityMode as Mode;
        Some(crate::ConnectionSecuritySpec {
            modes: vec![
                Mode::new(
                    "verify-full",
                    key!("ui.connection_security.mode.verify_identity"),
                    key!("ui.connection_security.description.verify_identity"),
                )
                .verifies_identity()
                .without_certificates(),
                Mode::new(
                    "disable",
                    key!("ui.connection_security.mode.disabled"),
                    key!("ui.connection_security.description.disabled"),
                )
                .permits_plaintext()
                .without_certificates(),
            ],
            default_mode: "verify-full".into(),
            local_default_mode: "verify-full".into(),
            mode_aliases: vec!["sslmode".into()],
            mode_option: "sslmode".into(),
            ca_aliases: vec![],
            ca_option: "sslrootcert".into(),
            trust_certificate_option: None,
            hostname_option: None,
        })
    }
    fn abilities(&self) -> EngineAbilities {
        EngineAbilities {
            sessions: true,
            periods: true,
            sign_in: true,
            deadlocks: false,
            measures_runs: false,
            writes: false,
            wait_sample: false,
            last_actual_plan: false,
            operator_progress: false,
            plan_estimates: false,
        }
    }
    fn sql_syntax(&self) -> SqlSyntax {
        SqlSyntax::Clickhouse
    }
    fn dialect(&self) -> &'static dyn Dialect {
        &ClickHouse
    }
    fn evidence_presentation(&self) -> EvidencePresentation {
        EvidencePresentation {
            storage_about: key!("ui.beyond_time.storage.about").into(),
            storage_untimed: key!("ui.beyond_time.storage.untimed").into(),
            health_empty: key!("ui.health.empty_hint").into(),
            generic_plan_context: key!("ui.plan_surface.generic_settings").into(),
            statistics_freshness: StatisticsFreshness::Unavailable,
            statistics_absence_known: false,
            workload_windowed_by_default: true,
            deadlock_time_window: false,
            retained_actual_plans: false,
        }
    }
    fn statement_metrics(&self) -> StatementMetrics {
        StatementMetrics {
            runtime_note: key!("core.engines.clickhouse.history").into(),
            ..StatementMetrics::new(PlanningStep::Planning, ReadUnit::RowsRead, None, WriteLog::Unavailable)
        }
    }
    fn cleanup_evidence(&self) -> CleanupEvidence {
        CleanupEvidence::Unavailable
    }
    fn constraint_validation(&self) -> ConstraintValidation {
        ConstraintValidation::Unsupported
    }
    fn partition_tail(&self) -> PartitionTail {
        PartitionTail::Unsupported
    }
    fn unavailable_wait_categories(&self) -> Option<&'static str> {
        Some(key!("core.engines.clickhouse.waits"))
    }
    fn wait_sampling(&self) -> Option<WaitSampling> {
        None
    }
    fn session_id_format(&self) -> SessionIdFormat {
        SessionIdFormat::NativeId
    }
    fn validate_session_id(&self, id: &str) -> crate::Result<()> {
        if !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control) {
            Ok(())
        } else {
            Err(Error::invalid(t!("core.engines.clickhouse.query_id")))
        }
    }
    fn estimated_plans_label(&self) -> &'static str {
        key!("core.engines.estimated_plans.explain")
    }
    fn signal_scope(&self, _: &ServerInfo, _: &str) -> crate::MeasurementScope {
        crate::MeasurementScope::Node
    }
    fn inactive_index_state(&self, _: &ServerInfo) -> crate::IndexState {
        crate::IndexState::Active
    }
    fn unused_index_advice(&self, _: &ServerInfo, _: Option<&str>) -> String {
        t!("core.engines.clickhouse.sort_key")
    }
    fn index_advice_sources(&self, _: &ServerInfo, _: bool) -> Option<String> {
        Some(t!("core.engines.clickhouse.sort_key"))
    }
    fn limitation_keys(&self) -> &'static [&'static str] {
        &[key!("core.engines.clickhouse.history")]
    }
    fn change_setting(&self, _: &ServerInfo, name: &str, value: &str) -> SettingChange {
        SettingChange { how: t!("core.engines.clickhouse.setting", name = name, value = value), sql: None }
    }
    fn refresh_statistics(&self, _: &str) -> Fix {
        Fix { summary: t!("core.engines.clickhouse.sort_key"), sql: None, cost: None }
    }
    fn wait_not_per_statement(&self, _: &ServerInfo, _: &str) -> String {
        t!("core.engines.clickhouse.waits")
    }
}

impl Dialect for ClickHouse {
    fn quote(&self, ident: &str) -> String {
        format!("`{}`", ident.replace('\\', "\\\\").replace('`', "\\`"))
    }
    fn create_index(&self, _: &NewIndex) -> String {
        format!("-- {}", t!("core.engines.clickhouse.sort_key"))
    }
    fn drop_index(&self, schema: &str, table: &str, index: &str) -> String {
        format!("ALTER TABLE {} DROP INDEX {};", self.qualified(&[schema, table]), self.quote(index))
    }
    fn refresh_statistics(&self, _: &str) -> String {
        format!("-- {}", t!("core.engines.clickhouse.sort_key"))
    }
    fn end_session(&self, session: &str) -> String {
        format!("KILL QUERY WHERE query_id = '{}' SYNC;", session.replace('\\', "\\\\").replace('\'', "\\'"))
    }
}
