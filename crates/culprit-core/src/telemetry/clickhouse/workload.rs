use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default, PartialEq, Eq)]
#[ts(export)]
pub struct ClickHousePopulation {
    pub executions: u64,
    pub elapsed_ms: u64,
    pub families: u64,
}

impl ClickHousePopulation {
    pub fn workload(&self, from: String, until: String, hours: f64, queries: Vec<crate::QueryStat>) -> crate::Workload {
        crate::Workload {
            population: None,
            population_catalog: None,
            source: "system.query_log".into(),
            source_label: Some(crate::message!("provider.clickhouse.history").into()),
            kind: Some(crate::WorkloadKind::ClickHouseQueryLog),
            windowed: true,
            log: None,
            window_start: Some(from),
            window_end: Some(until),
            window_hours: hours,
            total_time_ms: self.elapsed_ms as f64,
            total_cpu_ms: None,
            total_executions: self.executions as f64,
            query_count: self.families as f64,
            queries,
            timeline: None,
            untracked_time_ms: None,
            hourly_time_ms: None,
            min_logged_ms: None,
            history_inputs: vec![],
            counter_set: None,
        }
    }
}
impl ClickHouseQueryFamily {
    pub fn statement(&self, text: String) -> crate::QueryStat {
        crate::QueryStat {
            id: self.family.clone(),
            query_hash: Some(self.family.clone()),
            text,
            executions: self.executions as f64,
            total_time_ms: self.elapsed_ms as f64,
            mean_time_ms: self.elapsed_ms as f64 / self.executions as f64,
            max_time_ms: Some(self.max_ms as f64),
            rows: Some(self.rows_returned as f64),
            logical_reads: Some(self.rows_read as f64),
            first_seen: Some(self.first_seen.clone()),
            last_seen: Some(self.last_seen.clone()),
            costs: Some(crate::signals::QueryCosts { failed: Some(self.failures as f64), ..Default::default() }),
            native_identity: None,
            native_plan_identity: None,
            requests: None,
            resolved_write_target: None,
            parameters: None,
            object_name: None,
            stddev_time_ms: None,
            cpu_time_ms: None,
            physical_reads: None,
            temporary_work: None,
            plans: vec![],
            variant_family: None,
            plan: None,
            waits: vec![],
            waits_coverage: None,
            trend: None,
            spread: None,
            rows_affected: None,
            plan_id: None,
            plan_history: None,
        }
    }
    pub fn redact_statement(&mut self) {
        if let DefinitionPayload::Available { text, .. } = &self.sample {
            self.sample = DefinitionPayload::sql(text, crate::Engine::Clickhouse);
        }
    }
}
