//! Serialization formats recognized by the pure plan readers, independent of database drivers.
use crate::t;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum PlanFormat {
    ClickHouseJson,
    ShowplanXml,
    PostgresJson,
    YugabyteJson,
    PostgresText,
    MysqlJson,
    MysqlTree,
    SqliteText,
    Culprit,
}

impl PlanFormat {
    pub fn label(self) -> String {
        match self {
            PlanFormat::ClickHouseJson => t!("core.engines.clickhouse.plan_format"),
            PlanFormat::ShowplanXml => t!("engine.plans.format.showplan_xml"),
            PlanFormat::PostgresJson => t!("engine.plans.format.postgres_json"),
            PlanFormat::YugabyteJson => t!("engine.plans.format.yugabyte_json"),
            PlanFormat::PostgresText => t!("engine.plans.format.postgres_text"),
            PlanFormat::MysqlJson => t!("engine.plans.format.mysql_json"),
            PlanFormat::MysqlTree => t!("engine.plans.format.mysql_tree"),
            PlanFormat::SqliteText => t!("engine.plans.format.sqlite_text"),
            PlanFormat::Culprit => t!("engine.plans.format.culprit"),
        }
    }
}
