//! Pure, I/O-free static shard and admission-group planning for discovered database estates.
//!
//! Databases sharing an underlying compute boundary (Azure SQL logical server / Elastic Pool /
//! Managed Instance / Flexible Server, or AWS RDS instance / Aurora cluster) are grouped into a
//! single `admission_group` and deterministically assigned to the same static collector shard so
//! `FleetInventory::validate_assignments` and `MonitoringPolicy::Standard` never split shared-host
//! admission across independent collector processes.

use culprit_core::{
    AuthKind, Engine, Environment, Error, Result, t,
    telemetry::{
        CollectionBudget, CollectionSubject, FleetInventory, FleetSchedule, FleetTarget, MonitoringPolicy,
        ScheduledCollection,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_ONBOARDING_SHARDS: u32 = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingCandidate {
    pub alias: String,
    pub engine: Engine,
    pub environment: Environment,
    /// Shared compute boundary (server, elastic pool, managed instance, or RDS/Aurora cluster).
    pub server_group: String,
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    pub database: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub auth: AuthKind,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingOptions {
    pub shard_count: u32,
    pub shard_prefix: String,
    pub include_production: bool,
    pub explicit_query_history: bool,
}

impl Default for OnboardingOptions {
    fn default() -> Self {
        Self { shard_count: 1, shard_prefix: "shard".into(), include_production: true, explicit_query_history: false }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardedConnection {
    pub name: String,
    pub engine: Engine,
    pub environment: Environment,
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    pub auth: AuthKind,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
    pub shard: String,
    pub admission_group: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannedShard {
    pub shard: String,
    pub monitoring: MonitoringPolicy,
    pub connections: BTreeMap<String, OnboardedConnection>,
    pub inventory: FleetInventory,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OnboardingPlan {
    pub shards: Vec<PlannedShard>,
    pub skipped_production: u32,
    pub total_targets: u32,
}

pub fn slug_identifier(raw: &str, fallback: &str) -> String {
    let mut slug = String::new();
    let mut prev_dash = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash && !slug.is_empty() {
            slug.push('-');
            prev_dash = true;
        }
        if slug.len() >= 96 {
            break;
        }
    }
    let trimmed = slug.trim_matches('-');
    if trimmed.is_empty() { fallback.to_string() } else { trimmed.to_string() }
}

fn unique_identifier(base: &str, used: &mut BTreeSet<String>) -> String {
    if used.insert(base.to_string()) {
        return base.to_string();
    }
    let mut suffix = 2u32;
    loop {
        let candidate = format!("{base}-{suffix}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        suffix += 1;
    }
}

fn deterministic_shard_name(prefix: &str, admission_group: &str, shard_count: u32) -> String {
    if shard_count <= 1 {
        return format!("{prefix}-00");
    }
    let digest = Sha256::digest(admission_group.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let hash = u64::from_be_bytes(bytes);
    let bucket = (hash % u64::from(shard_count)) as u32;
    if shard_count > 100 { format!("{prefix}-{bucket:03}") } else { format!("{prefix}-{bucket:02}") }
}

fn engine_slug(engine: Engine) -> &'static str {
    match engine {
        Engine::Mssql => "mssql",
        Engine::Postgres => "postgres",
        Engine::Yugabyte => "yugabyte",
        Engine::Mysql => "mysql",
        Engine::Sqlite => "sqlite",
        Engine::Clickhouse => "clickhouse",
    }
}

pub fn plan_inventory(candidates: &[OnboardingCandidate], options: &OnboardingOptions) -> Result<OnboardingPlan> {
    let prefix = slug_identifier(&options.shard_prefix, "");
    if !(1..=MAX_ONBOARDING_SHARDS).contains(&options.shard_count)
        || prefix.is_empty()
        || prefix != options.shard_prefix
    {
        return Err(Error::invalid(t!("onboarding.invalid_options")));
    }

    let mut skipped_production = 0u32;
    let mut sorted: Vec<&OnboardingCandidate> = Vec::new();
    for candidate in candidates {
        if candidate.host.trim().is_empty() || !candidate.engine.has_server() {
            continue;
        }
        if candidate.environment.is_production() && !options.include_production {
            skipped_production += 1;
            continue;
        }
        sorted.push(candidate);
    }
    if sorted.is_empty() {
        return Err(Error::invalid(t!("onboarding.no_targets")));
    }

    sorted.sort_by(|a, b| {
        (engine_slug(a.engine), &a.server_group, &a.host, &a.database, &a.alias).cmp(&(
            engine_slug(b.engine),
            &b.server_group,
            &b.host,
            &b.database,
            &b.alias,
        ))
    });

    let mut used_connections = BTreeSet::new();
    let mut used_targets = BTreeSet::new();
    let mut seen_endpoints = BTreeSet::new();
    let mut shard_buckets: BTreeMap<String, (BTreeMap<String, OnboardedConnection>, Vec<FleetTarget>)> =
        BTreeMap::new();
    let mut total_targets = 0u32;

    for candidate in sorted {
        let endpoint_key = (
            candidate.engine,
            candidate.host.to_ascii_lowercase(),
            candidate.port,
            candidate.database.to_ascii_lowercase(),
        );
        if !seen_endpoints.insert(endpoint_key) {
            continue;
        }

        let group_source =
            if candidate.server_group.trim().is_empty() { &candidate.host } else { &candidate.server_group };
        let group_slug = slug_identifier(group_source, "server");
        let admission_group = format!("{}-{group_slug}", engine_slug(candidate.engine));
        let shard = deterministic_shard_name(&prefix, &admission_group, options.shard_count);

        let raw_alias = if candidate.alias.trim().is_empty() {
            if candidate.database.trim().is_empty() { group_source.as_str() } else { candidate.database.as_str() }
        } else {
            candidate.alias.as_str()
        };
        let conn_name = unique_identifier(&slug_identifier(raw_alias, "db"), &mut used_connections);
        let target_id = unique_identifier(&format!("{conn_name}-db"), &mut used_targets);

        let schedules = if options.explicit_query_history {
            vec![FleetSchedule {
                id: format!("{}.query-history", engine_slug(candidate.engine)),
                every_seconds: 900,
                jitter_seconds: 60,
                collection: ScheduledCollection::QueryHistory { lookback_seconds: 3_600, reconcile_seconds: 900 },
                budget: CollectionBudget::default(),
            }]
        } else {
            Vec::new()
        };

        let database = (!candidate.database.trim().is_empty()).then(|| candidate.database.clone());
        let connection = OnboardedConnection {
            name: conn_name.clone(),
            engine: candidate.engine,
            environment: candidate.environment,
            host: candidate.host.clone(),
            port: candidate.port,
            database,
            user: candidate.user.clone().filter(|u| !u.trim().is_empty()),
            auth: candidate.auth,
            options: candidate.options.clone(),
            shard: shard.clone(),
            admission_group: admission_group.clone(),
        };
        let target = FleetTarget {
            id: target_id,
            connection: conn_name.clone(),
            subject: CollectionSubject::Database,
            shard: shard.clone(),
            admission_group,
            schedules,
        };

        let entry = shard_buckets.entry(shard).or_default();
        entry.0.insert(conn_name, connection);
        entry.1.push(target);
        total_targets += 1;
    }

    if total_targets == 0 {
        return Err(Error::invalid(t!("onboarding.no_targets")));
    }

    let monitoring = if options.explicit_query_history {
        MonitoringPolicy::Explicit
    } else {
        MonitoringPolicy::Standard { budget: CollectionBudget::default() }
    };

    let mut shards = Vec::with_capacity(shard_buckets.len());
    for (shard_name, (connections, targets)) in shard_buckets {
        let inventory = FleetInventory { targets };
        monitoring.validate(&inventory, &shard_name)?;
        shards.push(PlannedShard { shard: shard_name, monitoring: monitoring.clone(), connections, inventory });
    }

    Ok(OnboardingPlan { shards, skipped_production, total_targets })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_candidate(alias: &str, engine: Engine, server: &str, host: &str, db: &str) -> OnboardingCandidate {
        OnboardingCandidate {
            alias: alias.into(),
            engine,
            environment: Environment::Production,
            server_group: server.into(),
            host: host.into(),
            port: Some(1433),
            database: db.into(),
            user: None,
            auth: AuthKind::Entra,
            options: BTreeMap::from([("encrypt".into(), "True".into())]),
        }
    }

    #[test]
    fn shared_server_databases_always_stay_in_one_admission_group_and_shard() {
        let candidates = vec![
            sample_candidate("orders", Engine::Mssql, "sql-pool-east", "sql-pool-east.database.windows.net", "orders"),
            sample_candidate(
                "billing",
                Engine::Mssql,
                "sql-pool-east",
                "sql-pool-east.database.windows.net",
                "billing",
            ),
            sample_candidate("ledger", Engine::Mssql, "sql-pool-east", "sql-pool-east.database.windows.net", "ledger"),
            sample_candidate(
                "analytics",
                Engine::Postgres,
                "pg-flex-west",
                "pg-flex-west.postgres.database.azure.com",
                "analytics",
            ),
        ];
        let plan = plan_inventory(
            &candidates,
            &OnboardingOptions { shard_count: 8, shard_prefix: "collector".into(), ..Default::default() },
        )
        .unwrap();

        assert_eq!(plan.total_targets, 4);
        let mut pool_shards = BTreeSet::new();
        let mut pool_groups = BTreeSet::new();
        for shard in &plan.shards {
            shard.monitoring.validate(&shard.inventory, &shard.shard).unwrap();
            for target in &shard.inventory.targets {
                if target.connection != "analytics" {
                    pool_shards.insert(target.shard.clone());
                    pool_groups.insert(target.admission_group.clone());
                }
            }
        }
        assert_eq!(pool_shards.len(), 1, "sibling databases on one server/pool must never split across shards");
        assert_eq!(pool_groups, BTreeSet::from(["mssql-sql-pool-east".to_string()]));
    }

    #[test]
    fn duplicate_aliases_deduplicate_and_production_filter_counts_skipped() {
        let mut staging = sample_candidate("app", Engine::Postgres, "pg-stg", "pg-stg.internal", "app");
        staging.environment = Environment::Staging;
        let mut dev = sample_candidate("app", Engine::Postgres, "pg-dev", "pg-dev.internal", "app");
        dev.environment = Environment::Development;
        let prod = sample_candidate("app", Engine::Postgres, "pg-prd", "pg-prd.internal", "app");

        let plan = plan_inventory(
            &[staging, dev, prod],
            &OnboardingOptions { include_production: false, ..Default::default() },
        )
        .unwrap();
        assert_eq!(plan.skipped_production, 1);
        assert_eq!(plan.total_targets, 2);
        let names: BTreeSet<_> = plan.shards[0].connections.keys().map(String::as_str).collect();
        assert_eq!(names, BTreeSet::from(["app", "app-2"]));
    }
}
