//! The scheduled baseline is limited to this compiled set of bounded system-table reads.
use crate::{component_version, queries, telemetry};
use culprit_core::{Result, telemetry::*};
use std::sync::OnceLock;

pub(crate) fn baseline_policy(recipe: &RecipeDescriptor) -> BuiltinRecipeSafety {
    static TEMPLATES: OnceLock<String> = OnceLock::new();
    let hash = TEMPLATES.get_or_init(|| {
        let templates = [
            include_str!("../../../../Cargo.lock"),
            include_str!("safety.rs"),
            include_str!("client.rs"),
            include_str!("telemetry.rs"),
            include_str!("identity.rs"),
            include_str!("health.rs"),
            include_str!("queries.rs"),
            &culprit_core::telemetry::CLICKHOUSE_PROFILE_EVENTS.join(","),
            include_str!("workload.rs"),
            queries::IDENTITY,
            queries::FAILURES,
            queries::ACTIVE_PARTS,
            queries::MERGES,
            queries::MUTATIONS,
            queries::REPLICAS,
            queries::REPLICATION_QUEUE,
            queries::DISTRIBUTION_QUEUE,
            queries::ASYNC_INSERTS,
            queries::WORKLOAD,
        ];
        digest(templates.join("\0").as_bytes())
    });
    BuiltinRecipeSafety {
        id: "clickhouse.bounded-system-tables".into(),
        version: 1,
        recipe_id: recipe.id.clone(),
        recipe_version: recipe.version,
        purpose: recipe.purpose,
        selector: recipe.selector,
        component: component_version(),
        templates_sha256: hash.clone(),
        budget: recipe.budget.clone(),
    }
}
pub fn verify_builtin_safety(policy: &BuiltinRecipeSafety) -> Result<bool> {
    Ok(telemetry::telemetry_recipes().iter().any(|recipe| {
        policy.matches_recipe(recipe)
            && matches!(&recipe.admission,RecipeAdmission::BuiltinSafety(expected) if expected.as_ref()==policy)
    }))
}
pub fn telemetry_profile_selectors() -> Vec<RecipeSelector> {
    vec![RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Operational))]
}
