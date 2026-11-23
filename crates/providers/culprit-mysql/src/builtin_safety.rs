//! Source-owned safety for bounded native metadata readers, distinct from measured target cost.
use crate::connect::Flavor;
use culprit_core::{Result, telemetry::*};
pub(crate) fn policy(
    id: &str,
    version: u32,
    selector: RecipeSelector,
    budget: &CollectionBudget,
) -> Option<BuiltinRecipeSafety> {
    let purpose = match (id, selector) {
        ("mysql.activity", RecipeSelector::Collection(TelemetryCollection::Activity)) => RecipePurpose::Baseline,
        ("mysql.settings", RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Settings))) => {
            RecipePurpose::Baseline
        }
        ("mysql.blocking", RecipeSelector::FocusedBlocking) => RecipePurpose::Investigation,
        _ => return None,
    };
    Some(BuiltinRecipeSafety {
        id: format!("{id}.mysql84-mariadb118-safety"),
        version: 1,
        recipe_id: id.into(),
        recipe_version: version,
        purpose,
        selector,
        component: crate::component_version(),
        templates_sha256: templates(),
        budget: budget.clone(),
    })
}
fn templates() -> String {
    let parts = [
        include_str!("../Cargo.toml"),
        include_str!("../../../../Cargo.lock"),
        include_str!("builtin_safety.rs"),
        include_str!("connect.rs"),
        include_str!("read.rs"),
        include_str!("read/accounting.rs"),
        include_str!("read/same_connection.rs"),
        include_str!("read/owned.rs"),
        include_str!("cancellation.rs"),
        include_str!("rows.rs"),
        include_str!("probe.rs"),
        include_str!("native/mod.rs"),
        include_str!("native/instance.rs"),
        include_str!("telemetry/mod.rs"),
        include_str!("telemetry/settings_profile.rs"),
        include_str!("telemetry/snapshot_io.rs"),
        include_str!("../../../culprit-read/src/snapshot.rs"),
        include_str!("../../../culprit-read/src/profile.rs"),
        include_str!("../../../culprit-core/src/telemetry/snapshot_acquisition.rs"),
        include_str!("../../../culprit-core/src/telemetry/settings.rs"),
        include_str!("../../../culprit-core/src/telemetry/settings/context.rs"),
        include_str!("../../../culprit-core/src/telemetry/settings/mysql.rs"),
        include_str!("native/settings.rs"),
        include_str!("telemetry/recipes.rs"),
        include_str!("native/activity.rs"),
        include_str!("native/activity/payload.rs"),
        include_str!("native/metadata.rs"),
        include_str!("native/local_context.rs"),
        include_str!("native/locks.rs"),
        include_str!("native/locks/selected.rs"),
        include_str!("telemetry/activity.rs"),
        include_str!("telemetry/blocking.rs"),
        include_str!("telemetry/blocking/participants.rs"),
        include_str!("telemetry/blocking/edges.rs"),
    ];
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    digest(&bytes)
}
pub fn verify(policy: &BuiltinRecipeSafety) -> Result<bool> {
    Ok(crate::telemetry::recipes().iter().any(|recipe| {
        policy.matches_recipe(recipe)
            && matches!(&recipe.admission,RecipeAdmission::BuiltinSafety(selected) if selected.as_ref()==policy)
    }))
}
pub(crate) fn verify_native(policy: &BuiltinRecipeSafety, flavor: Flavor, version: &str) -> Result<()> {
    if !supported(flavor, version) || !verify(policy)? {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}
pub(crate) fn verify_metadata(policy: &BuiltinRecipeSafety, meta: &crate::native::metadata::Metadata) -> Result<()> {
    verify_native(policy, meta.connection.flavor, &meta.connection.version)?;
    // The first owned native acceptance is a writable database. Replica/read-only support remains explicit
    // future qualification, rather than borrowing this policy from an unexercised native role.
    if meta.connection.read_only {
        return Err(culprit_core::Error::new(
            culprit_core::ErrorCode::Unsupported,
            culprit_core::t!("collection.unsupported"),
        ));
    }
    Ok(())
}
pub(crate) fn supported(flavor: Flavor, version: &str) -> bool {
    let version = if flavor == Flavor::Mariadb { version.strip_prefix("5.5.5-").unwrap_or(version) } else { version };
    let numeric = match flavor {
        Flavor::Mysql => version,
        Flavor::Mariadb => match version.split_once("-MariaDB") {
            Some((numeric, _)) => numeric,
            None => return false,
        },
    };
    let numbers = numeric.split('.').map(str::parse::<u32>).collect::<std::result::Result<Vec<_>, _>>();
    matches!((flavor, numbers.as_deref()), (Flavor::Mysql, Ok([8, 4, _])) | (Flavor::Mariadb, Ok([11, 8, _])))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_source_policies_preserve_unknown_cost_and_refuse_foreign_or_wider_authority() {
        let recipes = crate::telemetry::recipes();
        let selected = recipes
            .iter()
            .filter(|recipe| matches!(recipe.admission, RecipeAdmission::BuiltinSafety(_)))
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), 3);
        for recipe in selected {
            let RecipeAdmission::BuiltinSafety(policy) = &recipe.admission else { panic!() };
            assert!(verify(policy).unwrap());
            for (flavor, version) in [
                (Flavor::Mysql, "8.4.6"),
                (Flavor::Mariadb, "11.8.9-MariaDB-ubu2404"),
                (Flavor::Mariadb, "5.5.5-11.8.3-MariaDB"),
            ] {
                assert!(verify_native(policy, flavor, version).is_ok());
            }
            for (flavor, version) in [
                (Flavor::Mysql, "8.0.42"),
                (Flavor::Mysql, "9.0.1"),
                (Flavor::Mysql, "8.4.1-1"),
                (Flavor::Mariadb, "11.4.9-MariaDB"),
                (Flavor::Mariadb, "11.8.9"),
                (Flavor::Mysql, "8.4"),
                (Flavor::Mysql, ""),
            ] {
                assert!(verify_native(policy, flavor, version).is_err());
            }
            let mut changed = policy.as_ref().clone();
            changed.templates_sha256 = digest(b"foreign-reader");
            assert!(!verify(&changed).unwrap());
            changed = policy.as_ref().clone();
            changed.budget.max_queries += 1;
            assert!(!verify(&changed).unwrap());
        }
        assert!(
            recipes
                .iter()
                .filter(|recipe| !matches!(recipe.id.as_str(), "mysql.activity" | "mysql.blocking" | "mysql.settings"))
                .all(|recipe| matches!(recipe.admission, RecipeAdmission::Unqualified))
        );
    }
}
