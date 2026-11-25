//! Validated built-in safety for bounded SQL Server and Azure SQL metadata readers, not measured CPU or cost receipts.
use culprit_core::{Result, telemetry::*};

pub(crate) fn policy(
    recipe_id: &str,
    recipe_version: u32,
    selector: RecipeSelector,
    budget: &CollectionBudget,
) -> Option<BuiltinRecipeSafety> {
    let purpose = match (recipe_id, selector) {
        ("mssql.activity", RecipeSelector::Collection(TelemetryCollection::Activity)) => RecipePurpose::Baseline,
        ("mssql.settings", RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Settings))) => {
            RecipePurpose::Baseline
        }
        ("mssql.blocking", RecipeSelector::FocusedBlocking) => RecipePurpose::Investigation,
        (
            "mssql.readable-secondary-recovery",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::ReadableSecondaryRecovery)),
        ) => RecipePurpose::Investigation,
        _ => return None,
    };
    Some(BuiltinRecipeSafety {
        id: format!("{recipe_id}.sql16-azure-safety"),
        version: 1,
        recipe_id: recipe_id.into(),
        recipe_version,
        purpose,
        selector,
        component: crate::component_version(),
        templates_sha256: template_fingerprint(),
        budget: budget.clone(),
    })
}

fn template_fingerprint() -> String {
    let sources = [
        include_str!("../Cargo.toml"),
        include_str!("../../../../Cargo.lock"),
        include_str!("builtin_safety.rs"),
        include_str!("read.rs"),
        include_str!("local_source.rs"),
        include_str!("instance.rs"),
        include_str!("probe.rs"),
        include_str!("settings.rs"),
        include_str!("settings/frame.rs"),
        include_str!("database_properties.rs"),
        include_str!("activity/mod.rs"),
        include_str!("activity/sql.rs"),
        include_str!("activity/capture.rs"),
        include_str!("activity/database_context.rs"),
        include_str!("activity/census_scope.rs"),
        include_str!("activity/blocking.rs"),
        include_str!("activity/blocking/metadata_locks.rs"),
        include_str!("activity/participants.rs"),
        include_str!("activity/readable_secondary_recovery.rs"),
        include_str!("activity/readable_secondary_recovery/redo.rs"),
        include_str!("telemetry/mod.rs"),
        include_str!("telemetry/recipes.rs"),
        include_str!("telemetry/activity/mod.rs"),
        include_str!("telemetry/blocking.rs"),
        include_str!("telemetry/readable_secondary_recovery.rs"),
        include_str!("telemetry/state.rs"),
        include_str!("telemetry/state/settings.rs"),
        include_str!("telemetry/subject.rs"),
        include_str!("telemetry/point.rs"),
        include_str!("../../../culprit-read/src/profile.rs"),
        include_str!("../../../culprit-core/src/telemetry/snapshot_acquisition.rs"),
        include_str!("../../../culprit-core/src/telemetry/settings.rs"),
        include_str!("../../../culprit-core/src/telemetry/settings/context.rs"),
        include_str!("../../../culprit-core/src/telemetry/observation.rs"),
        include_str!("../../../culprit-core/src/telemetry/source_partition.rs"),
        include_str!("../../../culprit-core/src/telemetry/native_read_context.rs"),
        include_str!("../../../culprit-core/src/telemetry/readable_secondary_recovery.rs"),
    ];
    let mut bytes = Vec::new();
    for source in sources {
        bytes.extend_from_slice(&(source.len() as u64).to_le_bytes());
        bytes.extend_from_slice(source.as_bytes());
    }
    digest(&bytes)
}

pub fn verify(policy: &BuiltinRecipeSafety) -> Result<bool> {
    Ok(crate::telemetry::recipes().iter().any(|recipe| {
        policy.matches_recipe(recipe)
            && matches!(&recipe.admission, RecipeAdmission::BuiltinSafety(selected) if selected.as_ref() == policy)
    }))
}

pub(crate) fn verify_activity(policy: &BuiltinRecipeSafety, edition: i64, major: u32) -> Result<()> {
    let supported = (matches!(edition, 2..=4) && major == 16)
        || (matches!(
            edition,
            crate::probe::ENGINE_EDITION_AZURE_SQL_DATABASE | crate::probe::ENGINE_EDITION_MANAGED_INSTANCE
        ) && (12..=17).contains(&major));
    if !supported || !verify(policy)? {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

pub(crate) fn verify_settings(policy: &BuiltinRecipeSafety, owner: &crate::local_source::Context) -> Result<()> {
    let supported = (matches!(owner.activity.edition, 2..=4) && owner.activity.major == 16)
        || (owner.activity.edition == crate::probe::ENGINE_EDITION_MANAGED_INSTANCE
            && (12..=17).contains(&owner.activity.major));
    if !supported || !owner.is_qualified() || owner.activity.writable != Some(true) || !verify(policy)? {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

pub(crate) fn verify_blocking(policy: &BuiltinRecipeSafety, owner: &crate::local_source::Context) -> Result<()> {
    if owner.activity.edition != 3
        || owner.activity.major != 16
        || owner.activity.writable != Some(true)
        || owner.activity.replica_id.is_some()
        || !owner.is_qualified()
        || !owner.database_wide
        || !verify(policy)?
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

pub(crate) fn verify_readable_secondary_recovery(
    policy: &BuiltinRecipeSafety,
    owner: &crate::local_source::Context,
) -> Result<()> {
    if owner.activity.edition != 3
        || owner.activity.major != 16
        || owner.activity.writable != Some(false)
        || !owner.is_qualified()
        || !owner.database_wide
        || !verify(policy)?
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bounded_dmv_and_catalog_metadata_readers_are_source_validated_and_page_headers_stay_unqualified() {
        let recipes = crate::telemetry::recipes();
        let policies = recipes
            .iter()
            .filter_map(|recipe| match &recipe.admission {
                RecipeAdmission::BuiltinSafety(policy) => Some((recipe, policy)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(policies.len(), 4);
        for (recipe, policy) in policies {
            assert!(recipe.is_valid());
            assert!(verify(policy).unwrap());
            assert!(matches!(
                recipe.id.as_str(),
                "mssql.activity" | "mssql.blocking" | "mssql.readable-secondary-recovery" | "mssql.settings"
            ));
            for (edition, major) in [(3, 16), (2, 16), (5, 12), (8, 12), (8, 16)] {
                assert!(verify_activity(policy, edition, major).is_ok());
            }
            for (edition, major) in [(3, 15), (3, 17), (1, 16), (5, 11)] {
                assert!(verify_activity(policy, edition, major).is_err());
            }
            for fault in ["template", "component", "budget", "selector", "version", "recipe"] {
                let mut changed = policy.as_ref().clone();
                match fault {
                    "template" => changed.templates_sha256 = digest(b"not-the-reader"),
                    "component" => changed.component.version.as_mut().unwrap().push_str("-foreign"),
                    "budget" => changed.budget.max_queries += 1,
                    "selector" => changed.selector = RecipeSelector::CurrentRequestWork,
                    "version" => changed.version += 1,
                    "recipe" => changed.recipe_id = "mssql.page-wait-targets".into(),
                    _ => unreachable!(),
                }
                assert!(!verify(&changed).unwrap(), "{fault}");
            }
        }
        assert!(
            recipes
                .iter()
                .filter(|recipe| !matches!(
                    recipe.id.as_str(),
                    "mssql.activity" | "mssql.blocking" | "mssql.readable-secondary-recovery" | "mssql.settings"
                ))
                .all(|recipe| matches!(recipe.admission, RecipeAdmission::Unqualified))
        );
    }
}
