//! Validated built-in safety for bounded SQL Server and Azure SQL metadata readers, not measured CPU or cost receipts.
use culprit_core::{Result, telemetry::*};

#[cfg(test)]
pub(crate) const BUILTIN_SAFETY_IDS: &[&str] = &[
    "mssql.activity",
    "mssql.blocking",
    "mssql.cache-entry-memory",
    "mssql.current-execution-profile",
    "mssql.current-request-memory",
    "mssql.current-request-work",
    "mssql.deadlock-history",
    "mssql.focused-compiler-admission",
    "mssql.focused-page-task-holders",
    "mssql.focused-page-wait",
    "mssql.focused-page-wait-writers",
    "mssql.focused-request-memory",
    "mssql.focused-shared-compiler-admission",
    "mssql.focused-shared-tempdb-page-wait",
    "mssql.query-compilation",
    "mssql.readable-secondary-recovery",
    "mssql.retained-plans",
    "mssql.settings",
    "mssql.system-health-deadlocks",
    "mssql.task-space",
    "mssql.worker-starvation",
];

pub(crate) fn policy(
    recipe_id: &str,
    recipe_version: u32,
    selector: RecipeSelector,
    budget: &CollectionBudget,
) -> Option<BuiltinRecipeSafety> {
    let purpose = match (recipe_id, selector) {
        ("mssql.activity", RecipeSelector::Collection(TelemetryCollection::Activity)) => RecipePurpose::Baseline,
        (
            "mssql.deadlock-history" | "mssql.system-health-deadlocks",
            RecipeSelector::Collection(TelemetryCollection::DeadlockHistory),
        ) => RecipePurpose::Baseline,
        ("mssql.settings", RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::Settings))) => {
            RecipePurpose::Baseline
        }
        ("mssql.blocking", RecipeSelector::FocusedBlocking)
        | ("mssql.cache-entry-memory", RecipeSelector::CacheEntryMemory)
        | (
            "mssql.current-execution-profile",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::CurrentExecutionProfile)),
        )
        | (
            "mssql.readable-secondary-recovery",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::ReadableSecondaryRecovery)),
        )
        | ("mssql.worker-starvation", RecipeSelector::WorkerStarvation)
        | ("mssql.focused-compiler-admission", RecipeSelector::FocusedCompilationAdmission)
        | ("mssql.focused-shared-compiler-admission", RecipeSelector::FocusedCompilationAdmission)
        | ("mssql.focused-request-memory", RecipeSelector::FocusedRequestMemory)
        | (
            "mssql.current-request-memory",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::CurrentRequestMemory)),
        )
        | ("mssql.focused-page-wait", RecipeSelector::FocusedPageWait)
        | ("mssql.focused-shared-tempdb-page-wait", RecipeSelector::FocusedPageWait)
        | ("mssql.focused-page-task-holders", RecipeSelector::FocusedPageTaskHolders)
        | ("mssql.focused-page-wait-writers", RecipeSelector::FocusedPageWaitWriters)
        | ("mssql.current-request-work", RecipeSelector::CurrentRequestWork)
        | (
            "mssql.retained-plans",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::RetainedPlans)),
        )
        | ("mssql.task-space", RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::TaskSpace)))
        | (
            "mssql.query-compilation",
            RecipeSelector::Collection(TelemetryCollection::Snapshot(SnapshotDataset::QueryCompilation)),
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
        include_str!("activity/current_execution_profile.rs"),
        include_str!("activity/readable_secondary_recovery.rs"),
        include_str!("activity/readable_secondary_recovery/redo.rs"),
        include_str!("cache_entry_memory.rs"),
        include_str!("cache_entry_memory/sql.rs"),
        include_str!("deadlocks.rs"),
        include_str!("deadlocks/native.rs"),
        include_str!("deadlocks/database.rs"),
        include_str!("deadlocks/system_health.rs"),
        include_str!("telemetry/mod.rs"),
        include_str!("telemetry/recipes.rs"),
        include_str!("telemetry/activity/mod.rs"),
        include_str!("telemetry/events.rs"),
        include_str!("telemetry/blocking.rs"),
        include_str!("telemetry/cache_entry_memory.rs"),
        include_str!("telemetry/current_execution_profile.rs"),
        include_str!("telemetry/readable_secondary_recovery.rs"),
        include_str!("telemetry/worker_starvation.rs"),
        include_str!("telemetry/compiler_admission.rs"),
        include_str!("telemetry/request_memory.rs"),
        include_str!("telemetry/page_waits.rs"),
        include_str!("telemetry/current_request_work.rs"),
        include_str!("telemetry/retained_definitions.rs"),
        include_str!("telemetry/task_space.rs"),
        include_str!("telemetry/compilation.rs"),
        include_str!("telemetry/streamed.rs"),
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
        include_str!("../../../culprit-core/src/telemetry/cache_entry_memory.rs"),
        include_str!("../../../culprit-core/src/telemetry/current_execution_profile.rs"),
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
        assert_eq!(policies.len(), BUILTIN_SAFETY_IDS.len());
        for (recipe, policy) in policies {
            assert!(recipe.is_valid());
            assert!(verify(policy).unwrap());
            assert!(BUILTIN_SAFETY_IDS.contains(&recipe.id.as_str()), "{}", recipe.id);
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
                    "selector" => changed.selector = RecipeSelector::PageWaitTasks,
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
                .filter(|recipe| !BUILTIN_SAFETY_IDS.contains(&recipe.id.as_str()))
                .all(|recipe| matches!(recipe.admission, RecipeAdmission::Unqualified))
        );
    }
}
