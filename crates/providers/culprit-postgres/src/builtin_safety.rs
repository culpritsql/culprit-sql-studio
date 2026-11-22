//! Validated built-in safety for existing native metadata readers, not measured CPU or cost receipts.
use culprit_core::{Result, telemetry::*};

pub(crate) fn policy(
    recipe_id: &str,
    recipe_version: u32,
    selector: RecipeSelector,
    budget: &CollectionBudget,
) -> Option<BuiltinRecipeSafety> {
    let purpose = match (recipe_id, selector) {
        ("postgres.activity", RecipeSelector::Collection(TelemetryCollection::Activity)) => RecipePurpose::Baseline,
        ("postgres.blocking", RecipeSelector::FocusedBlocking) => RecipePurpose::Investigation,
        _ => return None,
    };
    Some(BuiltinRecipeSafety {
        id: format!("{recipe_id}.native-18-safety"),
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
        include_str!("native/activity.rs"),
        include_str!("native/activity/transaction_metadata.rs"),
        include_str!("native/activity/blocking.rs"),
        include_str!("native/activity/blocking/relation_locks.rs"),
        include_str!("native/activity/blocking/relation_locks/sql.rs"),
        include_str!("../../../culprit-core/src/blocking/native_lock.rs"),
        include_str!("native/local_context.rs"),
        include_str!("native/metadata.rs"),
        include_str!("read.rs"),
        include_str!("read/application.rs"),
        include_str!("read/owned.rs"),
        include_str!("read/response.rs"),
        include_str!("rows.rs"),
        include_str!("telemetry/activity.rs"),
        include_str!("telemetry/blocking.rs"),
    ];
    // Length separators distinguish concatenations. This is the compiled source/reader policy identity;
    // native major-version availability is checked independently before every population query.
    let mut bytes = Vec::new();
    for source in sources {
        bytes.extend_from_slice(&(source.len() as u64).to_le_bytes());
        bytes.extend_from_slice(source.as_bytes());
    }
    digest(&bytes)
}
pub fn verify(policy: &BuiltinRecipeSafety) -> Result<bool> {
    Ok(crate::telemetry::declarations().iter().any(|recipe| {
        policy.matches_recipe(recipe)
            && matches!(&recipe.admission, RecipeAdmission::BuiltinSafety(selected) if selected.as_ref() == policy)
    }))
}
pub(crate) fn verify_native(policy: &BuiltinRecipeSafety, version: u32) -> Result<()> {
    // Only the actual owned/tested major is eligible; ordinary explicit reads retain their existing support.
    if version / 10_000 != 18 || !verify(policy)? {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_the_two_exact_reader_policies_are_source_validated_and_neither_is_measured() {
        let recipes = crate::telemetry::declarations();
        let policies = recipes
            .iter()
            .filter_map(|recipe| match &recipe.admission {
                RecipeAdmission::BuiltinSafety(policy) => Some((recipe, policy)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(policies.len(), 2);
        for (recipe, policy) in policies {
            assert!(recipe.is_valid());
            assert!(verify(policy).unwrap());
            assert!(matches!(recipe.id.as_str(), "postgres.activity" | "postgres.blocking"));
            assert!(verify_native(policy, 180006).is_ok());
            for version in [170009, 190000] {
                assert!(verify_native(policy, version).is_err());
            }
            for fault in ["template", "component", "budget", "selector", "version", "recipe"] {
                let mut changed = policy.as_ref().clone();
                match fault {
                    "template" => changed.templates_sha256 = digest(b"not-the-reader"),
                    "component" => changed.component.version.as_mut().unwrap().push_str("-foreign"),
                    "budget" => changed.budget.max_queries += 1,
                    "selector" => changed.selector = RecipeSelector::CurrentRequestWork,
                    "version" => changed.version += 1,
                    "recipe" => changed.recipe_id = "postgres.current-request-work".into(),
                    _ => unreachable!(),
                }
                assert!(!verify(&changed).unwrap(), "{fault}");
            }
        }
        assert!(
            recipes
                .iter()
                .filter(|recipe| !matches!(recipe.id.as_str(), "postgres.activity" | "postgres.blocking"))
                .all(|recipe| matches!(recipe.admission, RecipeAdmission::Unqualified))
        );
    }
}
