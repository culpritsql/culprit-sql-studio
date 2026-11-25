//! Source-owned safety admission is not a claim of measured database work or portable cost.
use super::*;

/// Structural policy identity only. The selected trusted registration must compare the exact source policy;
/// deserializing these fields, choosing a small budget or recognizing a provider name grants no admission.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct BuiltinRecipeSafety {
    pub id: String,
    pub version: u32,
    pub recipe_id: String,
    pub recipe_version: u32,
    pub purpose: RecipePurpose,
    pub selector: RecipeSelector,
    pub component: crate::ComponentVersion,
    pub templates_sha256: String,
    pub budget: CollectionBudget,
}
impl BuiltinRecipeSafety {
    /// The exact compiled template fingerprint binds a built-in reader even when its build has no Git revision.
    /// Target-bound measured receipts retain their separate, stricter component requirement.
    pub fn component_is_valid(component: &crate::ComponentVersion) -> bool {
        valid_key(&component.component)
            && component.version.as_deref().is_some_and(valid_key)
            && component.revision.as_deref().is_none_or(valid_key)
    }
    pub fn is_valid(&self) -> bool {
        valid_key(&self.id)
            && self.version > 0
            && valid_key(&self.recipe_id)
            && self.recipe_version > 0
            && Self::component_is_valid(&self.component)
            && valid_digest(&self.templates_sha256)
            && self.budget.validate().is_ok()
            && match self.purpose {
                RecipePurpose::Baseline => {
                    matches!(self.selector, RecipeSelector::Collection(collection)
                    if !collection.is_local_output())
                }
                RecipePurpose::Investigation => {
                    !matches!(self.selector, RecipeSelector::Collection(_))
                        || SourceReadQuestion::ALL.iter().any(|question| question.selector() == self.selector)
                }
            }
    }
    /// Structural scope matching only; the selected source verifier still owns policy authority.
    pub(super) fn matches_execution_scope(&self, scope: &RecipeCostScope) -> bool {
        self.is_valid()
            && self.purpose == RecipePurpose::Investigation
            && scope.is_valid()
            && scope.native_epoch().is_some()
            && scope.partition().is_qualified()
            && match scope {
                RecipeCostScope::Activity(_) => !matches!(self.selector, RecipeSelector::Collection(_)),
                RecipeCostScope::SharedResource { .. } => matches!(
                    self.selector,
                    RecipeSelector::FocusedPageWait | RecipeSelector::FocusedCompilationAdmission
                ),
                RecipeCostScope::NativeRead(execution) => SourceReadQuestion::ALL
                    .iter()
                    .any(|question| question.selector() == self.selector && execution.accepts_question(*question)),
            }
    }
    pub fn matches_recipe(&self, recipe: &RecipeDescriptor) -> bool {
        self.is_valid()
            && self.recipe_id == recipe.id
            && self.recipe_version == recipe.version
            && self.purpose == recipe.purpose
            && self.selector == recipe.selector
            && self.budget == recipe.budget
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        serde_json::to_vec(&("builtin-recipe-safety-v1", self))
            .map(|bytes| digest(&bytes))
            .map_err(|_| CollectionStop::InvalidPage.error())
    }
}

/// Native measurements and built-in safety are deliberately different authorities. A built-in question carries
/// its exact authenticated ORIGINAL scope; its policy does not contain or invent a portable native epoch.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum RecipeExecutionAdmission {
    Measured { receipt: Box<RecipeCostReceipt> },
    BuiltinSafety { policy: Box<BuiltinRecipeSafety>, scope: Box<RecipeCostScope>, budget: CollectionBudget },
}
impl From<RecipeCostReceipt> for RecipeExecutionAdmission {
    fn from(receipt: RecipeCostReceipt) -> Self {
        Self::Measured { receipt: Box::new(receipt) }
    }
}
impl RecipeExecutionAdmission {
    pub fn for_recipe(
        recipe: &RecipeDescriptor,
        original: &DiagnosticScope,
        caller: &CollectionBudget,
    ) -> Result<Self> {
        Self::for_scope(recipe, RecipeCostScope::Activity(original.clone()), caller)
    }
    pub fn for_source_read(
        recipe: &RecipeDescriptor,
        execution: &SourceReadExecutionScope,
        caller: &CollectionBudget,
    ) -> Result<Self> {
        Self::for_scope(recipe, RecipeCostScope::NativeRead(execution.clone()), caller)
    }
    pub fn for_shared(
        recipe: &RecipeDescriptor,
        owner: &CollectionDescription,
        caller: &CollectionBudget,
    ) -> Result<Self> {
        let CollectionTarget::Shared(target) = &owner.target else {
            return Err(CollectionStop::InvalidPage.error());
        };
        let scope = RecipeCostScope::SharedResource {
            target: target.clone(),
            partition: owner.partition.clone(),
            source: owner.source.clone(),
        };
        Self::for_scope(recipe, scope, caller)
    }
    fn for_scope(recipe: &RecipeDescriptor, scope: RecipeCostScope, caller: &CollectionBudget) -> Result<Self> {
        let value = match &recipe.admission {
            RecipeAdmission::Measured(receipt) => Self::from(receipt.as_ref().clone()),
            RecipeAdmission::BuiltinSafety(policy)
                if policy.matches_recipe(recipe) && policy.purpose == RecipePurpose::Investigation =>
            {
                Self::BuiltinSafety {
                    policy: policy.clone(),
                    scope: Box::new(scope),
                    budget: policy.budget.intersection(caller),
                }
            }
            _ => return Err(CollectionStop::InvalidPage.error()),
        };
        if !value.is_valid() || value.budget().intersection(caller) != *value.budget() {
            return Err(CollectionStop::InvalidPage.error());
        }
        Ok(value)
    }
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Measured { receipt } => receipt.is_valid(),
            Self::BuiltinSafety { policy, scope, budget } => {
                policy.is_valid()
                    && budget.validate().is_ok()
                    && budget.intersection(&policy.budget) == *budget
                    && policy.matches_execution_scope(scope)
            }
        }
    }
    pub fn matches_recipe(&self, recipe: &RecipeDescriptor) -> bool {
        match (self, &recipe.admission) {
            (Self::Measured { receipt }, RecipeAdmission::Measured(selected)) => {
                receipt.matches_recipe(recipe)
                    && receipt.key().is_ok_and(|key| selected.key().is_ok_and(|selected| key == selected))
            }
            (Self::BuiltinSafety { policy, .. }, RecipeAdmission::BuiltinSafety(selected)) => {
                policy == selected && policy.matches_recipe(recipe)
            }
            _ => false,
        }
    }
    pub fn measured(&self) -> Option<&RecipeCostReceipt> {
        match self {
            Self::Measured { receipt } => Some(receipt),
            Self::BuiltinSafety { .. } => None,
        }
    }
    pub fn measured_mut(&mut self) -> Option<&mut RecipeCostReceipt> {
        match self {
            Self::Measured { receipt } => Some(receipt),
            Self::BuiltinSafety { .. } => None,
        }
    }
    pub fn scope(&self) -> &RecipeCostScope {
        match self {
            Self::Measured { receipt } => &receipt.scope,
            Self::BuiltinSafety { scope, .. } => scope,
        }
    }
    pub fn component(&self) -> &crate::ComponentVersion {
        match self {
            Self::Measured { receipt } => &receipt.component,
            Self::BuiltinSafety { policy, .. } => &policy.component,
        }
    }
    pub fn budget(&self) -> &CollectionBudget {
        match self {
            Self::Measured { receipt } => &receipt.tested_budget,
            Self::BuiltinSafety { budget, .. } => budget,
        }
    }
    pub fn purpose(&self) -> RecipePurpose {
        match self {
            Self::Measured { receipt } => receipt.purpose,
            Self::BuiltinSafety { policy, .. } => policy.purpose,
        }
    }
    pub fn selector(&self) -> RecipeSelector {
        match self {
            Self::Measured { receipt } => receipt.selector,
            Self::BuiltinSafety { policy, .. } => policy.selector,
        }
    }
    pub fn recipe_id(&self) -> &str {
        match self {
            Self::Measured { receipt } => &receipt.recipe_id,
            Self::BuiltinSafety { policy, .. } => &policy.recipe_id,
        }
    }
    pub fn recipe_version(&self) -> u32 {
        match self {
            Self::Measured { receipt } => receipt.recipe_version,
            Self::BuiltinSafety { policy, .. } => policy.recipe_version,
        }
    }
    pub fn cost_evidence(&self) -> Option<&DiagnosticEvidenceRef> {
        self.measured().map(|receipt| &receipt.evidence)
    }
    pub fn key(&self) -> Result<String> {
        if !self.is_valid() {
            return Err(CollectionStop::InvalidPage.error());
        }
        match self {
            Self::Measured { receipt } => receipt.key(),
            Self::BuiltinSafety { .. } => serde_json::to_vec(&("builtin-execution-admission-v1", self))
                .map(|bytes| digest(&bytes))
                .map_err(|_| CollectionStop::InvalidPage.error()),
        }
    }
}
