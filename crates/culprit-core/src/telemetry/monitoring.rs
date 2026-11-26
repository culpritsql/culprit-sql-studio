//! Standard enrollment is policy; only a selected registration and authenticated native evidence grant admission.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MonitoringPolicy {
    Explicit,
    Standard {
        #[serde(default)]
        budget: CollectionBudget,
    },
}
impl Default for MonitoringPolicy {
    fn default() -> Self {
        Self::Standard { budget: CollectionBudget::default() }
    }
}
impl MonitoringPolicy {
    pub fn validate(&self, inventory: &FleetInventory, shard: &str) -> Result<()> {
        match self {
            Self::Explicit => inventory.validate(shard),
            Self::Standard { budget } => {
                budget.validate()?;
                inventory.validate_assignments(shard)
            }
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MonitoringGapReason {
    Undeclared,
    Unqualified,
    MissingApproval,
    AdmissionUnsupported,
    RequiresSelectedTarget,
    AdditionalWorkUnqualified,
    InfeasibleCadence,
    ScheduleCapacity,
    ConflictingSchedule,
    SharedOwnerAlreadyScheduled,
    SharedAdmissionConflict,
    Capacity,
}

/// Configured logical labels only. Native proof, identifiers, SQL and credentials never enter enrollment output.
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MonitoringGap {
    pub target: String,
    pub recipe_id: Option<String>,
    pub reason: MonitoringGapReason,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MonitoringScheduleSummary {
    pub target: String,
    pub recipe_id: String,
    pub every_seconds: u32,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MonitoringEnrollmentSummary {
    pub assigned_targets: u32,
    pub explicit_schedules: u32,
    pub automatic: Vec<MonitoringScheduleSummary>,
    pub gaps: Vec<MonitoringGap>,
    pub omitted_gaps: u32,
}

/// A structural request for one approved source profile, independent of any episode or affected request.
/// It is deliberately not serializable or constructible by deserializing configuration. The selected provider
/// registration still authenticates the installed original approval before native construction and must bind
/// this expectation inside the existing native reader before its source work, including the final recheck.
#[derive(Clone)]
pub struct SourceProfileAdmission {
    route_revision: String,
    approval: SourceProfileApproval,
    budget: CollectionBudget,
    approval_is_active: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
    retired: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
#[derive(Clone)]
enum SourceProfileApproval {
    Measured(Box<RecipeCostReceipt>),
    Builtin { policy: Box<BuiltinRecipeSafety>, scope: Option<Box<RecipeCostScope>> },
}
impl SourceProfileAdmission {
    pub fn new(
        route_revision: String,
        qualification: RecipeCostReceipt,
        budget: CollectionBudget,
        approval_is_active: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        budget.validate()?;
        if !valid_digest(&route_revision)
            || !qualification.is_valid()
            || qualification.purpose != RecipePurpose::Baseline
            || budget.intersection(&qualification.tested_budget) != budget
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        Ok(Self {
            route_revision,
            approval: SourceProfileApproval::Measured(Box::new(qualification)),
            budget,
            approval_is_active: std::sync::Arc::new(approval_is_active),
            retired: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    /// Observed retirement is permanent for this original profile and all its clones. A later approval needs a
    /// new admitted profile; a callback returning true cannot revive a retired source lifetime.
    /// Liveness is necessary, not authority. Only the selected registration plus authenticated issued original
    /// may supply this check; a caller-created callback or structurally valid receipt grants no permission.
    pub fn approval_is_active(&self) -> bool {
        use std::sync::atomic::Ordering;
        if self.retired.load(Ordering::Acquire) {
            return false;
        }
        if !(self.approval_is_active)() {
            self.retired.store(true, Ordering::Release);
        }
        !self.retired.load(Ordering::Acquire)
    }
    pub fn route_revision(&self) -> &str {
        &self.route_revision
    }
    pub fn for_builtin(
        route_revision: String,
        policy: BuiltinRecipeSafety,
        scope: Option<RecipeCostScope>,
        budget: CollectionBudget,
        approval_is_active: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<Self> {
        budget.validate()?;
        if !valid_digest(&route_revision)
            || !policy.is_valid()
            || budget.intersection(&policy.budget) != budget
            || match policy.purpose {
                RecipePurpose::Baseline => scope.is_some(),
                RecipePurpose::Investigation => {
                    !scope.as_ref().is_some_and(|scope| policy.matches_execution_scope(scope))
                }
            }
        {
            return Err(CollectionStop::InvalidPage.error());
        }
        Ok(Self {
            route_revision,
            approval: SourceProfileApproval::Builtin { policy: Box::new(policy), scope: scope.map(Box::new) },
            budget,
            approval_is_active: std::sync::Arc::new(approval_is_active),
            retired: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    pub fn qualification(&self) -> Option<&RecipeCostReceipt> {
        match &self.approval {
            SourceProfileApproval::Measured(receipt) => Some(receipt),
            _ => None,
        }
    }
    pub fn builtin_policy(&self) -> Option<&BuiltinRecipeSafety> {
        match &self.approval {
            SourceProfileApproval::Builtin { policy, .. } => Some(policy),
            _ => None,
        }
    }
    pub fn scope(&self) -> Option<&RecipeCostScope> {
        match &self.approval {
            SourceProfileApproval::Measured(receipt) => Some(&receipt.scope),
            SourceProfileApproval::Builtin { scope, .. } => scope.as_deref(),
        }
    }
    pub fn selector(&self) -> RecipeSelector {
        match &self.approval {
            SourceProfileApproval::Measured(receipt) => receipt.selector,
            SourceProfileApproval::Builtin { policy, .. } => policy.selector,
        }
    }
    pub fn matches_recipe(&self, recipe: &RecipeDescriptor) -> bool {
        self.budget.intersection(&recipe.budget) == self.budget
            && match (&self.approval, &recipe.admission) {
                (SourceProfileApproval::Measured(receipt), RecipeAdmission::Measured(selected)) => {
                    receipt.key().is_ok_and(|key| selected.key().is_ok_and(|selected| key == selected))
                }
                (SourceProfileApproval::Builtin { policy, .. }, RecipeAdmission::BuiltinSafety(selected)) => {
                    policy == selected && policy.matches_recipe(recipe)
                }
                _ => false,
            }
    }
    pub fn matches_request(&self, request: &CollectionRequest) -> bool {
        self.approval_is_active()
            && request.validate().is_ok()
            && self.budget == request.budget
            && self.selector().matches(request)
            && self.matches_native_selection(request)
            && match &self.approval {
                SourceProfileApproval::Measured(receipt) => receipt.scope.subject() == request.subject,
                SourceProfileApproval::Builtin { scope: Some(scope), .. } => scope.subject() == request.subject,
                SourceProfileApproval::Builtin { scope: None, .. } => {
                    matches!(request.subject, CollectionSubject::Database | CollectionSubject::Shared { .. })
                }
            }
    }
    fn matches_native_selection(&self, request: &CollectionRequest) -> bool {
        let purpose = match &self.approval {
            SourceProfileApproval::Measured(receipt) => receipt.purpose,
            SourceProfileApproval::Builtin { policy, .. } => policy.purpose,
        };
        match (&request.selection, purpose, self.scope()) {
            (
                CollectionSelection::SourceRead { focus, execution },
                RecipePurpose::Investigation,
                Some(RecipeCostScope::NativeRead(scope)),
            ) => {
                execution.as_ref() == scope
                    && scope.accepts_question(focus.question)
                    && scope.matches_target(&focus.target)
            }
            (CollectionSelection::SourceRead { .. }, _, _)
            | (_, RecipePurpose::Investigation, Some(RecipeCostScope::NativeRead(_))) => false,
            _ => true,
        }
    }
    /// The first actual local member/role is frozen by SourceProfileGuard. A policy supplies no native incarnation.
    pub fn matches_native_context(&self, context: &NativeReadContext) -> bool {
        if !self.approval_is_active() || !context.is_valid() || !context.partition.is_qualified() {
            return false;
        }
        match &self.approval {
            SourceProfileApproval::Measured(receipt) => match &receipt.scope {
                RecipeCostScope::NativeRead(scope) => scope.matches_context(context),
                RecipeCostScope::SharedResource { target, partition, .. } => {
                    context.target == CollectionTarget::Shared(target.clone()) && context.partition == *partition
                }
                RecipeCostScope::Activity(_) => false,
            },
            SourceProfileApproval::Builtin { scope, .. } => match scope.as_deref() {
                Some(RecipeCostScope::Activity(scope)) => {
                    context.has_qualified_local_owner()
                        && context.database_writable.is_some()
                        && context.target == scope.target
                        && context.partition == scope.partition
                        && context.partition.native_epoch() == scope.native_epoch.as_deref()
                }
                Some(RecipeCostScope::NativeRead(scope)) => scope.matches_context(context),
                Some(RecipeCostScope::SharedResource { target, partition, .. }) => {
                    context.has_qualified_local_owner()
                        && context.database_writable.is_none()
                        && context.target == CollectionTarget::Shared(target.clone())
                        && context.partition == *partition
                }
                None => match &context.target {
                    CollectionTarget::Database(database) => {
                        (context.has_qualified_local_owner() && context.database_writable.is_some())
                            || (context.partition == SourcePartition::Target
                                && context.database_writable == Some(true)
                                && database.incarnation.is_some())
                    }
                    CollectionTarget::Shared(owner) => {
                        context.database_writable.is_none()
                            && match owner.kind {
                                SharedResourceKind::Instance => {
                                    context.has_qualified_local_owner()
                                        && matches!(
                                            &context.partition,
                                            SourcePartition::InstanceLocal { member: Some(member), .. }
                                                if member == owner
                                        )
                                }
                                SharedResourceKind::Pool => context.partition == SourcePartition::Target,
                                _ => false,
                            }
                    }
                    CollectionTarget::ConfiguredConnection(_) => false,
                },
            },
        }
    }
}

impl std::fmt::Debug for SourceProfileAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceProfileAdmission")
            .field("selector", &self.selector())
            .field("approval_active", &self.approval_is_active())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod config_tests;
