//! One native registration catalog owns declarations, dispatch and shared-resource routes, without source I/O.
use super::*;

#[derive(Clone, Copy)]
enum Route {
    Activity,
    Blocking,
    Jobs,
    MaintenanceProgress,
    CurrentRequestWork,
    TransactionMetadata,
    ReadableSecondaryRecovery,
    Snapshot(Recipe),
}
impl Route {
    fn collection(self) -> TelemetryCollection {
        match self {
            Self::Jobs => TelemetryCollection::Snapshot(SnapshotDataset::Jobs),
            Self::Activity => TelemetryCollection::Activity,
            Self::Blocking => TelemetryCollection::Activity,
            Self::MaintenanceProgress => TelemetryCollection::Snapshot(SnapshotDataset::MaintenanceProgress),
            Self::CurrentRequestWork => TelemetryCollection::Snapshot(SnapshotDataset::CurrentRequestWork),
            Self::TransactionMetadata => TelemetryCollection::Snapshot(SnapshotDataset::TransactionMetadata),
            Self::ReadableSecondaryRecovery => {
                TelemetryCollection::Snapshot(SnapshotDataset::ReadableSecondaryRecovery)
            }
            Self::Snapshot(recipe) => TelemetryCollection::Snapshot(recipe.dataset()),
        }
    }
    fn selector(self) -> RecipeSelector {
        match self {
            Self::MaintenanceProgress => RecipeSelector::MaintenanceProgress,
            Self::CurrentRequestWork => RecipeSelector::CurrentRequestWork,
            Self::Blocking => RecipeSelector::FocusedBlocking,
            _ => RecipeSelector::Collection(self.collection()),
        }
    }
    fn bounded_budget(self, requested: &CollectionBudget) -> CollectionBudget {
        match self {
            Self::Jobs => crate::native::jobs::bounded_budget(requested),
            Self::Activity => activity::bounded_budget(requested),
            Self::Blocking => blocking::bounded_budget(requested),
            Self::MaintenanceProgress => crate::progress::bounded_budget(requested),
            Self::CurrentRequestWork => crate::progress::current_work_budget(requested),
            Self::TransactionMetadata => transaction_metadata::bounded_budget(requested),
            Self::ReadableSecondaryRecovery => readable_secondary_recovery::bounded_budget(requested),
            Self::Snapshot(Recipe::Settings) => settings_profile::bounded_budget(requested),
            Self::Snapshot(Recipe::Statements) => statement_profile::bounded_budget(requested),
            Self::Snapshot(Recipe::Definitions) => statement_profile::definitions_budget(requested),
            Self::Snapshot(_) => requested.clone(),
        }
    }
    fn supports_profile(self) -> bool {
        matches!(self, Self::Activity | Self::Snapshot(Recipe::Settings | Recipe::Statements))
    }
    fn open_profile(
        self,
        connector: Connector,
        request: &CollectionRequest,
        profile: &SourceProfileAdmission,
    ) -> Result<Box<dyn TelemetrySource>> {
        match self {
            Self::Activity => activity::profile_source(connector, request, profile),
            Self::Snapshot(recipe @ (Recipe::Settings | Recipe::Statements)) => {
                request.subject.require_database()?;
                if self.bounded_budget(&request.budget) != request.budget {
                    return Err(native::invalid());
                }
                let guard = culprit_read::SourceProfileGuard::new(request, profile)?;
                snapshot_source_with_profile(connector, request, recipe, Some(guard))
            }
            _ => Err(unsupported()),
        }
    }
    fn open(self, connector: Connector, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        match self {
            Self::Jobs => crate::jobs::source(connector, request),
            Self::Activity => activity::source(connector, request),
            Self::TransactionMetadata => transaction_metadata::source(connector, request),
            Self::ReadableSecondaryRecovery => readable_secondary_recovery::source(connector, request),
            Self::Blocking => blocking::source(connector, request),
            Self::MaintenanceProgress | Self::CurrentRequestWork => crate::progress::source(connector, request),
            Self::Snapshot(recipe) => snapshot_source(connector, request, recipe),
        }
    }
}

struct Registration {
    id: &'static str,
    version: u32,
    purpose: RecipePurpose,
    subject: RecipeSubject,
    route: Route,
}
impl Registration {
    const fn snapshot(id: &'static str, recipe: Recipe) -> Self {
        Self {
            id,
            version: 1,
            purpose: RecipePurpose::Investigation,
            subject: RecipeSubject::Database,
            route: Route::Snapshot(recipe),
        }
    }
    const fn baseline(mut self) -> Self {
        self.purpose = RecipePurpose::Baseline;
        self
    }
    const fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }
    const fn shared(mut self, kind: SharedResourceKind) -> Self {
        self.subject = RecipeSubject::Shared(kind);
        self
    }
    fn descriptor(&self) -> RecipeDescriptor {
        RecipeDescriptor {
            id: self.id.into(),
            version: self.version,
            purpose: self.purpose,
            selector: self.route.selector(),
            subjects: vec![self.subject],
            budget: self.native_budget(),
            admission: crate::builtin_safety::policy(
                self.id,
                self.version,
                self.route.selector(),
                &self.native_budget(),
            )
            .map(|policy| RecipeAdmission::BuiltinSafety(Box::new(policy)))
            .unwrap_or(RecipeAdmission::Unqualified),
        }
    }
    fn accepts(&self, request: &CollectionRequest) -> bool {
        self.route.selector().matches(request) && self.subject.accepts(&request.subject)
    }
    fn native_budget(&self) -> CollectionBudget {
        self.route.bounded_budget(&CollectionBudget::upper_limits())
    }
    fn bounded_request(&self, request: &CollectionRequest) -> CollectionRequest {
        let mut request = request.clone();
        request.budget = request.budget.intersection(&self.native_budget());
        request
    }
}

const REGISTRATIONS: &[Registration] = &[
    Registration {
        id: "postgres.readable-secondary-recovery",
        version: 1,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::ReadableSecondaryRecovery,
    },
    Registration {
        id: "postgres.transaction-metadata",
        version: 2,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::TransactionMetadata,
    },
    Registration {
        id: "postgres.timescale-jobs",
        version: 3,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::Jobs,
    },
    Registration {
        id: "postgres.activity",
        version: 4,
        purpose: RecipePurpose::Baseline,
        subject: RecipeSubject::Database,
        route: Route::Activity,
    },
    Registration {
        id: "postgres.maintenance-progress",
        version: 2,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::MaintenanceProgress,
    },
    Registration {
        id: "postgres.current-request-work",
        version: 2,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::CurrentRequestWork,
    },
    Registration {
        id: "postgres.blocking",
        version: 4,
        purpose: RecipePurpose::Investigation,
        subject: RecipeSubject::Database,
        route: Route::Blocking,
    },
    Registration::snapshot("postgres.statement-counters", Recipe::Statements).version(3).baseline(),
    Registration::snapshot("postgres.statement-definitions", Recipe::Definitions).version(2),
    Registration::snapshot("postgres.operational", Recipe::Operational).version(2).baseline(),
    Registration::snapshot("postgres.instance-operational", Recipe::InstanceOperational)
        .shared(SharedResourceKind::Instance)
        .baseline(),
    Registration::snapshot("postgres.settings", Recipe::Settings).version(2).baseline(),
    Registration::snapshot("postgres.tables", Recipe::Tables).baseline(),
    Registration::snapshot("postgres.indexes", Recipe::Indexes).baseline(),
    Registration::snapshot("postgres.replication", Recipe::Replication).baseline(),
    Registration::snapshot("postgres.sequences", Recipe::Sequences).baseline(),
    Registration::snapshot("postgres.cleanup", Recipe::Cleanup).baseline(),
    Registration::snapshot("postgres.columns", Recipe::Catalog(native::catalog::Kind::Columns)),
    Registration::snapshot("postgres.foreign-keys", Recipe::Catalog(native::catalog::Kind::ForeignKeys)),
    Registration::snapshot("postgres.check-constraints", Recipe::Catalog(native::catalog::Kind::Checks)),
    Registration::snapshot("postgres.objects", Recipe::Catalog(native::catalog::Kind::Objects)),
    Registration::snapshot("postgres.triggers", Recipe::Catalog(native::catalog::Kind::Triggers)),
    Registration::snapshot("postgres.partitions", Recipe::Catalog(native::catalog::Kind::Partitions)),
    Registration::snapshot("postgres.statistics", Recipe::Catalog(native::catalog::Kind::Statistics)),
];

pub(crate) fn profile_selectors() -> Vec<RecipeSelector> {
    REGISTRATIONS
        .iter()
        .filter(|registration| registration.purpose == RecipePurpose::Baseline && registration.route.supports_profile())
        .map(|registration| registration.route.selector())
        .collect()
}
pub(crate) fn profile_source(
    connector: Connector,
    request: &CollectionRequest,
    profile: &SourceProfileAdmission,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    let registration = REGISTRATIONS
        .iter()
        .find(|registration| registration.purpose == RecipePurpose::Baseline && registration.accepts(request))
        .ok_or_else(unsupported)?;
    registration.route.open_profile(connector, request, profile)
}

pub(crate) fn declarations() -> Vec<RecipeDescriptor> {
    REGISTRATIONS.iter().map(Registration::descriptor).collect()
}

pub(super) fn collections_for(resource: &SharedResourceIdentity) -> Vec<TelemetryCollection> {
    if resource.engine != culprit_core::Engine::Postgres {
        return Vec::new();
    }
    let subject = CollectionSubject::Shared { resource: resource.clone() };
    REGISTRATIONS
        .iter()
        .filter(|registration| registration.subject.accepts(&subject))
        .map(|registration| registration.route.collection())
        .collect()
}

pub(crate) fn open(connector: Connector, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    if matches!(&request.subject, CollectionSubject::Shared { resource } if resource.engine != culprit_core::Engine::Postgres)
    {
        return Err(unsupported());
    }
    let registration =
        REGISTRATIONS.iter().find(|registration| registration.accepts(request)).ok_or_else(unsupported)?;
    let bounded = registration.bounded_request(request);
    bounded.budget.validate()?;
    registration.route.open(connector, &bounded)
}

fn unsupported() -> Error {
    Error::new(ErrorCode::Unsupported, culprit_core::t!("collection.unsupported"))
}

#[cfg(test)]
mod tests;
