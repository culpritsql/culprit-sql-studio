//! Test-only adapter over the exact selected production profile, with no custom policy or native reader.
use super::*;
use culprit_collector::capture::SourceFactory;
use culprit_core::Provider;
use culprit_read::cancellation::CollectionCancellation;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(super) struct Factory {
    pub(super) provider: crate::PostgresProvider,
    pub(super) admission: SourceProfileAdmission,
}
#[async_trait::async_trait]
impl SourceFactory for Factory {
    async fn open(&self, request: &CollectionRequest) -> Result<Box<dyn TelemetrySource>> {
        self.provider.telemetry_source_for_profile(request, &self.admission)
    }
}
pub(super) fn admission(request: &CollectionRequest, active: Arc<AtomicBool>) -> SourceProfileAdmission {
    let selected = crate::telemetry_recipes().into_iter().find(|recipe| recipe.selector.matches(request)).unwrap();
    let RecipeAdmission::BuiltinSafety(policy) = selected.admission else {
        panic!("apply the separate normal snapshot policy promotion locally before native acceptance")
    };
    assert!(crate::verify_builtin_safety(&policy).unwrap());
    let admission = SourceProfileAdmission::for_builtin(
        digest(b"owned-native-snapshot-route"),
        *policy,
        None,
        request.budget.clone(),
        move || active.load(Ordering::Acquire),
    )
    .unwrap();
    assert!(admission.qualification().is_none(), "source work cost remains unmeasured");
    admission
}
pub(super) async fn specification(cancellation: &CollectionCancellation) -> culprit_core::ConnectionSpec {
    crate::native_test_support::case::require_active(cancellation).unwrap();
    let specification = crate::native_test_support::connection_spec();
    crate::native_test_support::specification(specification.database.as_deref().unwrap(), CollectionBudget::default())
        .await
}
