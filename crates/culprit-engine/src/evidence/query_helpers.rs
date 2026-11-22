//! Query assets learn only the selected recovery namespace through the ordinary paid metadata/session owner.
use super::{
    EvidenceService, EvidenceSession,
    session::{MetadataAccounting, SessionUsage},
};
use culprit_core::Result;
use culprit_evidence::{
    analytical::{
        evidence::{EvidenceReadBudget, EvidenceReadMetrics},
        query_helpers::{QueryHelperBinding, QueryHelperPolicy},
    },
    delivery::{object_store::ObjectRecoveryReader, recovery::RecoveryRead},
};
use culprit_read::cancellation::CollectionCancellation;
impl EvidenceService {
    pub async fn query_helper_binding(
        &self,
        policy: QueryHelperPolicy,
        budget: EvidenceReadBudget,
        cancellation: &CollectionCancellation,
    ) -> Result<QueryHelperBinding> {
        self.session(budget, cancellation)?.query_helper_binding(policy).await
    }
}
impl EvidenceSession<'_> {
    pub async fn query_helper_binding(&mut self, policy: QueryHelperPolicy) -> Result<QueryHelperBinding> {
        let policy = &policy;
        self.perform(
            |_| policy.validate(),
            |service, budget, fence| async move {
                let accounting = MetadataAccounting::new(
                    service.recovery.clone(),
                    budget.max_queries,
                    budget.max_proof_bytes,
                    fence.clone(),
                );
                let root = ObjectRecoveryReader::open(accounting.clone(), service.contract.clone()).await?;
                fence.check()?;
                let binding = QueryHelperBinding::new((*policy).clone(), &service.contract, root.id())?;
                let metrics = EvidenceReadMetrics { proof_bytes: accounting.work()?.bytes, ..Default::default() };
                Ok((binding, SessionUsage { metrics, candidate_rows: 0 }))
            },
        )
        .await
    }
}
#[cfg(test)]
mod tests;
