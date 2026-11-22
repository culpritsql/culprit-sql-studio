use super::streamed::{SnapshotAcquisition, SnapshotRecipe, limit_parameter};
use crate::{
    read::{NativeRead, Parameter, invalid_data},
    retained_definitions,
    value::Rec,
};
use culprit_core::{Result, Source, telemetry::*};
pub(super) struct RetainedPlans;
const MAX_RETAINED_STATEMENTS: u32 = 64;
// A boxed/MI read uses three initial and three final authority projections, versus two and one on Azure SQL.
// Layout, two admission checks, one payload stream and binding check complete twelve reader calls.
// The same caller total also reserves the owned session SET; stricter callers cannot spend that slot twice.
const MAX_SOURCE_QUERIES: u32 = 13;
const MAX_STATEMENT_MS: u32 = 1_000;
const MAX_SOURCE_MS: u32 = 5_000;
const MAX_PAGE_BYTES: u32 = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: u32 = 64 * 1024 * 1024;
const MAX_PLAN_BYTES: u32 = 2 * 1024 * 1024;

/// Provider ceilings are admission limits, not measured native cost or permission to run automatically.
pub(super) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    caller.intersection(&CollectionBudget {
        page_rows: MAX_RETAINED_STATEMENTS,
        max_snapshot_rows: MAX_RETAINED_STATEMENTS,
        max_queries: MAX_SOURCE_QUERIES,
        statement_timeout_ms: MAX_STATEMENT_MS,
        total_timeout_ms: MAX_SOURCE_MS,
        page_bytes: MAX_PAGE_BYTES,
        total_bytes: MAX_TOTAL_BYTES,
        max_plan_bytes: MAX_PLAN_BYTES,
        max_sql_bytes: crate::actual::retained::MAX_STATEMENT_BYTES as u32,
        ..CollectionBudget::upper_limits()
    })
}
pub(super) struct Layout {
    native: retained_definitions::Layout,
    bindings: crate::relational_inputs::Bindings,
    selection: Option<CachedStatementContext>,
}
#[async_trait::async_trait]
impl SnapshotRecipe for RetainedPlans {
    type Layout = Layout;
    const ACQUISITION: SnapshotAcquisition = SnapshotAcquisition::KnownReadableDatabaseCache;
    const DATASET: SnapshotDataset = SnapshotDataset::RetainedPlans;
    const NAME: &'static str = "retained-plans-state";
    const SOURCE: &'static Source = &crate::sources::RETAINED_PLANS;
    const SOURCE_READ: Option<SourceReadQuestion> = Some(SourceReadQuestion::RelationalInputContext);
    async fn layout<R: NativeRead>(&self, reader: &mut R, budget: &CollectionBudget) -> Result<Option<Self::Layout>> {
        Ok(retained_definitions::layout(reader, budget).await?.map(|native| Layout {
            native,
            bindings: Default::default(),
            selection: None,
        }))
    }
    fn select(&self, layout: &mut Self::Layout, request: &CollectionRequest) -> Result<()> {
        layout.selection = None;
        if let CollectionSelection::SourceRead { focus, .. } = &request.selection {
            let SourceReadPrerequisite::ObservedCurrentRequestPlan { context, .. } = &focus.prerequisite else {
                return Err(invalid_data());
            };
            layout.selection = Some(context.as_ref().clone());
        }
        Ok(())
    }
    fn query(&self, layout: &Self::Layout) -> String {
        retained_definitions::acquisition_sql_for(&layout.native, layout.selection.is_some())
    }
    fn parameters(&self, layout: &Self::Layout, limit: usize) -> Result<Vec<Parameter>> {
        let mut parameters = limit_parameter(limit);
        if let Some(context) = &layout.selection {
            parameters.extend([
                Parameter::Text(context.identity.plan_handle.clone()),
                Parameter::Text(context.identity.sql_handle.clone()),
                Parameter::Integer(context.identity.statement_start_bytes.into()),
                Parameter::Integer(context.identity.statement_end_bytes.into()),
            ]);
        }
        Ok(parameters)
    }
    fn decode(&self, row: &Rec, layout: &Self::Layout, source: &CollectionDescription) -> Result<TelemetryRecord> {
        let context = crate::cached_statements::context(row)?;
        if layout.selection.as_ref().is_some_and(|selected| !selected.matches_locator(&context)) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let admitted = layout.bindings.contains(&context);
        let mut definition = retained_definitions::decode(row, &layout.native, admitted.then_some(source))?;
        if !admitted {
            definition.batch_plan.payload = DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected };
            definition.module_parameters = None;
            definition.current_type_schema = None;
            definition.relational_input_proof = None;
            definition.parameter_context_proof = None;
        }
        let record = ObjectDefinitionRecord::new(
            Self::SOURCE.name.into(),
            ObjectDefinitionFact::MssqlRetainedPlan(Box::new(definition)),
        )
        .inspect_err(|_error| {
            #[cfg(test)]
            retained_definitions::diagnostics::report("record", row, Some(source), None, _error);
        })?;
        Ok(TelemetryRecord::ObjectDefinition(record))
    }
    fn complete_record_scope(&self, record: &TelemetryRecord) -> bool {
        matches!(record,TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {fact:ObjectDefinitionFact::MssqlRetainedPlan(definition),..}) if definition.context.identity.has_plan_handle() && !matches!(definition.batch_plan.payload,DefinitionPayload::Unavailable{reason:PayloadUnavailable::NotCollected}))
    }
    fn observe_batch(&self, rows: &[Rec], layout: &mut Self::Layout) -> Result<()> {
        layout.bindings.observe(rows)
    }
    async fn recheck<R: NativeRead>(&self, reader: &mut R, layout: &Self::Layout) -> Result<()> {
        layout.bindings.require_verified(reader).await
    }
}
