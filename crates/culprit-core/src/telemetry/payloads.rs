//! One inventory of definition payloads and their locations in the native export contract.
use super::*;
use std::borrow::Cow;

/// Semantic content purpose supplied by the ONE typed native payload inventory, never by pointer/text guessing.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DefinitionContentKind {
    Sql,
    Plan,
    Expression,
    Definition,
    Script,
    Hint,
}
impl DefinitionContentKind {
    pub fn label(self) -> String {
        match self {
            Self::Sql => crate::t!("artifact.kind.sql"),
            Self::Plan => crate::t!("artifact.kind.plan"),
            Self::Expression => crate::t!("artifact.kind.expression"),
            Self::Definition => crate::t!("artifact.kind.definition"),
            Self::Script => crate::t!("artifact.kind.script"),
            Self::Hint => crate::t!("artifact.kind.hint"),
        }
    }
}
pub struct DefinitionReference<'a> {
    pub kind: DefinitionContentKind,
    pub pointer: Cow<'static, str>,
    pub payload: &'a DefinitionPayload,
}
fn reference<'a>(
    kind: DefinitionContentKind,
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    DefinitionReference { kind, pointer: pointer.into(), payload }
}
fn sql_reference<'a>(pointer: impl Into<Cow<'static, str>>, payload: &'a DefinitionPayload) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Sql, pointer, payload)
}
fn plan_reference<'a>(
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Plan, pointer, payload)
}
fn expression_reference<'a>(
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Expression, pointer, payload)
}
fn definition_reference<'a>(
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Definition, pointer, payload)
}
fn script_reference<'a>(
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Script, pointer, payload)
}
fn hint_reference<'a>(
    pointer: impl Into<Cow<'static, str>>,
    payload: &'a DefinitionPayload,
) -> DefinitionReference<'a> {
    reference(DefinitionContentKind::Hint, pointer, payload)
}
impl TelemetryRecord {
    pub fn definition_payloads(&self) -> impl Iterator<Item = DefinitionReference<'_>> {
        let payload = match self {
            Self::QueryDefinition(row) => Some(sql_reference("/record/data/sql", &row.sql)),
            Self::PlanDefinition(row) => Some(plan_reference("/record/data/plan", &row.plan)),
            Self::Activity(row) => Some(sql_reference("/record/data/statement", &row.statement)),
            Self::Operational(row) => return row.fact.definition_fields().into_iter(),
            Self::Tuning(row) => return row.fact.definition_fields().into_iter(),
            Self::ObjectDefinition(row) => return row.fact.definition_fields().into_iter(),
            // Redacted derived SQL is intentionally retained directly in Gold. It is not a native definition
            // payload; canonical export sanitizes it and the derived row validates its typed payloads.
            Self::CollectorConnection(_)
            | Self::DerivedIncident(_)
            | Self::DerivedFinding(_)
            | Self::DerivedChange(_)
            | Self::QueryRuntime(_)
            | Self::QueryWaits(_)
            | Self::ResourceSample(_)
            | Self::DiagnosticCheckpoint(_)
            | Self::Deadlock(_)
            | Self::WaitCounter(_)
            | Self::Replication(_)
            | Self::Setting(_) => None,
        };
        payload.into_iter().collect::<Vec<_>>().into_iter()
    }
}
impl ObjectDefinitionFact {
    pub fn definition_payloads(&self) -> impl Iterator<Item = DefinitionReference<'_>> {
        self.definition_fields().into_iter()
    }
    fn definition_fields(&self) -> Vec<DefinitionReference<'_>> {
        let payload =
            match self {
                Self::NativeCatalog(catalog) => {
                    return [
                        catalog.definition.as_ref().map(|value| {
                            definition_reference("/record/data/fact/data/definition/payload", &value.payload)
                        }),
                        catalog.predicate.as_ref().map(|value| {
                            expression_reference("/record/data/fact/data/predicate/payload", &value.payload)
                        }),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                }
                Self::Statement(statement) => Some(sql_reference("/record/data/fact/data/sql", &statement.sql)),
                Self::MssqlColumn(column) => {
                    return [
                        column.default.as_ref().map(|value| {
                            expression_reference("/record/data/fact/data/default/payload", &value.payload)
                        }),
                        column.computed.as_ref().map(|value| {
                            expression_reference("/record/data/fact/data/computed/payload", &value.payload)
                        }),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                }
                Self::MssqlCheckConstraint(value) => {
                    Some(expression_reference("/record/data/fact/data/definition/payload", &value.definition.payload))
                }
                Self::MssqlPartitionBoundary(value) => value
                    .display_value
                    .as_ref()
                    .map(|value| expression_reference("/record/data/fact/data/display_value/payload", &value.payload)),
                Self::MssqlCachedDefinition(value) => {
                    return [
                        Some(sql_reference("/record/data/fact/data/statement/payload", &value.statement.payload)),
                        Some(plan_reference(
                            "/record/data/fact/data/estimated_plan/payload",
                            &value.estimated_plan.payload,
                        )),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                }
                Self::RetainedPlan(value) => {
                    Some(plan_reference("/record/data/fact/data/definition/plan", &value.definition.plan))
                }
                Self::MssqlRetainedPlan(value) => {
                    return [
                        Some(sql_reference("/record/data/fact/data/statement/payload", &value.statement.payload)),
                        Some(plan_reference("/record/data/fact/data/batch_plan/payload", &value.batch_plan.payload)),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                }
                Self::MssqlForeignKey(_) | Self::MssqlObject(_) | Self::MssqlDmlTrigger(_) => None,
            };
        payload.into_iter().collect()
    }
}
impl OperationalFact {
    fn definition_fields(&self) -> Vec<DefinitionReference<'_>> {
        let payload = match self {
            Self::CatalogCounters(value) => {
                return [
                    value.catalog.definition.as_ref().map(|value| {
                        definition_reference("/record/data/fact/data/catalog/definition/payload", &value.payload)
                    }),
                    value.catalog.predicate.as_ref().map(|value| {
                        expression_reference("/record/data/fact/data/catalog/predicate/payload", &value.payload)
                    }),
                ]
                .into_iter()
                .flatten()
                .collect();
            }
            Self::ReadableSecondaryRecovery(value) => {
                return vec![sql_reference(
                    "/record/data/fact/data/focus/original/statement",
                    &value.focus.original.statement,
                )];
            }
            Self::PostgresTransactionMetadata(value) => {
                return value
                    .caches
                    .iter()
                    .enumerate()
                    .map(|(index, cache)| {
                        sql_reference(
                            format!("/record/data/fact/data/caches/{index}/actor/statement"),
                            &cache.actor.statement,
                        )
                    })
                    .collect();
            }
            Self::MssqlCacheEntryMemory(value) => {
                return value
                    .statements
                    .iter()
                    .enumerate()
                    .map(|(index, statement)| {
                        sql_reference(
                            format!("/record/data/fact/data/statements/{index}/statement"),
                            &statement.statement,
                        )
                    })
                    .collect();
            }
            Self::MssqlIndex(value) => value
                .filter
                .as_ref()
                .map(|value| expression_reference("/record/data/fact/data/filter/payload", &value.payload)),
            Self::ExecutionGrant(value) => value
                .request
                .as_ref()
                .map(|request| sql_reference("/record/data/fact/data/request/statement", &request.statement)),
            Self::TaskSpace(value) => value
                .request
                .as_ref()
                .map(|request| sql_reference("/record/data/fact/data/request/statement", &request.statement)),
            Self::PageWait(value) => {
                Some(sql_reference("/record/data/fact/data/request/statement", &value.request.statement))
            }
            Self::PageTaskHolders(value) => {
                let mut fields = Vec::new();
                if let Some(page) = &value.page {
                    fields
                        .push(sql_reference("/record/data/fact/data/page/request/statement", &page.request.statement));
                }
                for (bound, point) in [("before", &value.before), ("after", &value.after)] {
                    if let Some(request) = &point.holder.request {
                        fields.push(sql_reference(
                            format!("/record/data/fact/data/{bound}/holder/request/statement"),
                            &request.statement,
                        ));
                    }
                }
                return fields;
            }
            Self::PageWaitWriters(value) => {
                let mut fields = Vec::new();
                if let Some(page) = &value.page {
                    fields
                        .push(sql_reference("/record/data/fact/data/page/request/statement", &page.request.statement));
                }
                for (index, candidate) in value.candidates.iter().enumerate() {
                    fields.push(plan_reference(
                        format!("/record/data/fact/data/candidates/{index}/plan"),
                        &candidate.plan,
                    ));
                    if let Some(request) = &candidate.request {
                        fields.push(sql_reference(
                            format!("/record/data/fact/data/candidates/{index}/request/statement"),
                            &request.statement,
                        ));
                    }
                }
                return fields;
            }
            Self::MssqlCurrentExecutionProfile(value) => {
                let mut fields = vec![sql_reference("/record/data/fact/data/statement", &value.statement)];
                if let Some(work) = &value.work
                    && let MssqlCurrentRequestPlan::Observed { plan, .. } = &work.current_plan
                {
                    fields.push(plan_reference("/record/data/fact/data/work/current_plan/plan", plan));
                }
                if let MssqlExecutionProfiles::Observed {
                    operators: MssqlExecutionOperators::Observed { nodes, .. },
                    ..
                } = &value.profiles
                {
                    for (index, node) in nodes.iter().enumerate() {
                        fields.push(expression_reference(
                            format!("/record/data/fact/data/profiles/operators/nodes/{index}/predicate"),
                            &node.predicate,
                        ));
                        fields.push(expression_reference(
                            format!("/record/data/fact/data/profiles/operators/nodes/{index}/seek_predicate"),
                            &node.seek_predicate,
                        ));
                    }
                }
                return fields;
            }
            Self::MssqlWorkerStarvation(value) => {
                return value
                    .requests
                    .iter()
                    .enumerate()
                    .map(|(index, request)| {
                        sql_reference(
                            format!("/record/data/fact/data/requests/{index}/observation/statement"),
                            &request.observation.statement,
                        )
                    })
                    .collect();
            }
            Self::CompilerWaiter(value) => {
                Some(sql_reference("/record/data/fact/data/request/statement", &value.request.statement))
            }
            Self::PostgresLog(value) => Some(sql_reference("/record/data/fact/data/statement", &value.statement)),
            Self::MssqlExtendedEvent(value) => {
                Some(sql_reference("/record/data/fact/data/statement", &value.statement))
            }
            Self::CurrentRequestWork(value) => {
                let mut fields = vec![sql_reference("/record/data/fact/data/statement", &value.statement)];
                if let CurrentRequestWorkNative::Mssql { work } = &value.native
                    && let MssqlCurrentRequestPlan::Observed { plan, .. } = &work.current_plan
                {
                    fields.push(plan_reference("/record/data/fact/data/native/work/current_plan/plan", plan));
                }
                return fields;
            }
            Self::MssqlStatementGrants(value) => {
                Some(sql_reference("/record/data/fact/data/statement", &value.statement))
            }
            Self::MssqlQueryCompilation(value) => {
                Some(sql_reference("/record/data/fact/data/statement", &value.statement))
            }
            Self::MssqlInstanceResources(_)
            | Self::MssqlDatabaseFile(_)
            | Self::Space(_)
            | Self::MaintenanceProgress(_)
            | Self::MysqlScheduledEvent(_)
            | Self::MssqlAgentJob(_)
            | Self::MssqlAgentStep(_)
            | Self::MssqlAgentHistory(_)
            | Self::MssqlAgentContext(_)
            | Self::TimescaleJobState(_)
            | Self::TimescaleJobRun(_)
            | Self::TimescaleContinuousAggregate(_)
            | Self::SessionMemory(_)
            | Self::Sequence(_)
            | Self::FileIo(_)
            | Self::Counter(_)
            | Self::StatementCounters(_)
            | Self::RuntimeDistribution(_)
            | Self::DiagnosticRevision(_)
            | Self::CounterSet(_)
            | Self::Retention(_)
            | Self::RetentionCoverage(_)
            | Self::Queue(_)
            | Self::MssqlCdc(_)
            | Self::MssqlChangeFeed(_)
            | Self::MssqlBackupHistory(_)
            | Self::MssqlColumnstoreRowgroup(_)
            | Self::MssqlTableMemory(_)
            | Self::MysqlGroupReplication(_)
            | Self::MariadbGalera(_)
            | Self::MssqlFullTextPopulation(_)
            | Self::MssqlCleanupHorizon(_)
            | Self::ConfiguredLogRateLimits(_)
            | Self::HyperscaleLogComponent(_)
            | Self::DatabaseProperties(_)
            | Self::MssqlPersistentVersionStore(_)
            | Self::MssqlIndexOperations(_)
            | Self::MssqlTable(_)
            | Self::MssqlStatistics(_)
            | Self::MssqlMissingIndex(_)
            | Self::MssqlCachedRuntime(_)
            | Self::MssqlOutOfMemory(_)
            | Self::MysqlErrorLog(_)
            | Self::SuspectPages(_) => None,
        };
        payload.into_iter().collect()
    }
}
impl TuningFact {
    fn definition_fields(&self) -> Vec<DefinitionReference<'_>> {
        let payload = match self {
            Self::Recommendation(value) => Some(script_reference("/record/data/fact/data/script", &value.script)),
            Self::QueryHint(value) => Some(hint_reference("/record/data/fact/data/hint", &value.hint)),
            Self::AutomaticOption(_) | Self::ForcedPlan(_) => None,
        };
        payload.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_payloads_resolve_to_the_exact_native_value_without_inspecting_arbitrary_json_fields() {
        let column =
            serde_json::from_str::<MssqlColumnDefinition>(include_str!("../../../../fixtures/telemetry-column.json"))
                .unwrap();
        let record = TelemetryRecord::ObjectDefinition(
            ObjectDefinitionRecord::new("columns".into(), ObjectDefinitionFact::MssqlColumn(Box::new(column))).unwrap(),
        );
        let value = serde_json::json!({"record":record});
        let fields = record.definition_payloads().collect::<Vec<_>>();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].kind, DefinitionContentKind::Expression);
        for field in fields {
            assert_eq!(value.pointer(field.pointer.as_ref()).unwrap(), &serde_json::to_value(field.payload).unwrap());
        }
    }
}
