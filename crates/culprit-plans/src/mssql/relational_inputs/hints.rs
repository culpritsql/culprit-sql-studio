//! Exercise the one cached-statement parser under real SQL hint syntax; syntax supplies no input-kind authority.
use crate::mssql::{CachedInputContext, PlanInputOrigin, parse_cached_statement, test_read_context};
use culprit_core::{plan_facts::RelationalInputKind, telemetry::*};

const SQL: &str = "SELECT COUNT_BIG(*) AS completed FROM @Rows AS r INNER HASH JOIN dbo.Items AS s WITH(READCOMMITTEDLOCK) ON s.id=r.id OPTION(FORCE ORDER,MAXDOP 1,USE HINT('QUERY_PLAN_PROFILE'))";
const PREFIX: &str = "CREATE PROC dbo.consume @Rows dbo.RowsType READONLY AS ";

fn context(sql: &str) -> CachedStatementContext {
    CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: "0x0102".into(),
            sql_handle: "0x0304".into(),
            statement_start_bytes: (PREFIX.encode_utf16().count() * 2).try_into().unwrap(),
            statement_end_bytes: -1,
            created_without_offset: "2026-11-22T10:00:00.003".into(),
            plan_generation: "1".into(),
        },
        database_id: 5,
        query_hash: Some("0x0123456789ABCDEF".into()),
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: Some(((PREFIX.encode_utf16().count() + sql.encode_utf16().count()) * 2).to_string()),
        source_text_complete: true,
    }
}

fn xml(sql: &str, database: &str, alias: &str) -> String {
    format!(
        r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="{sql}" QueryHash="0x0123456789ABCDEF"><QueryPlan><RelOp NodeId="1" PhysicalOp="Hash Match" LogicalOp="Inner Join" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="1"><Hash><RelOp NodeId="2" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" Parallel="false" EstimateRows="1" EstimatedTotalSubtreeCost="1"><IndexScan><Object Table="[@Rows]" Alias="[{alias}]"/></IndexScan></RelOp><RelOp NodeId="3" PhysicalOp="Table Scan" LogicalOp="Table Scan" Parallel="false" EstimateRows="100" EstimatedTotalSubtreeCost="1"><TableScan><Object Database="[{database}]" Schema="[dbo]" Table="[Items]" Alias="[s]"/></TableScan></RelOp></Hash></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>"#
    )
}

fn input_kind(sql: &str, database: &str, alias: &str, readonly: bool) -> RelationalInputKind {
    original_plan(sql, database, alias, readonly).root.children[0].facts().relational_input.unwrap().qualified_kind()
}

fn parameters(context: &CachedStatementContext, sql: &str, readonly: bool) -> MssqlModuleParameters {
    MssqlModuleParameters {
        database_id: 5,
        object_id: 42,
        module_number: 1,
        sql_handle: context.identity.sql_handle.clone(),
        table_parameters: if readonly {
            culprit_core::sql::mssql_procedure_table_parameters(&format!("{PREFIX}{sql}")).unwrap()
        } else {
            Default::default()
        },
    }
}

fn original_plan(sql: &str, database: &str, alias: &str, readonly: bool) -> culprit_core::Plan {
    let context = context(sql);
    let parameters = parameters(&context, sql, readonly);
    let read = test_read_context("app");
    parse_cached_statement(
        &xml(sql, database, alias),
        CachedInputContext {
            statement: &context,
            module_parameters: Some(&parameters),
            text: Some(sql),
            origin: PlanInputOrigin::OriginalNative { read: Some(&read) },
        },
    )
    .unwrap()
}

#[test]
fn original_readonly_module_exact_native_objects_and_all_real_hints_qualify_only_the_selected_tvp_node() {
    assert_eq!(input_kind(SQL, "app", "r", true), RelationalInputKind::TableValuedParameter);
}

#[test]
fn accepted_hint_syntax_cannot_replace_native_readonly_namespace_alias_or_complete_closed_scope_proof() {
    for (sql, database, alias, readonly) in [
        (SQL, "app", "r", false),
        (SQL, "foreign", "r", true),
        (SQL, "app", "other", true),
        (&SQL.replace("QUERY_PLAN_PROFILE", "DISABLE_PARAMETER_SNIFFING"), "app", "r", true),
        (&SQL.replace("READCOMMITTEDLOCK", "INDEX(ix)"), "app", "r", true),
    ] {
        assert_eq!(input_kind(sql, database, alias, readonly), RelationalInputKind::Unknown);
    }
}

fn stored_definition(source: &CollectionDescription) -> MssqlCachedDefinition {
    let original = original_plan(SQL, "app", "r", true);
    let binding =
        MssqlRelationalInputBinding { node_id: 2, input: original.root.children[0].facts().relational_input.unwrap() };
    let context = context(SQL);
    let mut definition = MssqlCachedDefinition {
        module_parameters: Some(parameters(&context, SQL, true)),
        context,
        query_plan_hash: None,
        statement: stored_payload(SQL, DefinitionPayload::sql),
        estimated_plan: stored_payload(&xml(SQL, "app", "r"), DefinitionPayload::plan),
        current_type_schema: None,
        relational_input_proof: None,
    };
    definition.relational_input_proof =
        Some(MssqlRelationalInputProof::for_cached(source, &definition, vec![binding]).unwrap());
    definition
}

fn stored_payload(text: &str, redact: fn(&str, culprit_core::Engine) -> DefinitionPayload) -> MssqlCachedPayload {
    MssqlCachedPayload {
        source_utf16_bytes: Some((text.encode_utf16().count() * 2).to_string()),
        native_sha256: Some(digest(text.as_bytes())),
        payload: redact(text, culprit_core::Engine::Mssql),
    }
}

#[test]
fn exported_redacted_definition_reuses_exact_original_proof_and_missing_or_tampered_proof_cannot_rebind() {
    let source = crate::mssql::test_source(SnapshotDataset::CachedDefinitions);
    let definition = stored_definition(&source);
    assert!(definition.is_valid());
    let DefinitionPayload::Available { text, .. } = &definition.estimated_plan.payload else { panic!() };
    assert!(text.contains("USE HINT(&apos;?&apos;)") || text.contains("USE HINT('?')"));
    let [native_source] = source.observation.snapshot().unwrap().sources.as_slice() else {
        panic!("one original native definition source")
    };
    let row = TelemetryRecord::ObjectDefinition(
        ObjectDefinitionRecord::new(
            native_source.source.clone(),
            ObjectDefinitionFact::MssqlCachedDefinition(Box::new(definition)),
        )
        .unwrap(),
    );
    let capture = CaptureIdentity {
        observation_id: "controlled-original-hinted-cache".into(),
        revision: Some(CaptureRevision {
            outbox_id: "controlled-original-cache-outbox".into(),
            owner_epoch: "1".into(),
            sequence: "1".into(),
        }),
    };
    // The collection owner is not a substitute for the declared native row-source roster.
    let mut undeclared = row.clone();
    let TelemetryRecord::ObjectDefinition(other) = &mut undeclared else { panic!() };
    other.source = source.source.clone();
    assert!(ExportedRecord::new(&source, &capture, undeclared).is_err());
    let exported = ExportedRecord::new(&source, &capture, row).unwrap();
    let restored: ExportedRecord = serde_json::from_slice(&serde_json::to_vec(&exported).unwrap()).unwrap();
    let TelemetryRecord::ObjectDefinition(row) = restored.record else { panic!() };
    let ObjectDefinitionFact::MssqlCachedDefinition(definition) = row.fact else { panic!() };
    let parsed = crate::mssql::parse_cached_definition(&definition, &source).unwrap();
    assert_eq!(
        parsed.root.children[0].facts().relational_input.unwrap().qualified_kind(),
        RelationalInputKind::TableValuedParameter
    );

    let mut absent = definition.clone();
    absent.relational_input_proof = None;
    let parsed = crate::mssql::parse_cached_definition(&absent, &source).unwrap();
    assert_eq!(
        parsed.root.children[0].facts().relational_input.unwrap().qualified_kind(),
        RelationalInputKind::Unknown
    );
    for defect in ["binding", "scope", "body"] {
        let mut changed = definition.clone();
        match defect {
            "binding" => changed.relational_input_proof.as_mut().unwrap().bindings[0].node_id = 3,
            "scope" => changed.relational_input_proof.as_mut().unwrap().source_scope = digest(b"another-owner"),
            _ => changed.statement = stored_payload("SELECT * FROM @Rows", DefinitionPayload::sql),
        }
        assert!(crate::mssql::parse_cached_definition(&changed, &source).is_err(), "{defect}");
    }
}
