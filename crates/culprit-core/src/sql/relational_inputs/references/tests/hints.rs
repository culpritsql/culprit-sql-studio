use super::*;

fn both_inputs() -> Vec<MssqlRelationReference> {
    vec![
        parameter("@Rows", Some("r")),
        MssqlRelationReference {
            name: MssqlRelationName::Named(vec!["dbo".into(), "Items".into()]),
            alias: Some("s".into()),
        },
    ]
}

#[test]
fn native_join_algorithms_preserve_complete_original_alias_and_relation_inventory() {
    for join in [
        "HASH JOIN",
        "INNER HASH JOIN",
        "INNER LOOP JOIN",
        "INNER MERGE JOIN",
        "LEFT OUTER HASH JOIN",
        "LEFT LOOP JOIN",
        "RIGHT MERGE JOIN",
        "FULL OUTER HASH JOIN",
    ] {
        let sql = format!("SELECT COUNT_BIG(*) FROM @Rows AS r {join} dbo.Items AS s ON s.id=r.id");
        assert_eq!(mssql_relational_references(&sql), Some(both_inputs()), "{join}");
    }
    let sql = "SELECT r.hash FROM @Rows r INNER HASH JOIN dbo.Items s ON r.hash=s.hash AND r.id=s.id";
    assert_eq!(mssql_relational_references(sql), Some(both_inputs()));
}

#[test]
fn actual_serial_profile_statement_and_only_closed_supported_hint_clauses_keep_source_spelling() {
    let sql = "SELECT COUNT_BIG(*) AS completed FROM @Rows AS r INNER HASH JOIN dbo.Items AS s WITH(READCOMMITTEDLOCK) ON s.id=r.id OPTION(FORCE ORDER,MAXDOP 1,USE HINT('QUERY_PLAN_PROFILE'))";
    assert_eq!(mssql_relational_references(sql), Some(both_inputs()));
    for hints in ["READCOMMITTEDLOCK", "ROWLOCK", "UPDLOCK,HOLDLOCK", "XLOCK,ROWLOCK,HOLDLOCK", "NOWAIT"] {
        let sql = format!("SELECT r.id FROM @Rows r JOIN dbo.Items s WITH({hints}) ON s.id=r.id OPTION(MAXDOP 1)");
        assert_eq!(mssql_relational_references(&sql), Some(both_inputs()), "{hints}");
    }
    for option in [
        "FORCE ORDER",
        "USE HINT('QUERY_PLAN_PROFILE')",
        "MAXDOP 1, FORCE ORDER, USE /* original */ HINT /* original */ ('query_plan_profile')",
        "USE HINT('QUERY_PLAN_PROFILE'),MAX_GRANT_PERCENT=70,MIN_GRANT_PERCENT=0,HASH GROUP,MAXDOP 1,FORCE ORDER",
    ] {
        let sql = format!("SELECT N'not a hint' AS label,r.id FROM @Rows AS r WITH(HOLDLOCK) OPTION({option})");
        assert_eq!(mssql_relational_references(&sql), Some(vec![parameter("@Rows", Some("r"))]), "{option}");
    }
}

#[test]
fn removed_literals_comments_or_lexical_modes_cannot_forge_a_supported_hint_operand() {
    for hint in [
        "USE HINT()",
        "USE HINT('unknown')",
        "USE HINT(N'QUERY_PLAN_PROFILE')",
        "USE HINT(\"QUERY_PLAN_PROFILE\")",
        "USE HINT('QUERY_PLAN_PROFILE','unknown')",
        "USE 'QUERY_PLAN_PROFILE' HINT()",
        "USE HINT() 'QUERY_PLAN_PROFILE'",
        "USE HINT(/*'QUERY_PLAN_PROFILE'*/)",
        "USE HINT('QUERY_PLAN_'+'PROFILE')",
        "USE HINT('QUERY_PLAN_PROFILE''')",
        "USE HINT('QUERY_PLAN_PROFILE') 'ignored'",
        "USE HINT('QUERY_PLAN_PROFILE'), MAXDOP '1'",
        "MAXDOP 1 'QUERY_PLAN_PROFILE', USE HINT()",
        "USE HINT('QUERY_PLAN_PROFILE'),USE HINT('QUERY_PLAN_PROFILE')",
        "FORCE 'ignored' ORDER",
        "FORCE ORDER,FORCE ORDER",
        "USE PLAN '<ShowPlanXML/>'",
    ] {
        let sql = format!("SELECT * FROM @Rows r OPTION({hint})");
        assert!(mssql_relational_references(&sql).is_none(), "{hint}");
    }
    for table_hint in [
        "WITH()",
        "WITH(ROWLOCK 'ignored')",
        "WITH('ignored' ROWLOCK)",
        "WITH(ROWLOCK) 'ignored'",
        "WITH(ROWLOCK,ROWLOCK)",
        "WITH(UPDLOCK,XLOCK)",
        "WITH(ROWLOCK,READCOMMITTEDLOCK)",
        "WITH(INDEX(ix))",
        "WITH(FORCESEEK)",
        "WITH(READCOMMITTEDLOCK,dbo.hidden())",
        "WITH(ROWLOCK,) WITH(HOLDLOCK)",
        "WITH(ROWLOCK); SELECT * FROM @Other",
        "WITH(ROWLOCK) CROSS APPLY dbo.hidden(r.id) x",
    ] {
        let sql = format!("SELECT * FROM @Rows r {table_hint}");
        assert!(mssql_relational_references(&sql).is_none(), "{table_hint}");
    }
}

#[test]
fn hints_never_repair_hidden_scope_ambiguous_aliases_or_invalid_join_shapes() {
    for join in
        ["CROSS HASH JOIN", "CROSS LOOP JOIN", "RIGHT LOOP JOIN", "FULL LOOP JOIN", "REMOTE JOIN", "HASH LOOP JOIN"]
    {
        let sql = format!("SELECT r.id FROM @Rows r {join} dbo.Items s ON r.id=s.id");
        assert!(mssql_relational_references(&sql).is_none(), "{join}");
    }
    for body in [
        "SELECT r.id FROM @Rows r INNER HASH JOIN dbo.Items R ON r.id=R.id",
        "SELECT r.id FROM @Rows r INNER HASH JOIN dbo.hidden(r.id) s ON r.id=s.id",
        "SELECT * FROM @Rows r WITH(HOLDLOCK) UNION SELECT * FROM @Other",
        "SELECT (SELECT id FROM @Other) FROM @Rows r WITH(HOLDLOCK)",
        "SELECT * FROM (SELECT * FROM @Rows AS r WITH(HOLDLOCK)) AS d",
        "WITH p AS (SELECT * FROM @Rows AS r WITH(HOLDLOCK)) SELECT * FROM p AS q",
    ] {
        assert!(mssql_relational_references(&format!("{body} OPTION(FORCE ORDER,MAXDOP 1)")).is_none(), "{body}");
    }
}

#[test]
fn the_existing_lexer_retains_exact_literal_fragments_and_all_other_views_keep_their_safety_policy() {
    use crate::sql::lexer::{literal_fragments, literal_free_readings};
    assert_eq!(
        literal_fragments("USE HINT(/*'not a literal'*/'QUERY_PLAN_PROFILE')", Engine::Mssql),
        Some(vec!["'QUERY_PLAN_PROFILE'"])
    );
    assert!(literal_fragments("USE HINT(\"QUERY_PLAN_PROFILE\")", Engine::Mssql).is_none());
    assert!(literal_free_readings("MAXDOP '1'", Engine::Mssql).is_none());
    assert!(literal_free_readings("MAXDOP 1 /*'comment only'*/", Engine::Mssql).is_some());
    assert_eq!(literal_fragments("SELECT E'escaped\\value'", Engine::Postgres), Some(vec!["E'escaped\\value'"]));
    assert_eq!(literal_fragments("SELECT $$quoted$$", Engine::Postgres), Some(vec!["$$quoted$$"]));
    for engine in Engine::ALL {
        assert!(literal_free_readings("SELECT 'value'", engine).is_none());
        assert!(literal_free_readings("SELECT 1 /* 'comment' */", engine).is_some());
    }
}

#[test]
fn redacted_inventory_is_explicit_and_never_changes_original_unknown_hint_refusal() {
    let sql = "SELECT r.id FROM @Rows r WITH(HOLDLOCK) OPTION(USE HINT('?'),MAXDOP 1)";
    assert!(mssql_relational_references(sql).is_none());
    assert_eq!(mssql_redacted_relational_references(sql), Some(vec![parameter("@Rows", Some("r"))]));
    for replacement in ["'unknown'", "N'?'", "'?'+'extra'", "'?' 'extra'"] {
        assert!(mssql_redacted_relational_references(&sql.replace("'?'", replacement)).is_none());
    }
}
