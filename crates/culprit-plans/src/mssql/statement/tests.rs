use super::*;
use culprit_core::telemetry::{CachedStatementIdentity, StatementOrigin};

fn context(batch: &str, prefix: &str) -> CachedStatementContext {
    CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: "0x0102".into(),
            sql_handle: "0x0304".into(),
            statement_start_bytes: (prefix.encode_utf16().count() * 2).try_into().unwrap(),
            statement_end_bytes: -1,
            created_without_offset: "2020-01-01T10:00:00.003".into(),
            plan_generation: "1".into(),
        },
        database_id: 5,
        query_hash: Some("0x0123456789ABCDEF".into()),
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: Some((batch.encode_utf16().count() * 2).to_string()),
        source_text_complete: true,
    }
}

#[test]
fn complete_native_utf16_selection_keeps_supplementary_characters_and_the_native_end_offset() {
    let prefix = "SELECT N'😀'; ";
    let selected = "SELECT N'🔒' AS value";
    let trailing = "; SELECT 9";
    let batch = format!("{prefix}{selected}{trailing}");
    let mut context = context(&batch, prefix);
    context.identity.statement_end_bytes =
        ((prefix.encode_utf16().count() + selected.encode_utf16().count()) * 2 - 2).try_into().unwrap();
    assert_eq!(native_statement_from_prefix(&context, Some(&batch)).unwrap(), selected);
    context.identity.statement_end_bytes = -1;
    assert_eq!(native_statement_from_prefix(&context, Some(&batch)).unwrap(), format!("{selected}{trailing}"));
}

#[test]
fn missing_incomplete_or_inconsistent_native_inputs_keep_their_typed_unavailability() {
    let batch = "😀";
    let base = context(batch, "");
    assert_eq!(native_statement_from_prefix(&base, None), Err(PayloadUnavailable::Missing));
    let mut incomplete = base.clone();
    incomplete.source_text_complete = false;
    assert_eq!(native_statement_from_prefix(&incomplete, Some(batch)), Err(PayloadUnavailable::NotCollected));
    for defect in ["length", "sql_handle", "plan_handle", "surrogate", "range"] {
        let mut changed = base.clone();
        match defect {
            "length" => changed.source_text_bytes = Some("2".into()),
            "sql_handle" => changed.identity.sql_handle = "0x0000".into(),
            "plan_handle" => changed.identity.plan_handle = "0x0000".into(),
            "surrogate" => changed.identity.statement_start_bytes = 2,
            "range" => changed.identity.statement_end_bytes = 6,
            _ => unreachable!(),
        }
        assert_eq!(
            native_statement_from_prefix(&changed, Some(batch)),
            Err(PayloadUnavailable::SourceUnavailable),
            "{defect}"
        );
    }
}

#[test]
fn raw_statement_stays_transient_while_the_display_payload_is_redacted() {
    let selected = "SELECT N'private-plan-value' AS value";
    let context = context(selected, "");
    assert_eq!(native_statement_from_prefix(&context, Some(selected)).unwrap(), selected);
    let display = statement_from_prefix(&context, Some(selected));
    let DefinitionPayload::Available { text, .. } = display else { panic!("selected display statement") };
    assert!(!text.contains("private-plan-value"));
    assert!(text.contains("SELECT"));
}
