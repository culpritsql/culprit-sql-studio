//! The original-only guard is exercised directly so stored-redacted's earlier proof-generation refusal
//! cannot hide an accidental widening of the UTF-16 fallback used by other binding consumers.
use super::*;
use culprit_core::telemetry::*;

#[test]
fn full_batch_binding_itself_requires_qualified_original_scope_and_never_rebinds_stored_redacted_text() {
    let prefix = "CREATE PROCEDURE dbo.p @p int AS ";
    let fragment = "SELECT @p AS native_value";
    let full = format!("{prefix}{fragment}");
    let native = CachedStatementContext {
        identity: CachedStatementIdentity {
            plan_handle: format!("0x{}", "01".repeat(64)),
            sql_handle: format!("0x{}", "02".repeat(64)),
            statement_start_bytes: (prefix.encode_utf16().count() * 2).try_into().unwrap(),
            statement_end_bytes: (full.encode_utf16().count() * 2 - 2).try_into().unwrap(),
            created_without_offset: "2020-01-01T10:00:00.003".into(),
            plan_generation: "1".into(),
        },
        database_id: 5,
        query_hash: Some("0x0123456789ABCDEF".into()),
        origin: StatementOrigin::Application,
        monitoring_template_id: None,
        source_text_bytes: Some((full.encode_utf16().count() * 2).to_string()),
        source_text_complete: true,
    };
    let source = super::test_source(SnapshotDataset::RetainedPlans);
    assert!(
        input(&native, fragment, PlanInputOrigin::OriginalCapture { source: &source }).matches_original_text(&full)
    );
    assert!(!input(&native, fragment, PlanInputOrigin::StoredRedacted { proof: None }).matches_original_text(&full));
    assert!(!input(&native, fragment, PlanInputOrigin::OriginalNative { read: None }).matches_original_text(&full));
    let read = NativeReadContext::from_snapshot(&source).unwrap();
    assert!(
        input(&native, fragment, PlanInputOrigin::OriginalNative { read: Some(&read) }).matches_original_text(&full)
    );
    let mut unknown = read;
    unknown.partition = SourcePartition::InstanceLocal { member: None, epoch: None };
    assert!(
        !input(&native, fragment, PlanInputOrigin::OriginalNative { read: Some(&unknown) })
            .matches_original_text(&full)
    );
    assert!(
        input(&native, fragment, PlanInputOrigin::StoredRedacted { proof: None }).matches_original_text(fragment),
        "the existing direct path is unchanged"
    );
}

fn input<'a>(native: &'a CachedStatementContext, text: &'a str, origin: PlanInputOrigin<'a>) -> CachedInputContext<'a> {
    CachedInputContext { statement: native, module_parameters: None, text: Some(text), origin }
}
