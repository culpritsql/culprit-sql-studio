use super::*;
use crate::settings::{Kind, query};

#[test]
fn authority_is_part_of_the_catalog_projection_instead_of_a_second_native_result_set() {
    for kind in Kind::ALL {
        let sql = query(kind, COLUMNS);
        assert!(
            sql.contains(&format!("SELECT /* culprit */ {COLUMNS}"))
                || sql.starts_with(&format!("SELECT {COLUMNS}"))
                || kind == Kind::QueryStore && sql.contains(COLUMNS),
            "{kind:?}"
        );
        assert_eq!(
            sql.matches("AS culprit_settings_frame").count(),
            match kind {
                Kind::Database => 2,
                Kind::QueryStore => 4,
                _ => 1,
            },
            "{kind:?}"
        );
        assert!(!FRAME.contains("SELECT"));
        assert!(FRAME.starts_with("DECLARE /* culprit */"));
        for variable in
            ["@culprit_settings_database_id", "@culprit_settings_database_name", "@culprit_settings_updateability"]
        {
            assert_eq!(FRAME.matches(variable).count(), 1, "one fixed batch-start assignment");
            assert_eq!(COLUMNS.matches(variable).count(), 1, "reuse the captured value per catalog row");
        }
    }
}

#[test]
fn ordinary_query_store_options_share_the_native_projection_without_a_settings_frame() {
    assert_eq!(query(Kind::QueryStore, ""), crate::query_store::catalog_sql::options());
    assert_eq!(query(Kind::Database, ""), crate::database_properties::query());
    assert!(query(Kind::Scoped, "").starts_with("SELECT c.*"));
    assert!(query(Kind::Server, "").starts_with("SELECT name,"));
    assert!(!query(Kind::QueryStore, "").contains(MARKER));
}

#[test]
fn dynamic_database_properties_receive_the_original_batch_variables_and_keep_a_qualified_error_row() {
    let sql = query(Kind::Database, COLUMNS);
    assert!(sql.contains(&format!("sys.sp_executesql @culprit_database_properties{EXECUTION_PARAMETERS};")));
    let fallback = sql.split("BEGIN CATCH").nth(1).unwrap();
    assert!(fallback.contains(&format!("SELECT /* culprit */ {COLUMNS}")));
    let ordinary = query(Kind::Database, "");
    assert!(ordinary.contains("sys.sp_executesql @culprit_database_properties;"));
    assert!(!ordinary.contains("@culprit_settings_database_id"));
}

#[test]
fn dynamic_query_store_options_bind_the_original_frame_values_into_every_native_layout() {
    let sql = query(Kind::QueryStore, COLUMNS);
    assert!(sql.contains(&format!("sys.sp_executesql @culprit_query_store_options{EXECUTION_PARAMETERS};")));
    assert_eq!(sql.matches("AS culprit_settings_frame").count(), 4);
    let ordinary = query(Kind::QueryStore, "");
    assert!(ordinary.ends_with("sys.sp_executesql @culprit_query_store_options;"));
    assert!(!ordinary.contains("@culprit_settings_database_id"));
}
