//! Native scope syntax wraps the same original publication/membership templates.
use crate::{catalog::SchemaCatalog, schema::Schema};
use culprit_evidence::analytical::semantics::EligibilityScope;
pub(crate) fn arguments(selection: EligibilityScope) -> String {
    let resource = match selection {
        EligibilityScope::Resource => "resource={resource:String}, ",
        EligibilityScope::Namespace => "",
    };
    format!(
        "contract={{contract:String}}, store={{store:String}}, {resource}captured_from={{captured_from:DateTime64(9, 'UTC')}}, asof={{asof:DateTime64(9, 'UTC')}}"
    )
}
fn scope(template: &str, selection: EligibilityScope) -> String {
    let (value, filter) = match selection {
        EligibilityScope::Resource => ("resource={resource:String},", "ResourceId={resource:String}"),
        EligibilityScope::Namespace => ("", "1"),
    };
    let template = match selection {
        EligibilityScope::Resource => template.to_string(),
        EligibilityScope::Namespace => template
            .replacen("CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW", 1)
            .replacen(" AS", " SQL SECURITY INVOKER AS", 1),
    };
    template.replace("$SCOPE", selection.suffix()).replace("$RESOURCE_VALUE", value).replace("$RESOURCE_FILTER", filter)
}
pub(crate) fn published(catalog: &SchemaCatalog, selection: EligibilityScope) -> String {
    scope(include_str!("sql/published.sql"), selection)
        .replace("$COLUMNS", &catalog.publication().column_names())
        .replace("$SCHEMA", catalog.digest())
}
pub(crate) fn eligible(schema: &Schema, selection: EligibilityScope) -> String {
    scope(include_str!("sql/eligible.sql"), selection)
        .replace("$TABLE", schema.table())
        .replace("$COLUMNS", &schema.column_names())
}
pub(crate) fn rows(schema: &Schema, selection: EligibilityScope, revisions: bool) -> String {
    let template = if revisions {
        include_str!("sql/revisions.sql")
    } else {
        "CREATE VIEW IF NOT EXISTS $TABLE$SCOPERows AS SELECT $COLUMNS FROM $TABLE$SCOPEEligible($ARGS)"
    };
    scope(template, selection)
        .replace("$TABLE", schema.table())
        .replace("$COLUMNS", &schema.column_names())
        .replace("$ARGS", &arguments(selection))
}
