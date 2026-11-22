//! Both scopes render the same publication/membership guards. Native dialect syntax stays here.
use crate::{catalog::SchemaCatalog, projection::ScalarType, schema::Schema};
use culprit_evidence::analytical::semantics::EligibilityScope;
fn columns(schema: &Schema) -> String {
    schema.columns().iter().map(|column| column.name.as_str()).collect::<Vec<_>>().join(", ")
}
fn restore(schema: &Schema) -> String {
    let conversions = schema
        .columns()
        .iter()
        .filter_map(|column| match column.kind {
            ScalarType::JsonContainer => Some(format!("{}=todynamic({})", column.name, column.name)),
            ScalarType::ExactCounter => Some(format!("{}=todecimal({})", column.name, column.name)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if conversions.is_empty() { String::new() } else { format!("| extend {}", conversions.join(", ")) }
}
fn scope(template: &str, selection: EligibilityScope) -> String {
    let (argument, value, filter, membership, mismatch) = match selection {
        EligibilityScope::Resource => ("resource:string,", "resource,", "ResourceId == resource", "", ""),
        EligibilityScope::Namespace => {
            ("", "", "true", " PublishedResource=ResourceId,", " or ResourceId != PublishedResource")
        }
    };
    let template = match selection {
        EligibilityScope::Resource => template.to_string(),
        EligibilityScope::Namespace => template.replacen(".create function", ".create-or-alter function", 1),
    };
    template
        .replace("$SCOPE", selection.suffix())
        .replace("$RESOURCE_ARG", argument)
        .replace("$RESOURCE_VALUE", value)
        .replace("$RESOURCE_FILTER", filter)
        .replace("$RESOURCE_MEMBERSHIP", membership)
        .replace("$RESOURCE_MISMATCH", mismatch)
}
pub(crate) fn published(catalog: &SchemaCatalog, selection: EligibilityScope) -> String {
    let schema = catalog.publication();
    scope(include_str!("kql/published.kql"), selection)
        .replace("$PROJECTION", &schema.verification_projection())
        .replace("$COLUMNS", &columns(schema))
        .replace("$RESTORE", &restore(schema))
        .replace("$SCHEMA", catalog.digest())
}
pub(crate) fn eligible(schema: &Schema, selection: EligibilityScope) -> String {
    scope(include_str!("kql/eligible.kql"), selection)
        .replace("$TABLE", schema.table())
        .replace("$PROJECTION", &schema.verification_projection())
        .replace("$COLUMNS", &columns(schema))
        .replace("$RESTORE", &restore(schema))
}
pub(crate) fn rows(schema: &Schema, selection: EligibilityScope, revisions: bool) -> String {
    if revisions {
        return scope(include_str!("kql/revisions.kql"), selection)
            .replace("$TABLE", schema.table())
            .replace("$COLUMNS", &columns(schema));
    }
    let name = format!("{}{}", schema.table(), selection.suffix());
    let (argument, value) = match selection {
        EligibilityScope::Resource => ("resource:string,", "resource,"),
        EligibilityScope::Namespace => ("", ""),
    };
    let create = match selection {
        EligibilityScope::Resource => ".create function",
        EligibilityScope::Namespace => ".create-or-alter function",
    };
    format!(
        "{create} {name}Rows(contract:string,store:string,{argument}captured_from:datetime,asof:datetime) {{ {name}Eligible(contract,store,{value}captured_from,asof) }}"
    )
}
