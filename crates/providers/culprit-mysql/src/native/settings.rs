//! The existing allowlisted native SHOW query and its one checked scan/retained row projection.
use super::*;
use crate::{connect::Flavor, vars::VariableSet};
use culprit_core::telemetry::SettingObservation;

pub(crate) fn names(flavor: Flavor) -> Result<Vec<&'static str>> {
    flavor.settings_format().global_variable_names().ok_or_else(invalid)
}
pub(crate) fn query(flavor: Flavor) -> Result<String> {
    Ok(VariableSet::Configuration.query(&names(flavor)?))
}
pub(crate) fn decode(row: &Rec, flavor: Flavor) -> Result<SettingObservation> {
    let name = text(row, "variable_name")?.to_ascii_lowercase();
    flavor.settings_format().global_variable(&name, row.native_text("value")?).ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn one_native_decoder_preserves_global_nulls_units_and_compiled_default_provenance() {
        let row = Rec::from_values(vec![("variable_name", "SORT_BUFFER_SIZE".into()), ("value", "262144".into())]);
        let mysql = decode(&row, Flavor::Mysql).unwrap();
        let maria = decode(&row, Flavor::Mariadb).unwrap();
        assert_eq!(mysql.name, "sort_buffer_size");
        assert_eq!(mysql.unit.as_deref(), Some("B"));
        assert_eq!(mysql.value, None);
        assert_eq!(mysql.value_in_use.as_deref(), Some("262144"));
        assert_eq!(mysql.default_value.as_deref(), Some("262144"));
        assert_eq!(maria.default_value.as_deref(), Some("2097152"));
        let missing =
            Rec::from_values(vec![("variable_name", "sort_buffer_size".into()), ("value", mysql_async::Value::NULL)]);
        let missing = decode(&missing, Flavor::Mysql).unwrap();
        assert!(missing.value_in_use.is_none());
        assert!(Flavor::Mysql.settings_format().project_global_variable(&missing).is_none());
    }
    #[test]
    fn existing_tagged_allowlist_shape_never_reads_credential_variables_or_repairs_foreign_rows() {
        let sql = query(Flavor::Mysql).unwrap();
        assert!(sql.starts_with("SHOW GLOBAL VARIABLES WHERE Variable_name IN ("));
        assert!(!sql.contains("password") && !sql.contains("init_connect"));
        let foreign = Rec::from_values(vec![("variable_name", "password".into()), ("value", "private".into())]);
        assert!(decode(&foreign, Flavor::Mysql).is_err());
    }
}
