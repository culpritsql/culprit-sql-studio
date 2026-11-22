//! Already-decoded native schema facts share one app projection. These helpers confer no source authority.
use super::*;
use crate::{ConstraintIssue, ConstraintKind, ForeignKey};

/// Fields selected by the scan's native issue predicate. Retained callers qualify original flags first.
/// This is a pure conversion input, not a serialized observation or a proof of native completeness.
pub struct MssqlConstraintIssueFields {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub kind: ConstraintKind,
    pub disabled: bool,
}
impl MssqlConstraintIssueFields {
    pub fn into_constraint_issue(self) -> ConstraintIssue {
        ConstraintIssue {
            schema: self.schema,
            table: self.table,
            name: self.name,
            kind: self.kind,
            disabled: self.disabled,
            inheritance_child: false,
        }
    }
}
fn constraint_issue(
    fields: MssqlConstraintIssueFields,
    trusted: bool,
    not_for_replication: bool,
) -> Option<ConstraintIssue> {
    (!not_for_replication && (fields.disabled || !trusted)).then(|| fields.into_constraint_issue())
}
impl MssqlForeignKey {
    pub fn into_foreign_key(self) -> ForeignKey {
        ForeignKey {
            name: self.name,
            schema: self.parent_schema,
            table: self.parent_table,
            columns: self.columns.iter().map(|column| column.parent_column.clone()).collect(),
            ref_schema: self.referenced_schema,
            ref_table: self.referenced_table,
            ref_columns: self.columns.into_iter().map(|column| column.referenced_column).collect(),
            delete_rule: match self.delete_action {
                MssqlForeignKeyAction::NoAction => None,
                MssqlForeignKeyAction::Cascade => Some("CASCADE".into()),
                MssqlForeignKeyAction::SetNull => Some("SET NULL".into()),
                MssqlForeignKeyAction::SetDefault => Some("SET DEFAULT".into()),
            },
        }
    }
    pub fn constraint_issue(&self) -> Option<ConstraintIssue> {
        constraint_issue(
            MssqlConstraintIssueFields {
                schema: self.parent_schema.clone(),
                table: self.parent_table.clone(),
                name: self.name.clone(),
                kind: ConstraintKind::ForeignKey,
                disabled: self.disabled,
            },
            self.trusted,
            self.not_for_replication,
        )
    }
}
impl MssqlCheckConstraint {
    pub fn constraint_issue(&self) -> Option<ConstraintIssue> {
        constraint_issue(
            MssqlConstraintIssueFields {
                schema: self.parent_schema.clone(),
                table: self.parent_table.clone(),
                name: self.name.clone(),
                kind: ConstraintKind::Check,
                disabled: self.disabled,
            },
            self.trusted,
            self.not_for_replication,
        )
    }
}
impl MssqlDmlTrigger {
    pub fn event_codes(&self) -> String {
        [('I', self.insert_event), ('D', self.delete_event), ('U', self.update_event)]
            .into_iter()
            .filter_map(|(code, present)| (present == Some(true)).then_some(code))
            .collect()
    }
    /// Only the active base-table prefix belongs in TableStorage. Unknown event flags remain unknown.
    pub fn table_storage_option(&self) -> Option<String> {
        (!self.disabled && self.parent_type == "U").then(|| format!("trigger={}:{}", self.name, self.event_codes()))
    }
}
impl MssqlPartitionBoundary {
    /// Diagnostic converted text only. Native binary/type fingerprints stay on the original fact; this
    /// option does not reconstruct a lossless boundary or describe every index/partition as one table state.
    pub fn display_storage_option(&self, display_value: Option<&str>) -> Option<String> {
        if self.index_id > 1 {
            return None;
        }
        let id = self.boundary_id?;
        let value = display_value?;
        let comparison = if self.boundary_on_right { "LESS THAN" } else { "LESS THAN OR EQUAL" };
        Some(format!("partition_bound=VALUES {comparison} ({value}):p{id}"))
    }
}
