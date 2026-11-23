//! One public option roster supplies both the native projection and each setting's meaning.
use culprit_core::telemetry::SettingNature;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Availability {
    Base,
    WaitCapture,
    CapturePolicy,
}

struct Field {
    name: &'static str,
    native_type: &'static str,
    nature: SettingNature,
    availability: Availability,
}

// Public catalog columns: SQL Server 2016+; wait capture is 2017+, CUSTOM policy is 2019+.
// Catalog presence, rather than a boxed major-version assumption, also supports Azure SQL and MI.
// https://learn.microsoft.com/sql/relational-databases/system-catalog-views/sys-database-query-store-options-transact-sql
const FIELDS: &[Field] = &[
    field("desired_state", "smallint", SettingNature::Configuration, Availability::Base),
    field("desired_state_desc", "nvarchar(60)", SettingNature::Configuration, Availability::Base),
    field("actual_state", "smallint", SettingNature::State, Availability::Base),
    field("actual_state_desc", "nvarchar(60)", SettingNature::State, Availability::Base),
    field("readonly_reason", "int", SettingNature::State, Availability::Base),
    field("current_storage_size_mb", "bigint", SettingNature::Measurement, Availability::Base),
    field("flush_interval_seconds", "bigint", SettingNature::Configuration, Availability::Base),
    field("interval_length_minutes", "bigint", SettingNature::Configuration, Availability::Base),
    field("max_storage_size_mb", "bigint", SettingNature::Configuration, Availability::Base),
    field("stale_query_threshold_days", "bigint", SettingNature::Configuration, Availability::Base),
    field("max_plans_per_query", "bigint", SettingNature::Configuration, Availability::Base),
    field("query_capture_mode", "smallint", SettingNature::Configuration, Availability::Base),
    field("query_capture_mode_desc", "nvarchar(60)", SettingNature::Configuration, Availability::Base),
    field("size_based_cleanup_mode", "smallint", SettingNature::Configuration, Availability::Base),
    field("size_based_cleanup_mode_desc", "nvarchar(60)", SettingNature::Configuration, Availability::Base),
    field("wait_stats_capture_mode", "smallint", SettingNature::Configuration, Availability::WaitCapture),
    field("wait_stats_capture_mode_desc", "nvarchar(60)", SettingNature::Configuration, Availability::WaitCapture),
    field("capture_policy_execution_count", "int", SettingNature::Configuration, Availability::CapturePolicy),
    field(
        "capture_policy_total_compile_cpu_time_ms",
        "bigint",
        SettingNature::Configuration,
        Availability::CapturePolicy,
    ),
    field(
        "capture_policy_total_execution_cpu_time_ms",
        "bigint",
        SettingNature::Configuration,
        Availability::CapturePolicy,
    ),
    field("capture_policy_stale_threshold_hours", "int", SettingNature::Configuration, Availability::CapturePolicy),
];

const fn field(
    name: &'static str,
    native_type: &'static str,
    nature: SettingNature,
    availability: Availability,
) -> Field {
    Field { name, native_type, nature, availability }
}

pub(crate) fn fields() -> impl Iterator<Item = (&'static str, SettingNature)> {
    FIELDS.iter().map(|field| (field.name, field.nature))
}

/// One result set and dispatch. Absent or hidden versioned cohorts retain typed NULL, never a default.
pub(super) fn query(context_columns: &str, execution_parameters: &str) -> String {
    let wait_capture = availability(Availability::WaitCapture);
    let capture_policy = availability(Availability::CapturePolicy);
    let mut batch = format!(
        "DECLARE /* culprit */ @culprit_query_store_options nvarchar(max),\n\
         @culprit_query_store_wait_capture bit=CASE WHEN {wait_capture} THEN 1 ELSE 0 END,\n\
         @culprit_query_store_capture_policy bit=CASE WHEN {capture_policy} THEN 1 ELSE 0 END;\n"
    );
    for (branch, waits, policy) in [("IF", true, true), ("ELSE IF", true, false), ("ELSE IF", false, true)] {
        let query = statement(context_columns, waits, policy).replace('\'', "''");
        batch.push_str(&format!(
            "{branch} @culprit_query_store_wait_capture={} AND @culprit_query_store_capture_policy={}\n\
             SET /* culprit */ @culprit_query_store_options=N'{query}';\n",
            u8::from(waits),
            u8::from(policy),
        ));
    }
    let oldest = statement(context_columns, false, false).replace('\'', "''");
    batch.push_str(&format!(
        "ELSE SET /* culprit */ @culprit_query_store_options=N'{oldest}';\n\
         EXEC /* culprit */ sys.sp_executesql @culprit_query_store_options{execution_parameters};"
    ));
    batch
}

fn availability(cohort: Availability) -> String {
    FIELDS
        .iter()
        .filter(|field| field.availability == cohort)
        .map(|field| format!("COL_LENGTH(N'sys.database_query_store_options',N'{}') IS NOT NULL", field.name))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn statement(context_columns: &str, wait_capture: bool, capture_policy: bool) -> String {
    let fields = FIELDS
        .iter()
        .map(|field| match field.availability {
            Availability::WaitCapture if !wait_capture => unavailable(field),
            Availability::CapturePolicy if !capture_policy => unavailable(field),
            _ => format!("o.[{}]", field.name),
        })
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT /* culprit */ {context_columns}{},{fields},\
         HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION') AS metadata_visibility \
         FROM sys.database_query_store_options o",
        super::layout::columns!(),
    );
    // ReadQuery tags only executable outer SQL, so the complete inner statement must be tagged before quoting.
    crate::statement_origin::monitoring_batch(&sql)
}

fn unavailable(field: &Field) -> String {
    format!("CAST(NULL AS {}) AS [{}]", field.native_type, field.name)
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod native_tests;
