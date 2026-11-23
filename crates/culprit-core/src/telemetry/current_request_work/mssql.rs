//! Live request counters and a later, independently clocked cached statement are separate populations.
use super::*;

pub const MAX_CURRENT_REQUEST_WORK_PLAN_BYTES: usize = 64 * 1024;

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum MssqlCurrentPlanUnavailable {
    NotObserved,
    ChangedDuringRead,
    Ambiguous,
    PermissionDenied,
    Unsupported,
    NotCollected,
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum MssqlCurrentRequestPlan {
    /// The selected cache identity agrees at these endpoints. It is not an original execution attestation.
    Observed {
        observed_from: String,
        observed_until: String,
        context: Box<CachedStatementContext>,
        plan: DefinitionPayload,
    },
    Unavailable {
        reason: MssqlCurrentPlanUnavailable,
    },
}
impl MssqlCurrentRequestPlan {
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Observed { observed_from, observed_until, context, plan } => {
                ObservationBounds::new(observed_from, observed_until).is_some()
                    && context.is_valid()
                    && context.identity.has_plan_handle()
                    && context.identity.has_sql_handle()
                    && plan.is_valid()
                    && match plan {
                        DefinitionPayload::Available { text, .. } => text.len() <= MAX_CURRENT_REQUEST_WORK_PLAN_BYTES,
                        DefinitionPayload::Unavailable { .. } => true,
                    }
            }
            Self::Unavailable { .. } => true,
        }
    }
    pub fn matches_statement(&self, statement: &NativeIdentity) -> bool {
        if !mssql_request_statement_identity_is_valid(statement) {
            return false;
        }
        match self {
            Self::Unavailable { .. } => true,
            Self::Observed { context, .. } => {
                let identity = &context.identity;
                identity.sql_handle.strip_prefix("0x").is_some_and(|handle| handle.eq_ignore_ascii_case(&statement.id))
                    && statement.qualifiers.get("statement_start_offset")
                        == Some(&identity.statement_start_bytes.to_string())
                    && statement.qualifiers.get("statement_end_offset")
                        == Some(&identity.statement_end_bytes.to_string())
                    && statement.qualifiers.get("database_id") == Some(&context.database_id.to_string())
            }
        }
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct MssqlCurrentRequestWork {
    pub native_database_id: u32,
    pub checked_until: String,
    /// Native signed int milliseconds. A negative value is retained but is not a duration estimate.
    pub elapsed_ms: Option<i32>,
    pub cpu_ms: Option<i32>,
    /// Exact native request counters, not cached-plan totals or a server population.
    pub reads: Option<String>,
    pub writes: Option<String>,
    pub logical_reads: Option<String>,
    pub rows_returned: Option<String>,
    /// Same current native plan token at request endpoints, independently of cache metadata availability.
    pub request_plan: Option<NativeIdentity>,
    pub current_plan: MssqlCurrentRequestPlan,
}
impl MssqlCurrentRequestWork {
    pub fn is_valid(&self) -> bool {
        (1..=i16::MAX as u32).contains(&self.native_database_id)
            && canonical_time(&self.checked_until).is_ok()
            && [&self.reads, &self.writes, &self.logical_reads, &self.rows_returned]
                .into_iter()
                .flatten()
                .all(|value| exact_counter(value) && value.parse::<i64>().is_ok())
            && self.request_plan.as_ref().is_none_or(|plan| {
                plan.is_valid()
                    && plan.qualifiers.get("native_object").map(String::as_str)
                        == Some("sys.dm_exec_requests.plan_handle")
                    && !plan.id.is_empty()
                    && plan.id.len() <= 128
                    && plan.id.len().is_multiple_of(2)
                    && plan.id.bytes().all(|v| v.is_ascii_hexdigit())
                    && !plan.id.bytes().all(|v| v == b'0')
            })
            && self.current_plan.is_valid()
            && match &self.current_plan {
                MssqlCurrentRequestPlan::Observed { context, .. } => self.request_plan.as_ref().is_some_and(|plan| {
                    context
                        .identity
                        .plan_handle
                        .strip_prefix("0x")
                        .is_some_and(|handle| handle.eq_ignore_ascii_case(&plan.id))
                }),
                _ => true,
            }
    }
    pub(crate) fn fits_context(&self, context: &NativeReadContext) -> bool {
        let Some(bounds) = ObservationBounds::new(&context.observed_from, &context.observed_until) else {
            return false;
        };
        bounds.contains(&self.checked_until)
            && match &self.current_plan {
                MssqlCurrentRequestPlan::Observed { observed_from, observed_until, .. } => {
                    bounds.contains(observed_from)
                        && bounds.contains(observed_until)
                        && ObservationBounds::new(observed_until, &self.checked_until).is_some()
                }
                _ => true,
            }
    }
}

pub fn mssql_request_statement_identity(handle: &str, start: u32, end: i32, database: u32) -> Option<NativeIdentity> {
    if handle.is_empty()
        || handle.len() > 128
        || !handle.len().is_multiple_of(2)
        || !handle.bytes().all(|v| v.is_ascii_hexdigit())
        || handle.bytes().all(|v| v == b'0')
        || !start.is_multiple_of(2)
        || start > i32::MAX as u32
        || !(end == -1 || end >= 0 && end % 2 == 0 && end as u32 >= start)
        || !(1..=i16::MAX as u32).contains(&database)
    {
        return None;
    }
    Some(
        NativeIdentity::new(handle.to_ascii_uppercase())
            .qualified("native_object", "sys.dm_exec_requests.sql_handle")
            .qualified("statement_start_offset", start.to_string())
            .qualified("statement_end_offset", end.to_string())
            .qualified("database_id", database.to_string()),
    )
}
pub fn mssql_request_statement_identity_is_valid(identity: &NativeIdentity) -> bool {
    if !identity.is_valid() || identity.qualifiers.len() != 4 {
        return false;
    }
    let number = |key: &str| identity.qualifiers.get(key)?.parse::<i64>().ok();
    let (Some(start), Some(end), Some(database)) =
        (number("statement_start_offset"), number("statement_end_offset"), number("database_id"))
    else {
        return false;
    };
    let (Ok(start), Ok(end), Ok(database)) = (u32::try_from(start), i32::try_from(end), u32::try_from(database)) else {
        return false;
    };
    mssql_request_statement_identity(&identity.id, start, end, database)
        .is_some_and(|canonical| canonical.qualifiers == identity.qualifiers)
}
pub(super) fn mssql_actor_is_valid(actor: &ActivityKey) -> bool {
    let Some(request) = &actor.request else { return false };
    let native_id = |v: &str, session: bool| {
        v.parse::<u32>()
            .is_ok_and(|n| n.to_string() == v && n <= i32::MAX as u32 && (!session || n > 0 && n <= i16::MAX as u32))
    };
    let native_time = |v: &str| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S%.f").is_ok();
    native_id(&actor.session.id, true)
        && native_id(&request.id, false)
        && native_time(&actor.session.generation)
        && native_time(&request.generation)
        && actor.transaction.is_none()
}
