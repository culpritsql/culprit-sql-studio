//! The held source caller supplies its already-verified native owner and exact request. No pool is opened here.
use super::{Scope, parameters, sql};
use crate::{
    activity::participants::{Check, RequestState},
    local_source::Context,
    read::{NativeRead, ReadError, boolean, integer, invalid_data, optional_signed_integer, optional_text, text},
    value::Rec,
};
use culprit_core::{Result, telemetry::*};
use std::collections::BTreeSet;

pub(crate) struct SelectedProfileRows {
    pub observed_from: String,
    pub observed_until: String,
    pub rows: Vec<MssqlOperatorThreadProfile>,
    pub limited: bool,
}
pub(crate) enum SelectedProfiles {
    Observed(SelectedProfileRows),
    Unavailable(MssqlExecutionProfilesUnavailable),
}
pub(crate) async fn read<R: NativeRead>(
    reader: &mut R,
    owner: &Context,
    original: &Check,
    plan: &NativeIdentity,
    limit: usize,
) -> Result<SelectedProfiles> {
    if limit == 0 || limit > MAX_EXECUTION_PROFILE_ROWS || !owner.is_qualified() {
        return Err(invalid_data());
    }
    let (key, statement) = original.selector().ok_or_else(invalid_data)?;
    let generation = reader.connection_generation();
    if generation.is_none() || generation != owner.connection_generation() {
        return Err(CollectionStop::SourceChanged.error());
    }
    let rows = reader
        .query(
            &sql::profiles_for(sql::ProfileProjection::Threads { limit }),
            parameters(&key, &statement, plan)?,
            limit + 2,
        )
        .await
        .map_err(ReadError::into_error)?;
    if generation != reader.connection_generation() {
        return Err(CollectionStop::SourceChanged.error());
    }
    decode(owner, original, limit, rows)
}
fn decode(owner: &Context, original: &Check, limit: usize, rows: Vec<Rec>) -> Result<SelectedProfiles> {
    let mut guard = None;
    let mut profiles = Vec::new();
    for row in rows {
        match text(&row, "row_kind")?.as_str() {
            "guard" if guard.is_none() => guard = Some(row),
            "profile" => profiles.push(row),
            _ => return Err(invalid_data()),
        }
    }
    if profiles.len() > limit + 1 {
        return Err(CollectionStop::RowLimit.error());
    }
    let guard = guard.ok_or_else(invalid_data)?;
    let scope = Scope::read(&guard)?;
    let expected_role = owner.activity.writable.map(|writable| if writable { "READ_WRITE" } else { "READ_ONLY" });
    if scope.database != i64::from(owner.database_id)
        || scope.name != owner.activity.database.database
        || scope.role.as_deref() != expected_role
        || scope.request_page_server_read_capability != owner.activity.request_page_server_read_capability
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    let check = Check::decode(&guard)?;
    let started = canonical_time(&text(&guard, "profiles_observed_from")?)?;
    if ObservationBounds::new(&original.at, &started).is_none() || ObservationBounds::new(&started, &check.at).is_none()
    {
        return Err(CollectionStop::SourceChanged.error());
    }
    if !boolean(&guard, "session_established")? {
        return Ok(SelectedProfiles::Unavailable(MssqlExecutionProfilesUnavailable::UnverifiedParticipant));
    }
    let (key, statement) = original.selector().ok_or_else(invalid_data)?;
    let count = integer(&guard, "request_count")?;
    if !(0..=1).contains(&count) {
        return Err(invalid_data());
    }
    let state = check.selector_state(&key, &statement);
    let unavailable = match state {
        RequestState::Missing => Some(MssqlExecutionProfilesUnavailable::NotObserved),
        RequestState::Changed => Some(MssqlExecutionProfilesUnavailable::ParticipantChanged),
        RequestState::Unverified => Some(MssqlExecutionProfilesUnavailable::UnverifiedParticipant),
        RequestState::SameStatement if count != 1 => Some(MssqlExecutionProfilesUnavailable::NotObserved),
        RequestState::SameStatement if !check.matches_check(original) => {
            Some(MssqlExecutionProfilesUnavailable::PlanChanged)
        }
        RequestState::SameStatement => None,
    };
    if let Some(reason) = unavailable {
        return Ok(SelectedProfiles::Unavailable(reason));
    }
    let mut keys = BTreeSet::new();
    let mut decoded = Vec::new();
    for profile in profiles {
        if canonical_time(&text(&profile, "profiles_observed_from")?)? != started {
            return Err(invalid_data());
        }
        let profile = decode_profile(&profile)?;
        if !keys.insert((profile.node_id, profile.thread_id)) {
            return Err(invalid_data());
        }
        decoded.push(profile);
    }
    let limited = decoded.len() > limit;
    decoded.truncate(limit);
    Ok(SelectedProfiles::Observed(SelectedProfileRows {
        observed_from: started,
        observed_until: check.at,
        rows: decoded,
        limited,
    }))
}
fn decode_profile(row: &Rec) -> Result<MssqlOperatorThreadProfile> {
    let native = |name| optional_signed_integer(row, name).map(|value| value.map(|value| value.to_string()));
    let id = |name| u32::try_from(integer(row, name)?).map_err(|_| invalid_data());
    let signed = |name| optional_signed_integer(row, name);
    let profile = MssqlOperatorThreadProfile {
        node_id: id("node_id")?,
        thread_id: id("thread_id")?,
        task_address: optional_text(row, "task_address")?,
        physical_operator: text(row, "physical_operator_name")?,
        counters: MssqlOperatorThreadCounters {
            rows_returned: native("row_count")?,
            rows_read: native("actual_read_row_count")?,
            estimated_rows: native("estimate_row_count")?,
            elapsed_ms: native("elapsed_time_ms")?,
            cpu_ms: native("cpu_time_ms")?,
            logical_reads: native("logical_read_count")?,
            physical_reads: native("physical_read_count")?,
            read_ahead_reads: native("read_ahead_count")?,
            spill_write_pages: native("write_page_count")?,
            end_of_scans: native("end_of_scan_count")?,
            scans: native("scan_count")?,
            rebinds: native("rebind_count")?,
            rewinds: native("rewind_count")?,
        },
        open_ticks: native("open_time")?,
        close_ticks: native("close_time")?,
        database_id: signed("database_id")?
            .map(|value| i16::try_from(value).map_err(|_| invalid_data()))
            .transpose()?,
        object_id: signed("object_id")?.map(|value| i32::try_from(value).map_err(|_| invalid_data())).transpose()?,
        index_id: signed("index_id")?.map(|value| i32::try_from(value).map_err(|_| invalid_data())).transpose()?,
    };
    if !profile.is_valid() {
        return Err(invalid_data());
    }
    Ok(profile)
}

#[cfg(test)]
mod tests;
