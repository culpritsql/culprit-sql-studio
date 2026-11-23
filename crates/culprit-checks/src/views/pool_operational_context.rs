//! One rendering of the closed native pool limitations; prose never selects scope or availability.
use culprit_core::{Result, t, telemetry::*};
pub(super) fn limitations(source: &CollectionDescription) -> Result<Vec<String>> {
    let Some(authority) = mssql_pool_current_authority(source)? else {
        return Ok(Vec::new());
    };
    let Some(native) = authority.native.as_deref() else {
        return Ok(Vec::new());
    };
    let mut result = vec![t!("view.pool_operational.limit.scope")];
    if let MssqlPoolNativeScope::Unavailable { reason } = native.before {
        result.push(match reason {
            MssqlPoolNativeLimitation::UnqualifiedTier => t!("view.pool_operational.limit.tier"),
            MssqlPoolNativeLimitation::VisibilityUnavailable => t!("view.pool_operational.limit.visibility"),
            MssqlPoolNativeLimitation::ProcessEpochUnavailable => t!("view.pool_operational.limit.epoch"),
            MssqlPoolNativeLimitation::ComputeMemberUnavailable => t!("view.pool_operational.limit.member"),
            MssqlPoolNativeLimitation::MemberRosterUnavailable => t!("view.pool_operational.limit.roster"),
            MssqlPoolNativeLimitation::MemberRosterLimit => t!("view.pool_operational.limit.roster_limit"),
        });
    }
    Ok(result)
}
