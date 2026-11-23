//! Pool-wide views need the real anchor, effective visibility, native startup and a complete pool-local ID roster.
//! https://learn.microsoft.com/azure/azure-sql/database-watcher-data#data-collection-in-elastic-pools
//! These are current read fences; neither an anchor GUID nor a PID becomes a stable compute/pool incarnation.
use super::CurrentContext;
use crate::{
    local_source::process,
    read::{NativeRead, Parameter, ReadError, integer, invalid_data, text},
};
use culprit_core::{Result, telemetry::*};

const MEMBERS: &str = r#"SELECT /* culprit */ TOP (@P1) g.database_id,g.database_name,g.server_name,
 CONVERT(varchar(36),g.logical_database_guid) AS logical_database_guid,
 CONVERT(varchar(36),g.physical_database_guid) AS physical_database_guid,g.primary_group_id
 FROM sys.dm_user_db_resource_governance g ORDER BY g.database_id OPTION(MAXDOP 1)"#;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Context {
    pub current: CurrentContext,
    pub scope: MssqlPoolNativeScope,
    process: Option<process::ProcessEpoch>,
}
impl Context {
    pub(crate) async fn read(reader: &mut impl NativeRead) -> Result<Self> {
        let current = CurrentContext::read_native(reader).await?;
        Self::with_current(reader, current).await
    }
    async fn with_current(reader: &mut impl NativeRead, current: CurrentContext) -> Result<Self> {
        let limitation = match operational_qualification(&current.point) {
            MssqlPoolScopeQualification::Qualified => None,
            MssqlPoolScopeQualification::UnqualifiedTier => Some(MssqlPoolNativeLimitation::UnqualifiedTier),
            MssqlPoolScopeQualification::VisibilityUnavailable => {
                Some(MssqlPoolNativeLimitation::VisibilityUnavailable)
            }
        };
        if let Some(reason) = limitation {
            return Ok(Self { current, scope: MssqlPoolNativeScope::Unavailable { reason }, process: None });
        }
        current.check_transport(reader)?;
        let observation = process::read_azure(reader).await?;
        current.check_transport(reader)?;
        let process = observation.epoch;
        if current.point.native_tier.as_deref() == Some("Hyperscale") && observation.replica_id.is_none() {
            return Ok(Self {
                current,
                scope: MssqlPoolNativeScope::Unavailable {
                    reason: MssqlPoolNativeLimitation::ComputeMemberUnavailable,
                },
                process,
            });
        }
        let Some(epoch) = process.as_ref() else {
            return Ok(Self {
                current,
                scope: MssqlPoolNativeScope::Unavailable { reason: MssqlPoolNativeLimitation::ProcessEpochUnavailable },
                process: None,
            });
        };
        let rows = reader
            .query(MEMBERS, vec![Parameter::Integer((MAX_MSSQL_POOL_MEMBERS + 1) as i64)], MAX_MSSQL_POOL_MEMBERS + 1)
            .await
            .map_err(ReadError::into_error)?;
        current.check_transport(reader)?;
        let mut scope = if rows.len() > MAX_MSSQL_POOL_MEMBERS {
            MssqlPoolNativeScope::Unavailable { reason: MssqlPoolNativeLimitation::MemberRosterLimit }
        } else if rows.is_empty() {
            MssqlPoolNativeScope::Unavailable { reason: MssqlPoolNativeLimitation::MemberRosterUnavailable }
        } else {
            MssqlPoolNativeScope::Observed {
                process_epoch: epoch.key()?,
                anchor_replica_id: observation.replica_id,
                members: rows.iter().map(member).collect::<Result<Vec<_>>>()?,
            }
        };
        if !scope.is_valid_for(&current.point) {
            return Err(CollectionStop::SourceChanged.error());
        }
        if let Err(error) = scope.key() {
            if CollectionStop::from_error(&error) != CollectionStop::ByteLimit {
                return Err(error);
            }
            scope = MssqlPoolNativeScope::Unavailable { reason: MssqlPoolNativeLimitation::MemberRosterLimit };
        }
        Ok(Self { current, scope, process })
    }
    pub(crate) async fn recheck(&self, reader: &mut impl NativeRead) -> Result<Self> {
        let current = self.current.recheck_native(reader).await?;
        Self::with_current(reader, current).await
    }
    pub(crate) fn confirms(&self, after: &Self, last_at: &str) -> bool {
        self.current.confirms_native(&after.current, last_at)
            && self.scope == after.scope
            && self.process == after.process
    }
    pub(crate) fn authority(&self, after: &Self) -> Result<MssqlPoolCurrentAuthority> {
        if !self.confirms(after, &self.current.point.observed_at) {
            return Err(CollectionStop::SourceChanged.error());
        }
        let authority = MssqlPoolCurrentAuthority {
            before: self.current.point.clone(),
            after: after.current.point.clone(),
            native: Some(Box::new(MssqlPoolNativeAuthority { before: self.scope.clone(), after: after.scope.key()? })),
        };
        if !authority.is_valid() {
            return Err(invalid_data());
        }
        Ok(authority)
    }
    pub(crate) fn accepts_profile(&self, row: &crate::value::Rec) -> Result<bool> {
        let MssqlPoolNativeScope::Observed { anchor_replica_id, .. } = &self.scope else {
            return Ok(false);
        };
        Ok(crate::read::integer(row, "engine_edition")? == 5
            && crate::read::optional_text(row, "database_edition")? == self.current.point.native_tier
            && crate::replica::id(row, "replica_id")? == *anchor_replica_id
            && crate::read::database_writable(row)? == Some(self.current.point.database_writable))
    }
    pub(crate) fn accepts_counter_point(&self, row: &crate::value::Rec) -> Result<bool> {
        let matches = match &self.process {
            Some(process) => process.matches_counter_point(row)?,
            None => false,
        };
        Ok(matches
            && self.accepts_profile(row)?
            && crate::read::integer(row, "edition")? == 5
            && canonical_time(&crate::read::text(row, "observed_at")?)?
                == canonical_time(&crate::read::text(row, "counter_observed_at")?)?)
    }
}
fn member(row: &crate::value::Rec) -> Result<MssqlPoolNativeMember> {
    Ok(MssqlPoolNativeMember {
        database_id: u32::try_from(integer(row, "database_id")?).map_err(|_| invalid_data())?,
        database_name: text(row, "database_name")?,
        identity: NativeIdentity::new(text(row, "server_name")?)
            .qualified("logical_database_guid", text(row, "logical_database_guid")?)
            .qualified("physical_database_guid", text(row, "physical_database_guid")?),
        workload_group_id: u32::try_from(integer(row, "primary_group_id")?).map_err(|_| invalid_data())?,
    })
}
