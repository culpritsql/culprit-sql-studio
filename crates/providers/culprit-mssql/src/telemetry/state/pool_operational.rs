//! One explicitly selected pool profile. Existing native readers/decoders and StateSource page/finish owners stay single-owned.
use super::*;
use crate::resources::pool::native::Context;
use culprit_core::telemetry::{SharedResourceIdentity, SharedResourceKind};

// Azure profile1 + three native anchor/process/roster observations3x3 + five pressure admissions5
// + clerks1/scheduler-layout+rows2/counters1/files1/tempdb1 + the existing owned SET1 = 22.
const MAX_TOTAL_QUERIES: u32 = 22;
const MAX_CURRENT_ROWS: u32 = 10_000;
const MAX_PAGE_BYTES: u32 = 512 * 1024;
const MAX_TOTAL_BYTES: u32 = 8 * 1024 * 1024;
const STATEMENT_TIMEOUT_MS: u32 = 1000;
const TOTAL_TIMEOUT_MS: u32 = 15_000;
const FAMILIES: [point::Family; 5] = [
    point::Family::MemoryClerks,
    point::Family::Schedulers,
    point::Family::PerformanceCounters,
    point::Family::FileIo,
    point::Family::Operation(crate::operations::native::Kind::Temp),
];

pub(crate) fn bounded_budget(caller: &CollectionBudget) -> CollectionBudget {
    CollectionBudget {
        max_queries: caller.max_queries.min(MAX_TOTAL_QUERIES),
        max_snapshot_rows: caller.max_snapshot_rows.min(MAX_CURRENT_ROWS),
        page_rows: caller.page_rows.min(MAX_CURRENT_ROWS),
        page_bytes: caller.page_bytes.min(MAX_PAGE_BYTES),
        total_bytes: caller.total_bytes.min(MAX_TOTAL_BYTES),
        statement_timeout_ms: caller.statement_timeout_ms.min(STATEMENT_TIMEOUT_MS),
        total_timeout_ms: caller.total_timeout_ms.min(TOTAL_TIMEOUT_MS),
        ..caller.clone()
    }
}
pub(super) fn selected(request: &CollectionRequest) -> bool {
    request.selection == (CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational })
        && matches!(&request.subject, CollectionSubject::Shared { resource } if resource.engine == culprit_core::Engine::Mssql && resource.kind == SharedResourceKind::Pool)
}
pub(crate) struct Read {
    pub read: MssqlPoolOperationalRead,
    pub owner: Context,
}
pub(crate) async fn read(
    reader: &mut impl NativeRead,
    requested: &SharedResourceIdentity,
    budget: &CollectionBudget,
) -> Result<Read> {
    super::super::description::ensure_supported(reader).await?;
    let before = Context::read(reader).await?;
    if requested != &before.current.point.pool {
        return Err(CollectionStop::SourceChanged.error());
    }
    let mut rows = Vec::new();
    let mut sources = Vec::new();
    for family in FAMILIES {
        let Some(_) = before.scope.observed() else {
            let code = match before.scope {
                MssqlPoolNativeScope::Unavailable { reason: MssqlPoolNativeLimitation::VisibilityUnavailable } => {
                    ErrorCode::PermissionDenied
                }
                _ => ErrorCode::Unsupported,
            };
            sources.push(StateSourceCoverage::unavailable(family.source().name.into(), code, None));
            continue;
        };
        let remaining = (budget.max_snapshot_rows as usize).saturating_sub(rows.len());
        if remaining == 0 {
            return Err(CollectionStop::RowLimit.error());
        }
        before.current.check_transport(reader)?;
        crate::resources::admit_pool_resources(reader).await.map_err(ReadError::into_error)?;
        before.current.check_transport(reader)?;
        let result = read_family(reader, family, &before, remaining.min(MAX_BUFFERED_FAMILY_ROWS)).await;
        if let Err(changed) = before.current.check_transport(reader) {
            return Err(result.err().unwrap_or(changed));
        }
        sources.push(record_family(family, result, remaining, &mut rows)?);
    }
    let after = before.recheck(reader).await?;
    let authority = before.authority(&after)?;
    let observations = rows
        .into_iter()
        .map(|row| match row {
            TelemetryRecord::Operational(row) => Ok(row),
            _ => Err(invalid_data()),
        })
        .collect::<Result<Vec<_>>>()?;
    let source = CollectionDescription {
        target: CollectionTarget::Shared(before.current.point.pool.clone()),
        partition: SourcePartition::Target,
        source: MSSQL_POOL_OPERATIONAL_SOURCE.into(),
        continuity: SourceContinuity::Unverified,
        observation: SourceObservation::MssqlPoolCurrent {
            snapshot: StateSnapshotDescription {
                dataset: SnapshotDataset::Operational,
                observed_from: before.current.point.observed_at.clone(),
                observed_until: after.current.point.observed_at.clone(),
                records: observations.len().try_into().map_err(|_| invalid_data())?,
                sources,
            },
            authority: Box::new(authority),
        },
    };
    let read = MssqlPoolOperationalRead { source, observations };
    if !read.is_valid() {
        return Err(invalid_data());
    }
    Ok(Read { read, owner: before })
}
async fn read_family(
    reader: &mut impl NativeRead,
    family: point::Family,
    owner: &Context,
    limit: usize,
) -> Result<point::ReadOutcome> {
    match family {
        point::Family::PerformanceCounters => {
            let read = crate::performance::read_for_pool(reader, limit, owner).await?;
            let complete_scope = read.resource_governor_complete;
            let values =
                crate::performance::observations(read)?.into_iter().map(TelemetryRecord::Operational).collect();
            Ok(point::ReadOutcome::Observed(point::Records {
                values,
                complete_scope,
                error_code: None,
                pool_settings_point: None,
            }))
        }
        point::Family::FileIo => {
            let files = crate::storage_io::read_pool(reader, limit, owner).await?;
            if files.is_empty() {
                return Err(CollectionStop::SourceUnavailable.error());
            }
            let (_, members) = owner.scope.observed().ok_or_else(invalid_data)?;
            let complete_scope = members
                .iter()
                .all(|member| files.iter().any(|file| file.native_database_id == member.database_id.to_string()));
            let values = crate::storage_io::observations(files).into_iter().map(TelemetryRecord::Operational).collect();
            Ok(point::ReadOutcome::Observed(point::Records {
                values,
                complete_scope,
                error_code: None,
                pool_settings_point: None,
            }))
        }
        point::Family::MemoryClerks
        | point::Family::Schedulers
        | point::Family::Operation(crate::operations::native::Kind::Temp) => {
            family.read(reader, limit, None, None).await
        }
        _ => Err(invalid_data()),
    }
}

#[cfg(test)]
mod tests;
