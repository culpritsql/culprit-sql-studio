//! One closed Pool WaitCounters profile, sharing StateSource, membership, clocks and one SET reservation.
pub(in crate::telemetry) use super::wait_counters::bounded_budget;
use super::*;
pub(super) fn selected(request: &CollectionRequest) -> bool {
    request.selection == (CollectionSelection::Snapshot { dataset: SnapshotDataset::WaitCounters })
        && matches!(&request.subject,CollectionSubject::Shared{resource} if resource.engine==culprit_core::Engine::Mssql && resource.kind==SharedResourceKind::Pool)
}
pub(super) async fn subject<R: NativeRead>(
    reader: &mut R,
    requested: &CollectionSubject,
) -> Result<subject::SubjectRead> {
    super::super::description::ensure_supported(reader).await?;
    let identity =
        subject::Identity::PoolCurrent(Box::new(crate::resources::pool::CurrentContext::read(reader).await?));
    if !requested.accepts(&identity.target()) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(subject::SubjectRead { identity, configured_log_rates: Ok(None) })
}
pub(super) async fn closing<R: NativeRead>(
    owner: &subject::Identity,
    reader: &mut R,
    from: &str,
) -> Result<Option<(String, bool, MssqlPoolCurrentAuthority)>> {
    let subject::Identity::PoolCurrent(before) = owner else {
        return Ok(None);
    };
    let after = before.recheck(reader).await?;
    if !before.confirms(&after, from) {
        return Err(CollectionStop::SourceChanged.error());
    }
    Ok(Some((
        after.point.observed_at.clone(),
        after.metadata_visible,
        MssqlPoolCurrentAuthority { before: before.point.clone(), after: after.point, native: None },
    )))
}

#[cfg(test)]
mod tests;
