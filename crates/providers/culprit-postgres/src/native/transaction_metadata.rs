//! Exact existing cleanup decoding, bounded independently from backend/cache and shared SLRU context.
use super::*;
use culprit_core::telemetry::*;
use std::collections::BTreeSet;

pub(crate) struct Retainers {
    pub snapshot: RetentionSnapshot,
    pub native_rows: u32,
    pub limited: bool,
}
pub(crate) async fn read_retainers<R: QueryRead>(
    reader: &mut R,
    version: u32,
    standby: bool,
    limit: usize,
) -> Result<Retainers> {
    if !(1..=MAX_TRANSACTION_METADATA_RETAINERS).contains(&limit) {
        return Err(invalid());
    }
    // One coverage row plus the independently bounded holder prefix and one lookahead.
    let sql = cleanup::capture_query(version, standby, limit + 2)?;
    let rows = reader.query(&sql, limit + 2).await?;
    if rows.len() > limit + 2 {
        return Err(invalid());
    }
    let mut coverage = None;
    let mut holders = vec![];
    let mut identities = BTreeSet::new();
    for row in rows {
        if text(&row, "native_source")? == RetentionCoverage::SOURCE {
            if coverage.replace(cleanup::decode_coverage(&row, standby)?).is_some() {
                return Err(invalid());
            }
        } else {
            let holder = cleanup::decode(&row)?;
            if !identities.insert(holder.key()?) {
                return Err(invalid());
            }
            holders.push(holder);
        }
    }
    let native_rows = holders.len() as u32;
    let limited = holders.len() > limit;
    holders.truncate(limit);
    let coverage = coverage.ok_or_else(invalid)?;
    let sources = coverage
        .native_sources
        .iter()
        .map(|name| RetentionSourceCoverage {
            source: name.clone(),
            state: if limited {
                culprit_core::SourceState::Partial
            } else if holders.iter().any(|holder| holder.source == *name) {
                culprit_core::SourceState::Collected
            } else {
                culprit_core::SourceState::Empty
            },
        })
        .collect();
    let snapshot = RetentionSnapshot { holders, sources, coverage };
    if !snapshot.is_valid() {
        return Err(invalid());
    }
    Ok(Retainers { snapshot, native_rows, limited })
}

#[cfg(test)]
mod tests;
