//! Working checkpoints may retire resolved episodes. Acceptance follows retained authenticated revisions.
use super::archive::RetainedRevision;
use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::collector_service::monitoring) struct NativeHistory {
    pub first: Box<DiagnosticEpisode>,
    pub latest: Box<DiagnosticEpisode>,
    pub references: Vec<DiagnosticEvidenceRef>,
}
pub(in crate::collector_service::monitoring) fn for_episode(
    rows: &[RetainedRevision],
    episode_id: &str,
    first: &DiagnosticEvidenceRef,
) -> Result<NativeHistory> {
    let mut ordered = BTreeMap::new();
    let mut references = BTreeSet::new();
    for row in rows.iter().filter(|row| row.revision.episode.id == episode_id && row.revision.episode.first == *first) {
        let revision = &row.revision;
        let number = revision.episode.revision.parse::<u64>().map_err(|_| invalid())?;
        if number == 0
            || number.to_string() != revision.episode.revision
            || !revision.is_valid()
            || !references.insert(row.reference.key()?)
            || ordered.insert(number, row).is_some()
        {
            return Err(invalid());
        }
    }
    let first_row = ordered.get(&1).ok_or_else(invalid)?;
    let mut expected = 1_u64;
    let mut previous = None;
    for (number, row) in &ordered {
        if *number != expected
            || row.revision.previous_revision != previous
            || row.revision.episode.id != episode_id
            || row.revision.episode.first != *first
        {
            return Err(invalid());
        }
        previous = Some(number.to_string());
        expected = expected.checked_add(1).ok_or_else(invalid)?;
    }
    let latest = ordered.last_key_value().ok_or_else(invalid)?.1;
    Ok(NativeHistory {
        first: Box::new(first_row.revision.episode.clone()),
        latest: Box::new(latest.revision.episode.clone()),
        references: ordered.values().map(|row| row.reference.clone()).collect(),
    })
}
fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collected_evidence::RetainedEvidenceFixture;
    use culprit_evidence::analytical::test_support::{self, IncidentHistoryCase};
    use culprit_evidence::original_projection::OriginalProjection;

    #[tokio::test]
    async fn retained_resolved_history_keeps_first_and_resolution_when_no_terminal_episode_is_in_the_working_set() {
        let (source, records) = test_support::incident_history_evidence(IncidentHistoryCase::Resolved);
        let fixture = RetainedEvidenceFixture::new(source, records).await;
        let capture =
            fixture.read_families(&[culprit_evidence::analytical::evidence::EvidenceFamily::OperationalSamples]).await;
        let original = capture.verified_original().unwrap();
        let mut revisions = Vec::new();
        for (position, row) in original.records().iter().enumerate() {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::DiagnosticRevision(revision),
                ..
            }) = &row.row().record
            else {
                continue;
            };
            revisions.push(RetainedRevision {
                reference: original.reference(position).unwrap(),
                revision: revision.clone(),
            });
        }
        let first = revisions[0].revision.episode.first.clone();
        let episode_id = revisions[0].revision.episode.id.clone();
        let history = for_episode(&revisions, &episode_id, &first).unwrap();
        assert_eq!(history.first.revision, "1");
        assert_eq!(history.latest.lifecycle, DiagnosticLifecycle::Resolved);
        assert!(!history.latest.retained_in_checkpoint());
        assert_eq!(history.latest.first, first);
        assert!(history.latest.recovery.is_some());
        assert_eq!(history.references.len(), 3);
        let mut missing = revisions.clone();
        missing.remove(1);
        assert!(for_episode(&missing, &episode_id, &first).is_err());
        let mut duplicate = revisions.clone();
        duplicate.push(revisions[0].clone());
        assert!(for_episode(&duplicate, &episode_id, &first).is_err());
    }
}
