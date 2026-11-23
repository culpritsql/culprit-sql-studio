//! Only explicit original references add edges. Query labels, hashes and neighboring times add none.
use super::*;

#[derive(Default)]
pub(super) struct Ancestry {
    links: BTreeMap<String, IncidentEvidenceRelationship>,
}
impl Ancestry {
    pub(super) fn input_capture(
        &mut self,
        revision: &DiagnosticEvidenceRef,
        input: &DerivedInputReference,
    ) -> Result<()> {
        let role = match input.role {
            DerivedInputRole::Observation => DiagnosticAncestryRole::AnalysisObservation,
            DerivedInputRole::Catalog => DiagnosticAncestryRole::AnalysisCatalog,
            DerivedInputRole::Revision => DiagnosticAncestryRole::AnalysisRevision,
            DerivedInputRole::RelatedEvidence => DiagnosticAncestryRole::AnalysisRelatedEvidence,
        };
        self.insert(IncidentEvidenceRelationship {
            revision: revision.clone(),
            role,
            capture: input.capture.clone(),
            reference: None,
            catalog: input.catalog.clone(),
            availability: IncidentEvidenceAvailability::LimitReached,
        })
    }
    pub(super) fn observe(&mut self, revision: &DiagnosticRevision, original: &DiagnosticEvidenceRef) -> Result<()> {
        for link in revision.referenced_evidence()? {
            match link.target {
                DiagnosticAncestryTarget::Record(reference) => self.row(original, link.role, reference)?,
                DiagnosticAncestryTarget::Capture(capture) => self.capture(original, link.role, capture)?,
            }
        }
        Ok(())
    }

    fn row(
        &mut self,
        revision: &DiagnosticEvidenceRef,
        role: DiagnosticAncestryRole,
        reference: &DiagnosticEvidenceRef,
    ) -> Result<()> {
        self.insert(IncidentEvidenceRelationship {
            revision: revision.clone(),
            role,
            capture: reference.capture_ref(),
            reference: Some(reference.clone()),
            catalog: None,
            availability: IncidentEvidenceAvailability::LimitReached,
        })
    }
    fn capture(
        &mut self,
        revision: &DiagnosticEvidenceRef,
        role: DiagnosticAncestryRole,
        capture: &DiagnosticCaptureRef,
    ) -> Result<()> {
        self.insert(IncidentEvidenceRelationship {
            revision: revision.clone(),
            role,
            capture: capture.clone(),
            reference: None,
            catalog: None,
            availability: IncidentEvidenceAvailability::LimitReached,
        })
    }
    fn insert(&mut self, link: IncidentEvidenceRelationship) -> Result<()> {
        let key = digest(
            &serde_json::to_vec(&(&link.revision, link.role, &link.capture, &link.reference, &link.catalog))
                .map_err(|_| invalid())?,
        );
        self.links.insert(key, link);
        if self.links.len() > MAX_DIAGNOSTIC_ANCESTRY_LINKS {
            return Err(capacity());
        }
        Ok(())
    }
}
impl Assembly<'_> {
    pub(super) async fn authenticate(&mut self, ancestry: Ancestry) -> Result<Vec<IncidentEvidenceRelationship>> {
        let mut groups: BTreeMap<String, Vec<IncidentEvidenceRelationship>> = BTreeMap::new();
        for link in ancestry.links.into_values() {
            groups.entry(link.capture.key()?).or_default().push(link);
        }
        let mut relationships = Vec::new();
        for mut links in groups.into_values() {
            let expected = links[0].capture.clone();
            let range = range_id_for(self.contract, &expected.capture)?;
            let requested = links.iter().filter_map(|link| link.reference.clone()).collect::<Vec<_>>();
            let header = self.authenticate_header(&range, &expected).await?;
            if header != IncidentEvidenceAvailability::Authenticated {
                if let IncidentEvidenceAvailability::Unavailable { gaps } = &header {
                    self.gaps
                        .push(IncidentReadGap::CaptureUnavailable { capture: expected.clone(), gaps: gaps.clone() });
                }
                for link in &mut links {
                    link.availability = header.clone();
                }
            } else if requested.is_empty() {
                for link in &mut links {
                    link.availability = IncidentEvidenceAvailability::Authenticated;
                }
            } else {
                let selected = self.record_lookup(&requested).await?;
                for link in &mut links {
                    link.availability = match &link.reference {
                        Some(reference) if selected.selected(reference)?.is_none() => {
                            let gaps = selected.gaps(reference)?;
                            if gaps.is_empty() {
                                return Err(invalid());
                            }
                            self.gaps.push(IncidentReadGap::OriginalUnavailable {
                                reference: Box::new(reference.clone()),
                                gaps: gaps.clone(),
                            });
                            IncidentEvidenceAvailability::Unavailable { gaps }
                        }
                        _ => IncidentEvidenceAvailability::Authenticated,
                    };
                }
            }
            relationships.extend(links);
        }
        Ok(relationships)
    }
    pub(super) async fn authenticate_header(
        &mut self,
        range: &str,
        expected: &DiagnosticCaptureRef,
    ) -> Result<IncidentEvidenceAvailability> {
        let key = expected.key()?;
        if self.missing_originals.contains(&key) {
            return Ok(not_committed());
        }
        if let Some(capture) = self.captures.get(range) {
            references::validate_capture_reference(capture, expected)?;
            // Native/source coverage is independent; this state authenticates the header only.
            return Ok(if capture.gaps().iter().any(|gap| gap.reason == EvidenceGapReason::NotPublished) {
                IncidentEvidenceAvailability::Unavailable { gaps: capture.gaps().to_vec() }
            } else {
                IncidentEvidenceAvailability::Authenticated
            });
        }
        if !self.metadata.contains_key(range) {
            if self.captures.len() + self.metadata.len() + self.missing_originals.len() >= self.capture_limit {
                self.gaps.push(IncidentReadGap::CaptureLimit);
                return Ok(IncidentEvidenceAvailability::LimitReached);
            }
            match self.session.lookup_metadata_reference(expected).await? {
                crate::evidence::EvidenceMetadataLookup::Available(metadata) => {
                    self.metadata.insert(range.into(), *metadata);
                }
                crate::evidence::EvidenceMetadataLookup::NotCommitted => {
                    self.missing_originals.insert(key);
                    return Ok(not_committed());
                }
            }
        }
        let metadata = &self.metadata[range];
        if metadata.capture() != &expected.capture
            || metadata.capture_manifest_sha256() != expected.manifest_sha256
            || metadata.source().captured_range()?.scope_id != expected.source_scope_id
        {
            return Err(invalid());
        }
        Ok(if metadata.is_read_complete() {
            IncidentEvidenceAvailability::Authenticated
        } else {
            IncidentEvidenceAvailability::Unavailable { gaps: metadata.gaps().to_vec() }
        })
    }
}
pub(super) fn row_gaps(gaps: &[EvidenceGap], reference: &DiagnosticEvidenceRef) -> Vec<EvidenceGap> {
    gaps.iter()
        .filter(|gap| {
            gap.stream.is_none_or(|stream| stream == reference.stream)
                && gap.batch_id.as_ref().is_none_or(|batch| batch == &reference.batch_id)
                && gap.row_ordinal.is_none_or(|ordinal| reference.row_ordinal.checked_add(1) == Some(ordinal))
        })
        .cloned()
        .collect()
}

fn not_committed() -> IncidentEvidenceAvailability {
    IncidentEvidenceAvailability::Unavailable {
        gaps: vec![EvidenceGap {
            stream: None,
            batch_id: None,
            row_ordinal: None,
            reason: EvidenceGapReason::OriginalNotCommitted,
        }],
    }
}
