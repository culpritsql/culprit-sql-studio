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
            let families = links
                .iter()
                .filter_map(|link| link.reference.as_ref())
                .map(|reference| {
                    EvidenceFamily::ALL
                        .iter()
                        .find(|family| family.stream() == reference.stream)
                        .copied()
                        .ok_or_else(invalid)
                })
                .collect::<Result<Vec<_>>>()?;
            let mut selected = Vec::new();
            for family in families {
                if !selected.contains(&family) {
                    selected.push(family);
                }
            }
            if selected.is_empty() {
                let availability = self.authenticate_header(&range, &expected).await?;
                if let IncidentEvidenceAvailability::Unavailable { gaps } = &availability {
                    self.gaps
                        .push(IncidentReadGap::CaptureUnavailable { capture: expected.clone(), gaps: gaps.clone() });
                }
                for link in &mut links {
                    link.availability = availability.clone();
                }
            } else if let Some(capture) = self.capture(&range, &selected).await? {
                references::validate_capture_reference(&capture, &expected)?;
                for link in &mut links {
                    link.availability = match &link.reference {
                        Some(reference) => {
                            if references::selected_record(&capture, reference)?.is_some() {
                                IncidentEvidenceAvailability::Authenticated
                            } else {
                                let gaps = row_gaps(&capture, reference);
                                if gaps.is_empty() {
                                    return Err(invalid());
                                }
                                self.gaps.push(IncidentReadGap::OriginalUnavailable {
                                    reference: Box::new(reference.clone()),
                                    gaps: gaps.clone(),
                                });
                                IncidentEvidenceAvailability::Unavailable { gaps }
                            }
                        }
                        None => IncidentEvidenceAvailability::Authenticated,
                    };
                }
            }
            relationships.extend(links);
        }
        Ok(relationships)
    }
    async fn authenticate_header(
        &mut self,
        range: &str,
        expected: &DiagnosticCaptureRef,
    ) -> Result<IncidentEvidenceAvailability> {
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
            if self.captures.len() + self.metadata.len() >= MAX_INCIDENT_CAPTURES {
                self.gaps.push(IncidentReadGap::CaptureLimit);
                return Ok(IncidentEvidenceAvailability::LimitReached);
            }
            let metadata = self.session.read_metadata_reference(expected).await?;
            add_work(&mut self.work, metadata.metrics())?;
            self.metadata.insert(range.into(), metadata);
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
fn row_gaps(capture: &EvidenceCapture, reference: &DiagnosticEvidenceRef) -> Vec<EvidenceGap> {
    capture
        .gaps()
        .iter()
        .filter(|gap| {
            gap.stream.is_none_or(|stream| stream == reference.stream)
                && gap.batch_id.as_ref().is_none_or(|batch| batch == &reference.batch_id)
                && gap.row_ordinal.is_none_or(|ordinal| reference.row_ordinal.checked_add(1) == Some(ordinal))
        })
        .cloned()
        .collect()
}
