//! One reader projection words typed qualifications; no displayed text determines availability or authority.
use super::*;
impl WarehouseWindowReport {
    pub fn prepare_for_reader(&mut self) {
        self.qualifications.clear();
        let mut gap_kinds = std::collections::HashSet::new();
        for gap in &self.gaps {
            if gap_kinds.insert(std::mem::discriminant(gap)) {
                self.qualifications.push(gap.explanation());
            }
        }
        if !self.discovery_complete {
            self.qualifications.push(crate::t!("window.reader.partial"));
        }
        if let Some(workload) = self.workload.analysis() {
            let mut gap_kinds = std::collections::HashSet::new();
            let mut comparison_kinds = std::collections::HashSet::new();
            let mut context_issues = vec![];
            for gap in &workload.gaps {
                if gap_kinds.insert(std::mem::discriminant(gap)) {
                    self.qualifications.push(gap.explanation());
                }
            }
            for section in &workload.sections {
                for issue in &section.context_issues {
                    if !context_issues.contains(issue) {
                        context_issues.push(*issue);
                        self.qualifications.push(issue.explanation());
                    }
                }
                for gap in &section.comparison.gaps {
                    if comparison_kinds.insert(std::mem::discriminant(gap)) {
                        self.qualifications.push(gap.explanation());
                    }
                }
            }
        }
        for published in &mut self.published {
            published.limitations = published.context.description.limitations();
            if !published.read_complete {
                published.limitations.push(crate::t!("window.reader.incomplete"));
            }
            if matches!(published.rules, WindowPublishedRules::OtherRules { .. }) {
                published.limitations.push(crate::t!("window.reader.other_rules"));
            }
            for row in &mut published.changes {
                row.change.change.refresh_language();
            }
            for placement in published
                .findings
                .iter_mut()
                .map(|row| &mut row.placement)
                .chain(published.changes.iter_mut().map(|row| &mut row.placement))
            {
                placement.qualification = placement.explanation();
            }
        }
        // Keep the existing incident/caller/role renderer as the one owner, without cloning the report.
        self.incident_limitations = crate::warehouse_connection::prepare_incident_report(&mut self.incidents);
    }
}
impl WindowRowPlacement {
    pub fn explanation(&self) -> String {
        if !self.read_complete {
            return crate::t!("window.reader.row_incomplete");
        }
        match self.overlaps_window {
            Some(true) => crate::t!("window.reader.row_selected"),
            Some(false) => crate::t!("window.reader.row_outside"),
            None => crate::t!("window.reader.row_unplaced"),
        }
    }
}
impl WarehouseWindowGap {
    pub fn explanation(&self) -> String {
        match self {
            Self::NoPublishedOriginals => crate::t!("window.reader.no_originals"),
            Self::NoPublishedAnalyses => crate::t!("window.reader.no_answers"),
            Self::MorePublishedAnalyses => crate::t!("window.reader.more"),
            Self::PublicationDiscovery { .. } => crate::t!("window.reader.discovery_gap"),
            Self::OriginalLimit => crate::t!("window.reader.original_limit"),
            Self::UnsupportedOriginal { .. } => crate::t!("window.reader.unsupported"),
            Self::OriginalUnavailable { .. } => crate::t!("window.reader.unavailable"),
            Self::UnplacedNativeClock { .. } => crate::t!("window.reader.unplaced"),
            Self::RawWithoutPublishedAnalysis { .. } => crate::t!("window.reader.raw_only"),
            Self::UnavailableAnalysisInputs { .. } => crate::t!("window.reader.unavailable_inputs"),
            Self::IncompletePublishedAnalysis { .. } => crate::t!("window.reader.incomplete"),
        }
    }
}
impl crate::CollectedWindowGap {
    pub fn explanation(&self) -> String {
        match self {
            Self::NoNativeIntervals => crate::t!("window.reader.no_intervals"),
            Self::UnqualifiedScope { .. } => crate::t!("window.reader.scope"),
            Self::UnknownPopulation { .. } => crate::t!("window.reader.population"),
            Self::IncompleteOriginal { .. } => crate::t!("window.reader.incomplete_original"),
            Self::AmbiguousRevision { .. } => crate::t!("window.reader.revision"),
            Self::OverlappingIntervals { .. } => crate::t!("window.reader.overlap"),
            Self::SectionDoesNotCoverWindow { .. } => crate::t!("window.reader.coverage"),
            Self::NativeBoundaryExpanded { .. } => crate::t!("window.reader.boundary"),
            Self::ConflictingDefinition { .. } => crate::t!("window.reader.definition"),
        }
    }
}
