//! Published output remains tied to exact selected inputs. Neither arrival time nor a native label places a row.
use super::*;
mod placement;
use placement::ObservationTimes;

pub(super) async fn read(
    assembly: &mut Assembly<'_>,
    request: &WarehouseWindowQuery,
    gaps: &mut Vec<WarehouseWindowGap>,
) -> Result<Vec<WindowPublishedAnalysis>> {
    let captures = assembly.captures.values().cloned().collect::<Vec<_>>();
    let mut output = Vec::new();
    let mut bodies = BTreeMap::new();
    let mut covered = BTreeSet::new();
    for capture in captures {
        let Some(description) = capture.derived_description() else { continue };
        let analysis = reference(&capture)?;
        let mut complete = capture.is_read_complete();
        let mut basis = ObservationTimes::default();
        for input in &description.inputs {
            let range = range_id_for(assembly.contract, &input.capture.capture)?;
            let availability = assembly.authenticate_header(&range, &input.capture).await?;
            complete &= availability == IncidentEvidenceAvailability::Authenticated;
            covered.insert(input.capture.key()?);
            if input.role == DerivedInputRole::Observation {
                let source = assembly
                    .captures
                    .get(&range)
                    .map(|original| original.source())
                    .or_else(|| assembly.metadata.get(&range).map(EvidenceCaptureMetadata::source));
                basis.push(
                    input.capture.clone(),
                    source.map(WindowNativeClock::from_original).unwrap_or(WindowNativeClock::Unknown),
                )?;
            }
        }
        if !complete {
            gaps.push(WarehouseWindowGap::UnavailableAnalysisInputs { analysis: analysis.clone() });
        }
        let native_clock = basis.single_clock();
        let qualified = complete && native_clock.overlaps(&request.window)? == Some(true);
        if matches!(native_clock, WindowNativeClock::Unknown) {
            gaps.push(WarehouseWindowGap::UnplacedNativeClock { original: analysis.clone() });
        }
        let mut requested = BTreeMap::new();
        for record in capture.records() {
            let references = match &record.row().record {
                TelemetryRecord::DerivedFinding(finding) => finding.referenced_evidence().collect::<Vec<_>>(),
                TelemetryRecord::DerivedChange(change) => change.referenced_evidence().collect::<Vec<_>>(),
                TelemetryRecord::DerivedIncident(incident) => vec![&incident.revision_reference],
                _ => return Err(invalid()),
            };
            for reference in references {
                requested.insert(serde_json::to_vec(reference).map_err(|_| invalid())?, reference.clone());
                if requested.len() > MAX_DIAGNOSTIC_ANCESTRY_LINKS {
                    return Err(capacity());
                }
            }
        }
        let selected = assembly.record_lookup(&requested.into_values().collect::<Vec<_>>()).await?;
        let headers_complete = complete;
        let mut findings = Vec::new();
        let mut changes = Vec::new();
        for record in capture.records() {
            match &record.row().record {
                TelemetryRecord::DerivedFinding(finding) => {
                    let rows_complete = authenticate_rows(assembly, &selected, finding.referenced_evidence())?;
                    let row_complete = headers_complete && rows_complete;
                    let placement = basis.finding(finding, row_complete, &request.window)?;
                    complete &= row_complete;
                    findings.push(WindowPublishedFinding { finding: (**finding).clone(), placement });
                }
                TelemetryRecord::DerivedChange(change) => {
                    let rows_complete = authenticate_rows(assembly, &selected, change.referenced_evidence())?;
                    let row_complete = headers_complete && rows_complete;
                    let placement = placement::change(change, row_complete, &request.window)?;
                    complete &= row_complete;
                    changes.push(WindowPublishedChange { change: (**change).clone(), placement });
                }
                TelemetryRecord::DerivedIncident(incident) => {
                    let incident_clock = match &incident.original_interval {
                        Some(interval) => {
                            WindowNativeClock::Interval { from: interval.from.clone(), until: interval.until.clone() }
                        }
                        None => WindowNativeClock::Observation {
                            from: description.observed_from.clone(),
                            until: description.observed_until.clone(),
                        },
                    };
                    if incident_clock.overlaps(&request.window)? == Some(true) {
                        assembly.observe_incident(incident, description, &capture).await?;
                    }
                }
                _ => return Err(invalid()),
            }
        }
        if !complete {
            gaps.push(WarehouseWindowGap::IncompletePublishedAnalysis { analysis: analysis.clone() });
        }
        let body = complete.then(|| body_fingerprint(&capture)).transpose()?;
        let scope = capture.source().captured_range()?.scope_id;
        let key = (scope, description.analysis_id.clone());
        if let Some((index, previous)) = bodies.get(&key) {
            if body.as_ref().is_some_and(|body| body != previous) {
                return Err(DeliveryProblem::Conflict.error());
            }
            if body.is_some() {
                let previous: &mut WindowPublishedAnalysis = &mut output[*index];
                previous.copies.push(analysis);
                continue;
            }
        }
        if let Some(body) = body {
            bodies.insert(key, (output.len(), body));
        }
        output.push(WindowPublishedAnalysis {
            context: IncidentAnalysisContext { capture: analysis.clone(), description: description.clone() },
            limitations: vec![],
            copies: vec![analysis],
            native_clock,
            native_window_qualified: qualified && complete,
            rules: if description.rules_version == crate::checks_synthesis::BUILTIN_RULES_VERSION
                && description.analyzer == crate::ChecksSynthesizer::builtin_analyzer()
            {
                WindowPublishedRules::MatchesCurrentBuiltin
            } else {
                WindowPublishedRules::OtherRules { expected: crate::checks_synthesis::BUILTIN_RULES_VERSION.into() }
            },
            findings,
            changes,
            read_complete: complete,
        });
    }
    for capture in assembly.captures.values() {
        if capture.source().query_history().is_some() {
            let original = reference(capture)?;
            if !covered.contains(&original.key()?) {
                gaps.push(WarehouseWindowGap::RawWithoutPublishedAnalysis { original });
            }
        }
    }
    Ok(output)
}

fn authenticate_rows<'a>(
    assembly: &mut Assembly<'_>,
    selected: &super::super::records::RecordLookup,
    references: impl Iterator<Item = &'a DiagnosticEvidenceRef>,
) -> Result<bool> {
    let mut complete = true;
    for reference in references {
        if selected.selected(reference)?.is_none() {
            let gaps = selected.gaps(reference)?;
            assembly.gaps.push(IncidentReadGap::OriginalUnavailable { reference: Box::new(reference.clone()), gaps });
            complete = false;
        }
    }
    Ok(complete)
}

fn body_fingerprint(capture: &EvidenceCapture) -> Result<String> {
    let records = capture.records().iter().map(|row| &row.row().record).collect::<Vec<_>>();
    culprit_core::serialization::json_size(&records, MAX_DERIVED_OUTPUT_BYTES).map_err(|error| match error {
        JsonSizeError::LimitExceeded => capacity(),
        JsonSizeError::Serialization => invalid(),
    })?;
    Ok(digest(&serde_json::to_vec(&records).map_err(|_| invalid())?))
}
