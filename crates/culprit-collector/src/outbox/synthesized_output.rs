//! A diagnosis uses the same local capture, canonical files and Ready transaction as every other delivered range.
use super::{Files, problem, storage};
use crate::{collection_files::ObservationFiles, synthesis::SynthesisInput};
use culprit_core::serialization::{JsonSizeError, json_size};
use culprit_core::{ComponentVersion, Result, telemetry::*};
use rusqlite::{OptionalExtension, Transaction, params};

const OUTPUT_PAGE_BYTES: u32 = 512 * 1024;
const OUTPUT_BYTES: u32 = MAX_DERIVED_OUTPUT_BYTES as u32;

struct StoredOutput {
    observation_id: String,
    output_digest: String,
    receipt: Option<String>,
}

impl Files {
    pub(super) fn commit_synthesized_output(
        &mut self,
        input: &SynthesisInput,
        diagnosis: DerivedDiagnosis,
        analyzer: &ComponentVersion,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<DiagnosticCaptureRef> {
        check()?;
        json_size(&diagnosis, OUTPUT_BYTES as usize).map_err(output_size_error)?;
        input.validate_diagnosis(&diagnosis, analyzer)?;
        check()?;
        if !self.synthesis_mutation_ready()? {
            return Err(problem(OutboxProblem::RecoveryRequired));
        }
        let analysis_id = diagnosis.description.analysis_id.clone();
        let output_digest = content_digest(&diagnosis)?;
        let stored = self
            .journal
            .conn
            .query_row(
                "SELECT observation_id,output_digest,result_json FROM synthesized_outputs WHERE analysis_id=?1",
                [&analysis_id],
                |row| {
                    Ok(StoredOutput { observation_id: row.get(0)?, output_digest: row.get(1)?, receipt: row.get(2)? })
                },
            )
            .optional()
            .map_err(storage)?;
        if let Some(stored) = stored {
            if stored.output_digest != output_digest {
                return Err(DeliveryProblem::Conflict.error());
            }
            if let Some(encoded) = stored.receipt {
                let receipt: DiagnosticCaptureRef = super::journal::decode(&encoded)?;
                if !receipt.is_valid() || receipt.capture.observation_id != stored.observation_id {
                    return Err(integrity());
                }
                return Ok(receipt);
            }
            let capture = self.journal.capture(&stored.observation_id)?;
            if matches!(capture.state, OutboxCaptureState::Ready | OutboxCaptureState::Capturing) {
                return Err(problem(OutboxProblem::RecoveryRequired));
            }
            self.journal
                .conn
                .execute("DELETE FROM synthesized_outputs WHERE analysis_id=?1", [&analysis_id])
                .map_err(storage)?;
        }
        let request = CollectionRequest {
            subject: match &input.primary().source().target {
                CollectionTarget::Database(_) => CollectionSubject::Database,
                CollectionTarget::Shared(resource) => CollectionSubject::Shared { resource: resource.clone() },
            },
            selection: CollectionSelection::DerivedDiagnosis { analysis_id: analysis_id.clone() },
            budget: CollectionBudget {
                page_rows: 128,
                max_snapshot_rows: MAX_DERIVED_ROWS as u32,
                page_bytes: OUTPUT_PAGE_BYTES,
                total_bytes: OUTPUT_BYTES,
                max_pages: 128,
                max_queries: 1,
                ..Default::default()
            },
        };
        let reservation = self.reserve(input.primary().connection(), &request)?;
        let identity = reservation.entry.identity.clone();
        self.journal.conn.execute(
            "INSERT INTO synthesized_outputs(analysis_id,primary_observation_id,observation_id,output_digest,result_json) VALUES(?1,?2,?3,?4,NULL)",
            params![analysis_id, input.primary().original().capture.observation_id, identity.observation_id, output_digest],
        ).map_err(storage)?;
        let mut files = ObservationFiles::create(&reservation.directory, request, identity.clone(), None)?;
        let DerivedDiagnosis { description, incidents, findings, changes } = diagnosis;
        files.describe(CollectionDescription {
            target: input.primary().source().target.clone(),
            partition: SourcePartition::Target,
            source: DERIVED_DIAGNOSIS_SOURCE.into(),
            continuity: SourceContinuity::Unverified,
            observation: SourceObservation::DerivedDiagnosis(Box::new(description)),
        })?;
        let streams = [
            incidents.into_iter().map(|row| TelemetryRecord::DerivedIncident(Box::new(row))).collect::<Vec<_>>(),
            findings.into_iter().map(|row| TelemetryRecord::DerivedFinding(Box::new(row))).collect(),
            changes.into_iter().map(|row| TelemetryRecord::DerivedChange(Box::new(row))).collect(),
        ];
        for (index, rows) in streams.into_iter().enumerate() {
            check()?;
            files.write_stream(index, &rows)?;
        }
        // Before this boundary, cancellation leaves an incomplete local artifact. Once completion is admitted,
        // recovery and the Ready transaction own its outcome; a later caller cancellation cannot undo it.
        check()?;
        files.confirm_source(SourceCompletion::Verified)?;
        if files.finish(0)?.status != CollectionStatus::Complete {
            return Err(integrity());
        }
        self.finish(reservation)?;
        let encoded: String = self
            .journal
            .conn
            .query_row("SELECT result_json FROM synthesized_outputs WHERE analysis_id=?1", [&analysis_id], |row| {
                row.get(0)
            })
            .map_err(storage)?;
        super::journal::decode(&encoded)
    }
}

impl Files {
    /// Native leases span async work beyond this file mutex. Only an abandoned local output can be recovered here.
    pub(super) fn synthesis_mutation_ready(&mut self) -> Result<bool> {
        let unfinished = self.journal.unfinished()?;
        if unfinished.iter().any(|capture| !capture.request.collection().is_local_output()) {
            return Ok(false);
        }
        if !self.recovered || !unfinished.is_empty() {
            self.recover()?;
        }
        Ok(true)
    }
}

fn content_digest(diagnosis: &DerivedDiagnosis) -> Result<String> {
    // Analyzer, rules, input captures, catalog provenance and origin are bound by the validated analysis ID.
    // Destructure exhaustively so adding description fields requires an explicit replay-identity decision.
    let DerivedDiagnosisDescription {
        analysis_id,
        analyzer: _,
        rules_version: _,
        language: _,
        recorded_at: _,
        observed_from,
        observed_until,
        inputs: _,
        origin: _,
        input_read_complete,
        counts,
        output_coverage,
        finding_evaluations,
        change_evaluations,
        context_issues,
    } = &diagnosis.description;
    let mut buffer = crate::durable_fs::LimitedBuffer::new(OUTPUT_BYTES as usize);
    serde_json::to_writer(
        &mut buffer,
        &(
            analysis_id,
            observed_from,
            observed_until,
            input_read_complete,
            counts,
            output_coverage,
            finding_evaluations,
            change_evaluations,
            context_issues,
            &diagnosis.incidents,
            &diagnosis.findings,
            &diagnosis.changes,
        ),
    )
    .map_err(|_| if buffer.exceeded() { DeliveryProblem::Capacity.error() } else { integrity() })?;
    Ok(digest(&buffer.into_bytes()))
}

fn output_size_error(error: JsonSizeError) -> culprit_core::Error {
    match error {
        JsonSizeError::LimitExceeded => DeliveryProblem::Capacity.error(),
        JsonSizeError::Serialization => integrity(),
    }
}

pub(super) fn complete_output(tx: &Transaction<'_>, validated: &super::artifacts::ValidatedCapture) -> Result<()> {
    let source = validated.manifest.description.as_ref().ok_or_else(integrity)?;
    let SourceObservation::DerivedDiagnosis(description) = &source.observation else { return Ok(()) };
    let receipt = DiagnosticCaptureRef {
        capture: validated.manifest.capture.clone(),
        manifest_sha256: validated.manifest_sha.clone(),
        source_scope_id: source.captured_range()?.scope_id,
    };
    let updated = tx.execute(
        "UPDATE synthesized_outputs SET result_json=?1 WHERE analysis_id=?2 AND observation_id=?3 AND result_json IS NULL",
        params![super::journal::encode(&receipt)?, description.analysis_id, receipt.capture.observation_id],
    ).map_err(storage)?;
    if updated != 1 {
        return Err(integrity());
    }
    // Existing archive-owned derived pins keep referenced originals until this diagnosis is archived.
    for input in &description.inputs {
        tx.execute(
            "INSERT OR IGNORE INTO diagnostic_pins(owner,observation_id) VALUES(?1,?2)",
            params![format!("derived:{}", receipt.capture.observation_id), input.capture.capture.observation_id],
        )
        .map_err(storage)?;
    }
    Ok(())
}

fn integrity() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}

pub(super) fn charged_bytes(conn: &rusqlite::Connection) -> Result<u64> {
    let bytes: i64 = conn.query_row(
        "SELECT coalesce(sum(length(CAST(analysis_id AS BLOB))+length(CAST(primary_observation_id AS BLOB))+length(CAST(observation_id AS BLOB))+length(CAST(output_digest AS BLOB))+length(CAST(coalesce(result_json,'') AS BLOB))+128),0) FROM synthesized_outputs",
        [], |row| row.get(0),
    ).map_err(storage)?;
    super::journal::bytes_from_sql(bytes)
}
