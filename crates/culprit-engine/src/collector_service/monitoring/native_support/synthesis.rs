//! Observe the installed default pure analyzer, then verify its actual queued output through the raw archive.
use super::archive::{ArchivedCapture, NativeArchive};
use super::*;
use culprit_collector::synthesis::{AutomaticSynthesis, SynthesisInput, SynthesisTurn, Synthesizer};
use culprit_core::{ComponentVersion, serialization::json_size};
use std::sync::Mutex;

const MAX_EXPECTED_OUTPUTS: usize = 16;
struct ExpectedOutput {
    primary: DiagnosticCaptureRef,
    description: DerivedDiagnosisDescription,
    incident_digests: Vec<String>,
}
struct ObservingSynthesizer {
    real: Arc<dyn Synthesizer>,
    expected: Arc<Mutex<Vec<ExpectedOutput>>>,
}
impl Synthesizer for ObservingSynthesizer {
    fn analyzer(&self) -> ComponentVersion {
        self.real.analyzer()
    }
    fn rules_version(&self) -> &str {
        self.real.rules_version()
    }
    fn synthesize(&self, input: &SynthesisInput) -> Result<DerivedDiagnosis> {
        let output = self.real.synthesize(input).inspect_err(|error| {
            eprintln!("native fixture default synthesis failed: {:?} {:?}", error.code, error.detail);
        })?;
        json_size(&output, MAX_DERIVED_OUTPUT_BYTES).map_err(|_| DeliveryProblem::Capacity.error())?;
        // A capture can contain distinct attention and mechanism episodes for one original row.
        // Preserve every output under the same production byte limit and archive row allowance.
        let engine = input.primary().source().target.engine();
        let incident_digests = output
            .incidents
            .iter()
            .map(|incident| {
                // The public Core operation is also used by canonical export; stored identity is
                // checked independently by the real writer and BatchManifest validator below.
                let mut incident = incident.clone();
                incident.redact(engine);
                let record = TelemetryRecord::DerivedIncident(Box::new(incident));
                serde_json::to_vec(&record).map(|bytes| digest(&bytes)).map_err(|_| DeliveryProblem::Integrity.error())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut expected = self.expected.lock().map_err(|_| DeliveryProblem::Storage.error())?;
        if expected.len() >= MAX_EXPECTED_OUTPUTS {
            return Err(DeliveryProblem::Capacity.error());
        }
        expected.push(ExpectedOutput {
            primary: input.primary().original().clone(),
            description: output.description.clone(),
            incident_digests,
        });
        Ok(output)
    }
}
pub(in crate::collector_service::monitoring) struct NativeSynthesis {
    automatic: AutomaticSynthesis,
    expected: Arc<Mutex<Vec<ExpectedOutput>>>,
}
impl NativeSynthesis {
    pub async fn install(
        outbox: &mut Outbox,
        workspace: &Workspace,
        cancellation: &CollectionCancellation,
    ) -> Result<Self> {
        case::require_active(cancellation)?;
        let runtime = super::super::super::CollectorRuntime::default();
        let real = runtime.synthesizer(workspace)?;
        let expected = Arc::new(Mutex::new(vec![]));
        let runtime = runtime.with_synthesizer(Arc::new(ObservingSynthesizer { real, expected: expected.clone() }));
        let automatic = super::super::super::synthesis::configured(&runtime, workspace)?;
        super::super::super::synthesis::install(outbox, &automatic, cancellation).await?;
        Ok(Self { automatic, expected })
    }
    pub async fn drain(
        &self,
        outbox: &mut Outbox,
        archive: &mut NativeArchive,
        cancellation: &CollectionCancellation,
    ) -> Result<Vec<DerivedIncident>> {
        let mut incidents = Vec::new();
        for _ in 0..=MAX_EXPECTED_OUTPUTS {
            case::require_active(cancellation)?;
            let turn = case::admitted(
                cancellation,
                outbox.delivery().synthesize_pending(&self.automatic, archive.operation(), ()),
            )
            .await?;
            let (primary, output) = match turn {
                SynthesisTurn::Committed { primary, output, queue_retired: true } => (primary, output),
                SynthesisTurn::Idle => {
                    let backlog =
                        case::admitted(cancellation, outbox.delivery().synthesis_backlog(archive.operation(), ()))
                            .await?;
                    if backlog.pending != 0 || backlog.failed != 0 || backlog.failure_watermark.total != 0 {
                        return Err(DeliveryProblem::Integrity.error());
                    }
                    return Ok(incidents);
                }
                _ => return Err(DeliveryProblem::Integrity.error()),
            };
            let manifest = case::admitted(cancellation, outbox.verify(&output.capture.observation_id)).await?;
            if manifest.source_queries != 0
                || manifest.status != CollectionStatus::Complete
                || manifest.request.collection() != TelemetryCollection::DerivedDiagnosis
            {
                return Err(DeliveryProblem::Integrity.error());
            }
            let SourceObservation::DerivedDiagnosis(header) =
                &manifest.description.as_ref().ok_or_else(|| DeliveryProblem::Integrity.error())?.observation
            else {
                return Err(DeliveryProblem::Integrity.error());
            };
            let expected_digests = {
                let expected = self.expected.lock().map_err(|_| DeliveryProblem::Storage.error())?;
                let matching = expected
                    .iter()
                    .filter(|expected| {
                        expected.primary == primary && expected.description.analysis_id == header.analysis_id
                    })
                    .collect::<Vec<_>>();
                let [expected] = matching.as_slice() else { return Err(DeliveryProblem::Integrity.error()) };
                if expected.description != **header {
                    return Err(DeliveryProblem::Integrity.error());
                }
                expected.incident_digests.clone()
            };
            let entry = super::archive::retained(outbox, cancellation)
                .await?
                .into_iter()
                .find(|entry| entry.capture == output.capture)
                .ok_or_else(|| DeliveryProblem::Integrity.error())?;
            let stored = archive.capture(outbox, &entry, cancellation).await?;
            if stored.original != output || !stored.revisions.is_empty() {
                return Err(DeliveryProblem::Integrity.error());
            }
            verify_stored(&stored, &expected_digests)?;
            incidents.extend(stored.incidents);
        }
        Err(DeliveryProblem::Capacity.error())
    }
}
fn verify_stored(stored: &ArchivedCapture, expected: &[String]) -> Result<()> {
    let actual = stored
        .incidents
        .iter()
        .map(|incident| {
            serde_json::to_vec(&TelemetryRecord::DerivedIncident(Box::new(incident.clone())))
                .map(|bytes| digest(&bytes))
                .map_err(|_| DeliveryProblem::Integrity.error())
        })
        .collect::<Result<Vec<_>>>()?;
    if actual != expected {
        return Err(DeliveryProblem::Integrity.error());
    }
    Ok(())
}

pub(in crate::collector_service::monitoring) fn verify_incident(
    incident: &DerivedIncident,
    episode: &DiagnosticEpisode,
    revision: &DiagnosticEvidenceRef,
    victim: &ActivityObservation,
    owner: &NativeActivityIdentity,
    focused: &CaptureIdentity,
) {
    assert_eq!(incident.entry.episode_id, episode.id);
    assert_eq!(incident.entry.revision, episode.revision);
    assert_eq!(incident.entry.lifecycle, episode.lifecycle);
    assert_eq!(incident.entry.analysis, episode.analysis);
    assert_eq!(&incident.revision_reference, revision);
    assert_eq!(incident.entry.original.as_ref(), Some(revision));
    assert!(incident.entry.evidence.iter().any(|item| item.reference.as_ref() == Some(&episode.first)));
    assert_eq!(incident.affected_queries.len(), 1);
    assert!(
        matches!(&incident.affected_queries[0].identity,Some(DerivedQueryIdentity::Activity{actor,statement}) if actor==&victim.key && statement==&victim.statement_identity)
    );
    assert!(incident.affected_queries[0].causal_role.is_none());
    assert!(incident.affected_queries[0].evidence.contains(&episode.first));
    assert_eq!(incident.attribution_status, DerivedAttributionStatus::Established);
    assert_eq!(incident.culprit_queries.len(), 1);
    let culprit = &incident.culprit_queries[0];
    assert_eq!(culprit.causal_role.as_ref().unwrap().kind, DerivedCausalRoleKind::NativeBlockingOwner);
    assert!(
        matches!(&culprit.identity,Some(DerivedQueryIdentity::NativeWaitOwner(ActivityWaitOwner::Session{id,generation:Some(generation)})) if id==&owner.id && generation==&owner.generation)
    );
    assert!(!culprit.sql.is_available(), "an idle owner's previous SQL never becomes acquiring-query proof");
    assert!(culprit.evidence.iter().any(|reference| reference.capture == *focused));
    assert!(!incident.victim_summary.is_empty());
    assert!(incident.mechanism.is_some());
    assert!(!incident.developer_action.is_empty() && !incident.verification.is_empty());
}
