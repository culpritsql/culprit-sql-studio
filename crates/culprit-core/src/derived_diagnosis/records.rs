use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedDisplayContext {
    pub connection_alias: String,
    pub environment: Option<String>,
    /// An explicitly approved display label, never a native ownership key.
    pub database_name: Option<String>,
}
impl DerivedDisplayContext {
    pub fn is_valid(&self) -> bool {
        label(&self.connection_alias)
            && self.environment.as_deref().is_none_or(label)
            && self.database_name.as_deref().is_none_or(label)
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedIncident {
    pub analysis_id: String,
    pub database: Option<IncidentDatabaseSelector>,
    pub display: DerivedDisplayContext,
    pub entry: IncidentHistoryEntry,
    pub original_interval: Option<TelemetryInterval>,
    pub revision_reference: DiagnosticEvidenceRef,
    pub victim_summary: String,
    pub affected_queries: Vec<DerivedIncidentQuery>,
    pub culprit_queries: Vec<DerivedIncidentQuery>,
    pub candidate_queries: Vec<DerivedIncidentQuery>,
    pub attribution_status: DerivedAttributionStatus,
    pub mechanism: Option<String>,
    pub developer_action: String,
    pub verification: String,
    pub caller: Option<DerivedCaller>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedFinding {
    pub analysis_id: String,
    pub database: Option<IncidentDatabaseSelector>,
    pub display: DerivedDisplayContext,
    pub finding: Finding,
    pub inputs: Vec<DiagnosticEvidenceRef>,
    /// Redacted SQL of a separately established original query, never a query inferred from a label.
    pub query: Option<DerivedQuery>,
    pub cause: Option<DerivedCause>,
    /// Stored fixes are historical guidance. They never authorize execution against current catalog state.
    pub guidance: DerivedGuidance,
}
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedGuidance {
    HistoricalRequiresFreshVerification,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedQuery {
    pub identity: NativeQueryIdentity,
    pub reference: DiagnosticEvidenceRef,
    pub sql: DefinitionPayload,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedCause {
    pub summary: String,
    pub evidence: Vec<DiagnosticEvidenceRef>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedChange {
    pub analysis_id: String,
    pub database: Option<IncidentDatabaseSelector>,
    pub display: DerivedDisplayContext,
    pub change: ChangeEvent,
    pub inputs: Vec<DiagnosticEvidenceRef>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedDiagnosis {
    pub description: DerivedDiagnosisDescription,
    pub incidents: Vec<DerivedIncident>,
    pub findings: Vec<DerivedFinding>,
    pub changes: Vec<DerivedChange>,
}
impl DerivedDiagnosis {
    /// The normal capture writer gets one closed family conversion, never arbitrary JSON or native facts.
    pub fn into_records(self) -> impl Iterator<Item = TelemetryRecord> {
        self.incidents
            .into_iter()
            .map(|row| TelemetryRecord::DerivedIncident(Box::new(row)))
            .chain(self.findings.into_iter().map(|row| TelemetryRecord::DerivedFinding(Box::new(row))))
            .chain(self.changes.into_iter().map(|row| TelemetryRecord::DerivedChange(Box::new(row))))
    }
}

#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedAttributionStatus {
    /// No culprit is established. This is unknown, not proof that no cause exists.
    Unestablished,
    /// Retained candidates remain unproved and are never relabeled victims or culprits.
    CandidatesOnly,
    /// Listed culprits have original evidence; this does not claim exclusivity or global coverage.
    Established,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "kind", content = "data", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DerivedQueryIdentity {
    Activity {
        actor: ActivityKey,
        statement: Option<NativeIdentity>,
    },
    /// Exact shared native member, not the incident database's query population. The optional database is
    /// a DB-reported display label only; it grants neither incarnation nor business ownership.
    SharedActivity {
        /// Exact original SharedResourceIdentity::telemetry_id(); an opaque locator, never native authority.
        member_id: String,
        member_kind: SharedResourceKind,
        actor: ActivityKey,
        statement: Option<NativeIdentity>,
        database: Option<String>,
    },
    QueryHistory {
        query: NativeQueryIdentity,
        plan: Option<NativePlanIdentity>,
    },
    CachedStatement(CachedStatementContext),
    /// Point-native owner of a recorded wait; this is not a query identity or historical continuity.
    NativeWaitOwner(ActivityWaitOwner),
}
impl DerivedQueryIdentity {
    pub fn shared_scope_label(&self) -> Option<String> {
        let Self::SharedActivity { database, .. } = self else { return None };
        Some(match database {
            Some(database) => crate::t!("view.synthesis.shared_database", database = database),
            None => crate::t!("view.synthesis.shared_database_unknown"),
        })
    }
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedCaller {
    pub client: Option<ActivityClient>,
    pub session_context: Option<Box<ActivitySessionContext>>,
    pub evidence: DiagnosticEvidenceRef,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedIncidentQuery {
    #[serde(deserialize_with = "required_causal_role")]
    pub causal_role: Option<DerivedCausalRole>,
    pub identity: Option<DerivedQueryIdentity>,
    pub sql: DefinitionPayload,
    pub caller: Option<DerivedCaller>,
    pub evidence: Vec<DiagnosticEvidenceRef>,
}

impl DerivedCaller {
    /// Copy DB-reported client/session claims from this exact original actor. This is not application identity.
    /// The verified input owner separately authenticates the body and row receipt.
    pub fn from_activity(observation: &ActivityObservation, reference: &DiagnosticEvidenceRef) -> Result<Option<Self>> {
        if !observation.is_valid()
            || !reference.is_valid()
            || reference.native_key != NativeRecordIdentity::Activity(observation.key.clone())
        {
            return Err(invalid());
        }
        if observation.client.is_none() && observation.session_context.is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            client: observation.client.clone(),
            session_context: observation.session_context.clone(),
            evidence: reference.clone(),
        }))
    }
}

/// The established native role is narrower than an exclusive cause or the statement that acquired a resource.
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedCausalRoleKind {
    NativeBlockingOwner,
    NativePendingRelationLockGatekeeper,
    NativeGrantedRelationLockHolder,
    NativeReportedTaskHolder,
    AdmissionReservationContributor,
    AssignedWorkerContributor,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedCausalRole {
    pub kind: DerivedCausalRoleKind,
    pub explanation: crate::LocalizedText,
}
impl DerivedCausalRole {
    pub fn new(kind: DerivedCausalRoleKind) -> Self {
        let explanation = match kind {
            DerivedCausalRoleKind::NativeBlockingOwner => crate::message!("view.synthesis.cause.blocking_owner"),
            DerivedCausalRoleKind::NativePendingRelationLockGatekeeper => {
                crate::message!("view.synthesis.cause.pending_relation_gate")
            }
            DerivedCausalRoleKind::NativeGrantedRelationLockHolder => {
                crate::message!("view.synthesis.cause.granted_relation_holder")
            }
            DerivedCausalRoleKind::NativeReportedTaskHolder => crate::message!("view.synthesis.cause.task_holder"),
            DerivedCausalRoleKind::AssignedWorkerContributor => {
                crate::message!("view.worker_starvation.cause.assignment")
            }
            DerivedCausalRoleKind::AdmissionReservationContributor => {
                crate::message!("view.synthesis.cause.grant_contributor")
            }
        };
        Self { kind, explanation: crate::LocalizedText::new(explanation) }
    }
}

fn required_causal_role<'de, D: serde::Deserializer<'de>>(
    reader: D,
) -> std::result::Result<Option<DerivedCausalRole>, D::Error> {
    Option::<DerivedCausalRole>::deserialize(reader)
}
