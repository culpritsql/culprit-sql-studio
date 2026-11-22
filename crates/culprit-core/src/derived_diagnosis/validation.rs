use super::*;

fn references(references: &[DiagnosticEvidenceRef]) -> bool {
    let mut unique = BTreeSet::new();
    !references.is_empty()
        && references.len() <= MAX_DERIVED_REFERENCES
        && references.iter().all(|reference| reference.key().is_ok_and(|key| unique.insert(key)))
}
fn capture_level_or_rows(references: &[DiagnosticEvidenceRef]) -> bool {
    references.is_empty() || self::references(references)
}
fn public_context(
    analysis_id: &str,
    database: &Option<IncidentDatabaseSelector>,
    display: &DerivedDisplayContext,
) -> bool {
    valid_digest(analysis_id) && database.as_ref().is_none_or(IncidentDatabaseSelector::is_valid) && display.is_valid()
}
fn bounded(row: &impl Serialize) -> bool {
    serde_json::to_vec(row).is_ok_and(|bytes| bytes.len() <= MAX_DERIVED_RECORD_BYTES)
}
fn prose(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_DERIVED_SQL_BYTES
}
fn sql_payload(payload: &DefinitionPayload) -> bool {
    payload.is_valid()
        && match payload {
            DefinitionPayload::Available { text, .. } => text.len() <= MAX_DERIVED_SQL_BYTES,
            DefinitionPayload::Unavailable { .. } => true,
        }
}
fn revision_number(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 20
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok_and(|revision| revision > 0 && revision.to_string() == value)
}
impl DerivedCaller {
    pub fn is_valid(&self) -> bool {
        self.evidence.is_valid()
            && matches!(
                &self.evidence.native_key,
                NativeRecordIdentity::Activity(_) | NativeRecordIdentity::Operational { .. }
            )
            && (self.client.is_some() || self.session_context.is_some())
            && self.client.as_ref().is_none_or(ActivityClient::is_valid)
    }
}
impl DerivedIncidentQuery {
    pub fn is_valid(&self) -> bool {
        references(&self.evidence)
            && sql_payload(&self.sql)
            && self.valid_causal_role()
            && self.caller.as_ref().is_none_or(DerivedCaller::is_valid)
            && self.identity.as_ref().is_none_or(|identity| match identity {
                DerivedQueryIdentity::Activity { actor, statement } => {
                    actor.is_valid() && statement.as_ref().is_none_or(NativeIdentity::is_valid)
                }
                DerivedQueryIdentity::SharedActivity { member_id, member_kind, actor, statement, database } => {
                    valid_digest(member_id)
                        && matches!(member_kind, SharedResourceKind::Instance | SharedResourceKind::Node)
                        && actor.is_valid()
                        && actor.request.is_some()
                        && statement.as_ref().is_none_or(NativeIdentity::is_valid)
                        && database.as_deref().is_none_or(|value| value.len() <= 512)
                }
                DerivedQueryIdentity::QueryHistory { query, plan } => {
                    query.is_valid() && plan.as_ref().is_none_or(|plan| plan.is_valid() && plan.query == *query)
                }
                DerivedQueryIdentity::CachedStatement(context) => context.is_valid(),
                DerivedQueryIdentity::NativeWaitOwner(owner) => owner.is_valid() && owner.is_blocker(),
            })
    }
    fn valid_causal_role(&self) -> bool {
        let Some(role) = &self.causal_role else { return true };
        if !prose(role.explanation.text()) {
            return false;
        }
        match (&role.kind, &self.identity) {
            // This role owns the current pending request, unlike the SQL which acquired an existing lock.
            (
                DerivedCausalRoleKind::NativePendingRelationLockGatekeeper,
                Some(DerivedQueryIdentity::Activity { actor, statement }),
            ) => {
                actor.request.is_some()
                    && (!self.sql.is_available() || statement.is_some())
                    && self
                        .evidence
                        .iter()
                        .any(|reference| reference.native_key == NativeRecordIdentity::Activity(actor.clone()))
            }
            (
                DerivedCausalRoleKind::NativeBlockingOwner | DerivedCausalRoleKind::NativeGrantedRelationLockHolder,
                Some(DerivedQueryIdentity::NativeWaitOwner(owner)),
            ) => {
                !self.sql.is_available()
                    && match owner {
                        ActivityWaitOwner::Session { generation, .. } => generation.is_some(),
                        ActivityWaitOwner::Transaction { native_id, .. } => native_id != "0",
                        ActivityWaitOwner::Unknown { .. } => false,
                    }
            }
            (
                DerivedCausalRoleKind::NativeReportedTaskHolder
                | DerivedCausalRoleKind::AdmissionReservationContributor,
                Some(DerivedQueryIdentity::Activity { actor, statement: None }),
            ) => !self.sql.is_available() && actor.request.is_some(),
            (
                DerivedCausalRoleKind::AssignedWorkerContributor,
                Some(DerivedQueryIdentity::SharedActivity { actor, statement: None, .. }),
            ) => !self.sql.is_available() && actor.request.is_some(),
            _ => false,
        }
    }

    fn fits_description(&self, description: &DerivedDiagnosisDescription) -> bool {
        self.evidence.iter().all(|reference| description.contains_capture_for(reference))
            && self.caller.as_ref().is_none_or(|caller| description.contains_capture_for(&caller.evidence))
    }
}
impl DerivedIncident {
    pub fn key(&self) -> String {
        self.entry.revision_id.clone()
    }
    pub fn is_valid(&self) -> bool {
        let mut queries = self.affected_queries.iter().chain(&self.culprit_queries).chain(&self.candidate_queries);
        public_context(&self.analysis_id, &self.database, &self.display)
            && self.revision_reference.is_valid()
            && self.revision_reference.native_key
                == (NativeRecordIdentity::Operational {
                    source: DIAGNOSTICS_SOURCE.into(),
                    key: self.entry.revision_id.clone(),
                })
            && self.entry.controller_version > 0
            && valid_digest(&self.entry.episode_id)
            && valid_digest(&self.entry.revision_id)
            && valid_digest(&self.entry.target_id)
            && valid_digest(&self.entry.scope_id)
            && revision_number(&self.entry.revision)
            && match self.entry.previous_revision.as_deref() {
                None => self.entry.revision == "1",
                Some(previous) => {
                    revision_number(previous)
                        && previous.parse::<u64>().ok().and_then(|number| number.checked_add(1))
                            == self.entry.revision.parse::<u64>().ok()
                }
            }
            && utc(&self.entry.recorded_at)
            && self.entry.original.as_ref().is_none_or(|reference| *reference == self.revision_reference)
            && prose(&self.victim_summary)
            && prose(&self.developer_action)
            && prose(&self.verification)
            && self.mechanism.as_deref().is_none_or(prose)
            && queries.clone().count() <= MAX_DERIVED_QUERIES
            && queries.all(DerivedIncidentQuery::is_valid)
            && self.affected_queries.iter().chain(&self.candidate_queries).all(|query| query.causal_role.is_none())
            && self.culprit_queries.iter().all(|query| query.causal_role.is_some())
            && match self.attribution_status {
                DerivedAttributionStatus::Unestablished => self.culprit_queries.is_empty(),
                DerivedAttributionStatus::CandidatesOnly => {
                    self.culprit_queries.is_empty() && !self.candidate_queries.is_empty()
                }
                DerivedAttributionStatus::Established => !self.culprit_queries.is_empty(),
            }
            && self.caller.as_ref().is_none_or(DerivedCaller::is_valid)
            && self.original_interval.as_ref().is_none_or(|interval| {
                label(&interval.native_id)
                    && utc(&interval.from)
                    && utc(&interval.until)
                    && ObservationBounds::new(&interval.from, &interval.until).is_some()
            })
            && bounded(self)
    }
    pub fn fits_description(&self, description: &DerivedDiagnosisDescription) -> bool {
        self.is_valid()
            && self.analysis_id == description.analysis_id
            && self.display == description.display
            && description.contains_capture_for(&self.revision_reference)
            && self
                .affected_queries
                .iter()
                .chain(&self.culprit_queries)
                .chain(&self.candidate_queries)
                .all(|query| query.fits_description(description))
            && self.caller.as_ref().is_none_or(|caller| description.contains_capture_for(&caller.evidence))
    }
}
impl DerivedFinding {
    pub fn key(&self) -> String {
        self.finding.id.clone()
    }
    pub fn is_valid(&self) -> bool {
        public_context(&self.analysis_id, &self.database, &self.display)
            && label(&self.finding.id)
            && label(&self.finding.check)
            && capture_level_or_rows(&self.inputs)
            && self.finding.metrics.iter().all(|metric| metric.value.is_finite())
            && serde_json::to_vec(&self.finding)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Finding>(&bytes).ok())
                .is_some_and(|canonical| canonical == self.finding)
            && self.finding.impact.score.is_finite()
            && self.finding.impact.db_time_share.is_none_or(f64::is_finite)
            && self.finding.impact.time_ms_per_day.is_none_or(f64::is_finite)
            && self
                .query
                .as_ref()
                .is_none_or(|query| query.identity.is_valid() && query.reference.is_valid() && sql_payload(&query.sql))
            && self.cause.as_ref().is_none_or(|cause| prose(&cause.summary) && references(&cause.evidence))
            && bounded(self)
    }
    pub fn fits_description(&self, description: &DerivedDiagnosisDescription) -> bool {
        self.is_valid()
            && self.analysis_id == description.analysis_id
            && self.display == description.display
            && self.inputs.iter().all(|reference| description.contains_capture_for(reference))
            && self.query.as_ref().is_none_or(|query| description.contains_capture_for(&query.reference))
            && self
                .cause
                .as_ref()
                .is_none_or(|cause| cause.evidence.iter().all(|reference| description.contains_capture_for(reference)))
    }
}
impl DerivedChange {
    pub fn key(&self) -> String {
        self.change.id.clone()
    }
    pub fn is_valid(&self) -> bool {
        public_context(&self.analysis_id, &self.database, &self.display)
            && label(&self.change.id)
            && self.change.job_execution_is_valid()
            && self.change.job_execution.as_ref().is_none_or(|job| {
                self.inputs.iter().any(|input| {
                    input.stream == TelemetryStream::OperationalSamples
                        && matches!(&input.native_key, NativeRecordIdentity::Operational { source, key }
                        if source == &job.reference.source && key == &job.reference.observation_key)
                })
            })
            && capture_level_or_rows(&self.inputs)
            && bounded(self)
    }
    pub fn fits_description(&self, description: &DerivedDiagnosisDescription) -> bool {
        self.is_valid()
            && self.analysis_id == description.analysis_id
            && self.display == description.display
            && self.inputs.iter().all(|reference| description.contains_capture_for(reference))
    }
}
impl DerivedDiagnosis {
    pub fn validate(&self) -> Result<()> {
        let counts = DerivedDiagnosisCounts {
            incidents: self.incidents.len().try_into().map_err(|_| invalid())?,
            findings: self.findings.len().try_into().map_err(|_| invalid())?,
            changes: self.changes.len().try_into().map_err(|_| invalid())?,
        };
        let mut keys = BTreeSet::new();
        if !self.description.is_valid()
            || counts != self.description.counts
            || !self.incidents.iter().all(|row| row.fits_description(&self.description) && keys.insert((0, row.key())))
            || !self.findings.iter().all(|row| row.fits_description(&self.description) && keys.insert((1, row.key())))
            || !self.changes.iter().all(|row| row.fits_description(&self.description) && keys.insert((2, row.key())))
        {
            return Err(invalid());
        }
        Ok(())
    }
}
