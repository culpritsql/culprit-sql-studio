//! Text pagination has no loader or cache: every slice follows the same exact authenticated native body/EOF read.
use super::*;

pub(super) struct ArtifactTextRead {
    query: WarehouseArtifactQuery,
    matched: bool,
}
impl ArtifactTextRead {
    pub fn new(query: &WarehouseArtifactQuery) -> Result<Self> {
        query.validate()?;
        Ok(Self { query: query.clone(), matched: false })
    }
    pub fn payload(
        &mut self,
        reference: &DiagnosticEvidenceRef,
        location: &str,
        payload: &DefinitionPayload,
        byte_limit: usize,
    ) -> Result<Option<WarehouseArtifactText>> {
        match &self.query.text {
            WarehouseArtifactTextRead::Complete => {
                culprit_core::serialization::json_size(payload, byte_limit).map_err(|_| capacity())?;
                Ok(Some(WarehouseArtifactText::Complete { payload: payload.clone() }))
            }
            WarehouseArtifactTextRead::Inventory { chunk_chars } => {
                let DefinitionPayload::Available { text, sha256, redaction_version } = payload else {
                    return Ok(Some(WarehouseArtifactText::Complete { payload: payload.clone() }));
                };
                let characters = u32::try_from(text.chars().count()).map_err(|_| capacity())?;
                let next = if characters == 0 {
                    None
                } else {
                    Some(self.cursor(reference, location, sha256, *chunk_chars, None, 0)?)
                };
                Ok(Some(WarehouseArtifactText::Metadata {
                    sha256: sha256.clone(),
                    redaction_version: *redaction_version,
                    bytes: u64::try_from(text.len()).map_err(|_| capacity())?,
                    characters,
                    next,
                }))
            }
            WarehouseArtifactTextRead::Chunk { cursor } => {
                if cursor.reference_key != reference.key()?
                    || cursor.capture_key != reference.capture_ref().key()?
                    || cursor.location != location
                {
                    return Ok(None);
                }
                if self.matched {
                    return Err(invalid());
                }
                self.matched = true;
                cursor.validate()?;
                let DefinitionPayload::Available { text, sha256, redaction_version } = payload else {
                    return Err(DeliveryProblem::InvalidContract.error());
                };
                if sha256 != &cursor.payload_sha256
                    || self.binding(
                        reference,
                        location,
                        sha256,
                        cursor.chunk_chars,
                        cursor.previous_character,
                        cursor.next_character,
                    )? != cursor.binding
                {
                    return Err(DeliveryProblem::DestinationChanged.error());
                }
                let total = u32::try_from(text.chars().count()).map_err(|_| capacity())?;
                if total == 0 || cursor.next_character >= total {
                    return Err(DeliveryProblem::InvalidContract.error());
                }
                let from = cursor.next_character;
                let until = from.checked_add(cursor.chunk_chars).ok_or_else(capacity)?.min(total);
                if until <= from {
                    return Err(DeliveryProblem::InvalidContract.error());
                }
                let value = text.chars().skip(from as usize).take((until - from) as usize).collect::<String>();
                let next = if until == total {
                    None
                } else {
                    Some(self.cursor(reference, location, sha256, cursor.chunk_chars, Some(from), until)?)
                };
                Ok(Some(WarehouseArtifactText::Chunk {
                    sha256: sha256.clone(),
                    redaction_version: *redaction_version,
                    from_character: from,
                    until_character: until,
                    total_characters: total,
                    text: value,
                    next,
                }))
            }
        }
    }
    fn cursor(
        &self,
        reference: &DiagnosticEvidenceRef,
        location: &str,
        sha256: &str,
        chunk_chars: u32,
        previous_character: Option<u32>,
        next_character: u32,
    ) -> Result<WarehouseArtifactTextCursor> {
        Ok(WarehouseArtifactTextCursor {
            binding: self.binding(reference, location, sha256, chunk_chars, previous_character, next_character)?,
            reference_key: reference.key()?,
            capture_key: reference.capture_ref().key()?,
            location: location.into(),
            payload_sha256: sha256.into(),
            chunk_chars,
            previous_character,
            next_character,
        })
    }
    fn binding(
        &self,
        reference: &DiagnosticEvidenceRef,
        location: &str,
        sha256: &str,
        chunk_chars: u32,
        previous_character: Option<u32>,
        next_character: u32,
    ) -> Result<String> {
        serde_json::to_vec(&(
            "warehouse-artifact-text-v1",
            &self.query.resource,
            &self.query.references,
            &self.query.expected_sql_sha256,
            self.query.full_payloads,
            reference,
            location,
            sha256,
            chunk_chars,
            previous_character,
            next_character,
        ))
        .map(|value| digest(&value))
        .map_err(|_| invalid())
    }
}
