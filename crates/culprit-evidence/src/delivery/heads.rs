use super::evidence::RangeManifest;
use culprit_core::{Result, telemetry::*};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticHead {
    pub controller_key: String,
    pub controller_version: u32,
    pub target: CollectionTarget,
    pub parent: DiagnosticCaptureRef,
    pub capture: DiagnosticCaptureRef,
    pub range_id: String,
    pub manifest_digest: String,
}
impl DiagnosticHead {
    pub fn validate(&self) -> Result<()> {
        let expected = DiagnosticCheckpoint {
            controller_version: self.controller_version,
            target: self.target.clone(),
            input: self.parent.clone(),
            episodes: vec![],
        }
        .key()?;
        if expected != self.controller_key
            || !self.capture.is_valid()
            || !valid_digest(&self.range_id)
            || !valid_digest(&self.manifest_digest)
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn rank(&self) -> Result<(u64, u64)> {
        let revision = self.capture.capture.revision.as_ref().ok_or_else(invalid)?;
        Ok((revision.owner_epoch.parse().map_err(|_| invalid())?, revision.sequence.parse().map_err(|_| invalid())?))
    }
}
#[derive(Clone)]
pub struct DiagnosticHeadPage {
    pub heads: Vec<DiagnosticHead>,
    pub next_cursor: Option<String>,
    pub high_sequence: u64,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DiagnosticHeads(BTreeMap<String, DiagnosticHead>);
impl DiagnosticHeads {
    pub fn validate(&self) -> Result<()> {
        if self.0.len() > MAX_DIAGNOSTIC_EPISODES {
            return Err(DeliveryProblem::Capacity.error());
        }
        for (key, head) in &self.0 {
            head.validate()?;
            if key != &head.controller_key {
                return Err(invalid());
            }
        }
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn observe(&mut self, manifest: &RangeManifest, manifest_digest: &str) -> Result<()> {
        let SourceObservation::Diagnostics(description) = &manifest.source.observation else {
            return Ok(());
        };
        let key = DiagnosticCheckpoint {
            controller_version: description.controller_version,
            target: manifest.source.target.clone(),
            input: description.parent.clone(),
            episodes: vec![],
        }
        .key()?;
        let candidate = DiagnosticHead {
            controller_key: key.clone(),
            controller_version: description.controller_version,
            target: manifest.source.target.clone(),
            parent: description.parent.clone(),
            capture: capture_reference(manifest)?,
            range_id: manifest.range_id.clone(),
            manifest_digest: manifest_digest.into(),
        };
        candidate.validate()?;
        if let Some(previous) = self.0.get(&key) {
            if previous.capture.capture.revision.as_ref().map(|revision| &revision.outbox_id)
                != candidate.capture.capture.revision.as_ref().map(|revision| &revision.outbox_id)
            {
                return Err(DeliveryProblem::Conflict.error());
            }
            if previous.rank()? > candidate.rank()? {
                return Ok(());
            }
            if previous.rank()? == candidate.rank()? && previous != &candidate {
                return Err(invalid());
            }
        } else if self.0.len() == MAX_DIAGNOSTIC_EPISODES {
            return Err(DeliveryProblem::Capacity.error());
        }
        self.0.insert(key, candidate);
        Ok(())
    }
    pub fn page(&self, cursor: Option<&str>, limit: u32, high_sequence: u64) -> Result<DiagnosticHeadPage> {
        self.validate()?;
        if !(1..=128).contains(&limit) {
            return Err(DeliveryProblem::Capacity.error());
        }
        let snapshot = digest(&serde_json::to_vec(self).map_err(|_| invalid())?);
        let after = match cursor {
            Some(text) if text.len() <= 512 => {
                let (prior, after): (String, String) = serde_json::from_str(text).map_err(|_| invalid())?;
                if prior != snapshot || !valid_digest(&after) {
                    return Err(DeliveryProblem::Conflict.error());
                }
                after
            }
            Some(_) => return Err(invalid()),
            None => String::new(),
        };
        let mut heads = self
            .0
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .take(limit as usize + 1)
            .map(|(_, head)| head.clone())
            .collect::<Vec<_>>();
        let next_cursor = if heads.len() > limit as usize {
            heads.pop();
            Some(
                serde_json::to_string(&(snapshot, &heads.last().ok_or_else(invalid)?.controller_key))
                    .map_err(|_| invalid())?,
            )
        } else {
            None
        };
        Ok(DiagnosticHeadPage { heads, next_cursor, high_sequence })
    }
}

pub fn capture_reference(manifest: &RangeManifest) -> Result<DiagnosticCaptureRef> {
    let reference = DiagnosticCaptureRef {
        capture: manifest.capture.clone(),
        manifest_sha256: manifest.capture_manifest_sha256.clone(),
        source_scope_id: manifest.source.captured_range()?.scope_id,
    };
    if !reference.is_valid() {
        return Err(invalid());
    }
    Ok(reference)
}

fn invalid() -> culprit_core::Error {
    DeliveryProblem::Integrity.error()
}
