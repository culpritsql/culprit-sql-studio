//! Shared wording of typed native queue evidence; no lock or SQL classification occurs in a consumer.
use culprit_core::blocking::{MetadataQueueIssue, MetadataQueueResource, MetadataQueueStatus};
use culprit_core::{
    MetadataQueueExplanation, MetadataQueueReport, Result, format, t, telemetry::ActivityFrameEvidence,
};
pub(super) fn explain(frame: &ActivityFrameEvidence<'_>) -> Result<MetadataQueueReport> {
    let evidence = culprit_core::blocking::metadata_queue(frame)?;
    let summary = if !evidence.candidates.is_empty() {
        t!("view.metadata_queue.candidate.summary", count = evidence.candidates.len() as u32)
    } else {
        match evidence.status {
            MetadataQueueStatus::Observed => {
                if evidence
                    .cascades
                    .first()
                    .is_some_and(|case| matches!(&case.resource, MetadataQueueResource::MssqlObject(_)))
                {
                    t!("view.metadata_queue.summary.object_observed", count = evidence.cascades.len() as u32)
                } else {
                    t!("view.metadata_queue.summary.observed", count = evidence.cascades.len() as u32)
                }
            }
            MetadataQueueStatus::NotEstablished => t!("view.metadata_queue.summary.not_established"),
            MetadataQueueStatus::Unavailable => t!("view.metadata_queue.summary.unavailable"),
        }
    };
    let mut explanations: Vec<MetadataQueueExplanation> = evidence
        .cascades
        .iter()
        .map(|case| {
            let resource =
                case.object_label.as_deref().map(|label| format::escaped_preview(label, 256).text).unwrap_or_else(
                    || match &case.resource {
                        MetadataQueueResource::MssqlObject(tag) => t!(
                            "view.metadata_queue.resource.native_object",
                            database = &tag.database_id.to_string(),
                            object = &tag.associated_entity_id
                        ),
                        MetadataQueueResource::PostgresRelation(tag) => t!(
                            "view.metadata_queue.resource.native_relation",
                            database = &tag.database_oid,
                            relation = &tag.relation_oid
                        ),
                    },
                );
            MetadataQueueExplanation {
                key: case.key.clone(),
                victim_summary: t!(
                    "view.metadata_queue.victim",
                    count = case.affected.len() as u32,
                    gate = &case.gatekeeper.session.id
                ),
                mechanism: match &case.resource {
                    MetadataQueueResource::MssqlObject(_) => t!(
                        "view.metadata_queue.mechanism.object",
                        gate = &case.gatekeeper.session.id,
                        mode = &case.requested_mode,
                        resource = resource,
                        holders = case.holders.len() as u32
                    ),
                    MetadataQueueResource::PostgresRelation(_) => t!(
                        "view.metadata_queue.mechanism",
                        gate = &case.gatekeeper.session.id,
                        mode = &case.requested_mode,
                        resource = resource,
                        holders = case.holders.len() as u32
                    ),
                },
                investigation: t!("view.metadata_queue.investigation"),
                verify: match &case.resource {
                    MetadataQueueResource::MssqlObject(_) => t!("view.metadata_queue.verify.object"),
                    MetadataQueueResource::PostgresRelation(_) => t!("view.metadata_queue.verify"),
                },
            }
        })
        .collect();
    for candidate in &evidence.candidates {
        explanations.push(MetadataQueueExplanation {
            key: candidate.key.clone(),
            victim_summary: t!("view.metadata_queue.candidate.victim", session = &candidate.affected.session.id),
            mechanism: t!(
                "view.metadata_queue.candidate.mechanism",
                resource =
                    format::escaped_preview(&format!("{}.{}", candidate.resource.schema, candidate.resource.name), 256)
                        .text,
                pending = candidate.pending_exclusive.len() as u32,
                holders = candidate.granted_holders.len() as u32
            ),
            investigation: t!("view.metadata_queue.candidate.action"),
            verify: t!("view.metadata_queue.candidate.verify"),
        });
    }
    let mut limitations = vec![
        t!("view.metadata_queue.limit.scope"),
        t!("view.metadata_queue.limit.sql"),
        t!("view.metadata_queue.limit.point"),
    ];
    for issue in &evidence.issues {
        limitations.push(reason(*issue));
    }
    Ok(MetadataQueueReport { evidence, summary, explanations, limitations })
}
fn reason(issue: MetadataQueueIssue) -> String {
    match issue {
        MetadataQueueIssue::UnqualifiedOriginal => t!("view.metadata_queue.issue.original"),
        MetadataQueueIssue::IncompleteRead => t!("view.metadata_queue.issue.incomplete"),
        MetadataQueueIssue::NativeDetailsNotRecorded => t!("view.metadata_queue.issue.not_recorded"),
        MetadataQueueIssue::NativeVisibilityUnavailable => t!("view.metadata_queue.issue.visibility"),
        MetadataQueueIssue::UnqualifiedParticipant => t!("view.metadata_queue.issue.participant"),
        MetadataQueueIssue::PointChanged => t!("view.metadata_queue.issue.changed"),
        MetadataQueueIssue::LockEvidenceLimited => t!("view.metadata_queue.issue.limited"),
        MetadataQueueIssue::UnsupportedResource => t!("view.metadata_queue.issue.resource"),
        MetadataQueueIssue::UnknownModeOrEdge => t!("view.metadata_queue.issue.unknown_edge"),
        MetadataQueueIssue::MixedGrantedAndPending => t!("view.metadata_queue.issue.mixed"),
        MetadataQueueIssue::PendingBlockerNotExposed => t!("view.metadata_queue.issue.pending_native"),
        MetadataQueueIssue::NativeLifetimeNotRecorded => t!("view.metadata_queue.issue.native_lifetime"),
    }
}
