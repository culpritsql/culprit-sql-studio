//! Shared wording of typed native queue evidence; no lock or SQL classification occurs in a consumer.
use culprit_core::blocking::{MetadataQueueIssue, MetadataQueueResource, MetadataQueueStatus};
use culprit_core::{
    MetadataQueueExplanation, MetadataQueueReport, Result, format, t, telemetry::ActivityFrameEvidence,
};
pub(super) fn explain(frame: &ActivityFrameEvidence<'_>) -> Result<MetadataQueueReport> {
    let evidence = culprit_core::blocking::metadata_queue(frame)?;
    let summary = match evidence.status {
        MetadataQueueStatus::Observed => {
            t!("view.metadata_queue.summary.observed", count = evidence.cascades.len() as u32)
        }
        MetadataQueueStatus::NotEstablished => t!("view.metadata_queue.summary.not_established"),
        MetadataQueueStatus::Unavailable => t!("view.metadata_queue.summary.unavailable"),
    };
    let explanations = evidence
        .cascades
        .iter()
        .map(|case| {
            let resource =
                case.object_label.as_deref().map(|label| format::escaped_preview(label, 256).text).unwrap_or_else(
                    || match &case.resource {
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
                mechanism: t!(
                    "view.metadata_queue.mechanism",
                    gate = &case.gatekeeper.session.id,
                    mode = &case.requested_mode,
                    resource = resource,
                    holders = case.holders.len() as u32
                ),
                investigation: t!("view.metadata_queue.investigation"),
                verify: t!("view.metadata_queue.verify"),
            }
        })
        .collect();
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
        MetadataQueueIssue::UnqualifiedParticipant => t!("view.metadata_queue.issue.participant"),
        MetadataQueueIssue::PointChanged => t!("view.metadata_queue.issue.changed"),
        MetadataQueueIssue::LockEvidenceLimited => t!("view.metadata_queue.issue.limited"),
        MetadataQueueIssue::UnsupportedResource => t!("view.metadata_queue.issue.resource"),
        MetadataQueueIssue::UnknownModeOrEdge => t!("view.metadata_queue.issue.unknown_edge"),
        MetadataQueueIssue::MixedGrantedAndPending => t!("view.metadata_queue.issue.mixed"),
    }
}
