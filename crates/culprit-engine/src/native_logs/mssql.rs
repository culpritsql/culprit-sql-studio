//! Imported artifact placement is separate from native feed authority and query causality.
use super::*;
use culprit_core::{
    ImportedEventDeadlock, ImportedNativeEvents,
    events::{ChangeEvent, EventKind, EventSource},
    format, message,
    telemetry::MssqlExtendedEventKind,
};

pub(super) fn decode(input: &[u8], limits: NativeLogLimits, artifact: &str) -> DecodedLog {
    let batch = match culprit_plans::mssql::events::parse_xml(input, limits) {
        Ok(batch) => batch,
        Err(problem) => return DecodedLog::stopped(problem),
    };
    let mut timeline = vec![];
    let mut unplaced_positions = vec![];
    for row in &batch.records {
        if let Some(at) = &row.timestamp_utc {
            let subject = format!("{artifact}:{}", row.position);
            timeline.push(
                ChangeEvent::new(
                    EventKind::NativeEvent,
                    EventSource::ImportedArtifact,
                    &subject,
                    at,
                    match row.kind {
                        MssqlExtendedEventKind::Attention { .. } => message!("native_logs.mssql.attention.title"),
                        MssqlExtendedEventKind::ErrorReported { number, severity, .. } => message!(
                            "native_logs.mssql.error.title",
                            number = number.to_string(),
                            severity = format::count(severity as f64)
                        ),
                        MssqlExtendedEventKind::DeadlockReport { .. } => message!("native_logs.mssql.deadlock.title"),
                    },
                )
                .detail(message!("native_logs.mssql.timeline.detail")),
            );
        } else {
            unplaced_positions.push(row.position);
        }
    }
    let deadlocks = batch
        .deadlocks
        .into_iter()
        .enumerate()
        .map(|(ordinal, (position, mut graph))| {
            // Artifact identity keeps distinct supplied positions; it is never a native occurrence/execution count.
            graph.id = digest(format!("imported-xe-graph-v1:{artifact}:{position}:{ordinal}").as_bytes());
            ImportedEventDeadlock { position, graph }
        })
        .collect();
    DecodedLog {
        observations: batch
            .records
            .into_iter()
            .map(|row| NativeFailureRecord::MssqlExtendedEvent(Box::new(row)))
            .collect(),
        issues: batch.issues,
        stopped: batch.stopped,
        records_examined: batch.records_examined,
        non_failure_records: 0,
        native_events: Some(ImportedNativeEvents { timeline, unplaced_positions, deadlocks }),
    }
}
