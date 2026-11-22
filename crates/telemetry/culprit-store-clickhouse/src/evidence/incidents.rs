//! Candidate lookup shares native projection and limits with ordinary authenticated reads.
use super::*;
use culprit_core::telemetry::TelemetryStream;
use culprit_evidence::analytical::evidence::IncidentCandidateSelection;

pub(super) async fn read(
    store: &ClickHouseReadStore,
    contract: &Contract,
    request: &IncidentDiscoveryRequest,
    byte_limit: usize,
) -> Result<AnalyticalReadPage> {
    request.validate()?;
    let schema = store.catalog.get(TelemetryStream::DerivedIncidents)?;
    let mut parameters = BTreeMap::from([
        ("contract".into(), contract.id()?),
        ("schema".into(), contract.schema_digest().into()),
        ("database".into(), request.selection.database().key().into()),
        ("recorded_before".into(), nanos(&request.recorded_before)?),
    ]);
    let selected = match &request.selection {
        IncidentCandidateSelection::Around { from, until, .. } => {
            parameters.extend([("from".into(), nanos(from)?), ("until".into(), nanos(until)?)]);
            "((OriginalObservedFrom < fromUnixTimestamp64Nano({until:Int64}) AND OriginalObservedUntil >= fromUnixTimestamp64Nano({from:Int64})) OR (RecordedAt >= fromUnixTimestamp64Nano({from:Int64}) AND RecordedAt < fromUnixTimestamp64Nano({until:Int64})) OR (OriginalIntervalFrom < fromUnixTimestamp64Nano({until:Int64}) AND OriginalIntervalUntil >= fromUnixTimestamp64Nano({from:Int64})))"
        }
        IncidentCandidateSelection::Episode { episode_id, controller_version, .. } => {
            parameters.extend([
                ("episode".into(), episode_id.clone()),
                ("controller".into(), controller_version.to_string()),
            ]);
            "EpisodeId={episode:String} AND ControllerVersion={controller:Int64}"
        }
    };
    let after = if let Some(cursor) = &request.after {
        let position = &cursor.position;
        parameters.extend([
            ("after_episode".into(), position.episode_id.clone()),
            ("after_controller".into(), position.controller_version.to_string()),
            ("after_digits".into(), position.revision.len().to_string()),
            ("after_revision".into(), position.revision.clone()),
            ("after_range".into(), position.range_id.clone()),
            ("after_batch".into(), position.batch_id.clone()),
            ("after_ordinal".into(), position.row_ordinal.to_string()),
        ]);
        " AND (EpisodeId,ControllerVersion,length(toString(Revision)),toString(Revision),RangeId,BatchId,RowOrdinal)>({after_episode:String},{after_controller:Int64},{after_digits:UInt64},{after_revision:String},{after_range:String},{after_batch:String},{after_ordinal:Int64})"
    } else {
        ""
    };
    let text = format!(
        "SELECT DISTINCT {} FROM {} WHERE ContractId={{contract:String}} AND SchemaDigest={{schema:String}} AND DatabaseSelector={{database:String}} AND RecordedAt<=fromUnixTimestamp64Nano({{recorded_before:Int64}}) AND ({selected}){after} ORDER BY EpisodeId,ControllerVersion,length(toString(Revision)),toString(Revision),RangeId,BatchId,RowOrdinal LIMIT {}",
        schema.verification_projection(),
        quote(schema.table()),
        request.page_rows + 1,
    );
    let table = store
        .query
        .query(QueryRequest {
            text,
            parameters,
            columns: schema.verification_columns(),
            row_limit: request.page_rows as usize + 1,
            byte_limit,
        })
        .await?;
    Ok(AnalyticalReadPage { rows: table.rows, response_bytes: table.response_bytes })
}
fn nanos(value: &str) -> Result<String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|at| at.timestamp_nanos_opt())
        .map(|value| value.to_string())
        .ok_or_else(invalid)
}
