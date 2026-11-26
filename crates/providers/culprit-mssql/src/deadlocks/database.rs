//! Explicitly configured database-scoped deadlock evidence. Never read Azure's internal `dl` session:
//! https://learn.microsoft.com/azure/azure-sql/database/xevent-db-diff-from-svr#best-practices
use super::native::Event;
use crate::read::{NativeRead, Parameter, ReadError, integer, invalid_data, text};
use culprit_core::{
    Error, ErrorCode, Result, t, telemetry::CollectionStop, telemetry::canonical_time, telemetry::digest,
};
use std::collections::BTreeSet;

pub(super) const SESSION: &str = "culprit_deadlocks";
const MAX_MEMORY_KIB: i64 = 1024;
const MAX_EVENTS: usize = 256;
const MAX_XML_BYTES: i64 = 4 * 1024 * 1024;

// Read configuration before requesting target_data. The required explicit target limits bound the source,
// rather than assuming a TOP clause or a client timeout bounds serialization of an arbitrary ring buffer.
const CONFIGURATION: &str = r#"SELECT TOP (2) /* culprit */ c.event_session_id,
 CONVERT(varchar(40),CAST(s.create_time AS datetime2(7)),126) AS generation,
 CONVERT(bigint,m.value) AS memory_kib,CONVERT(bigint,n.value) AS maximum_events
FROM sys.database_event_sessions c
JOIN sys.dm_xe_database_sessions s ON s.name=c.name
JOIN sys.database_event_session_targets target ON target.event_session_id=c.event_session_id
JOIN sys.database_event_session_fields m ON m.event_session_id=c.event_session_id AND m.object_id=target.target_id AND m.name=N'max_memory'
JOIN sys.database_event_session_fields n ON n.event_session_id=c.event_session_id AND n.object_id=target.target_id AND n.name=N'max_events_limit'
WHERE c.name=@P1 AND target.name=N'ring_buffer' AND target.package=N'package0'
 AND (SELECT COUNT_BIG(*) FROM sys.database_event_session_targets x WHERE x.event_session_id=c.event_session_id)=1
 AND (SELECT COUNT_BIG(*) FROM sys.database_event_session_events e WHERE e.event_session_id=c.event_session_id)=1
 AND EXISTS(SELECT 1 FROM sys.database_event_session_events e
  JOIN sys.database_event_session_actions a ON a.event_session_id=e.event_session_id AND a.event_id=e.event_id
  WHERE e.event_session_id=c.event_session_id AND e.name=N'database_xml_deadlock_report' AND e.package=N'sqlserver'
   AND a.name=N'event_sequence' AND a.package=N'package0')
OPTION(MAXDOP 1)"#;

fn target_sql() -> String {
    let generation_matches = crate::sql_time::native_time_matches("s.create_time", "@P3");
    format!(
        r#"SELECT TOP (2) /* culprit */ DATALENGTH(t.target_data) AS target_bytes,
 CASE WHEN DATALENGTH(t.target_data)<=@P4 THEN t.target_data END AS target_xml
FROM sys.dm_xe_database_sessions s
JOIN sys.dm_xe_database_session_targets t ON t.event_session_address=s.address
JOIN sys.database_event_sessions c ON c.name=s.name
WHERE s.name=@P1 AND c.event_session_id=@P2 AND {generation_matches}
 AND t.target_name=N'ring_buffer' OPTION(MAXDOP 1)"#
    )
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredSession {
    event_session_id: i64,
    generation: String,
    maximum_events: usize,
}

impl ConfiguredSession {
    pub(crate) async fn probe<R: NativeRead>(io: &mut R) -> Result<Option<Self>> {
        let configuration =
            io.query(CONFIGURATION, vec![Parameter::Text(SESSION.into())], 2).await.map_err(ReadError::into_error)?;
        if configuration.is_empty() {
            return Ok(None);
        }
        let [configuration] = configuration.as_slice() else {
            return Ok(None);
        };
        let memory = integer(configuration, "memory_kib")?;
        let maximum_events = integer(configuration, "maximum_events")?;
        if !(1..=MAX_MEMORY_KIB).contains(&memory) || !(1..=MAX_EVENTS as i64).contains(&maximum_events) {
            return Ok(None);
        }
        let generation = crate::sql_time::native_time(&text(configuration, "generation")?)?;
        Ok(Some(Self {
            event_session_id: integer(configuration, "event_session_id")?,
            generation,
            maximum_events: maximum_events as usize,
        }))
    }

    pub(crate) async fn events<R: NativeRead>(
        &self,
        io: &mut R,
        database: &str,
        from: &str,
        until: &str,
        limit: usize,
        newest_first: bool,
    ) -> Result<Vec<Event>> {
        let rows = io
            .query(
                &target_sql(),
                vec![
                    Parameter::Text(SESSION.into()),
                    Parameter::Integer(self.event_session_id),
                    Parameter::Text(self.generation.clone()),
                    Parameter::Integer(MAX_XML_BYTES),
                ],
                2,
            )
            .await
            .map_err(ReadError::into_error)?;
        let [target] = rows.as_slice() else { return Err(CollectionStop::SourceChanged.error()) };
        if integer(target, "target_bytes")? > MAX_XML_BYTES {
            return Err(CollectionStop::ByteLimit.error());
        }
        let xml = text(target, "target_xml")?;
        if xml.len() > MAX_XML_BYTES as usize {
            return Err(CollectionStop::ByteLimit.error());
        }
        let mut events = decode(&xml, database, &self.generation, from, until, self.maximum_events)?;
        events.sort_by(|a, b| (&a.at, a.offset.len(), &a.offset).cmp(&(&b.at, b.offset.len(), &b.offset)));
        if newest_first {
            events.reverse();
        }
        events.truncate(limit);
        Ok(events)
    }
}

pub(super) async fn read<R: NativeRead>(
    io: &mut R,
    database: &str,
    from: &str,
    until: &str,
    limit: usize,
    newest_first: bool,
) -> Result<Vec<Event>> {
    ConfiguredSession::probe(io)
        .await?
        .ok_or_else(setup_required)?
        .events(io, database, from, until, limit, newest_first)
        .await
}

fn setup_required() -> Error {
    Error::new(ErrorCode::Unsupported, t!("provider.mssql.deadlocks.session_required"))
}

fn decode(
    xml: &str,
    database: &str,
    generation: &str,
    from: &str,
    until: &str,
    maximum_events: usize,
) -> Result<Vec<Event>> {
    let from = chrono::DateTime::parse_from_rfc3339(from).map_err(|_| invalid_data())?;
    let until = chrono::DateTime::parse_from_rfc3339(until).map_err(|_| invalid_data())?;
    if from >= until {
        return Err(invalid_data());
    }
    culprit_core::xml::read(xml, |document| {
        let document = document.map_err(|_| invalid_data())?;
        let root = document.root_element();
        if !root.has_tag_name("RingBufferTarget") {
            return Err(invalid_data());
        }
        match root.attribute("truncated") {
            Some("0") => {}
            Some("1") => return Err(CollectionStop::ByteLimit.error()),
            _ => return Err(invalid_data()),
        }
        let nodes = root.children().filter(|node| node.is_element()).collect::<Vec<_>>();
        let expected =
            root.attribute("eventCount").and_then(|count| count.parse::<usize>().ok()).ok_or_else(invalid_data)?;
        if expected != nodes.len() {
            return Err(invalid_data());
        }
        if nodes.len() > maximum_events {
            return Err(CollectionStop::RowLimit.error());
        }
        let source = digest(&serde_json::to_vec(&(database, SESSION, generation)).map_err(|_| invalid_data())?);
        let label = super::database_session();
        let mut sequences = BTreeSet::new();
        let mut events = Vec::new();
        for node in nodes {
            if !node.has_tag_name("event") || node.attribute("name") != Some("database_xml_deadlock_report") {
                return Err(invalid_data());
            }
            let at = canonical_time(node.attribute("timestamp").ok_or_else(invalid_data)?)?;
            let sequence = node
                .children()
                .filter(|node| node.has_tag_name("action") && node.attribute("name") == Some("event_sequence"))
                .collect::<Vec<_>>();
            let [sequence] = sequence.as_slice() else { return Err(invalid_data()) };
            let sequence = sequence
                .children()
                .find(|node| node.has_tag_name("value"))
                .and_then(|node| node.text())
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(invalid_data)?;
            if !sequences.insert(sequence) {
                return Err(invalid_data());
            }
            let time = chrono::DateTime::parse_from_rfc3339(&at).map_err(|_| invalid_data())?;
            if time < from || time >= until {
                continue;
            }
            let native_xml = &xml[node.range()];
            let graphs = node
                .descendants()
                .filter(|node| node.has_tag_name("deadlock"))
                .map(|node| {
                    culprit_plans::mssql::deadlocks::parse_deadlock_node(node, &label, Some(at.clone()))
                        .ok_or_else(invalid_data)
                })
                .collect::<Result<Vec<_>>>()?;
            if graphs.is_empty() {
                return Err(invalid_data());
            }
            let position =
                digest(&serde_json::to_vec(&(&source, sequence, &at, native_xml)).map_err(|_| invalid_data())?);
            events.push(Event { position, file_id: source.clone(), offset: sequence.to_string(), at, graphs });
        }
        Ok(events)
    })
}

#[cfg(test)]
pub(crate) mod tests;
