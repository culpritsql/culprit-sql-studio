//! Bounded supplied Extended Events XML. This is not a binary XEL decoder or a native feed authenticator.
use culprit_core::{
    Deadlock, Engine, NativeLogIssue, NativeLogLimits, NativeLogProblem,
    telemetry::{
        ActivityClient, DefinitionPayload, MssqlExtendedEventKind, MssqlExtendedEventObservation, PayloadUnavailable,
    },
};
use roxmltree::{Document, Node};
use std::collections::BTreeMap;

type Parsed<T> = std::result::Result<T, NativeLogProblem>;

pub struct ParsedEventXml {
    pub records: Vec<MssqlExtendedEventObservation>,
    pub deadlocks: Vec<(u32, Deadlock)>,
    pub issues: Vec<NativeLogIssue>,
    pub stopped: Option<NativeLogProblem>,
    pub records_examined: u32,
}

/// Decode only a complete XML document with event/event-list/ring-buffer framing. The byte, DOM and row limits are
/// independent; no decompression, file lookup, session configuration or current native context is consulted.
pub fn parse_xml(input: &[u8], limits: NativeLogLimits) -> Parsed<ParsedEventXml> {
    if !limits.is_valid() {
        return Err(NativeLogProblem::InvalidLimits);
    }
    if input.len() > limits.input_bytes {
        return Err(NativeLogProblem::InputLimit);
    }
    // The shared SSMS decoder handles BOMs. Refuse a truncated UTF-16 code unit before that decoder's framing.
    if matches!(input, [0xff, 0xfe, ..] | [0xfe, 0xff, ..]) && !input.len().is_multiple_of(2) {
        return Err(NativeLogProblem::InvalidEncoding);
    }
    let xml = crate::decode(input).map_err(|_| NativeLogProblem::InvalidEncoding)?;
    if xml.len() > limits.input_bytes {
        return Err(NativeLogProblem::InputLimit);
    }
    culprit_core::xml::read(&xml, |doc| {
        let doc = doc.map_err(|_| NativeLogProblem::MalformedRecord)?;
        parse_document(&doc, &xml, limits)
    })
}

fn parse_document(doc: &Document, xml: &str, limits: NativeLogLimits) -> Parsed<ParsedEventXml> {
    if !matches!(doc.root_element().tag_name().name(), "event" | "events" | "Events" | "RingBufferTarget") {
        return Err(NativeLogProblem::NativeShape);
    }
    if !doc.root_element().has_tag_name("event")
        && doc.root_element().children().any(|node| node.is_element() && !node.has_tag_name("event"))
    {
        return Err(NativeLogProblem::NativeShape);
    }
    let mut batch =
        ParsedEventXml { records: vec![], deadlocks: vec![], issues: vec![], stopped: None, records_examined: 0 };
    for event in doc.descendants().filter(|node| node.has_tag_name("event")) {
        if batch.records_examined as usize == limits.records {
            batch.stopped = Some(NativeLogProblem::RecordLimit);
            break;
        }
        let position = batch.records_examined;
        batch.records_examined += 1;
        let result = if xml[event.range()].len() > limits.record_bytes {
            Err(NativeLogProblem::RecordBytes)
        } else {
            parse_event(event, position, limits.field_bytes)
        };
        match result {
            Ok((row, graphs)) => {
                batch.records.push(row);
                batch.deadlocks.extend(graphs.into_iter().map(|graph| (position, graph)));
            }
            Err(problem) => batch.issues.push(NativeLogIssue { position, problem }),
        }
    }
    if batch.records_examined == 0 && doc.root_element().has_tag_name("event") {
        return Err(NativeLogProblem::NativeShape);
    }
    // A ring-buffer's own dropped/truncated signals qualify even a syntactically complete supplied document.
    if doc.root_element().has_tag_name("RingBufferTarget")
        && ["truncated", "droppedCount"]
            .into_iter()
            .any(|name| doc.root_element().attribute(name).is_some_and(|value| value != "0"))
    {
        batch.stopped = Some(NativeLogProblem::NativeShape);
    }
    Ok(batch)
}

fn parse_event(
    event: Node<'_, '_>,
    position: u32,
    field_bytes: usize,
) -> Parsed<(MssqlExtendedEventObservation, Vec<Deadlock>)> {
    if event.ancestors().skip(1).any(|parent| parent.has_tag_name("event"))
        || event.attribute("package") != Some("sqlserver")
    {
        return Err(NativeLogProblem::NativeShape);
    }
    validate_field_sizes(event, field_bytes)?;
    let fields = EventFields::new(event)?;
    let timestamp = event.attribute("timestamp").map(str::to_owned);
    let timestamp_utc = timestamp.as_deref().and_then(MssqlExtendedEventObservation::qualified_timestamp);
    let (kind, graphs) = native_kind(event, &fields, timestamp_utc.as_deref())?;
    let client = ActivityClient {
        application: fields.action("client_app_name", "sqlserver")?.map(str::to_owned),
        user: fields.action("username", "sqlserver")?.map(str::to_owned),
        host: fields.action("client_hostname", "sqlserver")?.map(str::to_owned),
    };
    let row = MssqlExtendedEventObservation {
        position,
        kind,
        timestamp,
        timestamp_utc,
        event_sequence: fields.action("event_sequence", "package0")?.map(str::to_owned),
        session_id: fields.action("session_id", "sqlserver")?.map(str::to_owned),
        request_id: fields.action("request_id", "sqlserver")?.map(str::to_owned),
        transaction_id: fields.action("transaction_id", "sqlserver")?.map(str::to_owned),
        database_id: fields.action("database_id", "sqlserver")?.map(str::to_owned),
        database: fields.action("database_name", "sqlserver")?.map(str::to_owned),
        client: (client.application.is_some() || client.user.is_some() || client.host.is_some()).then_some(client),
        statement: fields
            .action("sql_text", "sqlserver")?
            .map_or(DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected }, |sql| {
                DefinitionPayload::sql(sql, Engine::Mssql)
            }),
    };
    if !row.is_valid() {
        return Err(NativeLogProblem::NativeShape);
    }
    Ok((row, graphs))
}

fn native_kind(
    event: Node<'_, '_>,
    fields: &EventFields<'_, '_>,
    timestamp_utc: Option<&str>,
) -> Parsed<(MssqlExtendedEventKind, Vec<Deadlock>)> {
    let mut graphs = vec![];
    let kind = match event.attribute("name") {
        Some("attention") => {
            MssqlExtendedEventKind::Attention { native_duration: fields.data("duration")?.map(str::to_owned) }
        }
        Some("error_reported") => MssqlExtendedEventKind::ErrorReported {
            number: fields.required_number("error_number")?,
            severity: fields.required_number("severity")?,
            state: fields.required_number("state")?,
            intercepted: fields.boolean("is_intercepted")?,
            user_defined: fields.boolean("user_defined")?,
        },
        Some("xml_deadlock_report" | "database_xml_deadlock_report") => {
            let report = fields.data_node("xml_report").ok_or(NativeLogProblem::NativeShape)?;
            for node in report.descendants().filter(|node| node.has_tag_name("deadlock")) {
                let mut graph = super::deadlocks::parse_deadlock_node(
                    node,
                    "supplied-extended-events-xml",
                    timestamp_utc.map(str::to_owned),
                )
                .ok_or(NativeLogProblem::NativeShape)?;
                graph.redact(Engine::Mssql);
                graphs.push(graph);
            }
            MssqlExtendedEventKind::DeadlockReport { graphs: graphs.len() as u32 }
        }
        _ => return Err(NativeLogProblem::UnsupportedEvent),
    };
    Ok((kind, graphs))
}

fn validate_field_sizes(event: Node<'_, '_>, max: usize) -> Parsed<()> {
    for field in event.children().filter(|node| node.has_tag_name("data") || node.has_tag_name("action")) {
        let bytes = field
            .descendants()
            .filter(|node| node.is_text())
            .filter_map(|node| node.text())
            .try_fold(0usize, |used, text| used.checked_add(text.len()));
        if bytes.is_none_or(|bytes| bytes > max) {
            return Err(NativeLogProblem::FieldBytes);
        }
    }
    for node in event.descendants() {
        if node.attributes().any(|attribute| attribute.value().len() > max)
            || node.text().is_some_and(|text| text.len() > max)
        {
            return Err(NativeLogProblem::FieldBytes);
        }
    }
    Ok(())
}

struct EventFields<'a, 'input> {
    data: BTreeMap<&'a str, Node<'a, 'input>>,
    actions: BTreeMap<&'a str, Node<'a, 'input>>,
}
impl<'a, 'input> EventFields<'a, 'input> {
    fn new(event: Node<'a, 'input>) -> Parsed<Self> {
        let mut data = BTreeMap::new();
        let mut actions = BTreeMap::new();
        for field in event.children().filter(|child| child.has_tag_name("data") || child.has_tag_name("action")) {
            let name = field.attribute("name").ok_or(NativeLogProblem::NativeShape)?;
            let mut values = field.children().filter(|child| child.has_tag_name("value"));
            let value = values.next().ok_or(NativeLogProblem::NativeShape)?;
            let fields = if field.has_tag_name("data") { &mut data } else { &mut actions };
            if values.next().is_some() || fields.insert(name, value).is_some() {
                return Err(NativeLogProblem::DuplicateField);
            }
        }
        Ok(Self { data, actions })
    }
    fn data_node(&self, name: &str) -> Option<Node<'a, 'input>> {
        self.data.get(name).copied()
    }
    fn data(&self, name: &str) -> Parsed<Option<&'a str>> {
        Self::scalar(self.data_node(name))
    }
    fn scalar(node: Option<Node<'a, 'input>>) -> Parsed<Option<&'a str>> {
        node.map(|node| {
            if node.children().any(|child| child.is_element()) {
                return Err(NativeLogProblem::NativeShape);
            }
            Ok(node.text().unwrap_or(""))
        })
        .transpose()
    }
    fn action(&self, name: &str, package: &str) -> Parsed<Option<&'a str>> {
        let node = self.actions.get(name).copied();
        if let Some(node) = node
            && node.parent().and_then(|field| field.attribute("package")) != Some(package)
        {
            return Err(NativeLogProblem::NativeShape);
        }
        Self::scalar(node)
    }
    fn required_number<T: std::str::FromStr + ToString>(&self, name: &str) -> Parsed<T> {
        let raw = self.data(name)?.ok_or(NativeLogProblem::NativeShape)?;
        raw.parse::<T>()
            .ok()
            .filter(|value| value.to_string() == raw)
            .ok_or_else(|| NativeLogProblem::InvalidField(name.into()))
    }
    fn boolean(&self, name: &str) -> Parsed<Option<bool>> {
        self.data(name)?
            .map(|value| match value {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),
                _ => Err(NativeLogProblem::InvalidField(name.into())),
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
