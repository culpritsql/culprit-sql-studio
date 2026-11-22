//! Parse saved SQL Server graphs and native event envelopes without I/O.
use super::invalid_data;
use culprit_core::deadlock::classify;
use culprit_core::{
    Deadlock, DeadlockProcess, DeadlockResource, Engine, Error, LockHolder, Result, t, telemetry::canonical_time,
};
use roxmltree::{Document, Node};

/// Every deadlock in `xml`: a saved .xdl (`<deadlock-list>` or `<deadlock>`), a system_health event, or a ring buffer.
pub fn parse_deadlock_xml(xml: &str, source: &str) -> Result<Vec<Deadlock>> {
    parse_with_time(xml, source, None)
}

pub fn parse_with_time(xml: &str, source: &str, native_at: Option<&str>) -> Result<Vec<Deadlock>> {
    // The XEL reader supplies its documented UTC clock. Imported XML needs an explicit offset of its own.
    let native_at = native_at.map(canonical_time).transpose()?;
    culprit_core::xml::read(xml.trim_start_matches('\u{feff}'), |doc| {
        let doc = doc.map_err(|e| Error::invalid(t!("provider.mssql.deadlocks.unreadable", error = e.to_string())))?;
        deadlock_graphs(&doc, source, native_at.as_deref())
    })
}

fn deadlock_graphs(doc: &Document, source: &str, native_at: Option<&str>) -> Result<Vec<Deadlock>> {
    require_declared_graphs(doc)?;
    let mut out = Vec::new();
    for node in doc.descendants().filter(|n| n.has_tag_name("deadlock")) {
        // Ring buffers hold every kind of event; only deadlock reports carry a <deadlock>.
        let at = node
            .ancestors()
            .find(|a| a.has_tag_name("event"))
            .and_then(|e| e.attribute("timestamp"))
            .map(canonical_time)
            .transpose()?;
        if at.as_deref().zip(native_at).is_some_and(|(event, native)| event != native) {
            return Err(invalid_data());
        }
        let at = at.or_else(|| native_at.map(str::to_owned));
        out.push(parse_deadlock_node(node, source, at).ok_or_else(invalid_data)?);
    }
    if out.is_empty() && !doc.descendants().any(|n| n.has_tag_name("RingBufferTarget") || n.has_tag_name("event")) {
        return Err(Error::invalid(t!("provider.mssql.deadlocks.no_graph"))
            .with_hint(t!("provider.mssql.deadlocks.no_graph.hint")));
    }
    Ok(out)
}

fn require_declared_graphs(doc: &Document) -> Result<()> {
    for event in doc.descendants().filter(|node| {
        node.has_tag_name("event")
            && matches!(node.attribute("name"), Some("xml_deadlock_report" | "database_xml_deadlock_report"))
    }) {
        let has_graph = event.descendants().any(|node| {
            node.has_tag_name("deadlock")
                && node.ancestors().find(|ancestor| ancestor.has_tag_name("event")) == Some(event)
        });
        if !has_graph {
            return Err(invalid_data());
        }
    }
    Ok(())
}

fn child<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Option<Node<'a, 'i>> {
    node.children().find(|c| c.has_tag_name(name))
}

fn text_of(node: Node) -> Option<String> {
    let text: String = node.descendants().filter(|n| n.is_text()).filter_map(|n| n.text()).collect();
    let text = text.trim();
    (!text.is_empty() && !text.eq_ignore_ascii_case("unknown")).then(|| clip(text))
}

fn clip(text: &str) -> String {
    const MAX: usize = 4000;
    if text.chars().count() <= MAX {
        text.to_string()
    } else {
        format!("{}…", text.chars().take(MAX).collect::<String>())
    }
}

/// `db.schema.table` → `schema.table`.
fn object_of(name: &str) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() >= 2 { parts[parts.len() - 2..].join(".") } else { name.to_string() }
}

fn attr(node: Node, name: &str) -> Option<String> {
    node.attribute(name).map(str::trim).filter(|v| !v.is_empty()).map(str::to_string)
}

/// Decode one supplied graph. Native adapters validate their event clock and position before calling this parser.
pub fn parse_deadlock_node(node: Node, source: &str, at: Option<String>) -> Option<Deadlock> {
    let mut victims: Vec<String> = child(node, "victim-list")
        .map(|v| v.children().filter(|c| c.has_tag_name("victimProcess")).filter_map(|c| attr(c, "id")).collect())
        .unwrap_or_default();
    if let Some(v) = attr(node, "victim") {
        victims.push(v);
    }
    let processes: Vec<DeadlockProcess> = child(node, "process-list")?
        .children()
        .filter(|c| c.has_tag_name("process"))
        .map(|p| {
            let id = attr(p, "id").unwrap_or_default();
            let frame = child(p, "executionStack").and_then(|s| {
                s.children().filter(|f| f.has_tag_name("frame")).find_map(|f| text_of(f).map(|t| (f, t)))
            });
            let procedure = frame
                .as_ref()
                .and_then(|(f, _)| attr(*f, "procname"))
                .filter(|n| !matches!(n.to_ascii_lowercase().as_str(), "adhoc" | "unknown"))
                .map(|n| object_of(&n));
            DeadlockProcess {
                victim: victims.contains(&id),
                session: attr(p, "spid"),
                statement: frame.map(|(_, t)| t).or_else(|| child(p, "inputbuf").and_then(text_of)),
                procedure,
                login: attr(p, "loginname"),
                host: attr(p, "hostname"),
                application: attr(p, "clientapp"),
                database: attr(p, "currentdbname"),
                isolation: attr(p, "isolationlevel").map(|i| i.split(" (").next().unwrap_or(&i).to_string()),
                wait_mode: attr(p, "lockMode"),
                log_used: attr(p, "logused").and_then(|v| v.parse().ok()),
                id,
            }
        })
        .collect();
    if processes.is_empty() {
        return None;
    }
    let resources: Vec<DeadlockResource> = child(node, "resource-list")
        .map(|list| {
            list.children()
                .filter(|c| c.is_element())
                .enumerate()
                .map(|(i, r)| {
                    let holders = |list: &str, item: &str| -> Vec<LockHolder> {
                        child(r, list)
                            .map(|l| {
                                l.children()
                                    .filter(|c| c.has_tag_name(item))
                                    .filter_map(|c| Some(LockHolder { process: attr(c, "id")?, mode: attr(c, "mode") }))
                                    .collect()
                            })
                            .unwrap_or_default()
                    };
                    DeadlockResource {
                        id: attr(r, "id").unwrap_or_else(|| format!("{}{i}", r.tag_name().name())),
                        kind: r.tag_name().name().to_string(),
                        object: attr(r, "objectname").map(|o| object_of(&o)),
                        index: attr(r, "indexname"),
                        holders: holders("owner-list", "owner"),
                        waiters: holders("waiter-list", "waiter"),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let mut d = Deadlock::new(source, at, processes, resources);
    classify(Engine::Mssql, &mut d);
    Some(d)
}

#[cfg(test)]
mod tests;
