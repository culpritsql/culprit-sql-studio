//! Supplied PostgreSQL log/DETAIL text only. SQLSTATE, acquisition, timezone and native scope stay caller-owned.
use culprit_core::deadlock::classify;
use culprit_core::telemetry::{
    MAX_POSTGRES_DEADLOCK_EDGES, postgres_deadlock_cycle_is_valid, postgres_log_lock_mode_is_valid,
};
use culprit_core::{Deadlock, DeadlockProcess, DeadlockResource, Engine, LockHolder};

/// Deadlocks in pasted Postgres server log text (`ERROR: deadlock detected` with its DETAIL and CONTEXT lines).
pub fn parse_deadlock_log(text: &str) -> Vec<Deadlock> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    for (start, line) in lines.iter().enumerate() {
        if !line.contains("deadlock detected") {
            continue;
        }
        let block: Vec<&str> = lines[start + 1..]
            .iter()
            .take_while(|line| !line.contains("deadlock detected"))
            .take(40)
            .copied()
            .collect();
        let detail = block
            .iter()
            .take_while(|line| !pasted_section_start(line))
            .map(|line| line.split_once("DETAIL:").map_or(*line, |(_, detail)| detail))
            .collect::<Vec<_>>()
            .join("\n");
        let Some(mut deadlock) = detail_graph(&detail, log_time(line), "pasted log") else { continue };
        // Pasted text keeps its existing per-process statement presentation. Structured input instead supplies only
        // the reporting backend's separate native statement field: application SQL must not forge another process.
        for line in &block {
            attach_pasted_statement(&mut deadlock, line);
        }
        let reporter = bracketed_pid(line);
        let context = block.iter().find_map(|line| line.split_once("CONTEXT:").map(|(_, context)| context));
        qualify_deadlock(&mut deadlock, reporter.as_deref(), context);
        classify(Engine::Postgres, &mut deadlock);
        out.push(deadlock);
    }
    out
}

pub struct DeadlockLogContext<'a> {
    pub reporter: Option<&'a str>,
    pub statement: Option<&'a str>,
    pub context: Option<&'a str>,
    pub at: Option<String>,
}

/// SQLSTATE establishes a structured deadlock. The English DETAIL grammar can provide a graph, but a localized or
/// unsupported graph stays unavailable. No synthetic log header or application SQL is fed to this graph decoder.
pub fn parse_deadlock_details(detail: &str, context: DeadlockLogContext<'_>) -> Option<Deadlock> {
    let mut deadlock = detail_graph(detail, context.at, "postgres-structured-log")?;
    qualify_deadlock(&mut deadlock, context.reporter, context.context);
    if let (Some(statement), Some(victim)) =
        (context.statement, deadlock.processes.iter_mut().find(|process| process.victim))
    {
        victim.statement = Some(statement.to_owned());
    }
    classify(Engine::Postgres, &mut deadlock);
    Some(deadlock)
}

fn native_pid(value: &str) -> bool {
    value.parse::<i32>().is_ok_and(|pid| pid > 0)
}

/// PostgreSQL emits all wait edges before the explicit per-process query section. Arbitrary unsupported lines in
/// the graph cannot qualify a valid prefix; after the query boundary, multiline SQL is never parsed as another edge.
fn detail_graph(detail: &str, at: Option<String>, source: &str) -> Option<Deadlock> {
    let mut processes: Vec<DeadlockProcess> = Vec::new();
    let mut resources = Vec::new();
    for line in detail.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Some(pid) = detail_query_pid(line) {
            if !processes.iter().any(|process| process.id == pid) {
                return None;
            }
            break;
        }
        let (pid, mode, what, blocker) = detail_edge(line)?;
        if resources.len() == MAX_POSTGRES_DEADLOCK_EDGES {
            return None;
        }
        resources.push(DeadlockResource {
            id: what.into(),
            kind: what.split_whitespace().next()?.into(),
            object: None,
            index: None,
            holders: vec![LockHolder { process: blocker.into(), mode: None }],
            waiters: vec![LockHolder { process: pid.into(), mode: Some(mode.into()) }],
        });
        if !processes.iter().any(|process| process.id == pid) {
            processes.push(DeadlockProcess {
                id: pid.into(),
                session: Some(pid.into()),
                wait_mode: Some(mode.into()),
                ..Default::default()
            });
        }
    }
    if !postgres_deadlock_cycle_is_valid(resources.iter().flat_map(|resource| {
        resource
            .waiters
            .iter()
            .zip(&resource.holders)
            .map(|(waiter, holder)| (waiter.process.as_str(), holder.process.as_str()))
    })) {
        None
    } else {
        Some(Deadlock::new(source, at, processes, resources))
    }
}

fn detail_query_pid(line: &str) -> Option<&str> {
    let (pid, _) = line.strip_prefix("Process ")?.split_once(':')?;
    native_pid(pid).then_some(pid)
}

/// Pasted stderr records carry their other native sections outside DETAIL. Keep those headers out of the shared
/// native DETAIL decoder; an unknown line inside structured DETAIL must not be guessed into a section boundary.
fn pasted_section_start(line: &str) -> bool {
    let content = line.split_once("] ").map_or(line, |(_, content)| content).trim_start();
    ["HINT:", "CONTEXT:", "STATEMENT:", "ERROR:", "FATAL:", "PANIC:", "LOG:", "NOTICE:", "WARNING:", "INFO:"]
        .iter()
        .any(|prefix| content.starts_with(prefix))
}

fn detail_edge(line: &str) -> Option<(&str, &str, &str, &str)> {
    let (pid, rest) = line.strip_prefix("Process ")?.split_once(" waits for ")?;
    let (mode, rest) = rest.split_once(" on ")?;
    let (what, blocker) = rest.split_once("; blocked by process ")?;
    let blocker = blocker.strip_suffix('.')?;
    (native_pid(pid) && native_pid(blocker) && postgres_log_lock_mode_is_valid(mode) && !what.is_empty())
        .then_some((pid, mode, what, blocker))
}

fn attach_pasted_statement(deadlock: &mut Deadlock, line: &str) {
    let line = line.split_once("DETAIL:").map_or(line, |(_, detail)| detail).trim();
    if let Some((pid, statement)) = line.strip_prefix("Process ").and_then(|line| line.split_once(": "))
        && native_pid(pid)
        && let Some(process) = deadlock.processes.iter_mut().find(|process| process.id == pid)
    {
        process.statement = Some(statement.trim().into());
    }
}

fn qualify_deadlock(deadlock: &mut Deadlock, reporter: Option<&str>, context: Option<&str>) {
    for process in &mut deadlock.processes {
        process.victim = reporter == Some(process.id.as_str());
    }
    if let (Some(relation), Some(victim)) = (context.and_then(context_relation), reporter) {
        for resource in deadlock
            .resources
            .iter_mut()
            .filter(|resource| resource.waiters.iter().any(|waiter| waiter.process == victim))
        {
            resource.object = Some(relation.clone());
        }
    }
}

fn context_relation(context: &str) -> Option<String> {
    let context = context.trim();
    if !["while updating tuple ", "while deleting tuple ", "while locking tuple "]
        .iter()
        .any(|prefix| context.starts_with(prefix))
    {
        return None;
    }
    context.split_once(" in relation \"")?.1.strip_suffix('"').map(str::to_owned)
}

fn bracketed_pid(line: &str) -> Option<String> {
    let inside = line.split_once('[')?.1.split_once(']')?.0;
    inside.chars().all(|c| c.is_ascii_digit()).then(|| inside.to_string())
}

/// `2026-10-11 13:48:28.887 UTC [539] ERROR: …` → `2026-10-11T13:48:28Z` (only when the log says UTC).
fn log_time(line: &str) -> Option<String> {
    let mut parts = line.split_whitespace();
    let (date, time, zone) = (parts.next()?, parts.next()?, parts.next()?);
    let looks_like_date = date.len() == 10 && date.chars().filter(|c| *c == '-').count() == 2;
    (looks_like_date && matches!(zone, "UTC" | "GMT"))
        .then(|| format!("{date}T{}Z", time.split('.').next().unwrap_or(time)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use culprit_core::DeadlockPattern;

    pub(crate) const LOG: &str = "2026-10-11 13:48:28.887 UTC [539] ERROR:  deadlock detected
2026-10-11 13:48:28.887 UTC [539] DETAIL:  Process 539 waits for ShareLock on transaction 838; blocked by process 545.
\tProcess 545 waits for ShareLock on transaction 837; blocked by process 539.
\tProcess 539: UPDATE customers SET id = id WHERE id = 1
\tProcess 545: UPDATE orders SET id = id WHERE id = 1
2026-10-11 13:48:28.887 UTC [539] HINT:  See server log for query details.
2026-10-11 13:48:28.887 UTC [539] CONTEXT:  while updating tuple (0,1) in relation \"customers\"
2026-10-11 13:48:28.887 UTC [539] STATEMENT:  UPDATE customers SET id = id WHERE id = 1";

    #[test]
    fn a_pasted_log_deadlock_is_explained() {
        let found = parse_deadlock_log(LOG);
        assert_eq!(found.len(), 1);
        let d = &found[0];
        assert_eq!(d.at.as_deref(), Some("2026-10-11T13:48:28Z"));
        assert_eq!(d.processes.len(), 2);
        assert_eq!(d.victim().unwrap().id, "539");
        assert_eq!(d.processes[1].statement.as_deref(), Some("UPDATE orders SET id = id WHERE id = 1"));
        assert_eq!(d.pattern, DeadlockPattern::OppositeOrder);
        assert_eq!(d.objects, vec!["customers".to_string()]);
        assert!(d.fixes.iter().any(|f| f.detail.contains("40P01")));
        assert!(parse_deadlock_log("nothing here").is_empty());
    }

    #[test]
    fn structured_fields_do_not_turn_the_reporting_statement_into_another_process_edge() {
        let detail = "Process 539 waits for ShareLock on transaction 838; blocked by process 545.\nProcess 545 waits for ShareLock on transaction 837; blocked by process 539.";
        let statement = "Process 999 waits for ShareLock on transaction 1; blocked by process 539.";
        let graph = parse_deadlock_details(
            detail,
            DeadlockLogContext { reporter: Some("539"), statement: Some(statement), context: None, at: None },
        )
        .unwrap();
        assert_eq!(graph.processes.len(), 2);
        assert_eq!(graph.victim().unwrap().id, "539");
        assert_eq!(graph.victim().unwrap().statement.as_deref(), Some(statement));
        assert!(graph.processes.iter().all(|process| process.id != "999"));
        assert!(graph.at.is_none());
    }

    #[test]
    fn a_partial_cycle_or_unknown_detail_line_cannot_qualify_a_valid_prefix() {
        let context = || DeadlockLogContext { reporter: Some("539"), statement: None, context: None, at: None };
        let edge = "Process 539 waits for ShareLock on transaction 838; blocked by process 545.";
        assert!(parse_deadlock_details(edge, context()).is_none());
        let cycle = format!("{edge}\nProcess 545 waits for ShareLock on transaction 837; blocked by process 539.");
        assert!(parse_deadlock_details(&cycle, context()).is_some());
        assert!(parse_deadlock_details(&format!("{cycle}\nunsupported native detail"), context()).is_none());
    }
}
