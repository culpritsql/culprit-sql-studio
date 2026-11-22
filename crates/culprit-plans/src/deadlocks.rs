//! Deadlocks pasted or dropped in: SQL Server XML, PostgreSQL log text and InnoDB status.
//! Imported InnoDB clocks remain unknown without an independently supplied UTC witness.

use culprit_core::{Deadlock, Error, Result, t};

/// Largest pasted text Culprit reads.
const MAX_TEXT: usize = 8 * 1024 * 1024;

/// Every deadlock in `text`, whatever engine wrote it.
pub fn parse_deadlocks(text: &str) -> Result<Vec<Deadlock>> {
    if text.len() > MAX_TEXT {
        return Err(Error::invalid(t!("engine.deadlocks.too_large")));
    }
    let trimmed = text.trim_start_matches('\u{feff}').trim();
    let found = if trimmed.starts_with('<') || trimmed.contains("<deadlock") {
        sql_server(trimmed)?
    } else if trimmed.contains("LATEST DETECTED DEADLOCK") || trimmed.contains("*** (1) TRANSACTION") {
        innodb(trimmed)
    } else if trimmed.contains("deadlock detected") {
        postgres(trimmed)
    } else {
        Vec::new()
    };
    if found.is_empty() {
        return Err(Error::invalid(t!("engine.deadlocks.none_found")).with_hint(t!("engine.deadlocks.none_found_hint")));
    }
    Ok(found
        .into_iter()
        .map(|mut d| {
            d.source = "pasted".into();
            d
        })
        .collect())
}

fn sql_server(text: &str) -> Result<Vec<Deadlock>> {
    crate::mssql::deadlocks::parse_deadlock_xml(text, "pasted")
}

fn innodb(text: &str) -> Vec<Deadlock> {
    // A pasted report does not establish the server time zone.
    if text.contains("LATEST DETECTED DEADLOCK") {
        crate::mysql::deadlocks::parse_innodb_status(text, false)
    } else {
        crate::mysql::deadlocks::parse_innodb_status(&format!("LATEST DETECTED DEADLOCK\n{text}"), false)
    }
}

fn postgres(text: &str) -> Vec<Deadlock> {
    crate::postgres::deadlocks::parse_deadlock_log(text)
}

#[cfg(test)]
mod tests;
