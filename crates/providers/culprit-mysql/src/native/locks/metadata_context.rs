//! Two explicitly paid selected TABLE-ticket endpoints; no native pending-blocker edge exists here.
use super::*;
use crate::native::{integer, invalid, text};
use crate::read::QueryRead;
use culprit_core::blocking::*;
use culprit_core::telemetry::*;
use mysql_async::Value;

pub(crate) struct Snapshot {
    pub at: String,
    pub limit: u32,
    pub rows: u32,
    pub tickets: Vec<MysqlMetadataLockTicket>,
}
impl Snapshot {
    pub fn complete(&self) -> bool {
        self.rows <= self.limit
    }
    pub fn participant_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.tickets
            .iter()
            .filter_map(|ticket| ticket.owner_process_id.as_deref()?.parse::<u64>().ok())
            .filter(|id| *id > 0 && *id <= u32::MAX as u64)
    }
}
pub(crate) async fn read<R: QueryRead>(reader: &mut R, database: &str, thread: u64, limit: u32) -> Result<Snapshot> {
    if limit == 0 || limit >= MAX_BLOCKING_PARTICIPANTS {
        return Err(invalid());
    }
    let rows = reader.read(&query(limit), vec![Value::UInt(thread), Value::from(database)], limit as usize + 2).await?;
    let mut at = None;
    let mut tickets = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut count = 0;
    for row in &rows {
        match text(row, "kind")?.as_str() {
            "guard" => {
                let stamp = canonical_time(&text(row, "observed_at")?)?;
                if at.replace(stamp).is_some() {
                    return Err(invalid());
                }
            }
            "ticket" => {
                count += 1;
                let optional = |name| row.unsigned(name).map(|value| value.map(|value| value.to_string()));
                let transaction = MysqlMetadataTransactionEvent {
                    event_id: optional("transaction_event_id")?,
                    timer_start: optional("transaction_timer_start")?,
                    state: row.native_text("transaction_state")?,
                    end_event_id: optional("transaction_end_event_id")?,
                };
                let has_transaction = transaction.event_id.is_some()
                    || transaction.timer_start.is_some()
                    || transaction.state.is_some()
                    || transaction.end_event_id.is_some();
                let ticket = MysqlMetadataLockTicket {
                    address: integer(row, "ticket_address")?.to_string(),
                    resource: MysqlMetadataLockResource {
                        object_type: text(row, "object_type")?,
                        schema: text(row, "object_schema")?,
                        name: text(row, "object_name")?,
                    },
                    mode: text(row, "lock_type")?,
                    duration: text(row, "lock_duration")?,
                    status: text(row, "lock_status")?,
                    owner_thread_id: optional("owner_thread_id")?,
                    owner_process_id: optional("owner_process_id")?,
                    owner_event_id: optional("owner_event_id")?,
                    actor: None,
                    transaction: has_transaction.then_some(transaction),
                    matched: false,
                    settled: false,
                };
                if !ticket.is_valid() || !seen.insert(ticket.address.clone()) {
                    return Err(invalid());
                }
                if count <= limit {
                    tickets.push(ticket);
                }
            }
            _ => return Err(invalid()),
        }
    }
    if count > limit + 1 || rows.len() != count as usize + 1 {
        return Err(invalid());
    }
    Ok(Snapshot { at: at.ok_or_else(invalid)?, limit, rows: count, tickets })
}
pub(crate) fn same_ticket(left: &MysqlMetadataLockTicket, right: &MysqlMetadataLockTicket) -> bool {
    left.address == right.address
        && left.resource == right.resource
        && left.mode == right.mode
        && left.duration == right.duration
        && left.status == right.status
        && left.owner_thread_id == right.owner_thread_id
        && left.owner_event_id == right.owner_event_id
        && left.owner_process_id == right.owner_process_id
        && left.transaction == right.transaction
}
pub(crate) fn query(limit: u32) -> String {
    // A filter/LIMIT bounds selected/retained output, not all native Performance Schema manager work.
    // Both supported flavors expose this native layout. No sys TEMPTABLE or per-actor metadata scan.
    format!(
        r#"WITH roots AS (
 SELECT p.OBJECT_TYPE,p.OBJECT_SCHEMA,p.OBJECT_NAME FROM performance_schema.metadata_locks p
 WHERE p.OWNER_THREAD_ID=? AND p.OBJECT_TYPE='TABLE' AND p.OBJECT_SCHEMA=? AND p.LOCK_STATUS='PENDING'
), tickets AS (
 SELECT m.OBJECT_INSTANCE_BEGIN AS ticket_address,m.OBJECT_TYPE AS object_type,m.OBJECT_SCHEMA AS object_schema,
 m.OBJECT_NAME AS object_name,m.LOCK_TYPE AS lock_type,m.LOCK_DURATION AS lock_duration,m.LOCK_STATUS AS lock_status,
 m.OWNER_THREAD_ID AS owner_thread_id,m.OWNER_EVENT_ID AS owner_event_id,t.PROCESSLIST_ID AS owner_process_id,
 tx.EVENT_ID AS transaction_event_id,tx.TIMER_START AS transaction_timer_start,tx.END_EVENT_ID AS transaction_end_event_id,tx.STATE AS transaction_state
 FROM performance_schema.metadata_locks m LEFT JOIN performance_schema.threads t ON t.THREAD_ID=m.OWNER_THREAD_ID
 LEFT JOIN performance_schema.events_transactions_current tx ON tx.THREAD_ID=m.OWNER_THREAD_ID
 WHERE EXISTS(SELECT 1 FROM roots p WHERE m.OBJECT_TYPE=p.OBJECT_TYPE AND m.OBJECT_SCHEMA=p.OBJECT_SCHEMA AND m.OBJECT_NAME=p.OBJECT_NAME)
 ORDER BY m.OBJECT_INSTANCE_BEGIN LIMIT {lookahead}
)
SELECT 'guard' AS kind,DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%dT%H:%i:%s.%fZ') AS observed_at,
 NULL AS ticket_address,NULL AS object_type,NULL AS object_schema,NULL AS object_name,NULL AS lock_type,NULL AS lock_duration,NULL AS lock_status,
 NULL AS owner_thread_id,NULL AS owner_event_id,NULL AS owner_process_id,NULL AS transaction_event_id,NULL AS transaction_timer_start,NULL AS transaction_end_event_id,NULL AS transaction_state
UNION ALL SELECT 'ticket',NULL,ticket_address,object_type,object_schema,object_name,lock_type,lock_duration,lock_status,
 owner_thread_id,owner_event_id,owner_process_id,transaction_event_id,transaction_timer_start,transaction_end_event_id,transaction_state FROM tickets"#,
        lookahead = limit + 1
    )
}
#[cfg(test)]
pub(crate) mod tests;
