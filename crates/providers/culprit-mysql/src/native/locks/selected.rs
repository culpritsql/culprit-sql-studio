//! One selected projection over the existing lock layouts/decoder. No sys TEMPTABLE or formatted-query work.
use super::*;
use crate::{
    native::{self, metadata::Metadata},
    read::QueryRead,
};
use culprit_core::telemetry::*;
use mysql_async::Value;
#[derive(Clone, Copy)]
pub(crate) enum Family {
    Data,
    Metadata,
}
#[derive(Clone)]
pub(crate) struct Edge {
    pub pair: LockPair,
    pub waiter_thread: u64,
    pub owner_thread: u64,
    waiter_transaction: Option<String>,
    owner_transaction: Option<String>,
}
impl Edge {
    pub fn same_point(&self, other: &Self) -> bool {
        self.pair.session == other.pair.session
            && self.pair.blocking == other.pair.blocking
            && self.waiter_thread == other.waiter_thread
            && self.owner_thread == other.owner_thread
            && self.waiter_transaction == other.waiter_transaction
            && self.owner_transaction == other.owner_transaction
            && self.pair.identified_resource == other.pair.identified_resource
            && self.pair.mode == other.pair.mode
            && self.pair.object == other.pair.object
            && self.pair.index == other.pair.index
            && self.pair.resource == other.pair.resource
    }
}
pub(crate) struct Edges {
    pub edges: Vec<Edge>,
    pub limited: bool,
}
pub(crate) async fn instrumented<R: QueryRead>(reader: &mut R) -> Result<bool> {
    let row = native::one(
        reader,
        "SELECT ENABLED AS enabled FROM performance_schema.setup_instruments WHERE NAME='wait/lock/metadata/sql/mdl'",
        vec![],
    )
    .await?;
    match native::text(&row, "enabled")?.as_str() {
        "YES" => Ok(true),
        "NO" => Ok(false),
        _ => Err(native::invalid()),
    }
}
pub(crate) async fn read<R: QueryRead>(
    reader: &mut R,
    meta: &Metadata,
    family: Family,
    waiter: u64,
    waiter_thread: u64,
    limit: usize,
) -> Result<Edges> {
    let sql = query(meta, family, limit + 1)?;
    let selector = match family {
        Family::Data if meta.connection.flavor == Flavor::Mariadb => waiter,
        Family::Data | Family::Metadata => waiter_thread,
    };
    let rows = reader.read(&sql, vec![Value::UInt(selector)], limit + 1).await?;
    if rows.len() > limit + 1 {
        return Err(native::invalid());
    }
    let limited = rows.len() > limit;
    let edges = rows
        .iter()
        .take(limit)
        .map(|row| {
            let pair = super::decode_row(row)?;
            let waiter_thread = native::integer(row, "requesting_native_thread")?;
            let owner_thread = native::integer(row, "blocking_native_thread")?;
            if pair.session == 0
                || pair.blocking == 0
                || pair.session > u64::from(u32::MAX)
                || pair.blocking > u64::from(u32::MAX)
                || waiter_thread == 0
                || owner_thread == 0
            {
                return Err(native::invalid());
            }
            let transaction = |name| row.unsigned(name).map(|value| value.map(|value| value.to_string()));
            let (waiter_transaction, owner_transaction) = match family {
                Family::Metadata => (None, None),
                Family::Data => (transaction("requesting_transaction_id")?, transaction("blocking_transaction_id")?),
            };
            if matches!(family, Family::Data)
                && meta.connection.flavor == Flavor::Mariadb
                && owner_transaction.as_deref().is_none_or(|id| id == "0")
            {
                return Err(native::invalid());
            }
            Ok(Edge { pair, waiter_thread, owner_thread, waiter_transaction, owner_transaction })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Edges { edges, limited })
}
fn query(meta: &Metadata, family: Family, lookahead: usize) -> Result<String> {
    if lookahead == 0 || lookahead > MAX_BLOCKING_PARTICIPANTS as usize + 1 {
        return Err(native::invalid());
    }
    match family {
        Family::Metadata=>Ok(format!("SELECT wt.PROCESSLIST_ID AS session,bt.PROCESSLIST_ID AS blocking,p.LOCK_TYPE AS mode,'METADATA' AS resource,CONCAT(p.OBJECT_SCHEMA,'.',p.OBJECT_NAME) AS object,NULL AS index_name,NULL AS native_lock_id,NULL AS native_engine,p.OWNER_THREAD_ID AS requesting_native_thread,g.OWNER_THREAD_ID AS blocking_native_thread
 FROM performance_schema.metadata_locks p JOIN performance_schema.metadata_locks g ON g.OBJECT_TYPE=p.OBJECT_TYPE AND g.OBJECT_SCHEMA=p.OBJECT_SCHEMA AND g.OBJECT_NAME=p.OBJECT_NAME AND g.LOCK_STATUS='GRANTED'
 JOIN performance_schema.threads wt ON wt.THREAD_ID=p.OWNER_THREAD_ID JOIN performance_schema.threads bt ON bt.THREAD_ID=g.OWNER_THREAD_ID
 WHERE p.OWNER_THREAD_ID=? AND p.OBJECT_TYPE='TABLE' AND p.LOCK_STATUS='PENDING' AND p.OWNER_THREAD_ID<>g.OWNER_THREAD_ID
 AND (p.LOCK_TYPE='EXCLUSIVE' OR g.LOCK_TYPE='EXCLUSIVE') AND wt.PROCESSLIST_ID>0 AND bt.PROCESSLIST_ID>0 LIMIT {lookahead}")),
        Family::Data=>{
            let layout=row_waits(meta.connection.flavor,&meta.connection.version);
            let (columns,joins,selector)=match meta.connection.flavor {
                Flavor::Mysql=>("w.REQUESTING_THREAD_ID AS requesting_native_thread,w.BLOCKING_THREAD_ID AS blocking_native_thread,w.REQUESTING_ENGINE_TRANSACTION_ID AS requesting_transaction_id,w.BLOCKING_ENGINE_TRANSACTION_ID AS blocking_transaction_id,","","w.REQUESTING_THREAD_ID"),
                Flavor::Mariadb=>("wt.THREAD_ID AS requesting_native_thread,bt.THREAD_ID AS blocking_native_thread,r.trx_id AS requesting_transaction_id,b.trx_id AS blocking_transaction_id,"," JOIN performance_schema.threads wt ON wt.PROCESSLIST_ID=r.trx_mysql_thread_id JOIN performance_schema.threads bt ON bt.PROCESSLIST_ID=b.trx_mysql_thread_id","r.trx_mysql_thread_id"),
            };
            // The selected fields belong to the original native aliases. The shared FROM/join and decoder body
            // remain single-owned; ordinary Live SQL bytes and instrumentation-off behavior are unchanged.
            let projected=layout.sql.strip_prefix("SELECT ").ok_or_else(native::invalid)?;
            Ok(format!("SELECT {columns}{projected}{joins} WHERE {selector}=? LIMIT {lookahead}"))
        }
    }
}
