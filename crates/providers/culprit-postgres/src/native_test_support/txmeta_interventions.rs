//! Owned PostgreSQL 18 intervention fixture for Subtransaction / MultiXact metadata contention:
//! 1. Prepared transaction (`PREPARE TRANSACTION`) holding the oldest `CleanupHorizon::Data` XID alongside a
//!    logical slot (`catalog_xmin`), a newer `REPEATABLE READ` `backend_xmin` session, and a 66-savepoint
//!    overflowing backend (`subxact_overflowed = true`).
//! 2. `COMMIT PREPARED` intervention shifting the oldest `Data` horizon to the `REPEATABLE READ` session.
//! 3. `ROLLBACK` of the old snapshot + overflow backend, followed by real concurrent `FOR KEY SHARE` + `FOR SHARE`
//!    row-lock MultiXact creation advancing cluster `pg_stat_slru` (`multixact_offset`, `multixact_member`).
use super::{actor::Actor, case};
use crate::{connect::Connector, rows::quote_ident};
use culprit_core::{ConnectionSpec, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use std::time::Duration;

const DATABASE: &str = "culprit_smoke";
const PHASE_TIMEOUT: Duration = Duration::from_secs(10);
const WRITING_SAVEPOINTS: u32 = 66;
pub const MULTIXACT_ROWS: i32 = 32;
pub const TXMETA_RETAINER_APP: &str = "culprit-txmeta-retainer";
pub const TXMETA_OVERFLOW_APP: &str = "culprit-txmeta-overflow";
pub const TXMETA_FOCUS_APP: &str = "culprit-txmeta-focus";

pub struct TransactionMetadataInterventionFixture {
    pub specification: ConnectionSpec,
    pub cancellation: CollectionCancellation,
    pub focus_identity: NativeActivityIdentity,
    pub retainer_identity: NativeActivityIdentity,
    pub overflow_identity: NativeActivityIdentity,
    connector: Connector,
    marker: String,
    schema: String,
    prepared_gid: String,
    slot_name: String,
    prepared_active: bool,
    slot_active: bool,
    adv_key_1: i64,
    adv_key_2: i64,
    admin: Actor,
    gate_holder: Option<Actor>,
    focus: Option<Actor>,
    retainer: Option<Actor>,
    overflow: Option<Actor>,
    mx_key_share: Option<Actor>,
    mx_share: Option<Actor>,
}

impl TransactionMetadataInterventionFixture {
    pub async fn open() -> Self {
        let specification = Box::pin(super::specification(DATABASE, CollectionBudget::default())).await;
        let owner = super::owner();
        let marker = format!("culprit_owned_{owner}");
        let connector = Connector::new(&specification).unwrap();
        let cancellation = CollectionCancellation::default();
        let mut admin =
            Box::pin(Actor::open(&connector, DATABASE, &marker, "fixture-txmeta-admin", &cancellation)).await.unwrap();
        let cfg = admin
            .one(
                "SELECT pg_catalog.current_setting('server_version_num')::integer AS version, \
                 pg_catalog.current_setting('max_prepared_transactions')::integer AS prepared, \
                 pg_catalog.current_setting('max_replication_slots')::integer AS slots, \
                 pg_catalog.current_setting('wal_level') AS wal_level",
                &[],
            )
            .await
            .unwrap();
        assert!(cfg.get::<_, i32>("version") >= 160_000, "PG16+ required for pg_stat_get_backend_subxact");
        assert!(cfg.get::<_, i32>("prepared") >= 4, "disposable PG fixture must enable max_prepared_transactions");
        assert!(cfg.get::<_, i32>("slots") >= 2, "disposable PG fixture must enable replication slots");
        assert_eq!(cfg.get::<_, String>("wal_level"), "logical", "disposable PG fixture must enable logical slots");

        let schema = format!("culprit_txmeta_{owner}");
        let prepared_gid = format!("culprit_txmeta_gid_{owner}");
        let slot_name = format!("culprit_txmeta_slot_{owner}");
        let adv_key_1 = i64::from_str_radix(&owner[..12], 16).unwrap();
        let adv_key_2 = adv_key_1.wrapping_add(1);

        admin
            .execute(&format!(
                "CREATE SCHEMA {s}; \
                 CREATE TABLE {s}.items(id integer PRIMARY KEY, actor integer NOT NULL, child integer NOT NULL); \
                 INSERT INTO {s}.items SELECT g, 0, 0 FROM pg_catalog.generate_series(1, {MULTIXACT_ROWS}) AS g",
                s = quote_ident(&schema)
            ))
            .await
            .unwrap();

        Self {
            specification,
            cancellation,
            focus_identity: NativeActivityIdentity { id: "unstarted".into(), generation: "unstarted".into() },
            retainer_identity: NativeActivityIdentity { id: "unstarted".into(), generation: "unstarted".into() },
            overflow_identity: NativeActivityIdentity { id: "unstarted".into(), generation: "unstarted".into() },
            connector,
            marker,
            schema,
            prepared_gid,
            slot_name,
            prepared_active: false,
            slot_active: false,
            adv_key_1,
            adv_key_2,
            admin,
            gate_holder: None,
            focus: None,
            retainer: None,
            overflow: None,
            mx_key_share: None,
            mx_share: None,
        }
    }

    /// Arm 1: Logical slot holds an even older `catalog_xmin` (`Catalog` horizon only), prepared transaction
    /// holds `X_1` (`CleanupHorizon::Data`), `retainer` holds younger `backend_xid = X_2 > X_1` (`backend_xmin IS NULL`),
    /// `overflow` holds 66 writing child savepoints (`backend_xid = X_3 > X_2`, `subxact_overflowed = true`),
    /// and `focus` runs an active statement in the database.
    pub async fn start_competing_retainers_and_overflow(&mut self) {
        case::require_active(&self.cancellation).unwrap();
        let table = format!("{}.items", quote_ident(&self.schema));

        // 0. Hold two session advisory locks on `gate_holder` (zero XID, zero snapshot) so `overflow` can step
        //    from statement 1 (`adv_key_1`) to statement 2 (`adv_key_2`) after `COMMIT PREPARED` without aborting
        //    its 66-savepoint transaction.
        let mut gate_holder =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, "fixture-txmeta-gate", &self.cancellation))
                .await
                .unwrap();
        gate_holder
            .execute(&format!(
                "SELECT pg_catalog.pg_advisory_lock({}), pg_catalog.pg_advisory_lock({})",
                self.adv_key_1, self.adv_key_2
            ))
            .await
            .unwrap();
        self.gate_holder = Some(gate_holder);

        // 1. Create logical replication slot FIRST (before PREPARE TRANSACTION, because SnapBuildFindSnapshot
        //    waits for open/prepared write XIDs to finish!). Its catalog_xmin is oldest overall, proving
        //    Catalog-only horizons never mask CleanupHorizon::Data retainers.
        let _ = self
            .admin
            .one(
                "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&self.slot_name],
            )
            .await
            .unwrap();
        self.slot_active = true;

        // 2. Oldest Data XID: prepared transaction (`X_1`).
        let mut prep =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, "fixture-txmeta-prep", &self.cancellation))
                .await
                .unwrap();
        prep.execute(&format!(
            "BEGIN; INSERT INTO {table}(id, actor, child) VALUES (1000, 9, 0); PREPARE TRANSACTION '{}'",
            self.prepared_gid
        ))
        .await
        .unwrap();
        self.prepared_active = true;
        assert!(prep.retire().await);

        // 3. Younger session retainer (`X_2 > X_1`) holding `backend_xid` in READ COMMITTED (`backend_xmin IS NULL`).
        let mut retainer =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, TXMETA_RETAINER_APP, &self.cancellation))
                .await
                .unwrap();
        retainer
            .execute(&format!(
                "SET idle_in_transaction_session_timeout='45s'; \
                 BEGIN; \
                 INSERT INTO {table}(id, actor, child) VALUES (1001, 8, 1)"
            ))
            .await
            .unwrap();
        self.retainer_identity = retainer.identity.clone();
        self.retainer = Some(retainer);

        // Advance XID once more before overflow starts.
        self.admin.execute(&format!("INSERT INTO {table}(id, actor, child) VALUES (1002, 9, 2)")).await.unwrap();

        // 4. Overflowing backend (`X_3 > X_2 > X_1`): execute 66 writing child savepoints first so the open transaction
        //    sets `subxact_overflowed = true`, then wait on `adv_key_1` followed by `adv_key_2`.
        let mut overflow =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, TXMETA_OVERFLOW_APP, &self.cancellation))
                .await
                .unwrap();
        self.overflow_identity = overflow.identity.clone();
        let mut overflow_sql = "BEGIN;SAVEPOINT retained_children".to_owned();
        for child in 0..WRITING_SAVEPOINTS {
            let row_id = 2_000 + child as i32;
            overflow_sql.push_str(&format!(
                ";SAVEPOINT child_{child};INSERT INTO {table}(id,actor,child) VALUES({row_id},1,{child});RELEASE SAVEPOINT child_{child}"
            ));
        }
        overflow.execute(&overflow_sql).await.unwrap();
        overflow
            .start_workload(format!(
                "SELECT pg_catalog.pg_advisory_lock({}); SELECT pg_catalog.pg_advisory_lock({})",
                self.adv_key_1, self.adv_key_2
            ))
            .unwrap();
        self.overflow = Some(overflow);

        // 5. Focus backend: active statement in the database.
        self.restart_focus().await;
        self.wait_for_active_and_overflow(true).await;
    }

    async fn restart_focus(&mut self) {
        if let Some(mut old_focus) = self.focus.take() {
            old_focus.cancel_and_rollback().await.unwrap();
            assert!(old_focus.retire().await);
        }
        let table = format!("{}.items", quote_ident(&self.schema));
        let mut focus =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, TXMETA_FOCUS_APP, &self.cancellation))
                .await
                .unwrap();
        self.focus_identity = focus.identity.clone();
        focus.start_workload(format!("BEGIN;SELECT pg_catalog.pg_sleep(40) FROM {table} WHERE id = 1")).unwrap();
        self.focus = Some(focus);
        let focus_pid = self.focus_identity.id.parse::<i32>().unwrap();
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            let row = self
                .admin
                .one(
                    "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND state='active' AND query LIKE '%pg_sleep%')",
                    &[&focus_pid],
                )
                .await
                .unwrap();
            if row.get::<_, bool>(0) {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "focus actor did not enter active pg_sleep");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    async fn wait_for_active_and_overflow(&mut self, expect_prepared: bool) {
        let focus_pid = self.focus_identity.id.parse::<i32>().unwrap();
        let overflow_pid = self.overflow_identity.id.parse::<i32>().unwrap();
        let retainer_pid = self.retainer_identity.id.parse::<i32>().unwrap();
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            let row = self
                .admin
                .one(
                    "SELECT \
                     EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND state='active' AND query LIKE '%pg_sleep%'), \
                     EXISTS(SELECT 1 FROM pg_catalog.pg_stat_get_backend_idset() AS bid \
                            CROSS JOIN LATERAL pg_catalog.pg_stat_get_backend_subxact(bid) AS s \
                            WHERE pg_catalog.pg_stat_get_backend_pid(bid)=$2 AND s.subxact_overflowed IS TRUE) \
                     AND EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$2 AND state='active' AND wait_event_type='Lock'), \
                     EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$3 AND backend_xid IS NOT NULL AND backend_xmin IS NULL), \
                     EXISTS(SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE gid=$4)",
                    &[&focus_pid, &overflow_pid, &retainer_pid, &self.prepared_gid],
                )
                .await
                .unwrap();
            if row.get::<_, bool>(0)
                && row.get::<_, bool>(1)
                && row.get::<_, bool>(2)
                && row.get::<_, bool>(3) == expect_prepared
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "owned PG18 txmeta actors did not reach active/overflow state"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    /// Intervention A: commit the oldest prepared transaction (`X_1`) and step active statement snapshots past `X_1`
    /// so `retainer` (`X_2 < X_3`) becomes the oldest `CleanupHorizon::Data` holder while `overflow` preserves its
    /// open 66-savepoint transaction.
    pub async fn commit_prepared(&mut self) {
        if self.prepared_active {
            self.admin.execute(&format!("COMMIT PREPARED '{}'", self.prepared_gid)).await.unwrap();
            self.prepared_active = false;
        }
        if let Some(gate) = self.gate_holder.as_mut() {
            gate.execute(&format!("SELECT pg_catalog.pg_advisory_unlock({})", self.adv_key_1)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        self.restart_focus().await;
        self.wait_for_active_and_overflow(false).await;
    }

    /// Intervention A2: stop `overflow` (`X_3`), switch `retainer` to a read-only `REPEATABLE READ` snapshot
    /// (`backend_xmin IS NOT NULL`, `backend_xid IS NULL`), advance XID by 1, and refresh `focus` so `retainer`
    /// is the UNIQUE `CleanupHorizon::Data` holder in the entire database (`focus.backend_xmin > retainer.backend_xmin`).
    pub async fn switch_retainer_to_repeatable_read_xmin(&mut self) {
        if let Some(mut overflow) = self.overflow.take() {
            overflow.cancel_and_rollback().await.unwrap();
            assert!(overflow.retire().await);
        }
        let table = format!("{}.items", quote_ident(&self.schema));
        let retainer = self.retainer.as_mut().expect("retainer session must still be open");
        retainer
            .execute(
                "ROLLBACK; \
                 SET idle_in_transaction_session_timeout='45s'; \
                 BEGIN ISOLATION LEVEL REPEATABLE READ; \
                 SELECT pg_catalog.pg_current_snapshot()",
            )
            .await
            .unwrap();
        self.admin.execute(&format!("INSERT INTO {table}(id, actor, child) VALUES (1003, 9, 3)")).await.unwrap();
        self.restart_focus().await;
    }

    /// Intervention B: release the `REPEATABLE READ` retainer, stop the 66-savepoint overflow actor, and drop the slot.
    pub async fn clear_retainers_and_overflow(&mut self) {
        if let Some(mut retainer) = self.retainer.take() {
            retainer.execute("ROLLBACK").await.unwrap();
            assert!(retainer.retire().await);
        }
        if let Some(mut overflow) = self.overflow.take() {
            overflow.cancel_and_rollback().await.unwrap();
            assert!(overflow.retire().await);
        }
        if self.slot_active {
            let _ = self
                .admin
                .one("SELECT pg_catalog.pg_drop_replication_slot($1)::text", &[&self.slot_name])
                .await
                .unwrap();
            self.slot_active = false;
        }
    }

    /// Real concurrent `FOR KEY SHARE` + `FOR SHARE` row locking across two backends, allocating genuine MultiXact IDs
    /// and advancing `pg_stat_slru` (`multixact_offset`, `multixact_member`).
    pub async fn run_multixact_burst(&mut self) {
        let table = format!("{}.items", quote_ident(&self.schema));
        let mut mx_a =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, "culprit-txmeta-mx-a", &self.cancellation))
                .await
                .unwrap();
        let mut mx_b =
            Box::pin(Actor::open(&self.connector, DATABASE, &self.marker, "culprit-txmeta-mx-b", &self.cancellation))
                .await
                .unwrap();

        mx_a.execute(&format!(
            "BEGIN; SELECT sum(id) FROM (SELECT id FROM {table} WHERE id <= {MULTIXACT_ROWS} ORDER BY id FOR KEY SHARE) s"
        ))
        .await
        .unwrap();
        mx_b.execute(&format!(
            "BEGIN; SELECT sum(id) FROM (SELECT id FROM {table} WHERE id <= {MULTIXACT_ROWS} ORDER BY id FOR SHARE) s"
        ))
        .await
        .unwrap();

        // Force MultiXact member/offset lookups on those locked tuples from admin before releasing.
        let _ = self
            .admin
            .one(
                &format!("SELECT count(*) FROM (SELECT id FROM {table} WHERE id <= {MULTIXACT_ROWS} FOR KEY SHARE) s"),
                &[],
            )
            .await
            .unwrap();

        mx_b.execute("COMMIT").await.unwrap();
        mx_a.execute("COMMIT").await.unwrap();
        assert!(mx_b.retire().await);
        assert!(mx_a.retire().await);
        self.mx_key_share = None;
        self.mx_share = None;
    }

    /// Retire the focus actor so a follow-up `PostgresProvider::telemetry_source` observes an ended request (`records == 0`),
    /// then clean up all owned database state.
    pub async fn retire_focus_and_confirm_absent(&mut self) {
        let mut pids = Vec::new();
        if let Some(mut focus) = self.focus.take() {
            pids.push(focus.identity.id.parse::<i32>().unwrap());
            focus.cancel_and_rollback().await.unwrap();
            assert!(focus.retire().await);
        }
        if let Some(mut gate_holder) = self.gate_holder.take() {
            pids.push(gate_holder.identity.id.parse::<i32>().unwrap());
            let _ = gate_holder.execute("ROLLBACK").await;
            assert!(gate_holder.retire().await);
        }
        if !self.retainer_identity.id.starts_with("unstarted") {
            pids.push(self.retainer_identity.id.parse::<i32>().unwrap());
        }
        if !self.overflow_identity.id.starts_with("unstarted") {
            pids.push(self.overflow_identity.id.parse::<i32>().unwrap());
        }
        let deadline = tokio::time::Instant::now() + PHASE_TIMEOUT;
        loop {
            let row = self
                .admin
                .one("SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = ANY($1::integer[])", &[&pids])
                .await
                .unwrap();
            if row.get::<_, i64>(0) == 0 {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "retired PG18 txmeta actors remained in pg_stat_activity");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    pub async fn close(&mut self) {
        if let Some(mut focus) = self.focus.take() {
            let _ = focus.cancel_and_rollback().await;
            assert!(focus.retire().await);
        }
        if let Some(mut retainer) = self.retainer.take() {
            let _ = retainer.execute("ROLLBACK").await;
            assert!(retainer.retire().await);
        }
        if let Some(mut overflow) = self.overflow.take() {
            let _ = overflow.cancel_and_rollback().await;
            assert!(overflow.retire().await);
        }
        if let Some(mut gate_holder) = self.gate_holder.take() {
            let _ = gate_holder.execute("ROLLBACK").await;
            assert!(gate_holder.retire().await);
        }
        if let Some(mut mx) = self.mx_key_share.take() {
            let _ = mx.execute("ROLLBACK").await;
            let _ = mx.retire().await;
        }
        if let Some(mut mx) = self.mx_share.take() {
            let _ = mx.execute("ROLLBACK").await;
            let _ = mx.retire().await;
        }
        if self.prepared_active {
            let _ = self.admin.execute(&format!("ROLLBACK PREPARED '{}'", self.prepared_gid)).await;
            self.prepared_active = false;
        }
        if self.slot_active {
            let _ = self.admin.one("SELECT pg_catalog.pg_drop_replication_slot($1)::text", &[&self.slot_name]).await;
            self.slot_active = false;
        }
        self.admin.execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(&self.schema))).await.unwrap();
        assert!(self.admin.retire().await);
    }
}
