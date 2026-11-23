//! These scripted dispatch controls establish no native CPU/load or source safety qualification.
use super::*;
use crate::{progress::test_support, read::QueryRead};
use std::collections::VecDeque;

const FROM: &str = "2026-11-23T10:00:00Z";
const INITIAL_MEMBER: &str = "2026-11-23T10:00:00.010Z";
const FINAL_METADATA: &str = "2026-11-23T10:00:00.020Z";
const UNTIL: &str = "2026-11-23T10:00:00.030Z";
const START: &str = "2026-11-22T00:00:00Z";

struct Read {
    original: test_support::Reader,
    generation: Option<u64>,
    retire_after: Option<usize>,
}
#[async_trait::async_trait]
impl QueryRead for Read {
    fn connection_generation(&self) -> Option<u64> {
        self.generation
    }
    async fn query(&mut self, sql: &str, limit: usize) -> Result<Vec<Row>> {
        let result = self.original.query(sql, limit).await;
        // The first completed acquisition is legitimate; retirement never silently grants another generation.
        if self.original.calls.len() == 1 {
            self.generation = Some(1);
        }
        if self.retire_after == Some(self.original.calls.len()) {
            self.generation = None;
        }
        result
    }
}
fn setting(value: Option<&str>) -> Row {
    test_support::row(&[
        ("name", Some("autovacuum")),
        ("setting", value),
        ("unit", None),
        ("boot_val", Some("on")),
        ("source", Some("configuration file")),
        ("context", Some("sighup")),
        ("pending_restart", Some("f")),
    ])
}
fn metadata(writable: bool, at: &str) -> Row {
    test_support::metadata_at(180_000, if writable { "f" } else { "t" }, START, false, at)
}
fn scripted_reader(writable: bool) -> Read {
    Read {
        original: test_support::Reader {
            replies: VecDeque::from([
                Ok(vec![metadata(writable, FROM)]),
                Ok(vec![test_support::member_at("127.0.0.1", INITIAL_MEMBER)]),
                Ok(vec![setting(Some("off"))]),
                Ok(vec![metadata(writable, FINAL_METADATA)]),
                Ok(vec![test_support::member_at("127.0.0.1", UNTIL)]),
                Ok(vec![test_support::row(&[("age", Some("42"))])]),
            ]),
            ..Default::default()
        },
        generation: None,
        retire_after: None,
    }
}
fn with_metadata(row: Row, field: &str, value: &str) -> Row {
    const FIELDS: &[&str] = &[
        "build",
        "database_oid",
        "database",
        "version",
        "block_bytes",
        "standby",
        "started_at",
        "observed_at",
        "full_visibility",
        "replay_pause_requested_access",
        "replay_pause_state_access",
        "pgss_schema",
        "pgss_columns",
        "pgss_info",
    ];
    Row::from_fields(
        FIELDS.iter().map(|key| ((*key).into(), if *key == field { Some(value.into()) } else { row.text(key) })),
    )
}

#[tokio::test]
async fn scan_pays_four_paired_context_statements_and_preserves_primary_or_recovery_raw_values() {
    for writable in [true, false] {
        let mut reader = scripted_reader(writable);
        assert!(reader.generation.is_none());
        let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
        assert!(collected.failure.is_none());
        assert_eq!(
            reader.original.calls.len(),
            6,
            "four context statements plus original Settings and independent database age"
        );
        assert_eq!(reader.original.calls[2], query(SETTINGS, 1_000));
        assert_eq!(collected.values.len(), 2);
        assert_eq!(collected.read.observations[0].value_in_use.as_deref(), Some("off"));
        let PostgresSettingsAcquisition::Completed { context } = &collected.read.acquisition else { panic!() };
        assert_eq!(context.database_writable, Some(writable));
        assert!(context.target.database().unwrap().incarnation.is_none());
        assert!(context.has_qualified_local_owner());
        assert_eq!(context.observed_from, canonical_time(FROM).unwrap());
        assert_eq!(context.observed_until, canonical_time(UNTIL).unwrap());
        assert_eq!(collected.read.configuration(context.target.database()).setting("autovacuum").is_some(), writable);
        assert!(reader.original.replies.is_empty());
    }
}

#[tokio::test]
async fn initial_member_denial_preserves_raw_settings_but_a_later_member_cannot_repair_authority() {
    let mut reader = scripted_reader(true);
    reader.original.replies[1] =
        Err(culprit_core::Error::new(culprit_core::ErrorCode::PermissionDenied, "controlled member denial"));
    let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
    assert!(collected.failure.is_none(), "existing optional native-member permission policy remains unchanged");
    assert_eq!(reader.original.calls.len(), 6);
    assert_eq!(collected.read.observations[0].value_in_use.as_deref(), Some("off"));
    assert!(matches!(
        collected.read.acquisition,
        PostgresSettingsAcquisition::Unqualified { reason: PostgresSettingsAcquisitionGap::ContextUnavailable, .. }
    ));
    assert!(
        collected
            .read
            .configuration(Some(&culprit_core::DatabaseIdentity::server(
                culprit_core::Engine::Postgres,
                "shop",
                "fixture.invalid",
                5432,
                None
            )))
            .setting("autovacuum")
            .is_none()
    );
}

#[tokio::test]
async fn final_role_database_process_and_member_changes_preserve_rows_and_stop_before_independent_age() {
    for (field, value) in [
        ("standby", "t"),
        ("database_oid", "16385"),
        ("database", "another-db"),
        ("started_at", "2026-11-23T09:00:00Z"),
    ] {
        let mut reader = scripted_reader(true);
        reader.original.replies[3] = Ok(vec![with_metadata(metadata(true, FINAL_METADATA), field, value)]);
        let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
        assert_eq!(reader.original.calls.len(), 4, "known metadata drift admits no next native member/age lookup");
        assert_eq!(collected.read.observations.len(), 1);
        assert_eq!(
            collected.failure.as_ref().unwrap().detail,
            Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::SourceChanged))
        );
        assert!(matches!(
            collected.read.acquisition,
            PostgresSettingsAcquisition::Unqualified { reason: PostgresSettingsAcquisitionGap::SourceChanged, .. }
        ));
    }
    let mut reader = scripted_reader(true);
    reader.original.replies[4] = Ok(vec![test_support::member_at("127.0.0.2", UNTIL)]);
    let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
    assert_eq!(reader.original.calls.len(), 5);
    assert!(collected.failure.is_some());
    assert_eq!(collected.read.observations[0].value_in_use.as_deref(), Some("off"));
}

#[tokio::test]
async fn lost_optional_member_lease_never_dispatches_settings_or_uses_a_replacement_and_preserves_original_failure() {
    let mut reader = scripted_reader(true);
    reader.original.replies[1] = Err(CollectionStop::TimedOut.error());
    reader.retire_after = Some(2);
    let failure = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.err().unwrap();
    assert_eq!(failure.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::TimedOut)));
    assert_eq!(reader.original.calls.len(), 2);
    assert!(reader.generation.is_none());
    assert!(!reader.original.calls.iter().any(|sql| sql.contains("WHERE name IN")));

    let mut reader = scripted_reader(true);
    reader.original.replies[1] =
        Err(culprit_core::Error::new(culprit_core::ErrorCode::PermissionDenied, "controlled member denial"));
    reader.retire_after = Some(2);
    let failure = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.err().unwrap();
    assert_eq!(failure.detail, Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::SourceChanged)));
    assert_eq!(reader.original.calls.len(), 2);
}

#[tokio::test]
async fn final_cancel_or_unconfirmed_failure_retains_already_observed_rows_without_an_age_dispatch() {
    for stop in [CollectionStop::Cancelled, CollectionStop::CancellationUnconfirmed] {
        let mut reader = scripted_reader(true);
        reader.original.replies[3] = Err(stop.error());
        reader.retire_after = Some(4);
        let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
        assert_eq!(reader.original.calls.len(), 4);
        assert_eq!(
            collected.failure.as_ref().unwrap().detail,
            Some(culprit_core::ErrorDetail::CollectionStopped(stop))
        );
        assert_eq!(collected.read.observations[0].value_in_use.as_deref(), Some("off"));
        assert!(
            collected
                .read
                .configuration(Some(&culprit_core::DatabaseIdentity::server(
                    culprit_core::Engine::Postgres,
                    "shop",
                    "fixture.invalid",
                    5432,
                    None
                )))
                .setting("autovacuum")
                .is_none()
        );
    }
}

#[tokio::test]
async fn null_value_and_caller_query_exhaustion_preserve_raw_facts_without_default_or_budget_repair() {
    let mut reader = scripted_reader(true);
    reader.original.replies[2] = Ok(vec![setting(None)]);
    let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
    assert!(collected.read.observations[0].value_in_use.is_none());
    assert_eq!(collected.values.len(), 1, "only the independently read database-age value is displayable");
    let PostgresSettingsAcquisition::Completed { context } = &collected.read.acquisition else { panic!() };
    assert!(collected.read.configuration(context.target.database()).setting("autovacuum").is_none());

    let mut reader = scripted_reader(true);
    reader.original.query_limit = Some(5);
    let collected = crate::catalog::settings(&mut reader, "fixture.invalid", 5432).await.unwrap();
    assert_eq!(reader.original.calls.len(), 5);
    assert_eq!(
        collected.failure.as_ref().unwrap().detail,
        Some(culprit_core::ErrorDetail::CollectionStopped(CollectionStop::QueryLimit))
    );
    assert_eq!(collected.read.observations.len(), 1);
}
