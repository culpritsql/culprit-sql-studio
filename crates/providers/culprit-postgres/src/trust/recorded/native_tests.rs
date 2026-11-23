//! The existing supervisor fixture supplies real recorded native identity; no application SQL is executed here.
use crate::{
    connect::{Connector, INTERACTIVE},
    native_test_support::{self, case},
    read::{ApplicationReadScope, ReadSession},
};
use culprit_core::{Provider, RecordedStatement, SnapshotOptions, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires the existing nonce-owned PostgreSQL supervisor; recorded native entry/current estimated replan only"]
async fn owned_recorded_statement_resolves_full_native_entry_and_refuses_rebinding_before_planning() {
    let cancellation = CollectionCancellation::default();
    let database = native_test_support::connection_spec().database.unwrap();
    let specification = native_test_support::specification(&database, CollectionBudget::default()).await;
    let provider = crate::PostgresProvider::new(&specification).unwrap();
    let outcome = case::finish(Duration::from_secs(90), &cancellation, async {
        let scope = ApplicationReadScope::until(tokio::time::Instant::now() + Duration::from_secs(10));
        let mut reader = ReadSession::application(Connector::new(&specification)?, INTERACTIVE, scope);
        let own = crate::read::query_one(
            &mut reader,
            "SELECT oid::text AS userid FROM pg_catalog.pg_roles WHERE rolname=session_user",
        )
        .await;
        let own = reader.finish(own).await?.and_then(|row| row.text("userid")).unwrap();
        let snapshot = case::admitted(
            &cancellation,
            provider.snapshot(
                "recorded-native-entry",
                &SnapshotOptions { plans: 0, top_queries: 32, ..Default::default() },
            ),
        )
        .await?;
        let query = snapshot
            .queries()
            .iter()
            .find(|query| {
                query.text == "SELECT sum(value) FROM public.smoke_rows WHERE id > $1"
                    && query.native_identity.as_ref().is_some_and(|native| {
                        native.qualifiers.get("userid") == Some(&own)
                            && native.qualifiers.get("toplevel").map(String::as_str) == Some("true")
                    })
            })
            .expect("the existing supervisor seed records this login's exact top-level SELECT");
        let native = query.native_identity.as_ref().unwrap();
        assert!(query.id.parse::<i64>().is_err(), "the report ID is the actual composite, not a native scalar");
        let statement = RecordedStatement::new("SELECT 999999", &query.id, Some(native));
        let plan = case::admitted(&cancellation, provider.explain_recorded(statement)).await?;
        assert!(matches!(plan.kind, culprit_core::PlanKind::Estimated | culprit_core::PlanKind::Generic));
        for field in ["userid", "dbid", "postmaster_started_at", "toplevel", "stats_since"] {
            if !native.qualifiers.contains_key(field) {
                continue;
            }
            let mut changed = native.clone();
            let value = match field {
                "userid" | "dbid" => (changed.qualifiers[field].parse::<u32>().unwrap() + 1).to_string(),
                "toplevel" => "false".into(),
                _ => "2000-01-01T00:00:00Z".into(),
            };
            changed.qualifiers.insert(field.into(), value);
            assert!(
                case::admitted(
                    &cancellation,
                    provider.explain_recorded(RecordedStatement::new("SELECT 999999", &query.id, Some(&changed)))
                )
                .await
                .is_err(),
                "{field}"
            );
        }
        assert!(
            provider.explain_recorded(RecordedStatement::new("SELECT 999999", &native.query_id, None)).await.is_err()
        );
        Ok::<_, culprit_core::Error>(())
    })
    .await;
    assert!(!outcome.deadline_reached);
    assert_eq!(provider.source_shutdown(), SourceShutdown::Quiescent);
    outcome.value.unwrap();
}
