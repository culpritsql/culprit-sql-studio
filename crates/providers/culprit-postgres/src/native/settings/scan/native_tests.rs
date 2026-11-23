//! Uses the existing nonce guard and application ReadSession, not a collector request or new native owner.
use super::*;
use crate::{
    connect::{COLLECTOR, Connector},
    read::{ApplicationReadScope, NativeRead, ReadSession},
};

#[tokio::test]
#[ignore = "requires the existing nonce-owned PG18 fixture and explicit private native receipt directory"]
async fn ordinary_settings_keep_their_actual_read_role_and_pay_seven_dispatches_including_setup() {
    let base = crate::native_test_support::connection_spec();
    let specification =
        crate::native_test_support::specification(base.database.as_deref().unwrap(), CollectionBudget::default()).await;
    let connector = Connector::new(&specification).unwrap();
    let mut reader = ReadSession::application(connector.clone(), COLLECTOR, ApplicationReadScope::new());
    assert!(reader.connection_generation().is_none());
    let result = crate::catalog::settings(&mut reader, &connector.host, connector.port).await;
    assert_eq!(reader.close().await, SourceShutdown::Quiescent);
    let collected = result.unwrap();
    assert!(collected.failure.is_none());
    assert!(collected.read.is_valid());
    assert_eq!(reader.queries(), 7, "SET1 +before metadata/member2 +original Settings1 +after2 +independent age1");
    let metrics = reader.metrics().unwrap();
    assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
    assert_eq!((metrics.active_tasks, metrics.untracked_tasks), (0, 0));
    assert_eq!(metrics.templates.iter().map(|row| row.completed_tasks).sum::<u32>(), 7);
    let PostgresSettingsAcquisition::Completed { context } = &collected.read.acquisition else { panic!() };
    assert!(context.has_qualified_local_owner());
    assert!(context.target.database().unwrap().incarnation.is_none());
    assert!(context.database_writable.is_some());
    assert!(collected.read.observations.iter().any(|setting| setting.name == "autovacuum"));
    assert!(collected.values.iter().any(|setting| setting.name == "datfrozenxid_age"));
    let saved = serde_json::to_vec(&collected.read).unwrap();
    let restored: PostgresSettingsRead = serde_json::from_slice(&saved).unwrap();
    assert_eq!(restored, collected.read);
    crate::native_test_support::write_automatic_receipt(
        "builtin-safety",
        "ordinary-settings-read-role.json",
        &serde_json::json!({
            "source_queries_including_setup":reader.queries(),"source_metrics":metrics,
            "raw_settings_count":collected.read.observations.len(),
            "native_database_writable":context.database_writable,
            "native_database_incarnation_is_none":context.target.database().unwrap().incarnation.is_none(),
            "native_context_retained":true,"saved_read_equal":true,
            "source_profile_settings_six_unchanged":true,"cost_measured":false,
        }),
    )
    .unwrap();
}
