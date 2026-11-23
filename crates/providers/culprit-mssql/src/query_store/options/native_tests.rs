//! Changed projection correctness on the existing disposable fixture; this is not cost or automatic admission.
use super::*;
use crate::{
    local_source::Context,
    read::{NativeRead, measurement::shutdown_state},
    settings::{Kind, read, read_current},
    test_support::{
        OwnedSourceFixture,
        owned_case::{finish_case, until_cancelled},
        receipt_directory, save_cost_receipt,
    },
};
use culprit_core::{Result, telemetry::*};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::FutureExt;
use serde_json::json;
use std::{panic::AssertUnwindSafe, path::Path, time::Duration};
use tokio::time::Instant;

const SOURCE_QUERIES: u32 = 8;
const SETTINGS_ROWS: usize = 512;

#[tokio::test]
#[ignore = "requires nonce-owned loopback SQL Server, seeded native Query Store and a fresh CULPRIT_NATIVE_COST_RECEIPT_DIR; select query_store::options::native_tests explicitly"]
async fn owned_query_store_options_share_the_scan_and_retained_catalog_with_original_scope_and_close() {
    let directory = receipt_directory();
    let cancellation = CollectionCancellation::default();
    let (expired, fixture) = finish_case(
        Instant::now() + Duration::from_secs(10),
        &cancellation,
        Box::pin(OwnedSourceFixture::open("culprit-query-store-options-source")),
    )
    .await;
    let fixture = fixture.expect("nonce-owned endpoint admission");
    assert!(!expired, "original endpoint admission has settled");
    let (expired, result) = finish_case(
        Instant::now() + Duration::from_secs(30),
        &cancellation,
        Box::pin(run(&fixture, &directory, &cancellation)),
    )
    .await;
    result.expect("owned Query Store options source");
    assert!(!expired, "same bounded native work and original close settle before returning");
}

struct CapturedOptions {
    ordinary: Vec<SettingObservation>,
    retained: Vec<SettingObservation>,
    context: NativeReadContext,
}

async fn capture(reader: &mut impl NativeRead, port: u16) -> Result<CapturedOptions> {
    // The ordinary scan reuses these exact options from Probe. This selected test does not rerun its other probes.
    let ordinary = read(reader, Kind::QueryStore, SETTINGS_ROWS).await?;
    let before = Context::read(reader, "127.0.0.1", port).await?;
    let retained = read_current(reader, Kind::QueryStore, SETTINGS_ROWS, &before).await?;
    let after = before.recheck(reader, "127.0.0.1", port).await?;
    let context = before.completed(&after)?;
    Ok(CapturedOptions { ordinary, retained, context })
}

async fn run(fixture: &OwnedSourceFixture, directory: &Path, cancellation: &CollectionCancellation) -> Result<()> {
    let mut io = Box::new(
        Box::pin(crate::test_support::measured_read(
            fixture.pool.clone(),
            fixture.port,
            fixture.password(),
            &fixture.database,
            CollectionBudget {
                max_queries: SOURCE_QUERIES,
                max_snapshot_rows: SETTINGS_ROWS as u32,
                page_rows: 64,
                page_bytes: 64 * 1024,
                total_bytes: 256 * 1024,
                statement_timeout_ms: 1_000,
                total_timeout_ms: 5_000,
                ..Default::default()
            },
        ))
        .await?,
    );
    let started = Instant::now();
    let outcome = AssertUnwindSafe(until_cancelled(cancellation, Box::pin(capture(io.as_mut(), fixture.port))))
        .catch_unwind()
        .await;
    // The exact original source closes on success, native failure, cancellation and panic before assertions.
    let shutdown = io.close().await;
    let quiet = io.is_quiet().await;
    let native = outcome.as_ref().ok().and_then(|result| result.as_ref().ok());
    save_cost_receipt(
        directory,
        "query-store-settings-options-native",
        &json!({
            "scope":io.scope(), "accounting":io.report(), "source_queries":io.queries(),
            "source_query_ledger":{"ordinary_options":1,"original_context":3,"framed_options":1,"final_context":3},
            "shutdown":shutdown_state(shutdown), "independently_observed_quiet":quiet.as_ref().ok(),
            "client_elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            "fixture_admission_client_elapsed_ms":fixture.admission_elapsed_ms,
            "error":outcome.as_ref().ok().and_then(|result|result.as_ref().err()).map(|error|error.code),
            "ordinary_settings":native.map(|point|&point.ordinary), "retained_settings":native.map(|point|&point.retained),
            "original_context":native.map(|point|&point.context),
            "production_admission":"unqualified", "native_cpu_io_cost_qualified":false,
            "scope_limit":"only this owned endpoint's native version and edition; Azure SQL, MI, older versions and restricted permissions are not native-qualified by this case",
            "cancellation_limit":"source closes and independent observer checks quiet; active cancellation of this short metadata template is not proved",
            "cleanup":"same original reader closed; existing supervisor owns disposable database/container teardown; no Query Store setup, schema or data mutation",
        }),
    )?;
    let result = match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    if shutdown != SourceShutdown::Quiescent {
        return Err(CollectionStop::CancellationUnconfirmed.error());
    }
    let point = result?;
    if !quiet? {
        return Err(CollectionStop::SourceChanged.error());
    }
    assert_eq!(io.queries(), SOURCE_QUERIES);
    assert_eq!(point.context.target.database().unwrap().database, fixture.database);
    assert!(point.context.target.database().unwrap().incarnation.is_none());
    assert!(point.context.has_qualified_local_owner());
    assert_eq!(point.context.database_writable, Some(true));
    for values in [&point.ordinary, &point.retained] {
        assert_eq!(values.len(), FIELDS.len());
        for (name, nature) in fields() {
            let value = values.iter().find(|value| value.name == name).expect("declared native option alias");
            assert_eq!(value.nature, nature);
            assert_eq!(value.namespace, Kind::QueryStore.namespace());
            assert_eq!(value.scope, culprit_core::MeasurementScope::Database);
            assert_eq!(value.default_value, None);
            assert_eq!(value.native_is_default, None);
        }
        for required in [
            "actual_state_desc",
            "desired_state_desc",
            "max_storage_size_mb",
            "current_storage_size_mb",
            "readonly_reason",
        ] {
            assert!(
                values.iter().find(|value| value.name == required).unwrap().value.is_some(),
                "owned native {required}"
            );
        }
    }
    // State/storage can change independently between native points. Do not demand cross-point equality or infer a rate.
    crate::settings::tests::query_store::assert_row_contract(
        &point.context,
        &point.retained,
        "native-query-store-settings-options",
    );
    Ok(())
}
