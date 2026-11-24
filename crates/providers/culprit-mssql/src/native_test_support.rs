//! Explicit nonce-owned SQL Server actors for Engine native acceptance; no Collector dependency.
//! The existing page fixture retains the database, gate, worker handles and all failure cleanup.
pub use crate::activity::page_targets::native_tests::output_retention::{
    OUTPUT_APPLICATION, OUTPUT_STREAM_ROWS, OUTPUT_WAITER_APPLICATION, OutputRetentionFixture,
    run_output_retention_case, save_output_retention_receipt,
};
pub use crate::activity::page_targets::native_tests::serial_input_profile::{
    APPLICATION, FIXTURE_DISPATCH_CAP, INPUT_ROWS, SerialInputFixture, SourceEndpoint, run_serial_tvp_case,
    save_native_observation, save_native_receipt,
};
