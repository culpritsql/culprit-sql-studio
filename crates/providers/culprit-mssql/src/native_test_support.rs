//! Explicit nonce-owned SQL Server actors for Engine native acceptance; no Collector dependency.
//! The existing page fixture retains the database, gate, worker handles and all failure cleanup.
pub use crate::activity::page_targets::native_tests::ag_redo_blocking::{
    AG_REDO_STREAM_ROWS, AG_SECONDARY_READER_APP, AgRedoBlockingFixture, run_ag_redo_blocking_case,
    save_ag_redo_blocking_receipt,
};
pub use crate::activity::page_targets::native_tests::output_retention::{
    OUTPUT_APPLICATION, OUTPUT_STREAM_ROWS, OUTPUT_WAITER_APPLICATION, OutputRetentionFixture,
    run_output_retention_case, save_output_retention_receipt,
};
pub use crate::activity::page_targets::native_tests::parallel_operator_profile::{
    PARALLEL_APPLICATION, PARALLEL_MATCHING_ROWS, ParallelOperatorFixture, run_parallel_operator_case,
    save_parallel_operator_receipt,
};
pub use crate::activity::page_targets::native_tests::serial_input_profile::{
    APPLICATION, FIXTURE_DISPATCH_CAP, INPUT_ROWS, SerialInputFixture, SourceEndpoint, run_serial_tvp_case,
    save_native_observation, save_native_receipt,
};
pub use crate::activity::page_targets::native_tests::worker_starvation_native::{
    StarvationBurstStart, WORKER_LOCK_WAITER_APP, WORKER_PARALLEL_APP, WorkerStarvationFixture,
    run_worker_starvation_case, save_worker_starvation_receipt,
};
