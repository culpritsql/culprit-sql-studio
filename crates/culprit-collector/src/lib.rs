//! Source-independent telemetry capture and durable delivery. Credential selection belongs to the embedding runtime; capture receives authenticated sources.
mod blocking_state;
pub mod capture;
mod collection_files;
use culprit_read::durable_fs;
pub mod outbox;
mod record_verification;

pub mod delivery;
pub mod diagnostics;
pub mod fleet;
pub mod health;
pub mod service;
pub mod synthesis;

pub mod analytical;
pub mod lifecycle;

pub mod verified_original;
