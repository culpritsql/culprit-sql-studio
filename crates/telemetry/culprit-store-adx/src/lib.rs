//! ADX delivery contracts and protocol validation. Provisioning belongs to deployment tooling.
pub mod authentication;
pub mod catalog;
pub mod client;
pub mod control;
#[cfg(feature = "write")]
pub mod deployment;
mod eligibility;
pub mod evidence;
mod http;
#[cfg(feature = "write")]
pub mod ingestion;
pub mod projection;
pub mod query_assets;
#[cfg(feature = "write")]
pub mod queued;
#[cfg(feature = "write")]
pub mod ready;
pub mod receipts;
pub mod response;
pub mod schema;
#[cfg(feature = "write")]
pub mod sink;

#[cfg(all(test, feature = "write"))]
mod emulator;
#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "../../../../tests/support/http.rs"]
mod http_fixture;

#[cfg(all(test, feature = "write"))]
mod encoding_tests;
