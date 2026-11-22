//! ClickHouse analytical delivery with shared preparation, exact receipts and publication ownership.
pub mod catalog;
pub mod client;
#[cfg(feature = "write")]
pub mod deployment;
pub mod evidence;
#[cfg(all(test, feature = "write"))]
mod functional;
#[cfg(test)]
#[path = "../../../../tests/support/http.rs"]
mod http_fixture;
#[cfg(feature = "write")]
pub mod ingestion;
pub mod response;
pub mod schema;
#[cfg(feature = "write")]
pub mod sink;
