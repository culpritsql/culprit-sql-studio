//! Canonical warehouse evidence reading and immutable recovery authentication.
//! This crate has no collector, outbox, scheduler, source database or credential discovery dependency.
pub mod analytical;
#[doc(hidden)]
pub mod collection_activity;
pub mod collection_references;
pub mod delivery;

pub mod original_projection;

#[cfg(test)]
mod boundary_tests;
