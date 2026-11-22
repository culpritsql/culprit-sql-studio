//! Local batch encoding and publication. Read contracts and canonical proof verification live in culprit-evidence.
pub mod encoding;
pub mod publication;
pub mod sink;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
