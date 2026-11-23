//! Borrowed input for replanning a recorded statement. Opaque report IDs never become native authority.
use crate::telemetry::NativeQueryIdentity;

#[derive(Clone, Copy)]
pub struct RecordedStatement<'a> {
    sql: &'a str,
    source_id: Option<&'a str>,
    native_identity: Option<&'a NativeQueryIdentity>,
}
impl<'a> RecordedStatement<'a> {
    pub fn new(sql: &'a str, source_id: &'a str, native_identity: Option<&'a NativeQueryIdentity>) -> Self {
        Self { sql, source_id: Some(source_id), native_identity }
    }
    /// Logs without a native statistics identity retain their existing unscoped trust policy.
    pub fn unscoped(sql: &'a str) -> Self {
        Self { sql, source_id: None, native_identity: None }
    }
    pub fn sql(self) -> &'a str {
        self.sql
    }
    pub fn source_id(self) -> Option<&'a str> {
        self.source_id
    }
    pub fn native_identity(self) -> Option<&'a NativeQueryIdentity> {
        self.native_identity
    }
}
