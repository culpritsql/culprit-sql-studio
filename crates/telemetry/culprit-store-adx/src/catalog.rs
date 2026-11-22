//! The store selects physical columns; shared catalog construction owns stream coverage and fingerprints.
pub type SchemaCatalog = culprit_evidence::analytical::catalog::PhysicalCatalog<crate::schema::Schema>;
