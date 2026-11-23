//! Bounded investigation of a specifically selected committed capture. Native manifests and immutable projection
//! indexes establish trust; a warehouse row never becomes an arbitrary Snapshot or a claim of current state.
mod decode;
mod definition_links;
pub use definition_links::EvidenceDefinitionLink;
mod hydration;
mod incidents;
mod reader;
mod resources;
mod response_bytes;
mod types;

pub use incidents::{
    IncidentCandidateCursor, IncidentCandidateHint, IncidentCandidatePosition, IncidentCandidateSelection,
    IncidentDiscoveryPage, IncidentDiscoveryRequest, MAX_INCIDENT_CANDIDATE_PAGE_ROWS,
};
pub use reader::EvidenceReader;
pub use resources::{
    PublishedResourceClock, PublishedResourceContent, PublishedResourceHint, PublishedResourceQuery,
    ResourceDiscoveryPage, resource_namespace_id,
};
pub use types::*;

#[cfg(test)]
mod tests;
