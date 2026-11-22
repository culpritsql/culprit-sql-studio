//! Route-only discovery scope. A matching key never proves native incarnation, membership or action authority.
use crate::{
    DatabaseIdentity, Result,
    telemetry::{CollectionTarget, DeliveryProblem, digest},
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Serialize, TS, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
#[ts(export)]
pub struct IncidentDatabaseSelector(String);
impl IncidentDatabaseSelector {
    pub fn from_route(database: &DatabaseIdentity) -> Result<Self> {
        let mut route = database.clone();
        route.incarnation = None;
        let bytes = serde_json::to_vec(&("incident-database-route-v1", route))
            .map_err(|_| DeliveryProblem::Integrity.error())?;
        Self::from_key(&digest(&bytes))
    }
    pub fn from_key(key: &str) -> Result<Self> {
        if !valid_selector(key) {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        Ok(Self(key.into()))
    }
    pub fn key(&self) -> &str {
        &self.0
    }
    pub fn is_valid(&self) -> bool {
        valid_selector(&self.0)
    }
}

fn valid_selector(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
impl<'de> Deserialize<'de> for IncidentDatabaseSelector {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let key = String::deserialize(deserializer)?;
        Self::from_key(&key).map_err(serde::de::Error::custom)
    }
}

impl IncidentDatabaseSelector {
    /// A route-only search hint must describe this original target; it never supplies native ownership.
    pub fn matches_target(database: &Option<IncidentDatabaseSelector>, target: &CollectionTarget) -> bool {
        match (database, target) {
            (Some(selector), CollectionTarget::Database(identity)) => {
                IncidentDatabaseSelector::from_route(identity).is_ok_and(|expected| *selector == expected)
            }
            (None, CollectionTarget::Shared(_)) => true,
            _ => false,
        }
    }
}
