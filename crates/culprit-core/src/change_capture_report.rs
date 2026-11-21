//! Shared presentation shape; native CDC and change-feed facts and reports remain distinct.
use serde::{Deserialize, Serialize};
use ts_rs::TS;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum ChangeCaptureEntryKind {
    ScanSession,
    Aggregate,
    Error,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[ts(export)]
pub struct ChangeCaptureField {
    pub key: String,
    pub label: String,
    pub value: Option<String>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[ts(export)]
pub struct ChangeCaptureEntry {
    pub source: String,
    pub observation_key: String,
    pub kind: ChangeCaptureEntryKind,
    pub observed_at: String,
    pub title: String,
    pub explanation: String,
    pub fields: Vec<ChangeCaptureField>,
    pub investigation: String,
    pub verify: String,
}

/// Independent family explanations; each retained original supplies its own source/clock/coverage.
#[derive(Serialize, Deserialize, TS, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ChangeCaptureReports {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub cdc: Option<Vec<crate::CdcReport>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub change_feed: Option<Vec<crate::ChangeFeedReport>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn absent_child_and_explicit_empty_child_remain_distinct_without_a_combined_health_state() {
        let absent = ChangeCaptureReports::default();
        assert_eq!(serde_json::to_value(&absent).unwrap(), serde_json::json!({}));
        let empty_cdc = ChangeCaptureReports { cdc: Some(vec![]), change_feed: None };
        let json = serde_json::to_value(&empty_cdc).unwrap();
        assert_eq!(json, serde_json::json!({"cdc":[]}));
        let restored: ChangeCaptureReports = serde_json::from_value(json).unwrap();
        assert_eq!(restored.cdc.as_ref().unwrap().len(), 0);
        assert!(restored.change_feed.is_none());
    }
}
