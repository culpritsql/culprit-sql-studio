//! Neutral input authority is an exact original receipt. Physical warehouse ranges are resolved later.
use super::*;
#[derive(Serialize, Deserialize, TS, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[ts(export)]
pub enum DerivedInputRole {
    Observation,
    Catalog,
    Revision,
    RelatedEvidence,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "clock", rename_all = "kebab-case", deny_unknown_fields)]
#[ts(export)]
pub enum DerivedCatalogTime {
    Utc { at: String },
    NativeWithoutOffset { at: String },
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedCatalogProvenance {
    pub observed_from: String,
    pub observed_until: String,
    pub native_time: Option<DerivedCatalogTime>,
}
#[derive(Serialize, Deserialize, TS, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DerivedInputReference {
    pub capture: DiagnosticCaptureRef,
    pub role: DerivedInputRole,
    pub catalog: Option<DerivedCatalogProvenance>,
}
impl DerivedInputReference {
    pub fn is_valid(&self) -> bool {
        self.capture.is_valid()
            && self.catalog.as_ref().is_none_or(|catalog| {
                utc(&catalog.observed_from)
                    && utc(&catalog.observed_until)
                    && ObservationBounds::new(&catalog.observed_from, &catalog.observed_until).is_some()
                    && catalog.native_time.as_ref().is_none_or(|time| match time {
                        DerivedCatalogTime::Utc { at } => utc(at),
                        DerivedCatalogTime::NativeWithoutOffset { at } => {
                            at.len() <= 64 && chrono::NaiveDateTime::parse_from_str(at, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                        }
                    })
            })
    }
}
