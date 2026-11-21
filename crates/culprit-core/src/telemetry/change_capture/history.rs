//! Own independent recorded families and their existing append/sharing policy without combining native evidence.
use super::*;

#[derive(Serialize, Deserialize, TS, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RecordedChangeCaptureHistories {
    cdc: Vec<RecordedOperationalRead>,
    change_feed: Vec<RecordedOperationalRead>,
}
impl RecordedChangeCaptureHistories {
    pub fn originals(&self, family: MssqlChangeCaptureFamily) -> &[RecordedOperationalRead] {
        match family {
            MssqlChangeCaptureFamily::Cdc => &self.cdc,
            MssqlChangeCaptureFamily::ChangeFeed => &self.change_feed,
        }
    }

    /// Family interpretation precedes attachment; generic original integrity/idempotence/caps stay shared.
    pub fn append(&mut self, family: MssqlChangeCaptureFamily, read: Box<CapturedOperationalRead>) -> Result<()> {
        if !read.description.observation.snapshot().is_some_and(|header| family.valid_header(header)) {
            return Err(DeliveryProblem::InvalidContract.error());
        }
        let originals = match family {
            MssqlChangeCaptureFamily::Cdc => &mut self.cdc,
            MssqlChangeCaptureFamily::ChangeFeed => &mut self.change_feed,
        };
        append_recorded_operational_read(originals, read)
    }

    pub(crate) fn withdraw_for_sharing(&mut self) {
        for recorded in self.cdc.iter_mut().chain(&mut self.change_feed) {
            *recorded = RecordedOperationalRead::Unavailable {
                reason: crate::message!("view.change_capture.history.sharing_withheld"),
            };
        }
    }
}

#[cfg(test)]
mod tests;
