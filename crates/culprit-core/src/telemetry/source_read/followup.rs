//! One closed nomination owner consumes existing authenticated followup rows; no native work is admitted here.
use super::*;

impl SourceReadFocus {
    pub fn from_followup_input(
        question: SourceReadQuestion,
        episode: &DiagnosticEpisode,
        source: &CollectionDescription,
        reference: &DiagnosticEvidenceRef,
        row: &ExportedRecord,
    ) -> Result<Option<Self>> {
        match question {
            SourceReadQuestion::RelationalInputContext => Self::from_current_work(episode, source, reference, row),
            _ => Ok(None),
        }
    }
}
