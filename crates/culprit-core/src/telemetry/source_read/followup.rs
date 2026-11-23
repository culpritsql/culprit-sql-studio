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
            SourceReadQuestion::TransactionMetadata => {
                Self::from_transaction_metadata_continuation(episode, source, reference, row)
            }
            SourceReadQuestion::OutputGrantRetention => Self::from_output_activity(episode, source, reference, row),
            SourceReadQuestion::MetadataLockContext => {
                Self::from_metadata_lock_activity(episode, source, reference, row)
            }
            SourceReadQuestion::ReadableSecondaryRecovery => {
                Self::from_readable_secondary(episode, source, reference, row)
            }
            SourceReadQuestion::RelationalInputContext => Self::from_current_work(episode, source, reference, row),
            SourceReadQuestion::CurrentExecutionProfile => {
                Self::from_current_execution_work(episode, source, reference, row)
            }
            _ => Ok(None),
        }
    }
}
