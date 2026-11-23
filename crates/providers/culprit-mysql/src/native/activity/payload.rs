//! One bounded current-statement payload owner; native event/generation checks remain with its caller.
use super::*;
pub(crate) const COLUMNS: &str = "NULL AS info,CASE WHEN OCTET_LENGTH(e.SQL_TEXT)<=? THEN e.SQL_TEXT ELSE NULL END AS sql_text,OCTET_LENGTH(e.SQL_TEXT) AS sql_text_bytes";
// A selected cohort can retain sleeping/completed sessions as owners. Only a native current event may
// return statement text to the client; the decoder independently refuses previous SQL attribution.
const CURRENT_PARTICIPANT_SQL: &str =
    "CASE WHEN e.END_EVENT_ID IS NULL AND t.PROCESSLIST_COMMAND<>'Sleep' THEN e.SQL_TEXT ELSE NULL END";
pub(crate) fn current_participant_columns() -> String {
    bounded_columns(CURRENT_PARTICIPANT_SQL)
}
fn bounded_columns(sql_expression: &str) -> String {
    format!(
        "NULL AS info,CASE WHEN OCTET_LENGTH({sql_expression})<=? THEN {sql_expression} ELSE NULL END AS sql_text,OCTET_LENGTH({sql_expression}) AS sql_text_bytes"
    )
}
pub(crate) fn decode(row: &Rec, max_bytes: usize) -> Result<(DefinitionPayload, Option<String>)> {
    let sql = row.native_text("sql_text")?;
    let source_bytes = row.unsigned("sql_text_bytes")?;
    if sql.as_ref().is_some_and(|text| text.len() > max_bytes)
        || sql.as_ref().zip(source_bytes).is_some_and(|(text, bytes)| text.len() as u64 != bytes)
    {
        return Err(invalid());
    }
    let payload = match &sql {
        Some(sql) => DefinitionPayload::sql(sql, culprit_core::Engine::Mysql),
        None => DefinitionPayload::Unavailable {
            reason: if source_bytes.is_some_and(|bytes| bytes > max_bytes as u64) {
                PayloadUnavailable::Oversized
            } else {
                PayloadUnavailable::NotCollected
            },
        },
    };
    Ok((payload, sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ordinary_payload_bytes_stay_identical_and_cohort_text_and_size_both_require_the_native_current_event() {
        assert_eq!(bounded_columns("e.SQL_TEXT"), COLUMNS);
        let columns = current_participant_columns();
        assert_eq!(columns.matches(CURRENT_PARTICIPANT_SQL).count(), 3);
        assert_eq!(columns.matches('?').count(), 1);
        assert!(columns.ends_with(" AS sql_text_bytes"));
        assert!(columns.contains("e.END_EVENT_ID IS NULL AND t.PROCESSLIST_COMMAND<>'Sleep'"));
    }
}
