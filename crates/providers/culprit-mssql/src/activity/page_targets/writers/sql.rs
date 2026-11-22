//! Finite original selectors. Native payload functions receive only checked request handles and byte offsets.
use super::*;

pub(super) fn requests(metadata: bool, availability: crate::activity::page_server_reads::Availability) -> String {
    let columns = if metadata {
        format!(
            ",{},{} AS request_page_server_read_capability,CONVERT(varchar(40),SYSDATETIME(),126) AS native_observed_at,0 AS no_current_request,CASE WHEN r.database_id=DB_ID() THEN 1 ELSE 5 END AS scope_basis,CAST(NULL AS bigint) AS transaction_id,CAST(NULL AS varchar(40)) AS transaction_start",
            crate::activity::sql::request_columns(availability),
            crate::activity::page_server_reads::CAPABILITY,
        )
    } else {
        String::new()
    };
    requests::participant_query(
        &columns,
        if metadata { "LEFT JOIN sys.dm_exec_sessions b ON b.session_id=r.blocking_session_id" } else { "" },
    )
}

pub(super) fn payload_selectors(focus: &PageWaitWriterFocus, before: &BTreeMap<usize, RequestPoint>) -> Result<String> {
    let mut xml = String::from("<writers>");
    let mut count = 0;
    for (index, selected) in focus.candidates.iter().enumerate() {
        let point = before.get(&index).ok_or_else(invalid_data)?;
        if point.record.is_none() {
            continue;
        }
        let Some((_, Some(plan))) = point.check.identities(&point.check) else { continue };
        let locator = RequestLocator::from_identity(&selected.statement_identity).ok_or_else(invalid_data)?;
        let request = selected.key.request.as_ref().ok_or_else(invalid_data)?;
        xml.push_str(&format!("<row ordinal=\"{index}\" session=\"{}\" login=\"{}\" request=\"{}\" started=\"{}\" handle=\"{}\" begin=\"{}\" end=\"{}\" database=\"{}\" plan=\"{}\"/>",selected.key.session.id,crate::sql_time::native_time(&selected.key.session.generation)?,request.id,crate::sql_time::native_time(&request.generation)?,locator.handle,locator.start,locator.end,locator.database,plan.id));
        count += 1;
    }
    xml.push_str("</writers>");
    Ok(if count == 0 { String::new() } else { xml })
}

pub(super) fn payloads() -> String {
    crate::activity::participants::payloads::query(crate::activity::participants::payloads::Kind::Writers)
}

#[cfg(test)]
mod native_tests {
    use super::payloads;
    use crate::{
        read::{Parameter, ReadSession},
        test_support::OwnedSourceFixture,
    };
    use culprit_core::telemetry::{CollectionBudget, SourceShutdown};

    #[tokio::test]
    #[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
    async fn local_native_selected_writer_payloads_compile_with_empty_original_selectors() {
        let fixture = OwnedSourceFixture::open("culprit-writer-payload-sql").await.expect("native owned source");
        let mut reader = ReadSession::new(
            fixture.pool.clone(),
            CollectionBudget {
                max_queries: 1,
                statement_timeout_ms: 1_000,
                total_timeout_ms: 3_000,
                ..Default::default()
            },
        );
        let result = reader
            .query(
                &payloads(),
                vec![Parameter::Text("<writers/>".into()), Parameter::Integer(4 * 1024), Parameter::Integer(4 * 1024)],
                1,
            )
            .await;
        let shutdown = reader.close().await;
        let metrics = reader.read_metrics();
        // Native syntax errors must remain failures even when no original handles authorize payloads.
        // Confirm this same reader's shutdown before inspecting the result or accounting.
        assert_eq!(shutdown, SourceShutdown::Quiescent);
        let rows = result.expect("actual writer payload SQL and its quoted aliases must compile");
        assert!(rows.is_empty(), "empty original selectors cannot discover unsolicited native rows");
        assert_eq!(reader.queries(), 1);
        let metrics = metrics.expect("one native payload read must retain its accounting");
        assert!(metrics.is_valid() && !metrics.has_unconfirmed_work());
        assert_eq!(metrics.active_tasks, 0);
        assert_eq!(metrics.untracked_tasks, 0);
        assert_eq!(metrics.templates.len(), 1);
        let template = &metrics.templates[0];
        assert_eq!(template.completed_tasks, 1);
        assert_eq!(template.failures, 0);
        assert_eq!(template.rows_decoded, "0");
        eprintln!(
            "owned writer payload SQL compiled: native_queries=1,decoded_rows=0,shutdown=Quiescent,writer_qualification=false,native_cost_qualified=false"
        );
    }
}
