use super::*;
use crate::{query_store::payload::Payload, read::value_bytes};
use culprit_core::{Engine, telemetry::DefinitionPayload};
use flate2::{Compression, write::GzEncoder};
use std::io::Write;

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut writer = GzEncoder::new(Vec::new(), Compression::fast());
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

fn wire_row(column: &str, value: Val, size: usize) -> Rec {
    Rec::of(&[
        (column, value),
        ("source_bytes", Val::Int(size as i64)),
        ("encrypted", Val::Bool(false)),
        ("restricted", Val::Bool(false)),
    ])
}

fn restore(row: &mut Rec, limit: usize) -> Result<()> {
    let parameters = [Parameter::Text("<ids/>".into()), Parameter::Integer(limit as i64)];
    Decoder::for_columns(row.columns().map(|(name, _)| name), &parameters)?.unwrap().decode(row)
}

#[test]
fn original_plan_text_read_accounting_and_redacted_evidence_are_identical_across_encodings() {
    for padding in [1, COMPRESS_FROM_BYTES] {
        let xml = format!(
            r#"<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementText="SELECT 'private🦀'"><QueryPlan><ParameterList><ColumnReference Column="@p" ParameterCompiledValue="'private🦀'"/></ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence>{}</ShowPlanXML>"#,
            " ".repeat(padding)
        );
        let bytes = utf16(&xml);
        let n = bytes.len();
        let old = wire_row("payload", Val::Text(xml.clone()), n);
        let wire = if n >= COMPRESS_FROM_BYTES { gzip(&bytes) } else { bytes };
        let mut restored = wire_row(ENCODED_COLUMN, Val::Bytes(wire), n);
        restore(&mut restored, n).unwrap();
        assert_eq!(value_bytes(&old), value_bytes(&restored));
        let plan = Payload::read(&restored, Some(n), n).unwrap();
        assert_eq!(plan.text(), Some(xml.as_str()));
        let archived = DefinitionPayload::plan(plan.text().unwrap(), Engine::Mssql);
        assert!(matches!(&archived, DefinitionPayload::Available { .. }));
        assert_eq!(archived, DefinitionPayload::plan(&xml, Engine::Mssql));
    }
}

#[test]
fn byte_limits_and_invalid_transport_cannot_publish_partial_plans() {
    let n = COMPRESS_FROM_BYTES;
    let raw = vec![65; n];
    let valid = gzip(&raw);
    let mut extra = valid.clone();
    extra.extend_from_slice(&[0, 0]);
    let mut corrupt = valid.clone();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    for (body, size, limit) in [
        (valid.clone(), n, n - 1),
        (gzip(&vec![0; n + 100]), n, n),
        (valid[..valid.len() - 1].to_vec(), n, n),
        (extra, n, n),
        (corrupt, n, n),
        (valid, n + 2, n + 2),
        (vec![0x00, 0xd8], 2, 2),
        (vec![0], 1, 1),
    ] {
        let mut row = wire_row(ENCODED_COLUMN, Val::Bytes(body), size);
        assert!(restore(&mut row, limit).is_err());
    }
}

#[test]
fn withheld_payloads_preserve_the_existing_reasons() {
    use culprit_core::telemetry::PayloadUnavailable::*;
    for (encrypted, restricted, size, limit, expected) in [
        (true, false, 10, 10, Encrypted),
        (false, true, 10, 10, Restricted),
        (false, false, 20, 10, Oversized),
        (false, false, 0, 10, Missing),
    ] {
        let mut row = Rec::of(&[
            (ENCODED_COLUMN, Val::Null),
            ("source_bytes", Val::Int(size as i64)),
            ("encrypted", Val::Bool(encrypted)),
            ("restricted", Val::Bool(restricted)),
        ]);
        restore(&mut row, limit).unwrap();
        assert!(
            matches!(Payload::read(&row,Some(size),limit).unwrap(),Payload::Unavailable(reason) if reason==expected)
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback SQL Server and CULPRIT_MSSQL_SOURCE_TEST_PORT / CULPRIT_MSSQL_SOURCE_TEST_PASSWORD"]
async fn native_plan_transfer_restores_sql_server_utf16_and_gzip_before_budgeting() {
    use crate::{conn::Pool, read::ReadSession, test_support::config};
    use culprit_core::telemetry::{CollectionBudget, SourceShutdown};
    use std::sync::Arc;
    let port = std::env::var("CULPRIT_MSSQL_SOURCE_TEST_PORT").unwrap().parse().unwrap();
    let password = std::env::var("CULPRIT_MSSQL_SOURCE_TEST_PASSWORD").unwrap();
    let pool = Arc::new(Pool::new(config(port, "master", &password, "culprit"), None));
    let mut session = ReadSession::new(pool, CollectionBudget::default());
    for repeats in [3, COMPRESS_FROM_BYTES] {
        let raw = format!("<ShowPlanXML>{}</ShowPlanXML>", "λ🦀".repeat(repeats));
        let n = raw.encode_utf16().count() * 2;
        let mut charges = Vec::new();
        for transfer in [Transfer::Text, Transfer::GzipLargePlans] {
            let (value, column) = transfer.projection();
            let sql = format!(
                "SELECT /* culprit */ DATALENGTH(p.query_plan) AS source_bytes,0 AS encrypted,0 AS restricted,CASE WHEN DATALENGTH(p.query_plan)<=@P2 THEN {value} END AS {column} FROM (SELECT CONVERT(nvarchar(max),@P1) AS query_plan) p"
            );
            let rows =
                session.query(&sql, vec![Parameter::Text(raw.clone()), Parameter::Integer(n as i64)], 1).await.unwrap();
            assert_eq!(rows.len(), 1);
            charges.push(value_bytes(&rows[0]));
            assert_eq!(Payload::read(&rows[0], Some(n), n).unwrap().text(), Some(raw.as_str()));
        }
        assert_eq!(charges[0], charges[1]);
    }
    assert_eq!(session.close().await, SourceShutdown::Quiescent);
}
