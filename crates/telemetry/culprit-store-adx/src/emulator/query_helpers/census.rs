//! Compare the compact native census with the independent old-order oracle and real published rows.
mod oracle;
use super::*;
use culprit_core::ComponentVersion;

const MAX_COHORTS_PER_QUERY: usize = 32;

pub(crate) struct RawBatch {
    pub request: QueryRequest,
    pub expected: Vec<Vec<Value>>,
    pub cases: std::ops::Range<usize>,
    pub row_count: usize,
}
pub(super) async fn assert_raw_cohorts(emulator: &Emulator, analyzer: &ComponentVersion, rules: &str) {
    let mut cases = 0;
    let mut rows = 0;
    for batch in raw_batches(analyzer, rules) {
        cases += batch.cases.len();
        rows += batch.row_count;
        let actual = emulator.query(batch.request).await.expect("bounded raw census request").rows;
        assert_eq!(
            actual, batch.expected,
            "compact KQL must match the independent old-order oracle in every complete cohort batch"
        );
    }
    eprintln!("Owned emulator compact census matched {cases} raw cohorts ({rows} rows)");
    assert_nonnull_signatures(emulator).await;
}
pub(crate) fn raw_batches(analyzer: &ComponentVersion, rules: &str) -> Vec<RawBatch> {
    oracle::cohorts()
        .chunks(MAX_COHORTS_PER_QUERY)
        .enumerate()
        .map(|(batch, cohorts)| raw_batch(batch * MAX_COHORTS_PER_QUERY, cohorts, analyzer, rules))
        .collect()
}
fn raw_batch(first_case: usize, cohorts: &[Vec<oracle::Row>], analyzer: &ComponentVersion, rules: &str) -> RawBatch {
    let mut records = Vec::new();
    let mut expected = Vec::new();
    for (offset, rows) in cohorts.iter().enumerate() {
        let case = first_case + offset;
        records.extend(rows.iter().map(|row| literals(case, row)));
        for (key, flags) in oracle::legacy(rows) {
            let mut values = key_values(case, key);
            values.extend([json!(flags.selected), json!(flags.first)]);
            expected.push(values);
        }
    }
    let mut foreign_analyzer = (*analyzer).clone();
    foreign_analyzer.component.push_str("-oracle-foreign");
    let foreign_analyzer = serde_json::to_string(&foreign_analyzer).unwrap();
    let analyzer = serde_json::to_string(analyzer).unwrap();
    let foreign_rules = serde_json::to_string(&format!("{rules}-oracle-foreign")).unwrap();
    let rules = serde_json::to_string(rules).unwrap();
    let query = format!(
        r#"let Rows=datatable(ResourceId:string,TargetId:string,OriginalScopeId:string,OriginalSource:string,EpisodeId:string,ControllerVersion:long,Revision:decimal,RevisionId:string,OriginalRecordedAt:datetime,Lifecycle:string,OriginalReference:dynamic,RecordedAt:datetime,OracleAnalyzer:bool,OracleRules:bool,AnalysisId:string,DetailSha256:string,InputReadComplete:bool)
[
{}
]
| extend Analyzer=iff(OracleAnalyzer,dynamic({analyzer}),dynamic({foreign_analyzer})),RulesVersion=iff(OracleRules,{rules},{foreign_rules}),OutputCoverage=dynamic({{}}),OutputLimitations=dynamic([]),FindingEvaluations=dynamic([]),ChangeEvaluations=dynamic([]),ContextIssues=dynamic([]),OriginalObservedFrom=datetime(2020-01-01),OriginalObservedUntil=datetime(2020-01-02),InputReferences=dynamic([]),AnalysisOrigin="collected",Language="en";
FriendlySmokeEpisodeCensus(Rows) | project ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,SelectedInterpretation,HasFirstOriginal
| sort by ResourceId asc,TargetId asc,OriginalScopeId asc,OriginalSource asc,EpisodeId asc,ControllerVersion asc"#,
        records.join(",\n")
    );
    let mut columns = ["ResourceId", "TargetId", "OriginalScopeId", "OriginalSource", "EpisodeId"]
        .into_iter()
        .map(|name| Column { column_name: name.into(), column_type: "string".into() })
        .collect::<Vec<_>>();
    columns.extend([
        Column { column_name: "ControllerVersion".into(), column_type: "long".into() },
        Column { column_name: "SelectedInterpretation".into(), column_type: "bool".into() },
        Column { column_name: "HasFirstOriginal".into(), column_type: "bool".into() },
    ]);
    RawBatch {
        request: QueryRequest {
            text: query,
            parameters: BTreeMap::new(),
            columns,
            row_limit: 10_000,
            byte_limit: crate::response::MAX_RESPONSE_BYTES,
        },
        expected,
        cases: first_case..first_case + cohorts.len(),
        row_count: records.len(),
    }
}

fn key_values(case: usize, key: oracle::Key) -> Vec<Value> {
    vec![
        json!(format!("oracle-case-{case:03}-resource-{}", key[0])),
        json!(format!("target-{}", key[1])),
        json!(format!("scope-{}", key[2])),
        json!(format!("source-{}", key[3])),
        json!(format!("episode-{}", key[4])),
        json!(u64::from(key[5]) + 1),
    ]
}
fn literals(case: usize, row: &oracle::Row) -> String {
    let mut values = key_values(case, row.key).iter().map(Value::to_string).collect::<Vec<_>>();
    values.extend([
        row.revision.map(|value| format!("decimal({value}.0)")).unwrap_or_else(|| "decimal(null)".into()),
        serde_json::to_string(row.original_id).unwrap(),
        clock(row.original_at),
        serde_json::to_string(row.lifecycle).unwrap(),
        format!("dynamic({})", row.reference),
        clock(row.recorded_at),
        row.analyzer_matches.to_string(),
        row.rules_match.to_string(),
        serde_json::to_string(row.analysis).unwrap(),
        serde_json::to_string(row.detail).unwrap(),
        row.input_complete.to_string(),
    ]);
    values.join(",")
}
fn clock(seconds: Option<u32>) -> String {
    let Some(seconds) = seconds else {
        return "datetime(null)".into();
    };
    let start = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap();
    format!("datetime({})", (start + chrono::Duration::seconds(i64::from(seconds))).to_rfc3339())
}
async fn assert_nonnull_signatures(emulator: &Emulator) {
    let values = emulator.values(r#"datatable(RevisionId:string,OriginalRecordedAt:datetime,Lifecycle:string,OriginalReference:string)
["",datetime(null),"","", "null",datetime(null),"","", "",datetime(2020-01-01),"","", "a,b",datetime(null),"c","d", "a",datetime(null),"b,c","d", "a\"\\\n",datetime(null),"c","d"]
| project Signature=FriendlySmokeOriginalSignature(RevisionId,OriginalRecordedAt,Lifecycle,OriginalReference)"#.into(), BTreeMap::new(), vec![Column { column_name: "Signature".into(), column_type: "string".into() }]).await;
    let mut originals = std::collections::BTreeSet::new();
    for row in values {
        let signature = row[0].as_str().unwrap();
        let decoded: Value = serde_json::from_str(signature).unwrap();
        assert_eq!(decoded.as_array().unwrap().len(), 4);
        assert!(originals.insert(signature.to_owned()), "typed original classes must not collide or disappear");
    }
    assert_eq!(originals.len(), 6);
    let values = emulator.values(r#"datatable(InputReadComplete:bool,OutputCoverage:dynamic)
[bool(null),dynamic(null), false,dynamic(null), bool(null),dynamic({})]
| project Signature=FriendlySmokeInterpretationSignature("","",InputReadComplete,OutputCoverage,dynamic(null),dynamic(null),dynamic(null),dynamic(null),datetime(null),datetime(null),dynamic(null),"","")"#.into(),BTreeMap::new(),vec![Column { column_name: "Signature".into(), column_type: "string".into() }]).await;
    let mut interpretations = std::collections::BTreeSet::new();
    for row in values {
        let signature = row[0].as_str().unwrap();
        let decoded: Value = serde_json::from_str(signature).unwrap();
        assert_eq!(decoded.as_array().unwrap().len(), 13);
        assert!(
            interpretations.insert(signature.to_owned()),
            "NULL and empty typed interpretation classes must remain distinct"
        );
    }
    assert_eq!(interpretations.len(), 3);
    eprintln!("Owned emulator original/interpretation signatures kept all nullable and escaped classes");
}

pub(super) async fn assert_published_counts(emulator: &Emulator, asof: &str) {
    let query = format!(
        "let Rows=materialize(FriendlySmokeIncidentRows(datetime({asof})));let Complete=FriendlySmokeIncidentCompletenessFromRows(Rows,datetime({asof})) | summarize MissingRows=countif(not(IncidentRowsComplete)) by ResourceId;FriendlySmokeEpisodeCensus(Rows) | join kind=leftouter Complete on ResourceId | extend Selected=SelectedInterpretation and coalesce(MissingRows == 0,false) | summarize Originals=count(),Selected=countif(Selected),Missing=countif(Selected and not(HasFirstOriginal))"
    );
    let counts = emulator
        .values(
            query,
            BTreeMap::new(),
            ["Originals", "Selected", "Missing"]
                .into_iter()
                .map(|name| Column { column_name: name.into(), column_type: "long".into() })
                .collect(),
        )
        .await;
    let originals = count(emulator,&format!("FriendlySmokeIncidentRows(datetime({asof})) | distinct ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion")).await;
    let heads = count(emulator, &format!("FriendlySmokeHeads(datetime({asof}))")).await;
    let missing = count(emulator,&format!("FriendlySmokeHeads(datetime({asof})) | join kind=leftanti (FriendlySmokeConsistentOriginals(datetime({asof})) | where Revision == todecimal(1) | distinct ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion) on ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion")).await;
    assert_eq!(
        counts,
        vec![vec![json!(originals), json!(heads), json!(missing)]],
        "compact census must match genuine published heads and first-original keys"
    );
    let first_columns: Vec<Column> = [
        ("ResourceId", "string"),
        ("TargetId", "string"),
        ("OriginalScopeId", "string"),
        ("OriginalSource", "string"),
        ("EpisodeId", "string"),
        ("ControllerVersion", "long"),
        ("OriginalRecordedAt", "datetime"),
    ]
    .into_iter()
    .map(|(name, kind)| Column { column_name: name.into(), column_type: kind.into() })
    .collect();
    let consistent_first = emulator
        .values(
            format!(
                "FriendlySmokeConsistentOriginals(datetime({asof})) | where Revision == todecimal(1) | project ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,OriginalRecordedAt | sort by ResourceId asc,TargetId asc,OriginalScopeId asc,OriginalSource asc,EpisodeId asc,ControllerVersion asc"
            ),
            BTreeMap::new(),
            first_columns.clone(),
        )
        .await;
    let legacy_first = emulator
        .values(
            format!(
                "let Rows=materialize(FriendlySmokeIncidentRows(datetime({asof})));Rows | join kind=leftsemi (FriendlySmokeConsistentOriginalKeys(Rows)) on ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,Revision | where Revision == todecimal(1) | distinct ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,OriginalRecordedAt | sort by ResourceId asc,TargetId asc,OriginalScopeId asc,OriginalSource asc,EpisodeId asc,ControllerVersion asc"
            ),
            BTreeMap::new(),
            first_columns,
        )
        .await;
    assert_eq!(
        consistent_first, legacy_first,
        "single-pass ConsistentOriginals must preserve exact first-observation clocks"
    );
    eprintln!("Owned emulator compact census matched genuine published head/first counts");
}
