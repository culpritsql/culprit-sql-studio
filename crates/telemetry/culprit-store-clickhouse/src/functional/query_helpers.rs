use super::*;
use culprit_evidence::analytical::query_helpers::{QueryHelperBinding, QueryHelperPolicy};
use culprit_evidence::analytical::test_support;
async fn count(server: &Server, call: &str) -> u64 {
    server.values(format!("SELECT count() AS n FROM {call}"), BTreeMap::new(), &[("n", "UInt64")]).await[0][0]
        .as_u64()
        .unwrap()
}
async fn assert_first_unavailable(server: &Server, label: &str, asof: i64) {
    let query = format!(
        "SELECT DISTINCT State,UnavailableFirstObservations FROM FriendlySmokeStatus(resource='{label}',asof=fromUnixTimestamp64Nano({asof}),max_age_seconds=315360000)"
    );
    let state =
        server.values(query, BTreeMap::new(), &[("State", "String"), ("UnavailableFirstObservations", "UInt64")]).await;
    assert_eq!(state, vec![vec![json!("first-observation-unavailable"), json!(1)]]);
}
pub(super) async fn run(server: &Server, catalog: &SchemaCatalog) {
    let (source, output) = test_support::derived_diagnosis_output(false, false);
    assert_eq!(output.incidents[0].entry.revision, "1");
    assert_eq!(output.incidents[0].entry.lifecycle, DiagnosticLifecycle::Active);
    let label = output.description.display.connection_alias.replace('\\', "\\\\").replace('\'', "\\'");
    let original_time = chrono::DateTime::parse_from_rfc3339(&output.incidents[0].entry.recorded_at).unwrap();
    let from = (original_time - chrono::Duration::minutes(1)).timestamp_nanos_opt().unwrap();
    let until = (original_time + chrono::Duration::minutes(1)).timestamp_nanos_opt().unwrap();
    let later = (original_time + chrono::Duration::days(7)).timestamp_nanos_opt().unwrap();
    let after = (original_time + chrono::Duration::days(8)).timestamp_nanos_opt().unwrap();
    let args = format!(
        "resource='{label}',from=fromUnixTimestamp64Nano({from}),until=fromUnixTimestamp64Nano({until}),asof=fromUnixTimestamp64Nano({after})"
    );
    let first = Scenario::new(
        server,
        Recovery::capture(
            server.client.destination(),
            catalog.digest(),
            source.clone(),
            output.clone().into_records().collect(),
            6_100,
        ),
    )
    .await;
    let binding = QueryHelperBinding::new(
        QueryHelperPolicy {
            name: "FriendlySmoke".into(),
            analyzer: output.description.analyzer.clone(),
            rules_version: output.description.rules_version.clone(),
            history_from: "2019-01-01T00:00:00Z".into(),
        },
        &first.range.manifest().contract,
        first.range.store_id(),
    )
    .unwrap();
    for command in crate::query_assets::configured(binding).unwrap().commands {
        server.execute(&command);
    }
    first.dispatch().await;
    assert_eq!(count(server, &format!("FriendlySmokeIncidentsDuring(resource='{label}',window_from=fromUnixTimestamp64Nano({from}),window_until=fromUnixTimestamp64Nano({until}))")).await, 0,
        "a convenience wrapper cannot expose an unpublished Gold row or resource label");
    assert_eq!(count(server, &format!("FriendlySmokeResources(asof=fromUnixTimestamp64Nano({after}))")).await, 0);
    first.publish().await;
    let window = format!(
        "resource='{label}',window_from=fromUnixTimestamp64Nano({from}),window_until=fromUnixTimestamp64Nano({until})"
    );
    assert_eq!(count(server, &format!("FriendlySmokeIncidentsDuring({window})")).await, 1);
    let namespace_window = format!(
        "from=fromUnixTimestamp64Nano({from}),until=fromUnixTimestamp64Nano({until}),asof=fromUnixTimestamp64Nano({after})"
    );
    assert_eq!(count(server, &format!("FriendlySmokeIncidentWindow({namespace_window})")).await, 1);
    assert_eq!(count(server, &format!("FriendlySmokeNewIncidentWindow({namespace_window})")).await, 1);
    assert_eq!(
        count(
            server,
            &format!("FriendlySmokeCollectionStatus(asof=fromUnixTimestamp64Nano({after}),max_age_seconds=315360000)")
        )
        .await,
        0,
        "publishing Gold does not prove a fresh source acquisition"
    );

    for family in ["Findings", "Changes"] {
        assert_eq!(count(server, &format!("FriendlySmoke{family}During(resource='{label}',window_from=toDateTime64('2019-01-01',9,'UTC'),window_until=fromUnixTimestamp64Nano({after}))")).await, 1);
    }

    assert_eq!(count(server, &format!("FriendlySmokeRecordedIncidentAlertsDuring({window})")).await, 1);
    assert_eq!(count(server, &format!("FriendlySmokeRecordedIncidentAlertsDuring(resource='{label}',window_from=now64(9,'UTC')-INTERVAL 5 MINUTE,window_until=now64(9,'UTC'))")).await, 0,
        "an original from2020 published now is not a newly observed current outage");
    let provenance = server
        .values(
            format!("SELECT PublicationTimeKnown FROM FriendlySmokeIncidentsDuring({window})"),
            BTreeMap::new(),
            &[("PublicationTimeKnown", "Bool")],
        )
        .await;
    assert_eq!(provenance, vec![vec![json!(false)]]);
    let unknown = server.values(
        "SELECT State,PublicationTimeKnown FROM FriendlySmokeStatusNow(resource='not-observed',max_age_seconds=1800)".into(), BTreeMap::new(), &[("State","String"),("PublicationTimeKnown","Bool")],
    ).await;
    assert_eq!(unknown, vec![vec![json!("unknown-label"), json!(false)]]);
    let new_call = format!("FriendlySmokeNewIncidents({args})");
    assert_eq!(count(server, &new_call).await, 1);
    for family in ["Findings", "Changes"] {
        assert_eq!(count(server,&format!("FriendlySmoke{family}(resource='{label}',from=toDateTime64('2019-01-01',9,'UTC'),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after}))")).await,1);
    }
    let names = server
        .values(
            format!(
                "SELECT Alias,DatabaseNameKnown,toBool(OutputRowsComplete) AS OutputRowsComplete FROM FriendlySmokeResources(asof=fromUnixTimestamp64Nano({after}))"
            ),
            BTreeMap::new(),
            &[("Alias", "String"), ("DatabaseNameKnown", "Bool"), ("OutputRowsComplete", "Bool")],
        )
        .await;
    assert_eq!(names, vec![vec![json!(output.description.display.connection_alias), json!(false), json!(true)]]);
    let mut replay = output.clone();
    replay.description.recorded_at = (original_time + chrono::Duration::days(7)).to_rfc3339();
    replay.validate().unwrap();
    let mut replay_source = source;
    replay_source.observation = SourceObservation::DerivedDiagnosis(Box::new(replay.description.clone()));
    let replay = Scenario::new(
        server,
        Recovery::capture(
            server.client.destination(),
            catalog.digest(),
            replay_source,
            replay.into_records().collect(),
            6_101,
        ),
    )
    .await;
    replay.complete().await;
    assert_eq!(count(server,&format!("FriendlySmokeNewIncidents(resource='{label}',from=fromUnixTimestamp64Nano({later}),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after}))")).await,0);
    for (sequence, case) in [
        (6_102, test_support::FirstObservationCase::LaterActive),
        (6_103, test_support::FirstObservationCase::ConflictingClock),
    ] {
        let (source, output) = test_support::first_observation_evidence(false, case);
        let scenario = Scenario::new(
            server,
            Recovery::capture(
                server.client.destination(),
                catalog.digest(),
                source,
                output.into_records().collect(),
                sequence,
            ),
        )
        .await;
        scenario.complete().await;
    }
    assert_eq!(count(server,&format!("FriendlySmokeIncidents(resource='{label}',from=toDateTime64('2019-01-01',9,'UTC'),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after})) WHERE Lifecycle='active' AND Revision=toDecimal128(2,0)")).await,1);
    assert_eq!(
        count(server, &new_call).await,
        0,
        "conflicting original clocks withhold novelty even with a unique later active head"
    );
    assert_eq!(count(server,&format!("FriendlySmokeNewIncidents(resource='{label}',from=fromUnixTimestamp64Nano({later}),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after}))")).await,0,"the conflicting later clock cannot create a fresh alert");
    assert_first_unavailable(server, &label, after).await;
    let (resolved_source, resolved) = test_support::unresolved_derived_diagnosis_output();
    assert_eq!(resolved.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(resolved.incidents[0].entry.episode_id, output.incidents[0].entry.episode_id);
    let resolved = Scenario::new(
        server,
        Recovery::capture(
            server.client.destination(),
            catalog.digest(),
            resolved_source,
            resolved.into_records().collect(),
            6_104,
        ),
    )
    .await;
    resolved.complete().await;
    assert_eq!(count(server, &new_call).await, 0);
    assert_eq!(count(server, &format!("FriendlySmokeNewIncidentWindow({namespace_window})")).await, 0);
    assert_eq!(
        count(server, &format!("FriendlySmokeIncidentWindow({namespace_window}) WHERE Lifecycle='resolved'")).await,
        1
    );

    assert_eq!(count(server, &format!("FriendlySmokeRecordedIncidentAlertsDuring({window})")).await, 0);
    assert_eq!(count(server, &format!("FriendlySmokeIncidentsDuring({window}) WHERE Lifecycle='resolved'")).await, 1);
    let broad = format!(
        "resource='{label}',from=toDateTime64('2019-01-01',9,'UTC'),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after})"
    );
    assert_eq!(
        server
            .values(
                format!("SELECT count() AS n FROM FriendlySmokeIncidents({broad}) WHERE Lifecycle='resolved'"),
                BTreeMap::new(),
                &[("n", "UInt64")]
            )
            .await[0][0]
            .as_u64(),
        Some(1)
    );
    assert!(count(server, &format!("FriendlySmokeIncidentHistory({broad})")).await >= 2);
    let state=server.values(format!("SELECT State FROM FriendlySmokeStatus(resource='not-observed',asof=fromUnixTimestamp64Nano({after}),max_age_seconds=1800)"),BTreeMap::new(),&[("State","String")]).await;
    assert_eq!(state, vec![vec![json!("unknown-label")]]);
    server.execute(&format!("INSERT INTO CulpritIncidentsV1 SELECT * REPLACE (99 AS RowOrdinal) FROM CulpritIncidentsV1 WHERE RangeId='{}' LIMIT 1",resolved.range.manifest().range_id));
    assert_eq!(count(server, &new_call).await, 0);
    let states=server.values(format!("SELECT State FROM FriendlySmokeStatus(resource='{label}',asof=fromUnixTimestamp64Nano({after}),max_age_seconds=315360000)"),BTreeMap::new(),&[("State","String")]).await;
    assert!(states.iter().any(|row| row[0] == "incomplete-output"));
    let mut reference_label = None;
    for (sequence, case) in [
        (6_105, test_support::FirstObservationCase::LaterActive),
        (6_106, test_support::FirstObservationCase::Original),
        (6_107, test_support::FirstObservationCase::ConflictingReference),
    ] {
        let (source, output) = test_support::first_observation_evidence(true, case);
        reference_label = Some(output.description.display.connection_alias.replace('\\', "\\\\").replace('\'', "\\'"));
        let scenario = Scenario::new(
            server,
            Recovery::capture(
                server.client.destination(),
                catalog.digest(),
                source,
                output.into_records().collect(),
                sequence,
            ),
        )
        .await;
        scenario.complete().await;
        let selected = reference_label.as_ref().unwrap();
        let call = format!(
            "FriendlySmokeNewIncidents(resource='{selected}',from=fromUnixTimestamp64Nano({from}),until=fromUnixTimestamp64Nano({until}),asof=fromUnixTimestamp64Nano({after}))"
        );
        match case {
            test_support::FirstObservationCase::LaterActive => {
                assert_eq!(count(server, &call).await, 0, "a retained later head cannot invent revision1");
                assert_first_unavailable(server, selected, after).await;
            }
            test_support::FirstObservationCase::Original => {
                assert_eq!(count(server, &call).await, 1, "one consistent first original establishes recorded novelty");
            }
            _ => {}
        }
    }
    let reference_label = reference_label.unwrap();
    assert_eq!(count(server,&format!("FriendlySmokeIncidents(resource='{reference_label}',from=toDateTime64('2019-01-01',9,'UTC'),until=fromUnixTimestamp64Nano({after}),asof=fromUnixTimestamp64Nano({after})) WHERE Lifecycle='active' AND Revision=toDecimal128(2,0)")).await,1);
    assert_eq!(count(server,&format!("FriendlySmokeNewIncidents(resource='{reference_label}',from=fromUnixTimestamp64Nano({from}),until=fromUnixTimestamp64Nano({until}),asof=fromUnixTimestamp64Nano({after}))")).await,0,"conflicting first receipts cannot be collapsed into a known onset");
    assert_first_unavailable(server, &reference_label, after).await;
}
