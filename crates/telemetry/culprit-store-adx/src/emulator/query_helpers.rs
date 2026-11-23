mod deployment;
mod eligibility;
mod heads;
mod matrix;
use super::*;
use culprit_evidence::analytical::query_helpers::{QueryHelperBinding, QueryHelperPolicy};
use culprit_evidence::analytical::test_support;
fn binding(range: &CommittedRange, analyzer: culprit_core::ComponentVersion, rules: String) -> QueryHelperBinding {
    QueryHelperBinding::new(
        QueryHelperPolicy {
            name: "FriendlySmoke".into(),
            analyzer,
            rules_version: rules,
            history_from: "2019-01-01T00:00:00Z".into(),
        },
        &range.manifest().contract,
        range.store_id(),
    )
    .unwrap()
}
async fn count(emulator: &Emulator, call: &str) -> u64 {
    emulator
        .values(
            format!("{call} | summarize N=count()"),
            BTreeMap::new(),
            vec![Column { column_name: "N".into(), column_type: "long".into() }],
        )
        .await[0][0]
        .as_u64()
        .unwrap()
}
async fn assert_first_unavailable(emulator: &Emulator, label: &str, asof: &str) {
    let query = format!(
        "FriendlySmokeStatus({label},datetime({asof}),3650d) | project State,UnavailableFirstObservations | distinct State,UnavailableFirstObservations"
    );
    let state = emulator
        .values(
            query,
            BTreeMap::new(),
            vec![
                Column { column_name: "State".into(), column_type: "string".into() },
                Column { column_name: "UnavailableFirstObservations".into(), column_type: "long".into() },
            ],
        )
        .await;
    assert_eq!(state, vec![vec![json!("first-observation-unavailable"), json!(1)]]);
}
pub(super) async fn run(emulator: &Arc<Emulator>, catalog: &SchemaCatalog) {
    deployment::assert_compiled_template();
    let (source, output) = test_support::derived_diagnosis_output(false, false);
    assert_eq!(output.incidents[0].entry.revision, "1");
    assert_eq!(output.incidents[0].entry.lifecycle, DiagnosticLifecycle::Active);
    let label = serde_json::to_string(&output.description.display.connection_alias).unwrap();
    let original_time = chrono::DateTime::parse_from_rfc3339(&output.incidents[0].entry.recorded_at).unwrap();
    let from = (original_time - chrono::Duration::minutes(1)).to_rfc3339();
    let until = (original_time + chrono::Duration::minutes(1)).to_rfc3339();
    let later = (original_time + chrono::Duration::days(7)).to_rfc3339();
    let after = (original_time + chrono::Duration::days(8)).to_rfc3339();
    let first = Scenario::new(
        Recovery::capture("adx:test", catalog.digest(), source.clone(), output.clone().into_records().collect(), 6_100),
        emulator.clone(),
    )
    .await;
    let commands = crate::query_assets::configured(binding(
        &first.range,
        output.description.analyzer.clone(),
        output.description.rules_version.clone(),
    ))
    .unwrap()
    .commands;
    for command in &commands {
        emulator.management(true, command);
    }
    deployment::assert_stored_bodies(emulator, &commands);
    first.dispatch(emulator).await;
    let contract = serde_json::to_string(&first.range.manifest().contract.id().unwrap()).unwrap();
    let store = serde_json::to_string(first.range.store_id()).unwrap();
    assert_eq!(
        count(emulator, &format!("CulpritPublishedNamespaceV1({contract},{store},datetime(2019),now())")).await,
        0,
        "unpublished source headers cannot supply resource names or batch membership"
    );
    assert_eq!(
        count(emulator, &format!("CulpritIncidentsV1NamespaceRows({contract},{store},datetime(2019),now())")).await,
        0,
        "landed incident rows remain unavailable before their publication"
    );
    for stage in ["IncidentCompleteness", "HeadBasis", "Heads"] {
        assert_eq!(
            count(emulator, &format!("FriendlySmoke{stage}(now())")).await,
            0,
            "{stage} cannot promote an unpublished incident"
        );
    }
    let selection = emulator
        .values(
            format!("FriendlySmokeResourceSelection({label},now()) | project MatchCount,ResourceId"),
            BTreeMap::new(),
            vec![
                Column { column_name: "MatchCount".into(), column_type: "long".into() },
                Column { column_name: "ResourceId".into(), column_type: "string".into() },
            ],
        )
        .await;
    assert_eq!(selection, vec![vec![json!(0), json!("")]], "an unpublished alias cannot select a resource");
    let unpublished_window = format!("datetime({from}),datetime({until}),now()");
    assert_eq!(
        count(emulator, &format!("FriendlySmokeIncidentWindow({unpublished_window})")).await,
        0,
        "window projection cannot expose an unpublished head"
    );
    assert_eq!(
        count(emulator, &format!("FriendlySmokeIncidents({label},{unpublished_window})")).await,
        0,
        "resource selection composed with window projection cannot expose unpublished rows"
    );
    assert_eq!(
        count(emulator, &format!("FriendlySmokeIncidentsDuring({label},datetime({from}),datetime({until}))")).await,
        0,
        "a convenience wrapper cannot expose an unpublished Gold row or resource label"
    );
    assert_eq!(
        count(emulator, &format!("FriendlySmokeResources(datetime({after}))")).await,
        0,
        "unpublished names are not visible"
    );
    first.publish(emulator).await;
    let window = format!("{label},datetime({from}),datetime({until})");
    // Nonempty data is the failing boundary. Collect every stage and equivalent wrapper before the
    // production assertion; empty-only planning success cannot establish that the published path works.
    matrix::published(emulator, &commands, matrix::Window { resource: &label, from: &from, until: &until }).await;
    heads::assert_selection(emulator, &commands).await;
    assert_eq!(
        count(emulator, &format!("FriendlySmokeIncidentWindow(datetime({from}),datetime({until}),datetime({after}))"))
            .await,
        1
    );
    assert_eq!(
        count(
            emulator,
            &format!("FriendlySmokeNewIncidentWindow(datetime({from}),datetime({until}),datetime({after}))")
        )
        .await,
        1
    );
    assert_eq!(
        count(emulator, &format!("FriendlySmokeCollectionStatus(datetime({after}),3650d)")).await,
        0,
        "publishing Gold does not prove a fresh source acquisition"
    );

    for family in ["Findings", "Changes"] {
        assert_eq!(
            count(emulator, &format!("FriendlySmoke{family}During({label},datetime(2019),datetime({after}))")).await,
            1
        );
    }

    assert_eq!(count(emulator, &format!("FriendlySmokeRecordedIncidentAlertsDuring({window})")).await, 1);
    assert_eq!(
        count(emulator, &format!("FriendlySmokeRecordedIncidentAlertsDuring({label},ago(5m),now())")).await,
        0,
        "an original from2020 published now is not a newly observed current outage"
    );
    let provenance = emulator
        .values(
            format!("FriendlySmokeIncidentsDuring({window}) | project PublicationTimeKnown"),
            BTreeMap::new(),
            vec![Column { column_name: "PublicationTimeKnown".into(), column_type: "bool".into() }],
        )
        .await;
    assert_eq!(provenance, vec![vec![json!(false)]]);
    let unknown = emulator
        .values(
            "FriendlySmokeStatusNow(\"not-observed\",30m) | project State,PublicationTimeKnown".into(),
            BTreeMap::new(),
            vec![
                Column { column_name: "State".into(), column_type: "string".into() },
                Column { column_name: "PublicationTimeKnown".into(), column_type: "bool".into() },
            ],
        )
        .await;
    assert_eq!(unknown, vec![vec![json!("unknown-label"), json!(false)]]);
    // Exercise the actual namespace renderer with only its native row input changed. Keeping the
    // published RangeId/batch/ordinal intact prevents a conflict guard from masking the owner check.
    let schema = catalog.get(TelemetryStream::DerivedIncidents).unwrap();
    let eligible =
        crate::eligibility::eligible(schema, culprit_evidence::analytical::semantics::EligibilityScope::Namespace);
    let native_input = "    CulpritIncidentsV1\n";
    assert_eq!(eligible.matches(native_input).count(), 1);
    let foreign_resource = "f".repeat(64);
    assert_ne!(first.range.manifest().source.target.telemetry_id().unwrap(), foreign_resource);
    let foreign_resource = serde_json::to_string(&foreign_resource).unwrap();
    let foreign = eligible
        .replacen("CulpritIncidentsV1NamespaceEligible(", "FriendlySmokeForeignResource(", 1)
        .replacen(native_input, &format!("    CulpritIncidentsV1 | extend ResourceId={foreign_resource}\n"), 1);
    emulator.management(true, &foreign);
    let contract = serde_json::to_string(&first.range.manifest().contract.id().unwrap()).unwrap();
    let store = serde_json::to_string(first.range.store_id()).unwrap();
    assert_eq!(
        count(emulator, &format!("FriendlySmokeForeignResource({contract},{store},datetime(2019),datetime({after}))"))
            .await,
        0,
        "a valid published range/batch cannot lend its authority to a different row resource"
    );
    eligibility::assert_checked_cohort(emulator, &first.range, &after).await;
    let new_call = format!("FriendlySmokeNewIncidents({label},datetime({from}),datetime({until}),datetime({after}))");
    assert_eq!(count(emulator, &new_call).await, 1);
    assert_eq!(
        count(emulator, &format!("FriendlySmokeFindings({label},datetime(2019),datetime({after}),datetime({after}))"))
            .await,
        1
    );
    assert_eq!(
        count(emulator, &format!("FriendlySmokeChanges({label},datetime(2019),datetime({after}),datetime({after}))"))
            .await,
        1
    );
    let names = emulator
        .values(
            format!("FriendlySmokeResources(datetime({after})) | project Alias,DatabaseNameKnown"),
            BTreeMap::new(),
            vec![
                Column { column_name: "Alias".into(), column_type: "string".into() },
                Column { column_name: "DatabaseNameKnown".into(), column_type: "bool".into() },
            ],
        )
        .await;
    assert_eq!(names, vec![vec![json!(output.description.display.connection_alias), json!(false)]]);
    // Reanalysis changes interpretation time, never the original revision1 observation clock.
    let mut replay = output.clone();
    replay.description.recorded_at = later.clone();
    replay.validate().unwrap();
    let mut replay_source = source;
    replay_source.observation = SourceObservation::DerivedDiagnosis(Box::new(replay.description.clone()));
    let replay = Scenario::new(
        Recovery::capture("adx:test", catalog.digest(), replay_source, replay.into_records().collect(), 6_101),
        emulator.clone(),
    )
    .await;
    replay.dispatch(emulator).await;
    replay.publish(emulator).await;
    assert_eq!(
        count(
            emulator,
            &format!("FriendlySmokeNewIncidents({label},datetime({later}),datetime({after}),datetime({after}))")
        )
        .await,
        0
    );
    // A later unique active revision must not hide disagreement about the retained first observation.
    for (sequence, case) in [
        (6_102, test_support::FirstObservationCase::LaterActive),
        (6_103, test_support::FirstObservationCase::ConflictingClock),
    ] {
        let (source, output) = test_support::first_observation_evidence(false, case);
        let scenario = Scenario::new(
            Recovery::capture("adx:test", catalog.digest(), source, output.into_records().collect(), sequence),
            emulator.clone(),
        )
        .await;
        scenario.dispatch(emulator).await;
        scenario.publish(emulator).await;
    }
    assert_eq!(count(emulator,&format!("FriendlySmokeIncidents({label},datetime(2019),datetime({after}),datetime({after})) | where Lifecycle == \"active\" and Revision == todecimal(2)")).await,1);
    assert_eq!(
        count(emulator, &new_call).await,
        0,
        "conflicting original clocks withhold novelty even with a unique later active head"
    );
    assert_eq!(
        count(
            emulator,
            &format!("FriendlySmokeNewIncidents({label},datetime({later}),datetime({after}),datetime({after}))")
        )
        .await,
        0,
        "the conflicting later clock cannot create a fresh alert"
    );
    assert_first_unavailable(emulator, &label, &after).await;
    let (resolved_source, resolved) = test_support::unresolved_derived_diagnosis_output();
    assert_eq!(resolved.incidents[0].entry.lifecycle, DiagnosticLifecycle::Resolved);
    assert_eq!(resolved.incidents[0].entry.episode_id, output.incidents[0].entry.episode_id);
    let resolved = Scenario::new(
        Recovery::capture("adx:test", catalog.digest(), resolved_source, resolved.into_records().collect(), 6_104),
        emulator.clone(),
    )
    .await;
    resolved.dispatch(emulator).await;
    resolved.publish(emulator).await;
    assert_eq!(count(emulator, &new_call).await, 0, "later resolution suppresses old active onset");
    assert_eq!(
        count(
            emulator,
            &format!("FriendlySmokeNewIncidentWindow(datetime({from}),datetime({until}),datetime({after}))")
        )
        .await,
        0
    );
    assert_eq!(count(emulator, &format!("FriendlySmokeIncidentWindow(datetime({from}),datetime({until}),datetime({after})) | where Lifecycle == \"resolved\"")).await, 1);

    assert_eq!(count(emulator, &format!("FriendlySmokeRecordedIncidentAlertsDuring({window})")).await, 0);
    assert_eq!(
        count(emulator, &format!("FriendlySmokeIncidentsDuring({window}) | where Lifecycle == \"resolved\"")).await,
        1
    );

    assert_eq!(count(emulator,&format!("FriendlySmokeIncidents({label},datetime(2019),datetime({after}),datetime({after})) | where Lifecycle == \"resolved\"")).await,1);
    assert!(
        count(
            emulator,
            &format!("FriendlySmokeIncidentHistory({label},datetime(2019),datetime({after}),datetime({after}))")
        )
        .await
            >= 2
    );
    let state = emulator
        .values(
            format!("FriendlySmokeStatus(\"not-observed\",datetime({after}),30m) | project State"),
            BTreeMap::new(),
            vec![Column { column_name: "State".into(), column_type: "string".into() }],
        )
        .await;
    assert_eq!(state, vec![vec![json!("unknown-label")]]);
    // Existing exact-member conflict invalidates this published family; it cannot revive an older active head.
    {
        let objects = resolved.blobs.objects.lock().unwrap();
        let mut row: Value = serde_json::from_slice(
            objects.iter().find(|(key, _)| key.starts_with("ready/CulpritIncidentsV1/")).unwrap().1,
        )
        .unwrap();
        row["RowOrdinal"] = json!(99);
        emulator.ingest("CulpritIncidentsV1", &serde_json::to_vec(&row).unwrap());
    }
    assert_eq!(count(emulator, &new_call).await, 0);
    let state = emulator
        .values(
            format!("FriendlySmokeStatus({label},datetime({after}),3650d) | project State"),
            BTreeMap::new(),
            vec![Column { column_name: "State".into(), column_type: "string".into() }],
        )
        .await;
    assert!(state.iter().any(|row| row[0] == "incomplete-output"));
    let mut reference_label = None;
    for (sequence, case) in [
        (6_105, test_support::FirstObservationCase::LaterActive),
        (6_106, test_support::FirstObservationCase::Original),
        (6_107, test_support::FirstObservationCase::ConflictingReference),
    ] {
        let (source, output) = test_support::first_observation_evidence(true, case);
        reference_label = Some(serde_json::to_string(&output.description.display.connection_alias).unwrap());
        let scenario = Scenario::new(
            Recovery::capture("adx:test", catalog.digest(), source, output.into_records().collect(), sequence),
            emulator.clone(),
        )
        .await;
        scenario.dispatch(emulator).await;
        scenario.publish(emulator).await;
        let selected = reference_label.as_ref().unwrap();
        let call =
            format!("FriendlySmokeNewIncidents({selected},datetime({from}),datetime({until}),datetime({after}))");
        match case {
            test_support::FirstObservationCase::LaterActive => {
                assert_eq!(count(emulator, &call).await, 0, "a retained later head cannot invent revision1");
                assert_first_unavailable(emulator, selected, &after).await;
            }
            test_support::FirstObservationCase::Original => {
                assert_eq!(
                    count(emulator, &call).await,
                    1,
                    "one consistent first original establishes recorded novelty"
                );
            }
            _ => {}
        }
    }
    let reference_label = reference_label.unwrap();
    assert_eq!(count(emulator,&format!("FriendlySmokeIncidents({reference_label},datetime(2019),datetime({after}),datetime({after})) | where Lifecycle == \"active\" and Revision == todecimal(2)")).await,1);
    assert_eq!(
        count(
            emulator,
            &format!(
                "FriendlySmokeNewIncidents({reference_label},datetime({from}),datetime({until}),datetime({after}))"
            )
        )
        .await,
        0,
        "conflicting first receipts cannot be collapsed into a known onset"
    );
    assert_first_unavailable(emulator, &reference_label, &after).await;
}
