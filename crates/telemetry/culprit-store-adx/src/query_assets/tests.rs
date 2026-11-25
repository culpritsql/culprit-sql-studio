use super::*;
use culprit_core::ComponentVersion;
use culprit_evidence::delivery::contract::Contract;
fn binding() -> QueryHelperBinding {
    let catalog = SchemaCatalog::new().unwrap();
    let contract =
        Contract::new("pipeline".into(), "shard".into(), "warehouse".into(), catalog.digest().into()).unwrap();
    QueryHelperBinding::new(
        culprit_evidence::analytical::query_helpers::QueryHelperPolicy {
            name: "Pilot".into(),
            analyzer: ComponentVersion::package("culprit-checks", "1", Some("build-1")),
            rules_version: "rules-2".into(),
            history_from: "2019-01-01T00:00:00Z".into(),
        },
        &contract,
        &"b".repeat(64),
    )
    .unwrap()
}
#[test]
fn friendly_helpers_keep_publication_membership_and_closed_namespace_binding_for_all_three_gold_families() {
    let b = binding();
    let assets = configured(b.clone()).unwrap();
    let text = assets.commands.join("\n");
    assert!(text.contains(&b.contract_id) && text.contains(&b.recovery_store_id) && text.contains(&b.schema_digest));
    for table in ["CulpritIncidentsV1", "CulpritFindingsV1", "CulpritChangesV1"] {
        assert!(text.contains(&format!("{table}NamespaceEligible")));
        assert!(text.contains(&format!("{table}NamespaceRows")));
    }
    for guard in [
        "PublishedManifest",
        "PublishedSchema",
        "PublishedEpoch",
        "PublishedSequence",
        "PublishedReferencesComplete",
        "ExpectedRows",
        "RowOrdinal",
    ] {
        assert!(text.contains(guard), "{guard}");
    }
    for forbidden in
        ["skipvalidation", "ingestion_time()", "COUNT(DISTINCT Query", "argMax(", "CREATE TABLE", ".create table"]
    {
        assert!(!text.contains(forbidden), "{forbidden}");
    }
    assert!(!text.contains('$'));
    assert!(assets.script.starts_with(".execute database script with (ContinueOnErrors=false) <|\n"));
    assert!(assets.script.ends_with('\n'));
    assert!(assets.commands.iter().all(|command| assets.script.contains(command)));
    assert_eq!(assets.examples.len(), 18);
    assert!(
        assets
            .examples
            .iter()
            .any(|example| example.name == "fleet-summary" && example.query.contains("PilotFleetSummary("))
    );
    assert!(
        assets
            .examples
            .iter()
            .all(|example| !example.query.contains(&b.contract_id) && !example.query.contains(&b.recovery_store_id))
    );
    let mut foreign = b;
    foreign.schema_digest = "f".repeat(64);
    assert!(configured(foreign).is_err());
}
#[test]
fn episode_heads_precede_window_and_rules_selection_and_alerts_join_first_original_observation_to_latest_head() {
    let assets = configured(binding()).unwrap();
    let text = assets.commands.join("\n");
    let basis = text.find("PilotHeadBasis").unwrap();
    let heads = text.find("PilotHeads").unwrap();
    let incidents = text.find("PilotIncidents").unwrap();
    assert!(basis < heads && heads < incidents);
    assert!(text[basis..heads].contains("max(Revision)"));
    assert!(
        !text[basis..heads].contains("rules-2"),
        "newer resolved revisions cannot be hidden by pinning an older interpreter first"
    );
    let alert = text.split("PilotNewIncidentWindow").nth(1).unwrap().split("PilotNewIncidents").next().unwrap();
    assert!(
        alert.contains("FirstRecordedObservation")
            && alert.contains("OriginalRecordedAt")
            && alert.contains("PilotHeads")
    );
    assert!(alert.contains("active") && alert.contains("InputReadComplete"));
    assert!(!alert.contains("NativeFirstSeenAt") && !alert.contains("NativeLastSeenAt"));
    assert!(
        text.contains("OutputRowsComplete")
            && text.contains("IncidentRowsComplete")
            && text.contains("incomplete-output")
    );
    assert!(
        text.contains("ambiguous-label")
            && text.contains("unknown-label")
            && text.contains("head-interpretation-unavailable")
            && text.contains("stale")
    );
    for field in [
        "VictimSummary",
        "AffectedQueries",
        "CulpritQueries",
        "CandidateQueries",
        "Mechanism",
        "DeveloperAction",
        "Verification",
        "Caller",
    ] {
        assert!(text.contains(field));
    }
    assert!(text.contains("Header") && text.contains("display") && text.contains("connection_alias"));
    assert!(!text.contains("target.identity.database"));
}
#[test]
fn configured_rules_are_literal_values_and_never_identifiers_or_a_lexical_latest_policy() {
    let mut b = binding();
    b.policy.rules_version = "rules-2'\"\\value".into();
    let assets = configured(b).unwrap();
    let text = assets.commands.join("\n");
    assert!(!text.contains("max(RulesVersion)") && !text.contains("max(Analyzer)"));
    assert!(text.contains("rules-2"));
    let mut b = binding();
    b.policy.name = "Pilot);DROP".into();
    assert!(configured(b).is_err());
}

#[test]
fn namespace_membership_retains_the_published_resource_for_every_gold_family_while_scalar_scope_stays_local() {
    let assets = configured(binding()).unwrap();
    let catalog = SchemaCatalog::new().unwrap();
    for stream in NAMESPACE_QUERY_STREAMS {
        let schema = catalog.get(stream).unwrap();
        let namespace = assets
            .commands
            .iter()
            .find(|command| {
                command.starts_with(&format!(".create-or-alter function {}NamespaceEligible(", schema.table()))
            })
            .unwrap();
        assert!(namespace.contains("PublishedResource=ResourceId"));
        assert!(namespace.contains("or ResourceId != PublishedResource"));
        let scalar = eligibility::eligible(schema, EligibilityScope::Resource);
        assert!(scalar.contains("ResourceId == resource"));
        assert!(!scalar.contains("PublishedResource"));
        assert!(!scalar.contains('$'));
    }
}

#[test]
fn first_observation_consistency_precedes_its_window_and_is_shared_by_heads_alerts_and_status() {
    let assets = configured(binding()).unwrap();
    let definitions = &assets.commands;
    let original =
        definitions.iter().find(|command| command.contains("function PilotConsistentOriginalKeys(")).unwrap();
    for field in ["RevisionId", "OriginalRecordedAt", "Lifecycle", "OriginalReference"] {
        assert!(original.contains(field), "{field}");
    }
    assert!(!original.contains("rules-2") && !original.contains("window_from") && !original.contains("{from:"));
    let head = definitions
        .iter()
        .find(|command| command.contains("VIEW PilotHeadBasis ") || command.contains("function PilotHeadBasis("))
        .unwrap();
    assert!(head.contains("max(Revision)") && head.contains("PilotConsistentOriginalKeys(Rows)"));
    assert!(head.contains("PilotIncidentCompletenessFromRows(OriginalRows,asof)"));
    assert_eq!(head.matches("PilotIncidentRows(asof)").count(), 1);
    assert!(!head.contains("PilotCompleteness(asof)"));
    let alert = definitions
        .iter()
        .find(|command| {
            command.contains("VIEW PilotNewIncidentWindow ") || command.contains("function PilotNewIncidentWindow(")
        })
        .unwrap();
    assert!(alert.contains("PilotConsistentOriginals("));
    let status = definitions
        .iter()
        .find(|command| command.contains("VIEW PilotStatusRows ") || command.contains("function PilotStatusRows("))
        .unwrap();
    assert!(status.contains("PilotEpisodeCensus(Rows)"));
    let census = definitions.iter().find(|command| command.contains("function PilotEpisodeCensus(")).unwrap();
    assert!(census.contains("PilotOriginalRevisionCensus(Rows)"));
    assert!(original.contains("PilotOriginalRevisionCensus(Rows)"));
    assert!(status.contains("first-observation-unavailable") && status.contains("UnavailableFirstObservations"));
}

#[test]
fn normal_resource_windows_delegate_to_the_existing_published_gold_owners_without_plumbing_or_new_rules() {
    let assets = configured(binding()).unwrap();
    for (wrapper, owner) in [
        ("IncidentsDuring", "Incidents"),
        ("FindingsDuring", "Findings"),
        ("ChangesDuring", "Changes"),
        ("RecordedIncidentAlertsDuring", "NewIncidents"),
    ] {
        let command = assets
            .commands
            .iter()
            .find(|command| command.starts_with(&format!(".create-or-alter function Pilot{wrapper}(")))
            .unwrap();
        assert!(command.contains("resource:string,window_from:datetime,window_until:datetime"));
        assert!(command.contains(&format!("Pilot{owner}(resource,window_from,window_until,now())")));
        assert_eq!(command.matches("now()").count(), 1);
        assert!(command.contains("PublicationTimeKnown=false"));
        for forbidden in [
            "CulpritIncidentsV1",
            "CulpritFindingsV1",
            "CulpritChangesV1",
            "summarize",
            "join",
            "ingestion_time",
            "ContractId",
            "EpisodeId",
            "Revision",
            "arg_max",
        ] {
            assert!(!command.contains(forbidden), "{wrapper}: {forbidden}");
        }
    }
    let status = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function PilotStatusNow("))
        .unwrap();
    assert!(
        status.contains("resource:string,max_age:timespan") && status.contains("PilotStatus(resource,now(),max_age)")
    );
    let status = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function PilotStatusRows("))
        .unwrap();
    assert!(status.contains("InterpretationRecordedAt=todatetime(Header.recorded_at)"));
    assert!(status.contains("Header.observed_until") && status.contains("stale"));
}

#[test]
fn normal_packs_bind_human_labels_and_windows_as_typed_values_and_keep_alert_state_outside_the_query() {
    let b = binding();
    let assets = configured(b.clone()).unwrap();
    for name in ["incident-window", "finding-window", "change-window", "recorded-incident-alert-window"] {
        let query = &assets.examples.iter().find(|example| example.name == name).unwrap().query;
        assert!(
            query
                .starts_with("declare query_parameters(resource:string,window_from:datetime,window_until:datetime);\n")
        );
        assert!(query.ends_with("(resource,window_from,window_until)"));
        assert!(!query.contains(&b.contract_id) && !query.contains(&b.recovery_store_id));
        assert!(!query.contains("ingestion_time") && !query.contains("Revision") && !query.contains("summarize"));
    }
    let alert = assets
        .commands
        .iter()
        .find(|command| {
            command.contains("function PilotNewIncidentWindow(") || command.contains("VIEW PilotNewIncidentWindow ")
        })
        .unwrap();
    for field in [
        "Lifecycle",
        "EnrichmentStatus",
        "OriginalIntervalFrom",
        "OriginalIntervalUntil",
        "FirstRecordedObservation",
        "InterpretationRecordedAt",
    ] {
        assert!(alert.contains(field), "{field}");
    }
    let query = &assets.examples.iter().find(|example| example.name == "evidence-availability-alert").unwrap().query;
    assert!(query.contains("PilotStatusNow(resource,max_age)") && query.contains("ContextIssues"));
    assert!(query.contains("State != \"observed\"") && query.contains("InputReadComplete"));
}

#[test]
fn operator_imports_are_bound_paused_and_keep_data_quality_separate_from_recorded_event_novelty() {
    let assets = configured(binding()).unwrap();
    let dashboard = &assets.operator.dashboard;
    assert_eq!(dashboard["timezone"], "utc");
    assert_eq!(dashboard["__inputs"][0]["pluginId"], "grafana-azure-data-explorer-datasource");
    assert_eq!(dashboard["panels"].as_array().unwrap().len(), 8);
    assert!(
        dashboard["templating"]["list"].as_array().unwrap().is_empty(),
        "untrusted alias text is not interpolated into SQL"
    );
    let text = serde_json::to_string(dashboard).unwrap();
    assert!(text.contains("Victim") || text.contains("incident"));
    for panel in dashboard["panels"].as_array().unwrap() {
        assert_eq!(panel["type"], "table");
        assert_eq!(panel["targets"][0]["rawMode"], true);
        assert_eq!(panel["targets"][0]["queryType"], "KQL");
        assert_eq!(panel["targets"][0]["resultFormat"], "table");
        assert_eq!(panel["fieldConfig"]["defaults"]["custom"]["filterable"], true);
        assert!(panel["description"].as_str().unwrap().contains("501"));
    }
    let rules = assets.operator.alerts["groups"][0]["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 3);
    for rule in rules {
        assert_eq!(rule["isPaused"], true);
        assert!(rule["panelId"].as_u64().is_some_and(|id| (1..=8).contains(&id)));
        assert_eq!(rule["execErrState"], "Error");
        assert_eq!(rule["data"][0]["datasourceUid"], "REPLACE_WITH_EXISTING_DATASOURCE_UID");
        assert_eq!(rule["data"].as_array().unwrap().len(), 2, "numeric table rows go directly to a threshold");
        assert_eq!(rule["data"][1]["model"]["expression"], "A");
        assert!(rule["uid"].as_str().unwrap().len() <= 40);
        let labels = serde_json::to_string(&rule["labels"]).unwrap();
        assert!(!labels.contains("rules-2") && !labels.contains("Revision") && !labels.contains("Title"));
    }
    assert_eq!(rules[0]["noDataState"], "OK");
    assert_eq!(rules[1]["noDataState"], "OK");
    assert_eq!(rules[2]["noDataState"], "Alerting");
}

#[test]
fn operator_namespace_queries_keep_exact_source_populations_and_do_not_promote_empty_gold_to_fresh_collection() {
    let assets = configured(binding()).unwrap();
    let collection = assets
        .commands
        .iter()
        .find(|command| {
            command.contains("function PilotCollectionStatus(") || command.contains("VIEW PilotCollectionStatus ")
        })
        .unwrap();
    for identity in [
        "ResourceId",
        "SourceId",
        "LogicalSourceId",
        "CollectedUntil",
        "ReferenceCoverage",
        "SourceCoverage",
        "PublicationTimeKnown",
        "ConfiguredFleetKnown",
    ] {
        assert!(collection.contains(identity), "{identity}");
    }
    assert!(collection.contains("derived-diagnosis") && collection.contains("diagnostic-context"));
    assert!(!collection.contains("Header.recorded_at") && !collection.contains("'recorded_at'"));
    let status = assets
        .commands
        .iter()
        .find(|command| command.contains("function PilotStatusRows(") || command.contains("VIEW PilotStatusRows "))
        .unwrap();
    assert!(
        status.contains("ResourceId") && status.contains("OriginalHeadCount") && status.contains("SelectedHeadCount")
    );
    for state in [
        "unknown-clock",
        "stale",
        "incomplete-output",
        "partial",
        "head-interpretation-unavailable",
        "first-observation-unavailable",
    ] {
        assert!(status.contains(state));
    }
    let native = assets.commands.iter().find(|command| command.contains("CulpritDeadlocksV1NamespaceRows(")).unwrap();
    assert!(native.contains("HeadEpoch") && native.contains("HeadSequence") && native.contains("HeadVersion"));
    let events = assets
        .commands
        .iter()
        .find(|command| {
            command.contains("function PilotDeadlockWindow(") || command.contains("VIEW PilotDeadlockWindow ")
        })
        .unwrap();
    assert!(events.contains("EventAt") && events.contains("ResourceKind") && events.contains("LogicalSourceId"));
    assert!(
        !events.contains("AffectedQueries") && !events.contains("CulpritQueries"),
        "native counts are not query attribution"
    );
}

#[test]
fn operator_queries_have_one_numeric_alert_value_stable_native_labels_and_explicit_limit_errors() {
    let assets = configured(binding()).unwrap();
    let panels = assets.operator.dashboard["panels"].as_array().unwrap();
    for panel in panels {
        let query = panel["targets"][0]["query"].as_str().unwrap();
        assert!(query.contains("501"));
        assert!(!query.contains("ingestion_time") && !query.contains("NativeFirstSeenAt"));
    }
    let findings = panels[5]["targets"][0]["query"].as_str().unwrap();
    assert!(findings.contains("high") && findings.contains("needs-attention") && findings.contains("Inactive"));
    assert!(!findings.contains("critical"));
    let rules = assets.operator.alerts["groups"][0]["rules"].as_array().unwrap();
    let incident = rules[0]["data"][0]["model"]["query"].as_str().unwrap();
    assert!(
        incident.contains("PilotNewIncidentWindow(")
            && incident.contains("EpisodeId")
            && incident.contains("ControllerVersion")
    );
    for field in [
        "Title",
        "Revision",
        "RulesVersion",
        "FirstRecordedObservation",
        "NativeFirstSeenAt",
        "InterpretationRecordedAt",
        "take ",
        "LIMIT ",
    ] {
        assert!(!incident.contains(field), "{field} must not turn into a new alert identity or hide omitted instances");
    }
    let native = rules[1]["data"][0]["model"]["query"].as_str().unwrap();
    assert!(native.contains("DeadlockWindow(") && native.contains("EventPosition") && !native.contains("VictimCount"));
    let availability = rules[2]["data"][0]["model"]["query"].as_str().unwrap();
    for state in ["namespace-unavailable", "acquisition-unavailable", "diagnosis-unavailable", "observed-acquisition"] {
        assert!(availability.contains(state));
    }
    for rule in rules {
        let query = rule["data"][0]["model"]["query"].as_str().unwrap();
        assert!(query.contains("10000") && query.contains("8388608"));
    }
}

#[test]
fn namespace_revision_extension_keeps_the_existing_scalar_renderer_byte_for_byte() {
    let catalog = SchemaCatalog::new().unwrap();
    let schema = catalog.get(culprit_core::telemetry::TelemetryStream::DeadlockEvents).unwrap();
    let previous = r#".create function $TABLE$SCOPERows(contract:string,store:string,resource:string,captured_from:datetime,asof:datetime) {
let Rows=materialize($TABLE$SCOPEEligible(contract,store,resource,captured_from,asof) | where HasRecordId and ReferencesComplete);
let Epochs=Rows | summarize HeadEpoch=max(OwnerEpoch) by SourceId, RecordId;
let Owned=materialize(Rows | join kind=inner Epochs on SourceId, RecordId | where OwnerEpoch == HeadEpoch);
let Sequences=Owned | summarize HeadSequence=max(CaptureSequence) by SourceId, RecordId;
let Captured=materialize(Owned | join kind=inner Sequences on SourceId, RecordId | where CaptureSequence == HeadSequence);
let Versions=Captured | summarize HeadVersion=max(RepresentationVersion) by SourceId, RecordId;
Captured
| join kind=inner Versions on SourceId, RecordId
| where RepresentationVersion == HeadVersion
| project $COLUMNS
}
"#.replace("$SCOPE", "").replace("$TABLE", schema.table()).replace("$COLUMNS", &schema.columns().iter().map(|column| column.name.as_str()).collect::<Vec<_>>().join(", "));
    assert_eq!(eligibility::rows(schema, EligibilityScope::Resource, true), previous);
    let namespace = eligibility::rows(schema, EligibilityScope::Namespace, true);
    assert!(!namespace.contains("resource:string") && !namespace.contains("resource,") && !namespace.contains('$'));
    assert!(namespace.contains("CulpritDeadlocksV1NamespaceEligible(contract,store,captured_from,asof)"));
}

#[test]
fn optional_queued_batching_is_adx_owned_and_never_part_of_helper_or_dashboard_installation() {
    let assets = configured(binding()).unwrap();
    assert!(!assets.script.contains("ingestionbatching"));
    assert!(!serde_json::to_string(&assets.operator.dashboard).unwrap().contains("ingestionbatching"));
    let example = &assets.examples.iter().find(|example| example.name == "opt-in-adx-queued-batching").unwrap().query;
    assert!(example.starts_with(".alter-merge tables ("));
    for table in ["CulpritIncidentsV1", "CulpritFindingsV1", "CulpritChangesV1", "CulpritPublishedRangesV1"] {
        assert!(example.contains(table));
    }
    assert!(example.contains("MaximumBatchingTimeSpan") && example.contains("00:00:30"));
    assert!(!example.contains("streaming") && !example.contains("MaximumRawDataSizeMB"));
}

#[test]
fn latest_interpretation_census_precedes_latest_timestamp_and_conflict_refusal_without_older_fallback() {
    let assets = configured(binding()).unwrap();
    let heads =
        assets.commands.iter().find(|command| command.starts_with(".create-or-alter function PilotHeads(")).unwrap();
    let latest_order = heads.find("RecordedAt desc").unwrap();
    let latest_gate = heads.find("| where CulpritNotLatest == 0").unwrap();
    let census = heads.find("CulpritSigConflict=").unwrap();
    let accepted = heads.find("| where CulpritEpOrd == 1 and CulpritConflicts == 0").unwrap();
    assert!(
        latest_order < latest_gate && latest_gate < census && census < accepted,
        "a conflicting latest timestamp must not revive an older unique interpretation"
    );
    assert!(
        heads.contains("isnotnull(RecordedAt)"),
        "head selection must not return a row from an entirely unknown clock population"
    );
    assert_eq!(heads.matches("PilotHeadBasis(asof)").count(), 1);
    assert!(heads.contains("PilotInterpretationSignature(AnalysisId,DetailSha256,"));
    assert!(heads.contains("PilotConfiguredInterpretation(Analyzer,RulesVersion)"));
    assert!(heads.contains("| project-away InterpretationSignature,"));
    assert!(!heads.contains("take_any(*)") && !heads.contains("arg_max("));
    for command in &assets.commands {
        if !command.starts_with(".create-or-alter function PilotEpisodeCensus(") {
            assert!(
                !command.contains("arg_max("),
                "native revision, publication and time guards retain their original owner"
            );
        }
    }
}

#[test]
fn head_basis_applies_one_whole_resource_veto_after_original_consistency_and_raw_revision_selection() {
    let assets = configured(binding()).unwrap();
    let basis = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function PilotHeadBasis("))
        .unwrap();
    let maximum = basis.find("Revision=max(Revision) by ResourceId,").unwrap();
    let consistency = basis.find("PilotConsistentOriginalKeys(Rows)").unwrap();
    let raw_head = basis.find("| join kind=leftsemi Versions on ResourceId,").unwrap();
    let complete = basis.find("| join kind=leftsemi CompleteResources on ResourceId").unwrap();
    assert!(maximum < raw_head && consistency < raw_head && raw_head < complete);
    assert_eq!(basis.matches("CompleteResources on ResourceId").count(), 1);
    assert!(basis.contains("let Rows=OriginalRows;"));
    assert!(basis.trim_end().ends_with("| join kind=leftsemi CompleteResources on ResourceId\n}"));
    assert!(!basis.contains("RulesVersion") && !basis.contains("Analyzer"));
}

#[test]
fn status_uses_one_compact_episode_census_without_collapsing_latest_header_ties() {
    let assets = configured(binding()).unwrap();
    let status = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function PilotStatusRows("))
        .unwrap();
    assert_eq!(status.matches("PilotEpisodeCensus(Rows)").count(), 1);
    assert_eq!(status.matches("PilotIncidentRows(asof)").count(), 1);
    assert_eq!(status.matches("PilotIncidentCompletenessFromRows(Rows,asof)").count(), 1);
    assert!(
        !status.contains("PilotHeads(")
            && !status.contains("PilotConsistentOriginals(")
            && !status.contains("FirstCounts")
    );
    assert!(status.contains("SelectedInterpretation and coalesce(Missing == 0,false)"));
    assert!(status.contains("OriginalHeadCount=count(),SelectedHeadCount=countif(SelectedHead),UnavailableFirstCount=countif(SelectedHead and not(HasFirstOriginal)) by ResourceId"));
    assert!(status.contains("let Headers=materialize(PilotCompleteness(asof));"));
    assert!(status.contains("order by ResourceId asc,SourceId asc,LogicalSourceId asc,CollectedUntil desc"));
    assert!(status.contains("where CulpritNotLatest == 0"));
    assert!(!status.contains("arg_max"));
}

#[test]
fn bounded_signature_sets_keep_unknown_classes_and_raw_revision_selection_in_the_shared_census() {
    let assets = configured(binding()).unwrap();
    let definition = |name: &str| {
        assets
            .commands
            .iter()
            .find(|command| command.starts_with(&format!(".create-or-alter function Pilot{name}(")))
            .unwrap()
    };
    let signature = definition("OriginalSignature");
    assert!(signature.contains("tostring(pack_array(RevisionId,OriginalRecordedAt,Lifecycle,OriginalReference))"));
    let original = definition("OriginalRevisionCensus");
    assert!(original.contains("make_set(OriginalSignature,2)"));
    assert!(original.contains("UnknownSignatures=countif(isempty(OriginalSignature))"));
    assert!(original.contains(
        "OriginalValid=array_length(OriginalSignatures) == 1 and UnknownOriginals == 0 and UnknownSignatures == 0"
    ));
    let census = definition("EpisodeCensus");
    assert!(
        !census.contains("| where"),
        "unknown or unconfigured newer raw revisions cannot be filtered before head selection"
    );
    assert!(census.contains("make_set_if(InterpretationSignature,isnotnull(ConfiguredAt),2)"));
    assert!(census.contains("UnknownSignatures=countif(isnotnull(ConfiguredAt) and isempty(InterpretationSignature))"));
    assert!(census.contains("arg_max(ConfiguredAt,InterpretationValid) by ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,Revision"));
    assert!(census.contains("join kind=leftouter Interpretations on ResourceId,TargetId,OriginalScopeId,OriginalSource,EpisodeId,ControllerVersion,Revision"));
    assert!(census.contains("OriginalValid and isnotnull(Revision) and coalesce(InterpretationValid,false)"));
    assert!(census.contains("FirstOriginalCount=countif(Revision == todecimal(1) and OriginalValid),arg_max(Revision,SelectedInterpretation)"));
    assert!(
        census.contains("PilotInterpretationSignature(AnalysisId,DetailSha256,")
            && census.contains("PilotConfiguredInterpretation(Analyzer,RulesVersion)")
    );
    let consistent = definition("ConsistentOriginals");
    assert_eq!(consistent.matches("PilotIncidentRows(asof)").count(), 1);
    assert!(consistent.contains("PilotOriginalRevisionCensus(PilotIncidentRows(asof))"));
    assert!(!consistent.contains("join"));
}

#[test]
fn publication_and_eligibility_are_single_pass_without_self_joins_and_alerts_materialize_before_projection() {
    let assets = configured(binding()).unwrap();
    let published = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function CulpritPublishedNamespaceV1("))
        .unwrap();
    assert!(!published.contains("materialize(") && !published.contains("join"));
    assert!(
        published.contains("summarize Variants=count(),take_any(*) by RangeId")
            && published.contains("| where Variants == 1")
    );
    for stream in NAMESPACE_QUERY_STREAMS {
        let schema = SchemaCatalog::new().unwrap().get(stream).unwrap().table().to_owned();
        let eligible = assets
            .commands
            .iter()
            .find(|command| command.starts_with(&format!(".create-or-alter function {schema}NamespaceEligible(")))
            .unwrap();
        assert!(!eligible.contains("materialize(") && !eligible.contains("take_any(*)"));
        assert!(!eligible.contains("join kind=leftsemi") && !eligible.contains("join kind=leftanti"));
        assert!(eligible.contains("PresentRows == ExpectedRows") && eligible.contains("Variants != 1"));
    }
    let alert_wrapper = assets
        .commands
        .iter()
        .find(|command| command.starts_with(".create-or-alter function PilotRecordedIncidentAlertsDuring("))
        .unwrap();
    assert!(
        alert_wrapper.contains("let Selected=materialize(PilotNewIncidents(resource,window_from,window_until,now()));")
    );
}
