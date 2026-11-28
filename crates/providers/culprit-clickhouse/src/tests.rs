use super::*;
#[test]
fn collector_declares_valid_database_recipes() {
    assert!(recipe_catalog_is_valid(&telemetry_recipes()));
}

#[tokio::test]
#[ignore = "dev/smoke-clickhouse-source.py supplies a disposable HTTP endpoint"]
async fn disposable_http_source() {
    use std::env;
    let spec = ConnectionSpec {
        engine: Some(Engine::Clickhouse),
        host: "127.0.0.1".into(),
        port: Some(env::var("CULPRIT_TEST_CLICKHOUSE_PORT").unwrap().parse().unwrap()),
        user: Some("source_reader".into()),
        database: Some("orders".into()),
        password: Some(env::var("CULPRIT_TEST_CLICKHOUSE_PASSWORD").unwrap()),
        auth: culprit_core::AuthKind::Password,
        options: [("sslmode".into(), "disable".into())].into(),
        ..Default::default()
    };
    // Losing workload visibility still leaves independently authorized catalog evidence.
    let restricted_spec = ConnectionSpec { user: Some("catalog_reader".into()), ..spec.clone() };
    let restricted = ClickHouseProvider::new(&restricted_spec, Default::default()).unwrap();
    let catalog_only = restricted.snapshot("restricted", &SnapshotOptions::default()).await.unwrap();
    assert!(catalog_only.tables.iter().any(|t| t.name == "events"), "{:?}", catalog_only.notes);
    assert!(catalog_only.workload.is_none(), "{:?}", catalog_only.workload.as_ref().map(|w| w.query_count));
    assert_eq!(catalog_only.source_outcome("workload").unwrap().state, culprit_core::SourceState::Unavailable);
    assert_eq!(restricted.source_shutdown(), SourceShutdown::Quiescent);
    let provider = ClickHouseProvider::new(&spec, Default::default()).unwrap();
    assert_eq!(provider.probe().await.unwrap().database, "orders");
    let snapshot = provider.snapshot("test", &SnapshotOptions::default()).await.unwrap();
    assert!(
        snapshot
            .workload
            .as_ref()
            .is_some_and(|w| w.queries.iter().any(|q| q.executions == 2.0 && q.text.contains("SELECT sum(n)"))),
        "{:?}",
        snapshot.notes
    );
    assert!(
        snapshot
            .operations
            .clickhouse
            .iter()
            .any(|f| matches!(f,ClickHouseObservation::Failure(row) if row.exception_name=="TOO_MANY_PARTS")),
        "{:?}",
        snapshot.notes
    );
    assert!(
        snapshot.workload.as_ref().and_then(|w| w.timeline.as_ref()).is_some_and(|t| !t.is_empty()),
        "missing timeline: {:?}",
        snapshot.notes
    );
    if let Some(path) = env::var_os("CULPRIT_TEST_CLICKHOUSE_SNAPSHOT") {
        let shared = culprit_core::redact::redact_snapshot(&snapshot);
        std::fs::write(path, serde_json::to_string_pretty(&shared).unwrap() + "\n").unwrap();
    }
    for incident in [
        ClickHouseIncident::PartsLimit,
        ClickHouseIncident::QueryLimit,
        ClickHouseIncident::MutationFailure,
        ClickHouseIncident::AsyncInsertFailure,
    ] {
        assert!(snapshot.operations.clickhouse.iter().any(|point| point.incident() == Some(incident)), "{incident:?}");
    }
    let workload = snapshot.workload.as_ref().unwrap();
    let period = culprit_core::changes::Period {
        start: workload.window_start.clone().unwrap(),
        end: workload.window_end.clone().unwrap(),
    };
    let comparison = provider.period_stats(&period, &period, None).await.unwrap().unwrap();
    assert_eq!(comparison.baseline_total, comparison.recent_total);
    assert_eq!(comparison.baseline_total.executions, workload.total_executions);
    assert!(snapshot.tables.iter().any(|t| t.name == "events" && t.rows == Some(3.0)));
    assert!(snapshot.columns.iter().any(|c| c.table == "events" && c.name == "n"));
    let result = provider.run("SELECT n FROM events ORDER BY n", 2).await.unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(result.truncated);
    assert!(provider.run("DROP TABLE orders.events", 1).await.is_err());
    verify_interrupted_read(&provider).await;
    assert_eq!(
        provider.explain("SELECT n FROM events", ExplainMode::Estimated).await.unwrap().engine,
        Engine::Clickhouse
    );
    provider.live().await.unwrap();
    let request = CollectionRequest {
        subject: CollectionSubject::Database,
        selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Operational },
        budget: Default::default(),
    };
    let baseline = telemetry_recipes().remove(0);
    let RecipeAdmission::BuiltinSafety(policy) = baseline.admission else { panic!() };
    assert!(verify_builtin_safety(&policy).unwrap());
    let request = CollectionRequest { budget: baseline.budget.clone(), ..request };
    let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag = active.clone();
    let admission =
        SourceProfileAdmission::for_builtin(digest(b"route"), *policy, None, request.budget.clone(), move || {
            flag.load(std::sync::atomic::Ordering::Acquire)
        })
        .unwrap();
    let mut source = provider.telemetry_source_for_safety(&request, &admission).unwrap();
    let description = source.describe().await.unwrap();
    assert!(description.matches_request(&request));
    assert_eq!(diagnostic_original_source_kind(&description), Some(DiagnosticSourceKind::ClickHouseOperational));
    let mut cursor = None;
    let mut failures = 0;
    let mut saw_redacted_sample = false;
    loop {
        match source.read_page(TelemetryStream::OperationalSamples, cursor.as_deref()).await.unwrap() {
            CollectionRead::Page { records, next_cursor } => {
                for record in records {
                    if let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::ClickHouse(point),
                        ..
                    }) = &record
                        && let ClickHouseObservation::QueryFamily(family) = point.as_ref()
                        && let DefinitionPayload::Available { text, .. } = &family.sample
                        && text.contains("countIf")
                    {
                        assert!(!text.contains("fixture-private-value"));
                        assert!(family.population.executions >= family.executions);
                        saw_redacted_sample = true;
                    }
                    if matches!(&record,TelemetryRecord::Operational(OperationalObservation{fact:OperationalFact::ClickHouse(fact),..}) if matches!(fact.as_ref(),ClickHouseObservation::Failure(row) if row.exception_code==252))
                    {
                        assert!(matches!(
                            DiagnosticOriginalCondition::from_record(&description, &record),
                            Some(DiagnosticOriginalCondition::ClickHouse { incident: ClickHouseIncident::PartsLimit })
                        ));
                    }
                    if matches!(record,TelemetryRecord::Operational(OperationalObservation{fact:OperationalFact::ClickHouse(ref fact),..}) if matches!(fact.as_ref(),ClickHouseObservation::Failure(_)))
                    {
                        failures += 1;
                    }
                }
                cursor = next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            other => panic!("unexpected page {other:?}"),
        }
    }
    assert!(failures > 0);
    assert!(saw_redacted_sample);
    assert!(matches!(source.finish().await.unwrap(), SourceCompletion::Verified | SourceCompletion::Snapshot(_)));
    assert_eq!(source.close().await, SourceShutdown::Quiescent);
    let mut retired = provider.telemetry_source_for_safety(&request, &admission).unwrap();
    active.store(false, std::sync::atomic::Ordering::Release);
    assert!(provider.telemetry_source_for_safety(&request, &admission).is_err());
    assert!(retired.describe().await.is_err());
    assert_eq!(retired.close().await, SourceShutdown::Quiescent);
    let failure = snapshot
        .operations
        .clickhouse
        .iter()
        .find_map(|f| match f {
            ClickHouseObservation::Failure(row) if row.exception_code == 252 => Some(row),
            _ => None,
        })
        .unwrap();
    let focus = ClickHouseInvestigation {
        target: snapshot.identity.clone().unwrap(),
        request: failure.request(),
        table: Some("events".into()),
    };
    let request = CollectionRequest {
        selection: CollectionSelection::ClickHouseInvestigation { focus: Box::new(focus) },
        ..request
    };
    let mut focused = provider.telemetry_source(&request).unwrap();
    let detail = focused.describe().await.unwrap();
    assert!(detail.matches_request(&request));
    let CollectionRead::Page { records, .. } =
        focused.read_page(TelemetryStream::OperationalSamples, None).await.unwrap()
    else {
        panic!()
    };
    assert!(records.iter().any(|r| matches!(r,TelemetryRecord::Operational(row) if matches!(&row.fact,OperationalFact::ClickHouse(o) if matches!(o.as_ref(),ClickHouseObservation::Execution(_))))));
    assert!(records.iter().any(|r| matches!(r,TelemetryRecord::Operational(row) if matches!(&row.fact,OperationalFact::ClickHouse(o) if matches!(o.as_ref(),ClickHouseObservation::PartEvent(_))))));
    assert!(focused.finish().await.is_ok());
    assert_eq!(focused.close().await, SourceShutdown::Quiescent);
}

/// Dropping the consumer cannot authorize the next read until the native request actually returns.
async fn verify_interrupted_read(provider: &ClickHouseProvider) {
    use std::time::Duration;
    let request = provider.run("SELECT sleep(0.5)", 1);
    assert!(tokio::time::timeout(Duration::from_millis(50), request).await.is_err());
    assert_eq!(provider.source_shutdown(), SourceShutdown::Unconfirmed);
    assert!(provider.probe().await.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider.source_shutdown() != SourceShutdown::Quiescent {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server confirms completion of the outstanding request");
    assert!(provider.probe().await.is_ok());
}
