use super::*;

pub(super) fn policy() -> ChecksSynthesizer {
    ChecksSynthesizer::new(Arc::new(culprit_checks::Pipeline::builtin()), "retained-catalog-policy-1").unwrap()
}
pub(super) fn display() -> DerivedDisplayContext {
    DerivedDisplayContext {
        connection_alias: "Controlled catalog alias".into(),
        environment: None,
        database_name: None,
    }
}
pub(super) async fn query() -> EvidenceCapture {
    let original = test_support::query_compilation_followup_original();
    let fixture = Fixture::with_collector_bounds_and_sequence(
        original.source,
        original.records,
        1,
        "2020-01-01T12:01:00Z",
        "2020-01-01T12:01:01Z",
    )
    .await
    .unwrap();
    fixture.read().await
}
pub(super) async fn capture(
    source: CollectionDescription,
    records: Vec<TelemetryRecord>,
    sequence: u64,
    at: &str,
    family: EvidenceFamily,
) -> EvidenceCapture {
    let until = (chrono::DateTime::parse_from_rfc3339(at).unwrap() + chrono::Duration::seconds(1)).to_rfc3339();
    let fixture = Fixture::with_collector_bounds_and_sequence(source, records, sequence, at, &until).await.unwrap();
    fixture.read_families_with_recovery(&[family], RecoveryPayloadBudget::default()).await
}
pub(super) fn set_native_clock(source: &mut CollectionDescription, at: &str) {
    let SourceObservation::Snapshot(header) = &mut source.observation else { panic!() };
    header.observed_from = at.into();
    header.observed_until =
        (chrono::DateTime::parse_from_rfc3339(at).unwrap() + chrono::Duration::seconds(1)).to_rfc3339();
}
fn source(
    primary: &dyn OriginalProjection,
    dataset: SnapshotDataset,
    name: &str,
    at: &str,
    rows: usize,
    partial: bool,
) -> CollectionDescription {
    let mut source = primary.source().clone();
    source.source = "controlled-retained-catalog".into();
    source.partition = SourcePartition::Target;
    source.observation = SourceObservation::Snapshot(StateSnapshotDescription {
        dataset,
        observed_from: at.into(),
        observed_until: at.into(),
        records: rows as u32,
        sources: vec![StateSourceCoverage::observed(name.into(), rows as u32, !partial)],
    });
    set_native_clock(&mut source, at);
    source
}
fn native_table() -> MssqlTableSnapshot {
    let (_, rows) = test_support::table_full_text_evidence(test_support::TableFullTextCase::Enabled);
    let TelemetryRecord::Operational(OperationalObservation { fact: OperationalFact::MssqlTable(table), .. }) =
        rows.into_iter().next().unwrap()
    else {
        panic!()
    };
    *table
}
pub(super) fn table_rows(
    primary: &dyn OriginalProjection,
    at: &str,
    partial: bool,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let table = native_table();
    let records = vec![TelemetryRecord::Operational(OperationalObservation {
        source: source_id::TABLES.into(),
        key: table.identity.key().unwrap(),
        fact: OperationalFact::MssqlTable(Box::new(table)),
    })];
    (source(primary, SnapshotDataset::Tables, source_id::TABLES, at, records.len(), partial), records)
}
pub(super) async fn table(primary: &dyn OriginalProjection, at: &str, sequence: u64, partial: bool) -> EvidenceCapture {
    let (source, rows) = table_rows(primary, at, partial);
    capture(source, rows, sequence, at, EvidenceFamily::OperationalSamples).await
}
pub(super) async fn index(
    primary: &dyn OriginalProjection,
    at: &str,
    sequence: u64,
    other_literal: bool,
    unavailable: bool,
) -> EvidenceCapture {
    let (_, mut rows) = test_support::index_configuration_evidence(test_support::IndexConfigurationCase::Mixed);
    let table = native_table();
    let TelemetryRecord::Operational(row) = &mut rows[0] else { panic!() };
    let OperationalFact::MssqlIndex(index) = &mut row.fact else { panic!() };
    index.parent_object = table.identity;
    index.parent_schema = table.schema;
    index.parent_name = table.name;
    index.parent_modified_without_offset = table.modified_without_offset;
    index.configuration = None;
    index.usage = MssqlIndexUsage::NotCollected;
    index.columns.truncate(1);
    index.columns[0].column_id = 1;
    index.columns[0].name = "status".into();
    index.filter = Some(if unavailable {
        CatalogExpression {
            native_sha256: None,
            payload: DefinitionPayload::Unavailable { reason: PayloadUnavailable::NotCollected },
        }
    } else {
        CatalogExpression::sql(
            if other_literal { "[status] = N'other-catalog-canary'" } else { "[status] = N'private-catalog-canary'" },
            Engine::Mssql,
            MAX_CATALOG_EXPRESSION_BYTES,
        )
    });
    row.key = index.key().unwrap();
    assert!(row.is_valid());
    let source = source(primary, SnapshotDataset::Indexes, source_id::INDEXES, at, rows.len(), false);
    capture(source, rows, sequence, at, EvidenceFamily::OperationalSamples).await
}
pub(super) fn column_rows(primary: &dyn OriginalProjection, at: &str) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let table = native_table();
    let column = MssqlColumnDefinition {
        table: table.identity,
        schema: table.schema,
        table_name: table.name,
        table_modified_without_offset: table.modified_without_offset,
        column_id: 1,
        name: "status".into(),
        user_type_id: 56,
        system_type_id: 56,
        type_schema: "sys".into(),
        type_name: "int".into(),
        max_length: 4,
        precision: 10,
        scale: 0,
        nullable: false,
        identity: false,
        computed: None,
        default: None,
        collation: None,
    };
    let rows = vec![TelemetryRecord::ObjectDefinition(
        ObjectDefinitionRecord::new(source_id::COLUMNS.into(), ObjectDefinitionFact::MssqlColumn(Box::new(column)))
            .unwrap(),
    )];
    let source = source(primary, SnapshotDataset::Columns, source_id::COLUMNS, at, rows.len(), false);
    (source, rows)
}
pub(super) async fn columns(primary: &dyn OriginalProjection, at: &str, sequence: u64) -> EvidenceCapture {
    let (source, rows) = column_rows(primary, at);
    capture(source, rows, sequence, at, EvidenceFamily::ObjectDefinitions).await
}
pub(super) async fn statistics(primary: &dyn OriginalProjection, at: &str, sequence: u64) -> EvidenceCapture {
    let table = native_table();
    let statistic = MssqlStatisticsSnapshot {
        table: table.identity,
        schema: table.schema,
        table_name: table.name,
        statistics_id: 1,
        name: "native_statistics".into(),
        auto_created: true,
        user_created: false,
        no_recompute: false,
        memory_optimized: false,
        properties_available: true,
        last_updated_without_offset: Some("2020-01-01T09:00:00.003".into()),
        rows: Some("10".into()),
        rows_sampled: Some("10".into()),
        histogram_steps: Some(2),
        unfiltered_rows: Some("10".into()),
        modifications: Some("0".into()),
        modifications_reset: ResetMarker::Unknown,
    };
    let rows = vec![TelemetryRecord::Operational(OperationalObservation {
        source: source_id::STATISTICS.into(),
        key: statistic.key().unwrap(),
        fact: OperationalFact::MssqlStatistics(Box::new(statistic)),
    })];
    let source = source(primary, SnapshotDataset::Statistics, source_id::STATISTICS, at, rows.len(), false);
    let collected_at = (chrono::DateTime::parse_from_rfc3339(at).unwrap() + chrono::Duration::seconds(5)).to_rfc3339();
    capture(source, rows, sequence, &collected_at, EvidenceFamily::OperationalSamples).await
}
pub(super) fn settings_rows(
    primary: &dyn OriginalProjection,
    at: &str,
    value: Option<&str>,
    partial: bool,
) -> (CollectionDescription, Vec<TelemetryRecord>) {
    let rows = vec![TelemetryRecord::Setting(SettingObservation {
        source: source_id::SETTINGS.into(),
        namespace: "sys.database_scoped_configurations".into(),
        name: "MAXDOP".into(),
        scope: MeasurementScope::Database,
        nature: SettingNature::Configuration,
        value: value.map(str::to_owned),
        value_in_use: None,
        value_for_secondary: None,
        unit: None,
        native_context: None,
        native_source: None,
        pending_restart: None,
        native_is_default: None,
        default_value: None,
        default_source: SettingDefaultSource::Unknown,
    })];
    (source(primary, SnapshotDataset::Settings, source_id::SETTINGS, at, rows.len(), partial), rows)
}
pub(super) async fn settings(
    primary: &dyn OriginalProjection,
    at: &str,
    sequence: u64,
    value: Option<&str>,
    partial: bool,
) -> EvidenceCapture {
    let (source, rows) = settings_rows(primary, at, value, partial);
    capture(source, rows, sequence, at, EvidenceFamily::SettingObservations).await
}
