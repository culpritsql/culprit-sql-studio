use super::*;

struct Acquired {
    inner: Reader,
    execution: NativeReadContext,
    final_owner: Option<NativeReadContext>,
    final_settings: Option<NativeSettingsContext>,
}
fn execution() -> NativeReadContext {
    NativeReadContext {
        target: DatabaseIdentity::server(Engine::Postgres, "fixture", "fixture.invalid", 5432, None).into(),
        partition: SourcePartition::InstanceLocal {
            member: Some(SharedResourceIdentity {
                engine: Engine::Postgres,
                kind: SharedResourceKind::Instance,
                namespace: NativeIdentity::new("42"),
                native: NativeIdentity::new("127.0.0.1").qualified("port", "5432"),
            }),
            epoch: Some("native-process".into()),
        },
        observed_from: FROM.into(),
        observed_until: FROM.into(),
        database_writable: Some(true),
    }
}
#[async_trait::async_trait]
impl SnapshotReader for Acquired {
    type Row = Row;
    async fn describe(&mut self, request: &CollectionRequest) -> Result<CollectionDescription> {
        let mut source = self.inner.describe(request).await?;
        source.observation = SourceObservation::PagedSnapshotAcquisition {
            snapshot: PagedSnapshotDescription {
                dataset: SnapshotDataset::Settings,
                observed_from: FROM.into(),
                sources: vec!["fixture".into()],
            },
            execution: Box::new(self.execution.clone()),
            settings_context: Some(NativeSettingsContext {
                format: NativeSettingsFormat::PostgresSettings,
                version: "180006".into(),
            }),
        };
        Ok(source)
    }
    async fn begin(&mut self, request: &CollectionRequest) -> Result<()> {
        self.inner.begin(request).await
    }
    async fn next_batch(&mut self) -> Result<ReadBatch<Row>> {
        self.inner.next_batch().await
    }
    fn decode(&self, row: &Row, _: &CollectionDescription) -> Result<SnapshotRecord> {
        Ok(SnapshotRecord {
            source: row.source.into(),
            complete_scope: row.complete_scope,
            record: TelemetryRecord::Setting(SettingObservation {
                source: row.source.into(),
                namespace: "fixture".into(),
                name: format!("setting{}", row.value),
                scope: MeasurementScope::Server,
                nature: SettingNature::Configuration,
                value: None,
                value_in_use: Some(row.value.to_string()),
                value_for_secondary: None,
                unit: None,
                native_context: None,
                native_source: None,
                pending_restart: None,
                native_is_default: None,
                default_value: None,
                default_source: SettingDefaultSource::Unknown,
            }),
        })
    }
    async fn recheck(&mut self, source: &CollectionDescription) -> Result<SnapshotRecheck> {
        let mut recheck = self.inner.recheck(source).await?;
        recheck.execution = self.final_owner.clone().map(Box::new);
        recheck.settings_context = self.final_settings.clone();
        Ok(recheck)
    }
    fn query_count(&self) -> u32 {
        self.inner.query_count()
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        self.inner.read_metrics()
    }
    async fn close(&mut self) -> SourceShutdown {
        self.inner.close().await
    }
}
#[tokio::test]
async fn same_snapshot_pager_requires_typed_final_owner_and_keeps_data_namespace_separate() {
    for case in ["complete", "missing", "changed", "missing-format", "changed-format"] {
        let initial = execution();
        let mut final_owner = initial.clone();
        final_owner.observed_until = UNTIL.into();
        if case == "changed" {
            final_owner.database_writable = Some(false);
        }
        let reader = Acquired {
            inner: Reader::new([batch(&[1], true)]),
            execution: initial,
            final_owner: (case != "missing").then_some(final_owner),
            final_settings: (case != "missing-format").then(|| NativeSettingsContext {
                format: NativeSettingsFormat::PostgresSettings,
                version: if case == "changed-format" { "180007" } else { "180006" }.into(),
            }),
        };
        let request = CollectionRequest {
            subject: CollectionSubject::Database,
            selection: CollectionSelection::Snapshot { dataset: SnapshotDataset::Settings },
            budget: CollectionBudget::default(),
        };
        let mut source = SnapshotSource::new(reader, &request).unwrap();
        let original = source.describe().await.unwrap();
        let CollectionRead::Page { records, next_cursor } =
            source.read_page(TelemetryStream::SettingObservations, None).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(records.len(), 1);
        assert_eq!(next_cursor, None);
        let completion = source.finish().await;
        match case {
            "complete" => {
                let completed = original.completed(completion.unwrap()).unwrap().unwrap();
                assert!(completed.observation.snapshot_acquisition_context().is_some());
                assert_eq!(completed.partition, SourcePartition::Target);
            }
            "missing" | "missing-format" | "changed-format" => assert!(completion.is_err()),
            "changed" => assert_eq!(completion.unwrap(), SourceCompletion::Changed),
            _ => unreachable!(),
        }
        assert_eq!(source.close().await, SourceShutdown::Quiescent);
    }
}
