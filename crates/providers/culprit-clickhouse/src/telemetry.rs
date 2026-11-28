//! Freeze a bounded operational snapshot before publishing pages. The exporter receives native typed facts.
mod followup;
use crate::{
    ClickHouseProvider,
    client::Client,
    health::Read,
    identity::{Identity, instant},
    workload::Window,
};
use culprit_core::{DatabaseIdentity, Error, ErrorCode, Result, message, telemetry::*};
use culprit_read::{FrozenObservation, FrozenSource, ObservationReader};

const HISTORY_MINUTES: u32 = 15;
const MAX_ROWS_PER_SOURCE: u32 = 128;
const COLLECTION: TelemetryCollection = TelemetryCollection::Snapshot(SnapshotDataset::Operational);

pub fn telemetry_recipes() -> Vec<RecipeDescriptor> {
    let mut baseline = RecipeDescriptor {
        id: "clickhouse.operational".into(),
        version: 1,
        purpose: RecipePurpose::Baseline,
        selector: RecipeSelector::Collection(COLLECTION),
        subjects: vec![RecipeSubject::Database],
        budget: CollectionBudget {
            max_snapshot_rows: MAX_ROWS_PER_SOURCE * Read::ALL.len() as u32,
            ..CollectionBudget::default()
        },
        admission: RecipeAdmission::Unqualified,
    };
    let followup = RecipeDescriptor {
        id: "clickhouse.investigate-execution".into(),
        purpose: RecipePurpose::Investigation,
        selector: RecipeSelector::ClickHouseInvestigation,
        ..baseline.clone()
    };
    baseline.admission = RecipeAdmission::BuiltinSafety(Box::new(crate::safety::baseline_policy(&baseline)));
    vec![baseline, followup]
}

pub(crate) fn source(
    provider: &ClickHouseProvider,
    request: &CollectionRequest,
    profile: Option<&SourceProfileAdmission>,
) -> Result<Box<dyn TelemetrySource>> {
    request.validate()?;
    if !telemetry_recipes().iter().any(|r| r.selector.matches(request))
        || !RecipeSubject::Database.accepts(&request.subject)
    {
        return Err(Error::new(ErrorCode::Unsupported, message!("collection.unsupported")));
    }
    // Identity at both ends of capture plus final recheck all count against the native query budget.
    let required = match &request.selection {
        CollectionSelection::ClickHouseInvestigation { focus } => 4 + u32::from(focus.table.is_some()),
        _ => Read::ALL.len() as u32 + 4,
    };
    if request.budget.max_queries < required {
        return Err(CollectionStop::QueryLimit.error());
    }
    Ok(Box::new(FrozenSource::new(
        Reader {
            client: provider.client.with_budget(&request.budget, profile)?,
            host: provider.host.clone(),
            port: provider.port,
            identity: None,
            queries: 0,
        },
        request,
    )?))
}
struct Reader {
    client: Client,
    host: String,
    port: u16,
    identity: Option<DatabaseIdentity>,
    queries: u32,
}
impl Reader {
    async fn owner(&mut self) -> Result<Identity> {
        self.queries += 1;
        Identity::read(&self.client).await
    }
}
#[derive(Default)]
struct Captured {
    records: Vec<TelemetryRecord>,
    sources: Vec<StateSourceCoverage>,
}
impl Captured {
    fn add(&mut self, source: &str, rows: Vec<ClickHouseObservation>, complete: bool) -> Result<()> {
        self.sources.push(StateSourceCoverage::observed(source.into(), rows.len() as u32, complete));
        for point in rows {
            if !point.is_valid() || point.source_name() != source {
                return Err(CollectionStop::InvalidPage.error());
            }
            self.records.push(TelemetryRecord::Operational(OperationalObservation {
                source: source.into(),
                key: point.identity().key()?,
                fact: OperationalFact::ClickHouse(Box::new(point)),
            }));
        }
        Ok(())
    }
    fn unavailable(&mut self, client: &Client, source: &str, error: Error) -> Result<()> {
        if client.shutdown() == SourceShutdown::Unconfirmed
            || !matches!(error.code, ErrorCode::PermissionDenied | ErrorCode::QueryFailed | ErrorCode::Unsupported)
            || error.detail.is_some()
        {
            return Err(error);
        }
        self.sources.push(StateSourceCoverage::unavailable(source.into(), error.code, None));
        Ok(())
    }
}
impl Reader {
    async fn baseline(&mut self, owner: &Identity, window: &Window, row_limit: u32) -> Result<Captured> {
        let mut out = Captured::default();
        for (index, &read) in Read::ALL.iter().enumerate() {
            let remaining = row_limit.saturating_sub(out.records.len() as u32);
            let limit = MAX_ROWS_PER_SOURCE.min(remaining / (Read::ALL.len() + 1 - index) as u32);
            let source = read.source().name;
            self.queries += 1;
            match read.capture(&self.client, owner, window, limit).await {
                Ok(capture) => out.add(source, capture.rows, capture.complete)?,
                Err(error) => out.unavailable(&self.client, source, error)?,
            }
        }
        let remaining = MAX_ROWS_PER_SOURCE.min(row_limit.saturating_sub(out.records.len() as u32));
        self.queries += 1;
        match crate::workload::archive(&self.client, owner, window, remaining).await {
            Ok(capture) => out.add("system.query_log.families", capture.rows, capture.complete)?,
            Err(error) => out.unavailable(&self.client, "system.query_log.families", error)?,
        }
        Ok(out)
    }
}
#[async_trait::async_trait]
impl ObservationReader for Reader {
    async fn observe(&mut self, request: &CollectionRequest) -> Result<FrozenObservation> {
        let owner = self.owner().await?;
        let target = owner.target(&self.host, self.port);
        // A profiled source establishes its database incarnation on a new native HTTP session. Every
        // later request requires that same session on ClickHouse (session_check=1) before executing SQL.
        if self.client.profiled() && (target.incarnation.is_none() || !owner.supported_profile()) {
            return Err(CollectionStop::SourceChanged.error());
        }

        let started = owner.clock()?;
        let window = Window::ending(started, HISTORY_MINUTES)?;
        let Captured { records, sources } = match &request.selection {
            CollectionSelection::ClickHouseInvestigation { focus } => {
                self.followup(focus, &target, request.budget.max_snapshot_rows).await?
            }
            _ => self.baseline(&owner, &window, request.budget.max_snapshot_rows).await?,
        };
        let after = self.owner().await?;
        if after.target(&self.host, self.port) != target {
            return Err(CollectionStop::SourceChanged.error());
        }
        self.identity = Some(target.clone());
        let snapshot = StateSnapshotDescription {
            dataset: SnapshotDataset::Operational,
            observed_from: instant(started),
            observed_until: instant(after.clock()?),
            records: records.len() as u32,
            sources,
        };
        let observation = match &request.selection {
            CollectionSelection::ClickHouseInvestigation { focus } => {
                SourceObservation::ClickHouseInvestigation { snapshot, focus: focus.clone() }
            }
            _ => SourceObservation::Snapshot(snapshot),
        };
        Ok(FrozenObservation {
            description: CollectionDescription {
                partition: SourcePartition::Target,
                target: CollectionTarget::Database(target),
                source: CLICKHOUSE_OPERATIONAL_SOURCE.into(),
                continuity: SourceContinuity::Unverified,
                observation,
            },
            records,
        })
    }
    async fn verify(&mut self, _: &CollectionDescription) -> Result<bool> {
        let owner = self.owner().await?.target(&self.host, self.port);
        Ok(self.identity.as_ref() == Some(&owner))
    }
    fn query_count(&self) -> u32 {
        self.queries
    }
    fn read_metrics(&self) -> Option<ReadWorkMetrics> {
        None
    }
    async fn close(&mut self) -> SourceShutdown {
        self.client.shutdown()
    }
}
