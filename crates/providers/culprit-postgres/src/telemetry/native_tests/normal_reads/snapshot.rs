//! One owned child database isolates the native snapshot's one-plan positive from other tests' workload ranks.
use super::{case, specification};
use crate::{PostgresProvider, connect::Connector, native_test_support::actor::Actor, rows::quote_ident};
use culprit_core::{ConnectionSpec, Provider, Result, SnapshotOptions, telemetry::SourceShutdown};
use culprit_read::cancellation::CollectionCancellation;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

const CASE_TIMEOUT: Duration = Duration::from_secs(90);
const FIXTURE_ACTOR_QUERY_CAP: u32 = 11;
const MAX_NATIVE_DATABASE_NAME_BYTES: usize = 63;

pub(super) async fn run() {
    let cancellation = CollectionCancellation::default();
    let base = specification(&cancellation).await;
    let mut fixture = SnapshotDatabase::new(base, cancellation.clone());
    let completed = case::finish(
        CASE_TIMEOUT,
        &cancellation,
        AssertUnwindSafe(async {
            fixture.prepare().await?;
            let provider = fixture.provider.as_ref().expect("prepared native snapshot source");
            let server = case::admitted(&cancellation, provider.probe()).await?;
            let options = SnapshotOptions { plans: 1, top_queries: 32, ..Default::default() };
            let snapshot = case::admitted(&cancellation, provider.snapshot("owned-normal-readers", &options)).await?;
            Ok::<_, culprit_core::Error>((server, Box::new(snapshot)))
        })
        .catch_unwind(),
    )
    .await;
    // The body is joined on cancellation. Panic/error also reach the same original actors before returning.
    let source_shutdown = fixture.provider.as_ref().map(Provider::source_shutdown);
    let cleaned = Box::pin(fixture.close()).await;
    let queries = fixture.queries();
    eprintln!(
        "owned normal snapshot fixture {}",
        serde_json::json!({
            "native_retirement_and_child_drop_confirmed":cleaned,
            "fixture_actor_dispatches":queries,"fixture_actor_dispatch_cap":FIXTURE_ACTOR_QUERY_CAP,
            "ownership_admission_queries_including_setup":2,"original_provider_shutdown":source_shutdown.map(|state| format!("{state:?}")),
            "case_deadline_reached":completed.deadline_reached,
            "shared_statement_statistics_reset":false,"production_source_limits_changed":false,
        })
    );
    let result = match completed.value {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    let (server, snapshot) = result.unwrap();
    assert!(cleaned, "original source/actors must settle before the exact owned child can be dropped");
    assert_eq!(source_shutdown, Some(SourceShutdown::Quiescent));
    assert!(queries <= FIXTURE_ACTOR_QUERY_CAP);
    assert!(!completed.deadline_reached);
    assert_eq!(server.database, fixture.database);
    assert_eq!(snapshot.server.database, server.database);
    assert!(snapshot.identity.is_some());
    assert!(!snapshot.tables.is_empty(), "the real catalog must retain the owned fixture tables");
    assert!(
        snapshot.workload.as_ref().is_some_and(|workload| workload.queries.iter().any(|query| query.plan.is_some())),
        "the seeded native workload must traverse the original planning and parameterized table-metadata route"
    );
}

struct SnapshotDatabase {
    base: ConnectionSpec,
    database: String,
    cancellation: CollectionCancellation,
    admin: Option<Actor>,
    workload: Option<Actor>,
    provider: Option<PostgresProvider>,
    created: bool,
    acquiring: bool,
}
impl SnapshotDatabase {
    fn new(base: ConnectionSpec, cancellation: CollectionCancellation) -> Self {
        // The existing admission already checked this same cluster's native ownership nonce and loopback route.
        let database = format!("culprit_normal_{}", super::owned_fixture::owner());
        assert!(database.len() <= MAX_NATIVE_DATABASE_NAME_BYTES);
        Self {
            base,
            database,
            cancellation,
            admin: None,
            workload: None,
            provider: None,
            created: false,
            acquiring: false,
        }
    }

    async fn prepare(&mut self) -> Result<()> {
        let marker = format!("culprit_owned_{}", super::owned_fixture::owner());
        let admin_connector = Connector::new(&self.base)?;
        self.acquiring = true;
        self.admin = Some(
            Actor::open(
                &admin_connector,
                self.base.database.as_ref().expect("admitted native database"),
                &marker,
                "fixture-normal-database",
                &self.cancellation,
            )
            .await?,
        );
        self.acquiring = false;
        self.admin.as_mut().unwrap().execute(&format!("CREATE DATABASE {}", quote_ident(&self.database))).await?;
        self.created = true;
        let mut selected = self.base.clone();
        selected.database = Some(self.database.clone());
        let workload_connector = Connector::new(&selected)?;
        self.acquiring = true;
        self.workload = Some(
            Actor::open(&workload_connector, &self.database, &marker, "fixture-normal-workload", &self.cancellation)
                .await?,
        );
        self.acquiring = false;
        let workload = self.workload.as_mut().unwrap();
        for sql in [
            "CREATE EXTENSION pg_stat_statements",
            "CREATE TABLE public.native_plan_rows(id integer PRIMARY KEY,value integer NOT NULL)",
            "INSERT INTO public.native_plan_rows VALUES(1,3),(2,6),(3,9)",
            "ANALYZE public.native_plan_rows",
        ] {
            workload.execute(sql).await?;
        }
        // One real own query. Its inferred integer parameter is generically plannable without executing a plan.
        workload.start_workload("SELECT sum(value) FROM public.native_plan_rows WHERE id>0".into())?;
        workload.settle().await?;
        if !workload.retire().await {
            return Err(culprit_core::telemetry::CollectionStop::CancellationUnconfirmed.error());
        }
        self.provider = Some(PostgresProvider::new(&selected)?);
        Ok(())
    }

    async fn close(&mut self) -> bool {
        let mut settled = !self.acquiring
            && self.provider.as_ref().is_none_or(|provider| provider.source_shutdown() == SourceShutdown::Quiescent);
        if let Some(workload) = &mut self.workload {
            settled &= workload.retire().await;
        }
        // Uncertainty or a cancelled fixture admits no cleanup SQL. The existing owned supervisor still tears down
        // the disposable container; no FORCE, replacement backend or shared-statistics reset heals lost ownership.
        if self.created && settled && !self.cancellation.is_cancelled() {
            let dropped =
                self.admin.as_mut().unwrap().execute(&format!("DROP DATABASE {}", quote_ident(&self.database))).await;
            if dropped.is_ok() {
                self.created = false;
            } else {
                settled = false;
            }
        }
        if let Some(admin) = &mut self.admin {
            settled &= admin.retire().await;
        }
        settled && !self.created
    }

    fn queries(&self) -> u32 {
        self.admin.as_ref().map_or(0, |actor| actor.queries) + self.workload.as_ref().map_or(0, |actor| actor.queries)
    }
}
