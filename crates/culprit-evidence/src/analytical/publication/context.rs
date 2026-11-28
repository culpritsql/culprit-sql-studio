//! Public coverage is an explicit projection, not a partially serialized private native source description.
use culprit_core::{ObservedWaitResource, Result, telemetry::*};
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "kebab-case")]
enum PublicObservation<'a> {
    ClickHouseInvestigation {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
    },
    CollectorConnectionContext {
        attempt: &'a CaptureIdentity,
        snapshot_sha256: &'a str,
        collector_started_at: &'a str,
        collector_finished_at: &'a str,
    },
    QueryHistory(&'a QueryHistoryDescription),
    RetainedSamples(&'a SampleHistoryDescription),
    MssqlReadableResourcesContext {
        history: &'a SampleHistoryDescription,
        observed_role: Option<MssqlResourceReplicaRole>,
        limitations: Vec<MssqlReadableResourceLimitation>,
    },
    MssqlManagedInstanceResources {
        from: &'a str,
        until: &'a str,
        observed_at: &'a str,
        native_engine_edition: i64,
        coverage: &'a StateSourceCoverage,
        native_rows: u32,
        layout: &'a MssqlManagedInstanceResourceLayout,
    },
    RetainedEvents(&'a EventHistoryDescription),
    Activity(&'a ActivitySnapshotDescription),
    FocusedBlockingContext {
        snapshot: &'a ActivitySnapshotDescription,
        focus_key: String,
        binding: FocusedRequestBinding,
        membership: FocusedRequestMembership,
        request_end: BlockingRequestState,
        closure: &'a BlockingClosureDescription,
    },
    MetadataLockContext {
        snapshot: &'a ActivitySnapshotDescription,
        focus_key: &'a str,
        execution_key: String,
        binding: FocusedRequestBinding,
        membership: FocusedRequestMembership,
        request_end: BlockingRequestState,
        cohort_rows: u32,
    },
    Snapshot(&'a StateSnapshotDescription),
    SnapshotAcquisitionContext {
        snapshot: &'a StateSnapshotDescription,
        settings_context: Option<&'a NativeSettingsContext>,
        acquisition_key: String,
        database_writable: bool,
    },
    LocalResourceContext(LocalPointContext<'a>),
    SourceReadContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: &'a str,
        execution_key: String,
    },
    MysqlEvents {
        snapshot: &'a StateSnapshotDescription,
        metadata: &'a MysqlEventsMetadata,
    },
    DatabasePerformanceCountersContext(LocalPointContext<'a>),
    SuspectPagesContext(LocalPointContext<'a>),
    BackupHistory {
        snapshot: &'a StateSnapshotDescription,
        scope: &'a MssqlBackupHistoryScope,
        database_writable: bool,
    },
    MssqlAgentJobs {
        snapshot: &'a StateSnapshotDescription,
        metadata: &'a MssqlAgentJobsMetadata,
    },
    Jobs {
        snapshot: &'a StateSnapshotDescription,
        metadata: &'a TimescaleJobsMetadata,
    },
    FocusedPageWaitContext {
        snapshot: &'a StateSnapshotDescription,
        result: Box<FocusResult<'a>>,
    },
    FocusedRequestMemoryContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
        source: FocusSourceState,
        membership: RequestMemoryMembership,
    },
    FocusedCompilationAdmissionContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
        source: FocusSourceState,
        membership: CompilationAdmissionMembership,
    },
    MaintenanceProgressContext(FocusedRequestContext<'a>),
    CurrentRequestWorkContext {
        profile: CurrentRequestWorkProfile,
        #[serde(flatten)]
        context: FocusedRequestContext<'a>,
    },
    MssqlPoolCurrentContext {
        snapshot: &'a StateSnapshotDescription,
    },
    MssqlPoolSettingsContext {
        snapshot: &'a StateSnapshotDescription,
    },
    MssqlReplicationContext {
        snapshot: &'a StateSnapshotDescription,
        database_writable: bool,
    },
    GroupReplicationContext {
        snapshot: &'a StateSnapshotDescription,
        plugin_status: Option<&'a str>,
        members_layout: bool,
        statistics_layout: bool,
    },
    GaleraContext {
        snapshot: &'a StateSnapshotDescription,
        metadata: &'a MariadbGaleraMetadata,
    },
    TableMemoryContext {
        snapshot: &'a StateSnapshotDescription,
        target_key: String,
        metadata: &'a MssqlTableMemoryMetadata,
        database_writable: bool,
    },
    ColumnstoreRowgroupsContext {
        snapshot: &'a StateSnapshotDescription,
        target_key: String,
        index_type: u8,
        database_writable: bool,
    },
    FullTextPopulationsContext {
        snapshot: &'a StateSnapshotDescription,
        metadata: &'a MssqlFullTextPopulationMetadata,
        target_key: String,
        database_writable: bool,
    },
    CacheEntryMemoryContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
    },
    PageWaitWritersContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
        database_writable: Option<bool>,
    },
    PageWaitTargetsContext {
        snapshot: &'a StateSnapshotDescription,
        database_writable: Option<bool>,
        original_activity_key: Option<String>,
    },
    PageTaskHoldersContext {
        snapshot: &'a StateSnapshotDescription,
        focus_key: String,
        database_writable: Option<bool>,
    },
    DiagnosticContext(Value),
    DerivedDiagnosis(&'a culprit_core::DerivedDiagnosisDescription),
}
#[derive(Serialize)]
struct FocusedRequestContext<'a> {
    snapshot: &'a StateSnapshotDescription,
    focus_key: Option<String>,
    database_writable: Option<bool>,
    binding: FocusedRequestBinding,
    membership: FocusedRequestMembership,
}
impl<'a> FocusedRequestContext<'a> {
    fn new(snapshot: &'a StateSnapshotDescription, result: &FocusedRequestResult) -> Result<Self> {
        Ok(Self {
            snapshot,
            focus_key: result.focus.as_ref().map(|focus| focus.key()).transpose()?,
            database_writable: result.database_writable,
            binding: result.binding,
            membership: result.membership,
        })
    }
}

#[derive(Serialize)]
struct FocusResult<'a> {
    focus: Focus<'a>,
    observed_from: &'a str,
    observed_until: &'a str,
    source: FocusSourceState,
    request: PageWaitRequestState,
    wait: PageWaitRelation,
}
#[derive(Serialize)]
struct Focus<'a> {
    key: String,
    parent: &'a DiagnosticEvidenceRef,
    source: &'a str,
    epoch: &'a str,
    original_observed_at: &'a str,
    statement_identity: &'a NativeIdentity,
    resource: &'a ObservedWaitResource,
}

/// Only public acquisition coverage/role. Original database/member/fork proof stays in the private manifest.
#[derive(Serialize)]
struct LocalPointContext<'a> {
    snapshot: &'a StateSnapshotDescription,
    database_writable: bool,
}
impl<'a> LocalPointContext<'a> {
    fn new(snapshot: &'a StateSnapshotDescription, database_writable: bool) -> Self {
        Self { snapshot, database_writable }
    }
}

pub(super) fn observation(value: &SourceObservation) -> Result<Value> {
    let projected = match value {
        SourceObservation::CollectorConnection(value) => PublicObservation::CollectorConnectionContext {
            attempt: &value.attempt,
            snapshot_sha256: &value.snapshot_sha256,
            collector_started_at: &value.observed_from,
            collector_finished_at: &value.observed_until,
        },
        SourceObservation::QueryHistory(value) => PublicObservation::QueryHistory(value),
        SourceObservation::RetainedSamples(value) => PublicObservation::RetainedSamples(value),
        SourceObservation::MssqlReadableResources(value) => PublicObservation::MssqlReadableResourcesContext {
            history: &value.history,
            observed_role: value.observed_role,
            limitations: value.limitations(),
        },
        SourceObservation::MssqlManagedInstanceResources(history) => PublicObservation::MssqlManagedInstanceResources {
            from: &history.from,
            until: &history.until,
            observed_at: &history.observed_at,
            native_engine_edition: history.native_engine_edition,
            coverage: &history.coverage,
            native_rows: history.native_rows,
            layout: &history.layout,
        },
        SourceObservation::RetainedEvents(value) => PublicObservation::RetainedEvents(value),
        SourceObservation::Activity(value) => PublicObservation::Activity(value),
        SourceObservation::FocusedBlocking { snapshot, result } => PublicObservation::FocusedBlockingContext {
            snapshot,
            focus_key: result.focus.key()?,
            binding: result.binding,
            membership: result.membership,
            request_end: result.request_end,
            closure: &result.closure,
        },
        SourceObservation::MetadataLockContext { snapshot, focus_key, execution, result } => {
            PublicObservation::MetadataLockContext {
                snapshot,
                focus_key,
                execution_key: SourceReadExecutionScope::from_context(
                    SourceReadQuestion::MetadataLockContext,
                    execution,
                )?
                .key()?,
                binding: result.request.binding,
                membership: result.request.membership,
                request_end: result.request.request_end,
                cohort_rows: result.cohort.len() as u32,
            }
        }
        SourceObservation::Snapshot(value) => PublicObservation::Snapshot(value),
        SourceObservation::SnapshotAcquisition { snapshot, execution, settings_context } => {
            PublicObservation::SnapshotAcquisitionContext {
                snapshot,
                settings_context: settings_context.as_ref(),
                acquisition_key: digest(
                    &serde_json::to_vec(execution).map_err(|_| DeliveryProblem::Integrity.error())?,
                ),
                database_writable: execution.database_writable.ok_or_else(|| DeliveryProblem::Integrity.error())?,
            }
        }
        SourceObservation::LocalResource { snapshot, database_writable } => {
            PublicObservation::LocalResourceContext(LocalPointContext::new(snapshot, *database_writable))
        }
        SourceObservation::SourceRead { snapshot, focus_key, execution } => PublicObservation::SourceReadContext {
            snapshot,
            focus_key,
            execution_key: SourceReadExecutionScope::from_context(
                SourceReadQuestion::for_dataset(snapshot.dataset)
                    .ok_or_else(|| culprit_core::telemetry::CollectionStop::InvalidPage.error())?,
                execution,
            )?
            .key()?,
        },
        SourceObservation::PageWaitTargets { snapshot, database_writable, original_activity } => {
            PublicObservation::PageWaitTargetsContext {
                snapshot,
                database_writable: *database_writable,
                original_activity_key: original_activity.as_ref().map(DiagnosticCaptureRef::key).transpose()?,
            }
        }
        SourceObservation::Jobs { snapshot, metadata } => PublicObservation::Jobs { snapshot, metadata },
        SourceObservation::MysqlEvents { snapshot, metadata } => PublicObservation::MysqlEvents { snapshot, metadata },
        SourceObservation::DatabasePerformanceCounters { snapshot, database_writable, .. } => {
            PublicObservation::DatabasePerformanceCountersContext(LocalPointContext::new(snapshot, *database_writable))
        }
        SourceObservation::SuspectPages { snapshot, database_writable, .. } => {
            PublicObservation::SuspectPagesContext(LocalPointContext::new(snapshot, *database_writable))
        }
        SourceObservation::BackupHistory { snapshot, scope, database_writable } => {
            PublicObservation::BackupHistory { snapshot, scope, database_writable: *database_writable }
        }
        SourceObservation::MssqlAgentJobs { snapshot, metadata } => {
            PublicObservation::MssqlAgentJobs { snapshot, metadata }
        }
        SourceObservation::MssqlPoolCurrent { snapshot, .. } => PublicObservation::MssqlPoolCurrentContext { snapshot },
        SourceObservation::MssqlPoolSettings { snapshot, .. } => {
            PublicObservation::MssqlPoolSettingsContext { snapshot }
        }
        SourceObservation::MssqlReplication { snapshot, database_writable, .. } => {
            PublicObservation::MssqlReplicationContext { snapshot, database_writable: *database_writable }
        }
        SourceObservation::GroupReplication { snapshot, metadata } => PublicObservation::GroupReplicationContext {
            snapshot,
            plugin_status: metadata.plugin_status.as_deref(),
            members_layout: metadata.members_layout,
            statistics_layout: metadata.statistics_layout,
        },
        SourceObservation::Galera { snapshot, metadata } => PublicObservation::GaleraContext { snapshot, metadata },
        SourceObservation::ClickHouseInvestigation { snapshot, focus } => {
            PublicObservation::ClickHouseInvestigation { snapshot, focus_key: focus.key()? }
        }
        SourceObservation::TableMemory { snapshot, target, metadata, database_writable } => {
            PublicObservation::TableMemoryContext {
                snapshot,
                target_key: target.key()?,
                metadata,
                database_writable: *database_writable,
            }
        }
        SourceObservation::ColumnstoreRowgroups { snapshot, target, index_type, database_writable } => {
            PublicObservation::ColumnstoreRowgroupsContext {
                snapshot,
                target_key: target.key()?,
                index_type: *index_type,
                database_writable: *database_writable,
            }
        }
        SourceObservation::FullTextPopulations { snapshot, target, metadata, database_writable } => {
            PublicObservation::FullTextPopulationsContext {
                snapshot,
                metadata,
                target_key: target.key()?,
                database_writable: *database_writable,
            }
        }
        SourceObservation::CacheEntryMemory { snapshot, focus } => {
            PublicObservation::CacheEntryMemoryContext { snapshot, focus_key: focus.key()? }
        }
        SourceObservation::PageWaitWriters { snapshot, focus, database_writable } => {
            PublicObservation::PageWaitWritersContext {
                snapshot,
                focus_key: focus.key()?,
                database_writable: *database_writable,
            }
        }
        SourceObservation::PageTaskHolders { snapshot, focus, database_writable } => {
            PublicObservation::PageTaskHoldersContext {
                snapshot,
                focus_key: focus.key()?,
                database_writable: *database_writable,
            }
        }
        SourceObservation::FocusedRequestMemory { snapshot, result } => {
            PublicObservation::FocusedRequestMemoryContext {
                snapshot,
                focus_key: result.focus.key()?,
                source: result.source,
                membership: result.membership,
            }
        }
        SourceObservation::FocusedCompilationAdmission { snapshot, result } => {
            PublicObservation::FocusedCompilationAdmissionContext {
                snapshot,
                focus_key: result.focus.key()?,
                source: result.source,
                membership: result.membership,
            }
        }
        SourceObservation::MaintenanceProgress { snapshot, result } => {
            PublicObservation::MaintenanceProgressContext(FocusedRequestContext::new(snapshot, result)?)
        }
        SourceObservation::CurrentRequestWork { profile, snapshot, result } => {
            PublicObservation::CurrentRequestWorkContext {
                profile: *profile,
                context: FocusedRequestContext::new(snapshot, result)?,
            }
        }
        SourceObservation::FocusedPageWait { snapshot, result } => PublicObservation::FocusedPageWaitContext {
            snapshot,
            result: Box::new(FocusResult {
                focus: Focus {
                    key: result.focus.key()?,
                    parent: &result.focus.participant().parent,
                    source: &result.focus.participant().source,
                    epoch: &result.focus.participant().epoch,
                    original_observed_at: &result.focus.participant().original_observed_at,
                    statement_identity: &result.focus.participant().statement_identity,
                    resource: &result.focus.resource,
                },
                observed_from: &result.observed_from,
                observed_until: &result.observed_until,
                source: result.source,
                request: result.request,
                wait: result.wait,
            }),
        },
        SourceObservation::DerivedDiagnosis(value) => PublicObservation::DerivedDiagnosis(value),
        SourceObservation::Diagnostics(value) => {
            PublicObservation::DiagnosticContext(super::super::investigations::diagnostic_context(value)?)
        }
        SourceObservation::PagedSnapshot(_)
        | SourceObservation::PagedSnapshotAcquisition { .. }
        | SourceObservation::PagedSourceRead { .. } => {
            return Err(DeliveryProblem::Integrity.error());
        }
    };
    serde_json::to_value(projected).map_err(|_| DeliveryProblem::Integrity.error())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod database_counter_tests {
    use super::*;
    #[test]
    fn selected_database_public_observation_retains_scope_counts_and_role_without_recovery_owned_proof() {
        let (source, _) = crate::analytical::test_support::database_performance_counter_evidence(
            crate::analytical::test_support::DatabasePerformanceCounterCase::Exact,
        );
        let public = observation(&source.observation).unwrap();
        assert_eq!(public["kind"], "database-performance-counters-context");
        assert_eq!(public["data"]["database_writable"], true);
        assert_eq!(public["data"]["snapshot"]["records"], 1);
        let text = serde_json::to_string(&public).unwrap();
        for private in [
            "native_database",
            "private-database-guid-canary",
            "private-fork-guid-canary",
            "private-file-guid-canary",
            "private-process-canary",
        ] {
            assert!(!text.contains(private));
        }
    }
}

#[cfg(test)]
mod suspect_page_tests {
    use super::*;
    use crate::analytical::test_support::{self, SuspectPageHistoryCase};
    #[test]
    fn each_retained_history_state_has_a_curated_header_without_private_native_authority() {
        for case in SuspectPageHistoryCase::ALL {
            let (source, _) = test_support::suspect_page_history_evidence(case);
            let public = observation(&source.observation).unwrap();
            assert_eq!(public["kind"], "suspect-pages-context");
            assert_eq!(
                public["data"]["snapshot"],
                serde_json::to_value(source.observation.snapshot().unwrap()).unwrap()
            );
            assert_eq!(public["data"]["database_writable"], true);
            let fields = public["data"].as_object().unwrap();
            assert_eq!(fields.len(), 2);
            assert!(fields.contains_key("snapshot") && fields.contains_key("database_writable"));
            assert!(serde_json::from_value::<SourceObservation>(public.clone()).is_err());
            let text = public.to_string();
            for private in [
                "native_database",
                "private-database-guid-canary",
                "private-fork-guid-canary",
                "private-file-guid-canary",
                "private-process-canary",
            ] {
                assert!(!text.contains(private), "{case:?}: {private}");
            }
        }
    }
    #[test]
    fn selected_counter_and_history_public_discriminators_stay_distinct_with_identical_private_dto_shape() {
        let (counter, _) =
            test_support::database_performance_counter_evidence(test_support::DatabasePerformanceCounterCase::Exact);
        let (history, _) = test_support::suspect_page_history_evidence(SuspectPageHistoryCase::Active);
        let counter = observation(&counter.observation).unwrap();
        let history = observation(&history.observation).unwrap();
        assert_eq!(counter["kind"], "database-performance-counters-context");
        assert_eq!(history["kind"], "suspect-pages-context");
        assert_ne!(counter["data"]["snapshot"]["dataset"], history["data"]["snapshot"]["dataset"]);
        assert_eq!(
            counter["data"].as_object().unwrap().keys().collect::<Vec<_>>(),
            history["data"].as_object().unwrap().keys().collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod table_memory_tests {
    use super::*;
    use crate::analytical::test_support::{TableMemoryCase, table_memory_evidence};
    #[test]
    fn public_selected_table_context_keeps_exact_target_digest_not_private_member_epoch_proof() {
        for case in TableMemoryCase::ALL {
            let (source, _) = table_memory_evidence(case);
            let SourceObservation::TableMemory { target, metadata, snapshot, database_writable } = &source.observation
            else {
                panic!()
            };
            let public = observation(&source.observation).unwrap();
            assert_eq!(public["kind"], "table-memory-context");
            assert_eq!(public["data"]["target_key"], target.key().unwrap());
            assert_eq!(public["data"]["metadata"], serde_json::to_value(metadata).unwrap());
            assert_eq!(public["data"]["snapshot"], serde_json::to_value(snapshot).unwrap());
            assert_eq!(public["data"]["database_writable"], *database_writable);
            assert_eq!(public["data"].as_object().unwrap().len(), 4);
            assert!(serde_json::from_value::<SourceObservation>(public.clone()).is_err());
            for private in [
                "private-machine-canary",
                "private-instance-canary",
                "private-process-and-database-canary",
                "created_without_offset",
                "object_id",
                "native_database",
            ] {
                assert!(!public.to_string().contains(private), "{case:?}: {private}");
            }
        }
    }
}

#[cfg(test)]
mod group_replication_tests {
    use super::*;
    #[test]
    fn native_group_context_publication_keeps_counts_and_layout_but_withholds_native_group_and_process_proof() {
        for case in crate::analytical::test_support::GroupReplicationCase::ALL {
            let (source, _) = crate::analytical::test_support::group_replication_evidence(case);
            let public = observation(&source.observation).unwrap();
            assert_eq!(public["kind"], "group-replication-context");
            assert_eq!(
                public["data"]["snapshot"],
                serde_json::to_value(source.observation.snapshot().unwrap()).unwrap()
            );
            let text = serde_json::to_string(&public).unwrap();
            for private in [
                "aaaaaaaa-0000-4000-8000-000000000001",
                "bbbbbbbb-0000-4000-8000-000000000002",
                "private-observer-process-canary",
            ] {
                assert!(!text.contains(private));
            }
        }
    }
}

#[cfg(test)]
mod hyperscale_tests {
    use super::*;
    #[test]
    fn selected_replication_public_context_keeps_coverage_and_role_but_withholds_original_native_scope() {
        for case in [
            crate::analytical::test_support::ReplicationCase::MssqlHyperscale,
            crate::analytical::test_support::ReplicationCase::MssqlHyperscaleUnavailable,
        ] {
            let (source, _) = crate::analytical::test_support::replication_evidence(case);
            let public = observation(&source.observation).unwrap();
            assert_eq!(public["kind"], "mssql-replication-context");
            assert_eq!(
                public["data"]["snapshot"],
                serde_json::to_value(source.observation.snapshot().unwrap()).unwrap()
            );
            assert_eq!(public["data"]["database_writable"], true);
            assert!(public["data"].get("scope").is_none());
            assert!(!public.to_string().contains("91487244-730d-4b83-8e49-d482241c7288"));
            assert!(serde_json::from_value::<SourceObservation>(public).is_err());
        }
    }
}

#[cfg(test)]
mod galera_tests {
    use super::*;
    #[test]
    fn native_group_context_publication_keeps_counts_and_layout_but_withholds_native_group_and_process_proof() {
        for case in crate::analytical::test_support::GaleraCase::ALL {
            let (source, _) = crate::analytical::test_support::galera_evidence(case);
            let public = observation(&source.observation).unwrap();
            assert_eq!(public["kind"], "galera-context");
            assert_eq!(
                public["data"]["snapshot"],
                serde_json::to_value(source.observation.snapshot().unwrap()).unwrap()
            );
            let text = serde_json::to_string(&public).unwrap();
            for private in [
                "aaaaaaaa-0000-4000-8000-000000000001",
                "bbbbbbbb-0000-4000-8000-000000000002",
                "private-galera-process-canary",
                "iFL88RG7XPipmQ5HOnA9iLqJw6w=",
                "mariadb-server-uid",
            ] {
                assert!(!text.contains(private));
            }
        }
    }
}

#[cfg(test)]
mod pool_settings_tests {
    use super::*;
    #[test]
    fn curated_pool_settings_context_keeps_original_coverage_but_withholds_member_guid_scope() {
        let (source, _) = crate::analytical::test_support::pool_settings_evidence(
            crate::analytical::test_support::PoolSettingsCase::Exact,
        );
        let value = observation(&source.observation).unwrap();
        assert_eq!(value["kind"], "mssql-pool-settings-context");
        assert!(value["data"].get("membership").is_none());
        assert_eq!(value["data"]["snapshot"], serde_json::to_value(source.observation.snapshot().unwrap()).unwrap());
        assert!(!value.to_string().contains("11111111-1111-1111-1111-111111111111"));
        assert!(serde_json::from_value::<SourceObservation>(value).is_err());
    }
}

#[cfg(test)]
mod pool_operational_tests;

#[cfg(test)]
mod readable_resource_tests {
    use super::*;
    #[test]
    fn readable_history_public_context_keeps_role_and_unknowns_without_native_reader_authority() {
        let mut database =
            culprit_core::DatabaseIdentity::server(culprit_core::Engine::Mssql, "shop", "fixture.invalid", 1433, None);
        database.incarnation = Some("11111111-1111-1111-1111-111111111111".into());
        let native = SourceObservation::MssqlReadableResources(Box::new(MssqlReadableResourceHistory {
            history: SampleHistoryDescription {
                from: "2020-01-01T06:00:00Z".into(),
                until: "2020-01-01T06:01:00Z".into(),
                observed_at: "2020-01-01T06:02:00Z".into(),
                available_from: None,
                available_until: None,
                expected_samples: 0,
                nominal_period_ms: Some(15000),
            },
            current_reader: Box::new(NativeReadContext {
                target: database.into(),
                partition: SourcePartition::InstanceLocal { member: None, epoch: Some("a".repeat(64)) },
                observed_from: "2020-01-01T06:02:00Z".into(),
                observed_until: "2020-01-01T06:02:00Z".into(),
                database_writable: Some(false),
            }),
            observed_role: None,
        }));
        let public = observation(&native).unwrap();
        assert_eq!(public["kind"], "mssql-readable-resources-context");
        assert!(public["data"]["observed_role"].is_null());
        assert_eq!(public["data"]["limitations"].as_array().unwrap().len(), 4);
        let text = public.to_string();
        for field in ["current_reader", "process_epoch", "database_writable", "11111111-1111-1111-1111-111111111111"] {
            assert!(!text.contains(field));
        }
        assert!(!text.contains(&"a".repeat(64)));
        assert!(serde_json::from_value::<SourceObservation>(public).is_err());
    }
}

#[cfg(test)]
mod collector_connection_tests {
    use super::*;

    #[test]
    fn collector_control_projection_keeps_client_clocks_and_no_native_admission_authority() {
        let (source, _) = crate::analytical::test_support::collector_connection_evidence(
            crate::analytical::test_support::CollectorConnectionCase::MissingRecorder,
        );
        let SourceObservation::CollectorConnection(original) = &source.observation else { panic!() };
        let public = observation(&source.observation).unwrap();
        assert_eq!(public["kind"], "collector-connection-context");
        assert_eq!(public["data"]["attempt"], serde_json::to_value(&original.attempt).unwrap());
        assert_eq!(public["data"]["snapshot_sha256"], original.snapshot_sha256);
        assert_eq!(public["data"]["collector_started_at"], original.observed_from);
        assert_eq!(public["data"]["collector_finished_at"], original.observed_until);
        assert_eq!(public["data"].as_object().unwrap().len(), 4);
        for field in
            ["target", "partition", "member", "epoch", "database_writable", "native", "observed_from", "observed_until"]
        {
            assert!(public["data"].get(field).is_none(), "{field}");
        }
        assert!(serde_json::from_value::<SourceObservation>(public).is_err());
        assert!(NativeReadContext::from_activity(&source).is_err());
        assert!(NativeReadContext::from_snapshot(&source).is_err());
        assert!(!source.is_partition_qualified());
        assert!(source.target.database().is_none());
    }
}

#[cfg(test)]
mod snapshot_acquisition_tests;
