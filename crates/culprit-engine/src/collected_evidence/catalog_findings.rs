//! Standalone database catalog, index, constraint, and statistics captures feed pure schema/maintenance checks
//! without requiring a QueryHistory or StatementCounters capture.
use super::*;
use culprit_core::{
    Capability, ColumnInfo, ConstraintIssue, ConstraintKind, DerivedContextIssue, Engine, ForeignKey, IndexStat,
    Report, StatisticsInfo, TableStat, TableStorage, telemetry::*,
};
use std::collections::BTreeMap;

pub(crate) struct RetainedCatalogAnalysis {
    pub report: Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
    pub companion_inputs: Vec<u16>,
    pub admitted_inputs: Vec<u16>,
    pub issues: Vec<DerivedContextIssue>,
}

pub(crate) fn supports_catalog_findings(original: &dyn OriginalProjection) -> bool {
    original.source().target.database().is_some()
        && original.source().observation.snapshot().is_some_and(|snapshot| {
            matches!(
                snapshot.dataset,
                SnapshotDataset::Tables
                    | SnapshotDataset::Indexes
                    | SnapshotDataset::Columns
                    | SnapshotDataset::Statistics
                    | SnapshotDataset::ForeignKeys
                    | SnapshotDataset::CheckConstraints
                    | SnapshotDataset::Triggers
                    | SnapshotDataset::Partitions
            )
        })
}

pub(crate) fn analyze_catalog_originals(
    primary: &dyn OriginalProjection,
    originals: &[&dyn OriginalProjection],
    pipeline: &culprit_checks::Pipeline,
) -> Result<RetainedCatalogAnalysis> {
    let source = primary.source();
    let database = source.target.database().ok_or_else(invalid)?.clone();
    let header = source.observation.snapshot().ok_or_else(invalid)?;
    if !supports_catalog_findings(primary)
        || !source.partition_is_consistent()
        || !source.partition.is_qualified()
        || !primary.original().is_valid()
        || !header.is_valid()
        || primary.records().len() > header.records as usize
    {
        return Err(invalid());
    }

    let mut limitations = vec![
        limit(None, CollectedAnalysisLimit::MissingCurrentServerMetadata),
        limit(None, CollectedAnalysisLimit::MissingSettings),
        limit(None, CollectedAnalysisLimit::MissingResourceMeasurements),
    ];
    let analysis_key = primary.original().key()?;
    let empty_catalogs = catalogs::CatalogContext::default();
    let policy = AnalysisPolicy {
        analysis_key: &analysis_key,
        pipeline,
        origin: culprit_core::AnalysisOrigin::RetainedInput,
        catalogs: &empty_catalogs,
    };
    let mut snapshot = super::snapshot(primary, database.clone(), None, !primary.is_read_complete(), &policy);
    snapshot.taken_at = header.observed_until.clone();
    if database.engine != Engine::Sqlite {
        // Server engines record per-relation catalog sources (`tables`, `indexes`, `columns`, `foreign_keys`)
        // rather than SQLite's monolithic `catalog` source.
        snapshot.sources.retain(|entry| entry.source != source_id::CATALOG);
    }

    let (admitted_inputs, projected_datasets, issues, settings_complete) = match database.engine {
        Engine::Mssql => {
            let context = catalogs::CatalogContext::from_catalog_primary(primary, originals)?;
            context.apply(&mut snapshot);
            (
                context.inputs().to_vec(),
                context.projected_datasets().to_vec(),
                context.issues().to_vec(),
                context.settings_complete(),
            )
        }
        Engine::Postgres | Engine::Yugabyte => {
            let selected = select_non_mssql_heads(primary, originals)?;
            let (admitted, projected, issues) = project_postgres_catalogs(selected, &mut snapshot)?;
            (admitted, projected, issues, false)
        }
        Engine::Mysql => {
            let selected = select_non_mssql_heads(primary, originals)?;
            let (admitted, projected, issues) = project_mysql_catalogs(&database.database, selected, &mut snapshot)?;
            (admitted, projected, issues, false)
        }
        _ => (vec![], vec![], vec![], false),
    };

    if settings_complete {
        limitations.retain(|limitation| limitation.reason != CollectedAnalysisLimit::MissingSettings);
    }

    // Grant structural schema/statistics capabilities only when complete datasets were admitted.
    // IndexUsage remains withheld so a point catalog capture never claims 7-day multi-replica index absence.
    if projected_datasets.iter().any(|ds| matches!(ds, SnapshotDataset::Tables | SnapshotDataset::Indexes)) {
        grant_capability(&mut snapshot, Capability::TableStats);
    }
    if projected_datasets.contains(&SnapshotDataset::Statistics)
        || (matches!(database.engine, Engine::Postgres | Engine::Yugabyte)
            && projected_datasets.contains(&SnapshotDataset::Tables))
    {
        grant_capability(&mut snapshot, Capability::Statistics);
    }

    let companion_inputs = admitted_inputs.iter().copied().filter(|&idx| idx > 0).collect();
    let report =
        run_retained_snapshot(snapshot, pipeline, culprit_core::AnalysisOrigin::RetainedInput, &mut limitations);
    Ok(RetainedCatalogAnalysis { report, limitations, companion_inputs, admitted_inputs, issues })
}

fn grant_capability(snapshot: &mut Snapshot, capability: Capability) {
    if !snapshot.server.capabilities.contains(&capability) {
        snapshot.server.capabilities.push(capability);
    }
    snapshot.server.missing.retain(|missing| missing.capability != capability);
}

struct NonMssqlSelection<'a> {
    heads: Vec<(u16, &'a dyn OriginalProjection)>,
    issues: Vec<DerivedContextIssue>,
}

fn same_non_mssql_scope(primary: &dyn OriginalProjection, original: &dyn OriginalProjection) -> bool {
    if primary.source().target != original.source().target
        || !primary.source().partition_is_consistent()
        || !original.source().partition_is_consistent()
        || !primary.source().partition.is_qualified()
        || !original.source().partition.is_qualified()
        || primary.source().partition != original.source().partition
    {
        return false;
    }
    let primary_writable = NativeReadContext::from_snapshot(primary.source()).ok().and_then(|c| c.database_writable);
    let candidate_writable = NativeReadContext::from_snapshot(original.source()).ok().and_then(|c| c.database_writable);
    match (primary_writable, candidate_writable) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

fn select_non_mssql_heads<'a>(
    primary: &'a dyn OriginalProjection,
    originals: &[&'a dyn OriginalProjection],
) -> Result<NonMssqlSelection<'a>> {
    let as_of = validation::time(primary.collected_until())?;
    let primary_dataset = catalogs::selection::dataset(primary).ok_or_else(invalid)?;
    let mut heads: Vec<(SnapshotDataset, u16, &'a dyn OriginalProjection, bool)> =
        vec![(primary_dataset, 0, primary, false)];
    let mut issues = Vec::new();
    let add_issue = |issues: &mut Vec<DerivedContextIssue>, issue: DerivedContextIssue| {
        if !issues.contains(&issue) {
            issues.push(issue);
        }
    };
    for (index, &original) in originals.iter().enumerate().skip(1) {
        if original.capture() == primary.capture() {
            continue;
        }
        let Some(ds) = catalogs::selection::dataset(original) else {
            continue;
        };
        if !same_non_mssql_scope(primary, original) {
            add_issue(&mut issues, DerivedContextIssue::UnqualifiedCatalogScope);
            continue;
        }
        let Ok(until) = validation::time(original.collected_until()) else {
            add_issue(&mut issues, DerivedContextIssue::UnknownCatalogClock);
            continue;
        };
        if until > as_of {
            add_issue(&mut issues, DerivedContextIssue::CatalogNotRetainedAsOf);
            continue;
        }
        if ds == primary_dataset {
            continue;
        }
        let index = index.try_into().map_err(|_| invalid())?;
        if let Some(head) = heads.iter_mut().find(|h| h.0 == ds) {
            let current = validation::time(head.2.collected_until())?;
            if until > current {
                *head = (ds, index, original, false);
            } else if until == current {
                head.3 = true;
            }
        } else {
            heads.push((ds, index, original, false));
        }
    }
    let mut selected = Vec::new();
    for (_, index, original, ambiguous) in heads {
        if ambiguous {
            add_issue(&mut issues, DerivedContextIssue::AmbiguousCatalog);
        } else if !catalogs::complete(original) {
            add_issue(&mut issues, DerivedContextIssue::PartialCatalog);
        } else {
            selected.push((index, original));
        }
    }
    Ok(NonMssqlSelection { heads: selected, issues })
}

fn set_canonical_source(snapshot: &mut Snapshot, source_name: &str, empty: bool, partial: bool) {
    let state = if partial {
        SourceState::Partial
    } else if empty {
        SourceState::Empty
    } else {
        SourceState::Collected
    };
    let reason = partial
        .then(|| culprit_core::LocalizedText::new(culprit_core::message!("view.synthesis.context.catalog_projection")));
    if let Some(existing) = snapshot.sources.iter_mut().find(|entry| entry.source == source_name) {
        existing.state = state;
        existing.reason = reason;
    } else {
        snapshot.sources.push(SourceOutcome { source: source_name.into(), state, reason });
    }
}

fn counter_number(set: &NativeCounterSet, key: &str) -> Option<f64> {
    match &set.counters.iter().find(|sample| sample.key == key)?.value {
        MeasurementValue::Integer(raw) => raw.parse::<f64>().ok(),
        MeasurementValue::Real(value) => Some(*value),
    }
}

fn project_postgres_catalogs(
    selection: NonMssqlSelection<'_>,
    snapshot: &mut Snapshot,
) -> Result<(Vec<u16>, Vec<SnapshotDataset>, Vec<DerivedContextIssue>)> {
    let NonMssqlSelection { heads, mut issues } = selection;
    let mut admitted_inputs = Vec::new();
    let mut projected_datasets = Vec::new();
    let mut col_names_by_oid: BTreeMap<(String, u32), String> = BTreeMap::new();
    let mut staged_indexes: Option<(u16, &dyn OriginalProjection)> = None;

    // Pass 1: project Columns, Tables, ForeignKeys, CheckConstraints so column OID -> name lookup is ready for Indexes.
    for &(index, original) in &heads {
        let ds = catalogs::selection::dataset(original).ok_or_else(invalid)?;
        if ds == SnapshotDataset::Indexes {
            staged_indexes = Some((index, original));
            continue;
        }
        let mut ok = true;
        match ds {
            SnapshotDataset::Tables => {
                for row in original.records() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CounterSet(set),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| set.properties.get(k).cloned().unwrap_or_default();
                    let opt = |k: &str| set.properties.get(k).cloned();
                    let table = TableStat {
                        schema: prop("schema_name"),
                        name: prop("table_name"),
                        kind: None,
                        definition: None,
                        rows: counter_number(set, "rows").map(|v| v.max(0.0)),
                        total_bytes: counter_number(set, "total_bytes"),
                        data_bytes: counter_number(set, "data_bytes"),
                        index_bytes: counter_number(set, "index_bytes"),
                        in_row_bytes: counter_number(set, "in_row_bytes"),
                        lob_bytes: counter_number(set, "lob_bytes"),
                        overflow_bytes: None,
                        has_primary_key: matches!(prop("has_pk").as_str(), "t" | "true"),
                        is_heap: None,
                        seq_scans: counter_number(set, "seq_scan"),
                        seq_rows_read: counter_number(set, "seq_tup_read"),
                        index_scans: counter_number(set, "idx_scan"),
                        inserts: counter_number(set, "n_tup_ins"),
                        updates: counter_number(set, "n_tup_upd"),
                        deletes: counter_number(set, "n_tup_del"),
                        dead_rows: counter_number(set, "n_dead_tup"),
                        modifications_since_analyze: counter_number(set, "n_mod_since_analyze"),
                        last_vacuum: opt("last_vacuum"),
                        last_analyze: opt("last_analyze"),
                        forwarded_records: None,
                        forwarded_fetches: None,
                        xid_age: counter_number(set, "xid_age"),
                        storage: None,
                        mssql_full_text: None,
                        mssql_storage_configuration: None,
                        mssql_tracked_by_cdc: None,
                        mssql_heap_configuration: None,
                        mssql_temporal_configuration: None,
                        partition_of: opt("partition_of"),
                        counters_since: None,
                    };
                    snapshot.statistics.push(StatisticsInfo {
                        schema: table.schema.clone(),
                        table: table.name.clone(),
                        name: table.name.clone(),
                        last_updated: table.last_analyze.clone(),
                        rows: table.rows,
                        rows_sampled: None,
                        modifications: table.modifications_since_analyze,
                        auto_created: false,
                    });
                    snapshot.tables.push(table);
                }
                set_canonical_source(snapshot, source_id::TABLES, snapshot.tables.is_empty(), !ok);
            }
            SnapshotDataset::Columns => {
                for row in original.records() {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
                    let Some(ordinal) = prop("ordinal").parse::<u32>().ok() else {
                        ok = false;
                        continue;
                    };
                    let rel_oid = prop("relation_oid");
                    let col_name = prop("column_name");
                    if !rel_oid.is_empty() && !col_name.is_empty() {
                        col_names_by_oid.insert((rel_oid, ordinal), col_name.clone());
                    }
                    let default = match &def.definition {
                        Some(CatalogExpression { payload: DefinitionPayload::Available { text, .. }, .. }) => {
                            Some(text.clone())
                        }
                        _ => None,
                    };
                    snapshot.columns.push(ColumnInfo {
                        schema: prop("schema_name"),
                        table: prop("table_name"),
                        name: col_name,
                        data_type: prop("data_type"),
                        nullable: prop("nullable") == "true",
                        generated: !prop("identity").is_empty()
                            || !prop("generated").is_empty()
                            || default.as_deref().is_some_and(|v| v.starts_with("nextval(")),
                        default,
                        ordinal,
                    });
                }
                set_canonical_source(snapshot, source_id::COLUMNS, snapshot.columns.is_empty(), !ok);
            }
            SnapshotDataset::ForeignKeys => {
                for row in original.records() {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
                    let cols: Option<Vec<String>> = serde_json::from_str(&prop("columns")).ok();
                    let ref_cols: Option<Vec<String>> = serde_json::from_str(&prop("ref_columns")).ok();
                    let (Some(columns), Some(ref_columns)) = (cols, ref_cols) else {
                        ok = false;
                        continue;
                    };
                    let name = prop("name");
                    let schema = prop("schema_name");
                    let table = prop("table_name");
                    if prop("validated") == "false" {
                        snapshot.constraints.push(ConstraintIssue {
                            schema: schema.clone(),
                            table: table.clone(),
                            name: name.clone(),
                            kind: ConstraintKind::ForeignKey,
                            disabled: false,
                            inheritance_child: false,
                        });
                    }
                    snapshot.foreign_keys.push(ForeignKey {
                        name,
                        schema,
                        table,
                        columns,
                        ref_schema: prop("ref_schema"),
                        ref_table: prop("ref_table"),
                        ref_columns,
                        delete_rule: match prop("delete_rule").as_str() {
                            "c" => Some("CASCADE".into()),
                            "n" => Some("SET NULL".into()),
                            "d" => Some("SET DEFAULT".into()),
                            "r" => Some("RESTRICT".into()),
                            _ => None,
                        },
                    });
                }
                set_canonical_source(snapshot, source_id::FOREIGN_KEYS, snapshot.foreign_keys.is_empty(), !ok);
            }
            SnapshotDataset::CheckConstraints => {
                for row in original.records() {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
                    if prop("validated") == "false" && prop("inheritance_child") != "true" {
                        let kind = if prop("constraint_type") == "f" {
                            ConstraintKind::ForeignKey
                        } else {
                            ConstraintKind::Check
                        };
                        snapshot.constraints.push(ConstraintIssue {
                            schema: prop("schema_name"),
                            table: prop("table_name"),
                            name: prop("name"),
                            kind,
                            disabled: false,
                            inheritance_child: false,
                        });
                    }
                }
                set_canonical_source(snapshot, source_id::CHECK_CONSTRAINTS, snapshot.constraints.is_empty(), !ok);
            }
            _ => {}
        }
        if !ok && !issues.contains(&DerivedContextIssue::PartialCatalog) {
            issues.push(DerivedContextIssue::PartialCatalog);
        }
        admitted_inputs.push(index);
        if ok {
            projected_datasets.push(ds);
        }
    }

    // Pass 2: project Indexes using resolved column OIDs when available.
    if let Some((index, original)) = staged_indexes {
        let mut ok = true;
        for row in original.records() {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::CatalogCounters(native),
                ..
            }) = &row.row().record
            else {
                ok = false;
                continue;
            };
            let def = &native.catalog;
            let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
            let table_oid = def.identity.qualifiers.get("table_oid").cloned().unwrap_or_default();
            let filter = match &def.predicate {
                None => None,
                Some(CatalogExpression { payload: DefinitionPayload::Available { text, .. }, .. }) => {
                    Some(text.clone())
                }
                Some(_) => {
                    ok = false;
                    continue;
                }
            };
            let key_nums: Vec<i32> =
                prop("native_keys").split_whitespace().filter_map(|s| s.parse::<i32>().ok()).collect();
            let key_count = prop("native_key_count").parse::<usize>().unwrap_or(key_nums.len());
            let options: Vec<i32> =
                prop("native_options").split_whitespace().filter_map(|s| s.parse::<i32>().ok()).collect();
            if key_nums.is_empty() || key_nums.iter().any(|&attnum| attnum <= 0) {
                ok = false;
                continue;
            }
            let mut key_cols = Vec::new();
            let mut include_cols = Vec::new();
            let mut resolved_all = true;
            for (pos, &attnum) in key_nums.iter().enumerate() {
                let Some(col_name) = col_names_by_oid.get(&(table_oid.clone(), attnum as u32)) else {
                    resolved_all = false;
                    break;
                };
                if pos < key_count {
                    let desc = options.get(pos).is_some_and(|opt| (opt & 1) == 1);
                    key_cols.push(if desc { format!("{col_name} DESC") } else { col_name.clone() });
                } else {
                    include_cols.push(col_name.clone());
                }
            }
            if !resolved_all {
                ok = false;
                continue;
            }
            let valid = matches!(prop("native_valid").as_str(), "t" | "true");
            let ready = matches!(prop("indisready").as_str(), "t" | "true");
            snapshot.indexes.push(IndexStat {
                mssql_configuration: None,
                mssql_columnstore: None,
                docdb: None,
                filter_facts: None,
                schema: prop("schema_name"),
                table: prop("table_name"),
                name: prop("index_name"),
                kind: prop("kind"),
                columns: key_cols,
                include: include_cols,
                filter,
                is_unique: matches!(prop("indisunique").as_str(), "t" | "true"),
                is_primary_key: matches!(prop("indisprimary").as_str(), "t" | "true"),
                is_clustered: false,
                is_disabled: !(valid && ready),
                state: None,
                maintained: Some(ready),
                has_statistics: None,
                size_bytes: counter_number(&native.counters, "estimated_bytes"),
                reads: counter_number(&native.counters, "idx_scan"),
                writes: None,
                seeks: None,
                scans: counter_number(&native.counters, "idx_scan"),
                lookups: None,
                last_read: None,
                definition: None,
                contention: None,
                created_at_latest: None,
                partition_of: def.properties.get("partition_of").cloned(),
                opclasses: None,
                counters_since: None,
            });
        }
        set_canonical_source(snapshot, source_id::INDEXES, snapshot.indexes.is_empty(), !ok);
        if !ok && !issues.contains(&DerivedContextIssue::PartialCatalog) {
            issues.push(DerivedContextIssue::PartialCatalog);
        }
        admitted_inputs.push(index);
        if ok {
            projected_datasets.push(SnapshotDataset::Indexes);
        }
    }

    if projected_datasets.contains(&SnapshotDataset::ForeignKeys)
        && projected_datasets.contains(&SnapshotDataset::CheckConstraints)
    {
        set_canonical_source(snapshot, source_id::CONSTRAINTS, snapshot.constraints.is_empty(), false);
    }
    admitted_inputs.sort_unstable();
    Ok((admitted_inputs, projected_datasets, issues))
}

fn project_mysql_catalogs(
    database: &str,
    selection: NonMssqlSelection<'_>,
    snapshot: &mut Snapshot,
) -> Result<(Vec<u16>, Vec<SnapshotDataset>, Vec<DerivedContextIssue>)> {
    let NonMssqlSelection { heads, mut issues } = selection;
    let mut admitted_inputs = Vec::new();
    let mut projected_datasets = Vec::new();
    let mut table_engines: BTreeMap<String, String> = BTreeMap::new();
    let mut staged_indexes: Option<(u16, &dyn OriginalProjection)> = None;

    for &(index, original) in &heads {
        let ds = catalogs::selection::dataset(original).ok_or_else(invalid)?;
        if ds == SnapshotDataset::Indexes {
            staged_indexes = Some((index, original));
            continue;
        }
        let mut ok = true;
        match ds {
            SnapshotDataset::Tables => {
                for row in original.records() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CatalogCounters(native),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| native.catalog.properties.get(k).cloned().unwrap_or_default();
                    let name = prop("name");
                    let engine_name = prop("engine");
                    if !engine_name.is_empty() {
                        table_engines.insert(name.to_ascii_lowercase(), engine_name.clone());
                    }
                    let data = counter_number(&native.counters, "data_bytes");
                    let idx_bytes = counter_number(&native.counters, "index_bytes");
                    let storage = (!engine_name.is_empty())
                        .then(|| TableStorage { options: vec![format!("engine={engine_name}")], ..Default::default() });
                    snapshot.tables.push(TableStat {
                        schema: database.to_owned(),
                        name,
                        kind: None,
                        definition: None,
                        rows: counter_number(&native.counters, "row_estimate"),
                        total_bytes: match (data, idx_bytes) {
                            (None, None) => None,
                            (d, i) => Some(d.unwrap_or(0.0) + i.unwrap_or(0.0)),
                        },
                        data_bytes: data,
                        index_bytes: idx_bytes,
                        in_row_bytes: None,
                        lob_bytes: None,
                        overflow_bytes: None,
                        has_primary_key: matches!(prop("has_pk").as_str(), "1" | "true" | "YES"),
                        is_heap: None,
                        seq_scans: None,
                        seq_rows_read: None,
                        index_scans: None,
                        inserts: None,
                        updates: None,
                        deletes: None,
                        dead_rows: None,
                        modifications_since_analyze: None,
                        last_vacuum: None,
                        last_analyze: None,
                        forwarded_records: None,
                        forwarded_fetches: None,
                        xid_age: None,
                        storage,
                        mssql_full_text: None,
                        mssql_storage_configuration: None,
                        mssql_tracked_by_cdc: None,
                        mssql_heap_configuration: None,
                        mssql_temporal_configuration: None,
                        partition_of: None,
                        counters_since: None,
                    });
                }
                set_canonical_source(snapshot, source_id::TABLES, snapshot.tables.is_empty(), !ok);
            }
            SnapshotDataset::Columns => {
                for row in original.records() {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
                    let Some(ordinal) = prop("ordinal").parse::<u32>().ok() else {
                        ok = false;
                        continue;
                    };
                    snapshot.columns.push(ColumnInfo {
                        schema: database.to_owned(),
                        table: prop("table_name"),
                        name: prop("name"),
                        data_type: prop("data_type"),
                        nullable: prop("nullable").eq_ignore_ascii_case("YES"),
                        default: None,
                        generated: prop("extra").to_ascii_lowercase().contains("auto_increment"),
                        ordinal,
                    });
                }
                set_canonical_source(snapshot, source_id::COLUMNS, snapshot.columns.is_empty(), !ok);
            }
            SnapshotDataset::ForeignKeys => {
                let mut grouped: BTreeMap<(String, String), Vec<(u32, &NativeCatalogDefinition)>> = BTreeMap::new();
                for row in original.records() {
                    let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let prop = |k: &str| def.properties.get(k).cloned().unwrap_or_default();
                    let ord = prop("ordinal").parse::<u32>().unwrap_or(0);
                    grouped.entry((prop("table_name"), prop("name"))).or_default().push((ord, def));
                }
                for ((table, name), mut parts) in grouped {
                    parts.sort_by_key(|(ord, _)| *ord);
                    let first = parts[0].1;
                    let prop = |d: &NativeCatalogDefinition, k: &str| d.properties.get(k).cloned().unwrap_or_default();
                    snapshot.foreign_keys.push(ForeignKey {
                        name,
                        schema: database.to_owned(),
                        table,
                        columns: parts.iter().map(|(_, d)| prop(d, "column_name")).collect(),
                        ref_schema: prop(first, "ref_schema"),
                        ref_table: prop(first, "ref_table"),
                        ref_columns: parts.iter().map(|(_, d)| prop(d, "ref_column")).collect(),
                        delete_rule: first.properties.get("delete_rule").cloned(),
                    });
                }
                set_canonical_source(snapshot, source_id::FOREIGN_KEYS, snapshot.foreign_keys.is_empty(), !ok);
            }
            SnapshotDataset::CheckConstraints => {
                set_canonical_source(snapshot, source_id::CHECK_CONSTRAINTS, original.records().is_empty(), false);
            }
            _ => {}
        }
        if !ok && !issues.contains(&DerivedContextIssue::PartialCatalog) {
            issues.push(DerivedContextIssue::PartialCatalog);
        }
        admitted_inputs.push(index);
        if ok {
            projected_datasets.push(ds);
        }
    }

    if let Some((index, original)) = staged_indexes {
        let mut ok = true;
        let mut grouped: BTreeMap<(String, String), Vec<(u32, &NativeCatalogCounters)>> = BTreeMap::new();
        for row in original.records() {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::CatalogCounters(native),
                ..
            }) = &row.row().record
            else {
                ok = false;
                continue;
            };
            let prop = |k: &str| native.catalog.properties.get(k).cloned().unwrap_or_default();
            let seq = prop("seq").parse::<u32>().unwrap_or(0);
            grouped.entry((prop("table_name"), prop("index_name"))).or_default().push((seq, native));
        }
        for ((table, name), mut parts) in grouped {
            parts.sort_by_key(|(seq, _)| *seq);
            let mut cols = Vec::new();
            let mut part_ok = true;
            for (_, part) in &parts {
                let Some(col) = part.catalog.properties.get("column_name").filter(|s| !s.is_empty()) else {
                    part_ok = false;
                    break;
                };
                let base = match part.catalog.properties.get("sub_part").and_then(|s| s.parse::<u64>().ok()) {
                    Some(prefix) => format!("{col}({prefix})"),
                    None => col.clone(),
                };
                let desc = part.catalog.properties.get("collation").is_some_and(|c| c == "D");
                cols.push(if desc { format!("{base} DESC") } else { base });
            }
            if !part_ok {
                ok = false;
                continue;
            }
            let first = &parts[0].1.catalog;
            let primary_key = name.eq_ignore_ascii_case("PRIMARY");
            let unique = first.properties.get("non_unique").is_some_and(|v| v == "0");
            let innodb =
                table_engines.get(&table.to_ascii_lowercase()).is_some_and(|eng| eng.eq_ignore_ascii_case("InnoDB"));
            snapshot.indexes.push(IndexStat {
                mssql_configuration: None,
                mssql_columnstore: None,
                docdb: None,
                filter_facts: None,
                schema: database.to_owned(),
                table,
                name,
                kind: first.properties.get("index_type").cloned().unwrap_or_default().to_ascii_lowercase(),
                columns: cols,
                include: Vec::new(),
                filter: None,
                is_unique: unique,
                is_primary_key: primary_key,
                is_clustered: primary_key && innodb,
                is_disabled: first.properties.get("visible").is_some_and(|v| v.eq_ignore_ascii_case("NO")),
                state: None,
                maintained: Some(true),
                has_statistics: None,
                size_bytes: None,
                reads: None,
                writes: None,
                seeks: None,
                scans: None,
                lookups: None,
                last_read: None,
                definition: None,
                contention: None,
                created_at_latest: None,
                partition_of: None,
                opclasses: None,
                counters_since: None,
            });
        }
        set_canonical_source(snapshot, source_id::INDEXES, snapshot.indexes.is_empty(), !ok);
        if !ok && !issues.contains(&DerivedContextIssue::PartialCatalog) {
            issues.push(DerivedContextIssue::PartialCatalog);
        }
        admitted_inputs.push(index);
        if ok {
            projected_datasets.push(SnapshotDataset::Indexes);
        }
    }

    admitted_inputs.sort_unstable();
    Ok((admitted_inputs, projected_datasets, issues))
}
