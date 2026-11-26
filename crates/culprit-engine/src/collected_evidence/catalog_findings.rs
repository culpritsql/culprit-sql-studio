//! Standalone database catalog, index, constraint, sequence, and statistics captures feed pure schema/maintenance
//! checks without requiring a QueryHistory or StatementCounters capture.
use super::*;
use culprit_core::{
    Capability, CatalogObjectRef, ColumnInfo, ConstraintKind, DerivedContextIssue, Engine, ForeignKey, IndexStat,
    Report, StatisticsInfo, TableStat, TableStorage,
    catalog_projection::{
        apply_mysql_mixed_collations, counter_number, mysql_delete_rule, mysql_index_key_part, mysql_integer_bounds,
        mysql_sequence_capacity, pg_archive_opclasses, pg_catalog_column, pg_catalog_constraint_issue,
        pg_catalog_foreign_key, pg_sequence_capacity, pg_table_stat, push_unique_constraint_issue,
    },
    telemetry::*,
};
use std::collections::BTreeMap;

pub(crate) type CatalogProvenance = BTreeMap<CatalogObjectRef, Vec<(u16, usize)>>;

pub(crate) fn record_provenance(map: &mut CatalogProvenance, key: CatalogObjectRef, input: u16, position: usize) {
    let entries = map.entry(key).or_default();
    if !entries.contains(&(input, position)) {
        entries.push((input, position));
    }
}

pub(crate) struct RetainedCatalogAnalysis {
    pub report: Report,
    pub limitations: Vec<CollectedAnalysisLimitation>,
    pub companion_inputs: Vec<u16>,
    pub issues: Vec<DerivedContextIssue>,
    pub provenance: CatalogProvenance,
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
                    | SnapshotDataset::Sequences
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

    let (admitted_inputs, projected_datasets, issues, settings_complete, provenance) = match database.engine {
        Engine::Mssql => {
            let context = catalogs::CatalogContext::from_catalog_primary(primary, originals)?;
            context.apply(&mut snapshot);
            (
                context.inputs().to_vec(),
                context.projected_datasets().to_vec(),
                context.issues().to_vec(),
                context.settings_complete(),
                context.provenance().clone(),
            )
        }
        Engine::Postgres => {
            let selected = select_non_mssql_heads(primary, originals)?;
            let p = project_postgres_catalogs(selected, &mut snapshot)?;
            (p.admitted_inputs, p.projected_datasets, p.issues, false, p.provenance)
        }
        Engine::Yugabyte => {
            let selected = select_non_mssql_heads(primary, originals)?;
            let p = project_yugabyte_catalogs(selected, &mut snapshot)?;
            (p.admitted_inputs, p.projected_datasets, p.issues, false, p.provenance)
        }
        Engine::Mysql => {
            let selected = select_non_mssql_heads(primary, originals)?;
            let p = project_mysql_catalogs(&database.database, selected, &mut snapshot)?;
            (p.admitted_inputs, p.projected_datasets, p.issues, false, p.provenance)
        }
        _ => (vec![], vec![], vec![], false, CatalogProvenance::new()),
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
    Ok(RetainedCatalogAnalysis { report, limitations, companion_inputs, issues, provenance })
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

fn project_pg_family_columns_fks_constraints(
    ds: SnapshotDataset,
    index: u16,
    original: &dyn OriginalProjection,
    allow_inheritance_child: bool,
    col_names_by_oid: &mut BTreeMap<(String, u32), String>,
    provenance: &mut CatalogProvenance,
    snapshot: &mut Snapshot,
) -> bool {
    let mut ok = true;
    match ds {
        SnapshotDataset::Columns => {
            for (pos, row) in original.records().iter().enumerate() {
                let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                    fact: ObjectDefinitionFact::NativeCatalog(def),
                    ..
                }) = &row.row().record
                else {
                    ok = false;
                    continue;
                };
                let Some(col) = pg_catalog_column(def, None) else {
                    ok = false;
                    continue;
                };
                if let Some(rel_oid) = def.properties.get("relation_oid").filter(|s| !s.is_empty())
                    && !col.name.is_empty()
                {
                    col_names_by_oid.insert((rel_oid.clone(), col.ordinal), col.name.clone());
                }
                record_provenance(
                    provenance,
                    CatalogObjectRef::Column {
                        schema: col.schema.clone(),
                        table: col.table.clone(),
                        column: col.name.clone(),
                    },
                    index,
                    pos,
                );
                snapshot.columns.push(col);
            }
            set_canonical_source(snapshot, source_id::COLUMNS, snapshot.columns.is_empty(), !ok);
        }
        SnapshotDataset::ForeignKeys => {
            for (pos, row) in original.records().iter().enumerate() {
                let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                    fact: ObjectDefinitionFact::NativeCatalog(def),
                    ..
                }) = &row.row().record
                else {
                    ok = false;
                    continue;
                };
                let Some(fk) = pg_catalog_foreign_key(def) else {
                    ok = false;
                    continue;
                };
                if let Some(issue) = pg_catalog_constraint_issue(def, ConstraintKind::ForeignKey, false) {
                    record_provenance(
                        provenance,
                        CatalogObjectRef::Constraint {
                            schema: issue.schema.clone(),
                            table: issue.table.clone(),
                            name: issue.name.clone(),
                        },
                        index,
                        pos,
                    );
                    push_unique_constraint_issue(&mut snapshot.constraints, issue);
                }
                record_provenance(
                    provenance,
                    CatalogObjectRef::ForeignKey {
                        schema: fk.schema.clone(),
                        table: fk.table.clone(),
                        name: fk.name.clone(),
                    },
                    index,
                    pos,
                );
                snapshot.foreign_keys.push(fk);
            }
            set_canonical_source(snapshot, source_id::FOREIGN_KEYS, snapshot.foreign_keys.is_empty(), !ok);
        }
        SnapshotDataset::CheckConstraints => {
            for (pos, row) in original.records().iter().enumerate() {
                let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                    fact: ObjectDefinitionFact::NativeCatalog(def),
                    ..
                }) = &row.row().record
                else {
                    ok = false;
                    continue;
                };
                if let Some(issue) = pg_catalog_constraint_issue(def, ConstraintKind::Check, allow_inheritance_child) {
                    record_provenance(
                        provenance,
                        CatalogObjectRef::Constraint {
                            schema: issue.schema.clone(),
                            table: issue.table.clone(),
                            name: issue.name.clone(),
                        },
                        index,
                        pos,
                    );
                    push_unique_constraint_issue(&mut snapshot.constraints, issue);
                }
            }
            set_canonical_source(snapshot, source_id::CHECK_CONSTRAINTS, snapshot.constraints.is_empty(), !ok);
        }
        _ => {}
    }
    ok
}

fn resolve_pg_family_index_columns(
    table_oid: &str,
    index_name: &str,
    key_nums: &[i32],
    key_count: usize,
    options: &[i32],
    expressions_md5: Option<&str>,
    col_names_by_oid: &BTreeMap<(String, u32), String>,
) -> Option<(Vec<String>, Vec<String>)> {
    if key_nums.is_empty() || key_nums.iter().any(|&attnum| attnum < 0) {
        return None;
    }
    let mut key_cols = Vec::new();
    let mut include_cols = Vec::new();
    for (pos, &attnum) in key_nums.iter().enumerate() {
        let col_name = if attnum == 0 {
            match expressions_md5.filter(|s| !s.is_empty()) {
                Some(md5) => format!("(expr:{md5}:{pos})"),
                None => format!("(expr:{index_name}:{pos})"),
            }
        } else {
            col_names_by_oid.get(&(table_oid.to_owned(), attnum as u32))?.clone()
        };
        if pos < key_count {
            let desc = options.get(pos).is_some_and(|opt| (opt & 1) == 1);
            key_cols.push(if desc { format!("{col_name} DESC") } else { col_name });
        } else {
            include_cols.push(col_name);
        }
    }
    Some((key_cols, include_cols))
}

struct NonMssqlProjection {
    admitted_inputs: Vec<u16>,
    projected_datasets: Vec<SnapshotDataset>,
    issues: Vec<DerivedContextIssue>,
    provenance: CatalogProvenance,
}

type MysqlFkGroup<'a> = BTreeMap<(String, String), Vec<(u32, usize, &'a NativeCatalogDefinition)>>;
type MysqlIndexGroup<'a> = BTreeMap<(String, String), Vec<(u32, usize, &'a NativeCatalogCounters)>>;

fn project_postgres_catalogs(selection: NonMssqlSelection<'_>, snapshot: &mut Snapshot) -> Result<NonMssqlProjection> {
    let NonMssqlSelection { heads, mut issues } = selection;
    let mut admitted_inputs = Vec::new();
    let mut projected_datasets = Vec::new();
    let mut provenance = CatalogProvenance::new();
    let mut col_names_by_oid: BTreeMap<(String, u32), String> = BTreeMap::new();
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
                for (pos, row) in original.records().iter().enumerate() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CounterSet(set),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let table = pg_table_stat(set);
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Table { schema: table.schema.clone(), table: table.name.clone() },
                        index,
                        pos,
                    );
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Statistics {
                            schema: table.schema.clone(),
                            table: table.name.clone(),
                            name: table.name.clone(),
                        },
                        index,
                        pos,
                    );
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
            SnapshotDataset::Columns | SnapshotDataset::ForeignKeys | SnapshotDataset::CheckConstraints => {
                ok = project_pg_family_columns_fks_constraints(
                    ds,
                    index,
                    original,
                    true,
                    &mut col_names_by_oid,
                    &mut provenance,
                    snapshot,
                );
            }
            SnapshotDataset::Sequences => {
                for (pos, row) in original.records().iter().enumerate() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CatalogCounters(native),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let Some(seq) = pg_sequence_capacity(native) else {
                        ok = false;
                        continue;
                    };
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Sequence { name: seq.name.clone() },
                        index,
                        pos,
                    );
                    snapshot.operations.sequences.push(seq);
                }
                set_canonical_source(
                    snapshot,
                    source_id::SEQUENCE_CAPACITY,
                    snapshot.operations.sequences.is_empty(),
                    !ok,
                );
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
        for (pos, row) in original.records().iter().enumerate() {
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
            let index_name = prop("index_name");
            let key_nums: Vec<i32> =
                prop("native_keys").split_whitespace().filter_map(|s| s.parse::<i32>().ok()).collect();
            let key_count = prop("native_key_count").parse::<usize>().unwrap_or(key_nums.len());
            let options: Vec<i32> =
                prop("native_options").split_whitespace().filter_map(|s| s.parse::<i32>().ok()).collect();
            let expr_md5 = def.properties.get("expressions_md5").map(String::as_str);
            let Some((key_cols, include_cols)) = resolve_pg_family_index_columns(
                &table_oid,
                &index_name,
                &key_nums,
                key_count,
                &options,
                expr_md5,
                &col_names_by_oid,
            ) else {
                ok = false;
                continue;
            };
            let valid = matches!(prop("native_valid").as_str(), "t" | "true");
            let ready = matches!(prop("indisready").as_str(), "t" | "true");
            let opclasses = pg_archive_opclasses(def, key_cols.len());
            let schema = prop("schema_name");
            let table = prop("table_name");
            record_provenance(
                &mut provenance,
                CatalogObjectRef::Index { schema: schema.clone(), table: table.clone(), index: index_name.clone() },
                index,
                pos,
            );
            snapshot.indexes.push(IndexStat {
                mssql_configuration: None,
                mssql_columnstore: None,
                docdb: None,
                filter_facts: None,
                schema,
                table,
                name: index_name,
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
                opclasses,
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
    Ok(NonMssqlProjection { admitted_inputs, projected_datasets, issues, provenance })
}

fn project_yugabyte_catalogs(selection: NonMssqlSelection<'_>, snapshot: &mut Snapshot) -> Result<NonMssqlProjection> {
    let NonMssqlSelection { heads, mut issues } = selection;
    let mut admitted_inputs = Vec::new();
    let mut projected_datasets = Vec::new();
    let mut provenance = CatalogProvenance::new();
    let mut col_names_by_oid: BTreeMap<(String, u32), String> = BTreeMap::new();
    let mut pk_tables_from_indexes: Vec<(String, String)> = Vec::new();
    let mut tables_had_has_pk = true;
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
                for (pos, row) in original.records().iter().enumerate() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CatalogCounters(native),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let props = &native.catalog.properties;
                    let schema = props.get("schema_name").cloned().unwrap_or_default();
                    let name = props.get("name").or_else(|| props.get("table_name")).cloned().unwrap_or_default();
                    if name.is_empty() {
                        ok = false;
                        continue;
                    }
                    if !props.contains_key("has_pk") {
                        tables_had_has_pk = false;
                    }
                    let has_pk = props.get("has_pk").is_some_and(|v| matches!(v.as_str(), "t" | "true"));
                    let kind = Some(if props.get("kind").is_some_and(|k| k == "p") {
                        "partitioned".into()
                    } else {
                        "docdb".into()
                    });
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Table { schema: schema.clone(), table: name.clone() },
                        index,
                        pos,
                    );
                    snapshot.tables.push(TableStat {
                        schema,
                        name,
                        kind,
                        definition: None,
                        rows: counter_number(&native.counters, "estimated-rows").filter(|v| v.is_finite() && *v >= 0.0),
                        total_bytes: None,
                        data_bytes: None,
                        index_bytes: None,
                        in_row_bytes: None,
                        lob_bytes: None,
                        overflow_bytes: None,
                        has_primary_key: has_pk,
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
                        storage: None,
                        mssql_full_text: None,
                        mssql_storage_configuration: None,
                        mssql_tracked_by_cdc: None,
                        mssql_heap_configuration: None,
                        mssql_temporal_configuration: None,
                        partition_of: props.get("partition_of").cloned(),
                        counters_since: None,
                    });
                }
                set_canonical_source(snapshot, source_id::TABLES, snapshot.tables.is_empty(), !ok);
            }
            SnapshotDataset::Columns | SnapshotDataset::ForeignKeys | SnapshotDataset::CheckConstraints => {
                // YSQL native validation says nothing about PostgreSQL heap inheritance/constraint-exclusion behavior.
                ok = project_pg_family_columns_fks_constraints(
                    ds,
                    index,
                    original,
                    false,
                    &mut col_names_by_oid,
                    &mut provenance,
                    snapshot,
                );
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
        for (pos, row) in original.records().iter().enumerate() {
            let TelemetryRecord::Operational(OperationalObservation {
                fact: OperationalFact::CatalogCounters(native),
                ..
            }) = &row.row().record
            else {
                ok = false;
                continue;
            };
            let def = &native.catalog;
            let prop_opt = |a: &str, b: &str| def.properties.get(a).or_else(|| def.properties.get(b));
            let table_oid = def
                .properties
                .get("relation_oid")
                .or_else(|| def.identity.qualifiers.get("table_oid"))
                .cloned()
                .unwrap_or_default();
            if def.properties.contains_key("predicate_md5") && def.predicate.is_none() {
                // Partial index predicate text was not deparsed during background metadata capture.
                ok = false;
                continue;
            }
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
            let index_name = prop_opt("name", "index_name").cloned().unwrap_or_default();
            let raw_keys = prop_opt("key_numbers", "native_keys").map_or("", String::as_str);
            let key_nums: Vec<i32> = raw_keys.split_whitespace().filter_map(|s| s.parse::<i32>().ok()).collect();
            let key_count = prop_opt("key_count", "native_key_count")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(key_nums.len());
            let options: Vec<i32> = prop_opt("options", "native_options")
                .map_or("", String::as_str)
                .split_whitespace()
                .filter_map(|s| s.parse::<i32>().ok())
                .collect();
            let expr_md5 = prop_opt("expression_md5", "expressions_md5").map(String::as_str);
            let Some((key_cols, include_cols)) = resolve_pg_family_index_columns(
                &table_oid,
                &index_name,
                &key_nums,
                key_count,
                &options,
                expr_md5,
                &col_names_by_oid,
            ) else {
                ok = false;
                continue;
            };
            let primary = prop_opt("primary", "indisprimary").is_some_and(|v| matches!(v.as_str(), "t" | "true"));
            let unique = prop_opt("unique", "indisunique").is_some_and(|v| matches!(v.as_str(), "t" | "true"));
            let ready = prop_opt("ready", "indisready").is_some_and(|v| matches!(v.as_str(), "t" | "true"));
            let valid = prop_opt("valid", "native_valid").is_some_and(|v| matches!(v.as_str(), "t" | "true"));
            let schema = def.properties.get("schema_name").cloned().unwrap_or_default();
            let table = def.properties.get("table_name").cloned().unwrap_or_default();
            if primary {
                pk_tables_from_indexes.push((schema.clone(), table.clone()));
            }
            let opclasses = pg_archive_opclasses(def, key_cols.len());
            record_provenance(
                &mut provenance,
                CatalogObjectRef::Index { schema: schema.clone(), table: table.clone(), index: index_name.clone() },
                index,
                pos,
            );
            snapshot.indexes.push(IndexStat {
                mssql_configuration: None,
                mssql_columnstore: None,
                docdb: None,
                filter_facts: None,
                schema,
                table,
                name: index_name,
                kind: prop_opt("access_method", "kind").cloned().unwrap_or_default(),
                columns: key_cols,
                include: include_cols,
                filter,
                is_unique: unique,
                is_primary_key: primary,
                is_clustered: primary,
                is_disabled: !(valid && ready),
                state: None,
                maintained: Some(ready),
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
                partition_of: def.properties.get("partition_of").cloned(),
                opclasses,
                counters_since: None,
            });
        }
        if !tables_had_has_pk && ok {
            for (schema, table) in pk_tables_from_indexes {
                if let Some(tbl) = snapshot.tables.iter_mut().find(|t| t.schema == schema && t.name == table) {
                    tbl.has_primary_key = true;
                }
            }
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
    Ok(NonMssqlProjection { admitted_inputs, projected_datasets, issues, provenance })
}

fn project_mysql_catalogs(
    database: &str,
    selection: NonMssqlSelection<'_>,
    snapshot: &mut Snapshot,
) -> Result<NonMssqlProjection> {
    let NonMssqlSelection { heads, mut issues } = selection;
    let mut admitted_inputs = Vec::new();
    let mut projected_datasets = Vec::new();
    let mut provenance = CatalogProvenance::new();
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
                for (pos, row) in original.records().iter().enumerate() {
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
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Table { schema: database.to_owned(), table: name.clone() },
                        index,
                        pos,
                    );
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
                let mut staged_cols: Vec<ColumnInfo> = Vec::new();
                let mut staged_colls: Vec<Option<String>> = Vec::new();
                for (pos, row) in original.records().iter().enumerate() {
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
                    let table = prop("table_name");
                    let col_name = prop("name");
                    record_provenance(
                        &mut provenance,
                        CatalogObjectRef::Column {
                            schema: database.to_owned(),
                            table: table.clone(),
                            column: col_name.clone(),
                        },
                        index,
                        pos,
                    );
                    staged_colls.push(def.properties.get("collation_name").cloned());
                    staged_cols.push(ColumnInfo {
                        schema: database.to_owned(),
                        table,
                        name: col_name,
                        data_type: prop("data_type"),
                        nullable: prop("nullable").eq_ignore_ascii_case("YES"),
                        default: None,
                        generated: prop("extra").to_ascii_lowercase().contains("auto_increment"),
                        ordinal,
                    });
                }
                let coll_refs: Vec<Option<&str>> = staged_colls.iter().map(|c| c.as_deref()).collect();
                apply_mysql_mixed_collations(&mut staged_cols, &coll_refs);
                snapshot.columns.extend(staged_cols);
                set_canonical_source(snapshot, source_id::COLUMNS, snapshot.columns.is_empty(), !ok);
            }
            SnapshotDataset::ForeignKeys => {
                let mut grouped: MysqlFkGroup<'_> = BTreeMap::new();
                for (pos, row) in original.records().iter().enumerate() {
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
                    grouped.entry((prop("table_name"), prop("name"))).or_default().push((ord, pos, def));
                }
                for ((table, name), mut parts) in grouped {
                    parts.sort_by_key(|(ord, _, _)| *ord);
                    let first = parts[0].2;
                    let prop = |d: &NativeCatalogDefinition, k: &str| d.properties.get(k).cloned().unwrap_or_default();
                    for &(_, pos, _) in &parts {
                        record_provenance(
                            &mut provenance,
                            CatalogObjectRef::ForeignKey {
                                schema: database.to_owned(),
                                table: table.clone(),
                                name: name.clone(),
                            },
                            index,
                            pos,
                        );
                    }
                    snapshot.foreign_keys.push(ForeignKey {
                        name,
                        schema: database.to_owned(),
                        table,
                        columns: parts.iter().map(|(_, _, d)| prop(d, "column_name")).collect(),
                        ref_schema: prop(first, "ref_schema"),
                        ref_table: prop(first, "ref_table"),
                        ref_columns: parts.iter().map(|(_, _, d)| prop(d, "ref_column")).collect(),
                        delete_rule: mysql_delete_rule(first.properties.get("delete_rule").map(String::as_str)),
                    });
                }
                set_canonical_source(snapshot, source_id::FOREIGN_KEYS, snapshot.foreign_keys.is_empty(), !ok);
            }
            SnapshotDataset::CheckConstraints => {
                for (pos, row) in original.records().iter().enumerate() {
                    if let TelemetryRecord::ObjectDefinition(ObjectDefinitionRecord {
                        fact: ObjectDefinitionFact::NativeCatalog(def),
                        ..
                    }) = &row.row().record
                    {
                        let table = def.properties.get("table_name").cloned().unwrap_or_default();
                        let name = def.properties.get("name").cloned().unwrap_or_default();
                        if !table.is_empty() && !name.is_empty() {
                            record_provenance(
                                &mut provenance,
                                CatalogObjectRef::Constraint { schema: database.to_owned(), table, name },
                                index,
                                pos,
                            );
                        }
                    } else {
                        ok = false;
                    }
                }
                set_canonical_source(snapshot, source_id::CHECK_CONSTRAINTS, original.records().is_empty(), !ok);
            }
            SnapshotDataset::Sequences => {
                for (pos, row) in original.records().iter().enumerate() {
                    let TelemetryRecord::Operational(OperationalObservation {
                        fact: OperationalFact::CatalogCounters(native),
                        ..
                    }) = &row.row().record
                    else {
                        ok = false;
                        continue;
                    };
                    let props = &native.catalog.properties;
                    let col_type = props.get("column_type").map_or("", String::as_str);
                    if mysql_integer_bounds(col_type).is_none() {
                        ok = false;
                        continue;
                    }
                    let next_value =
                        native.counters.counters.iter().find(|c| c.key == "next_value").map(|c| match &c.value {
                            MeasurementValue::Integer(raw) => raw.clone(),
                            MeasurementValue::Real(v) => (*v as u64).to_string(),
                        });
                    let (Some(name), Some(current), Some(step)) =
                        (props.get("name").cloned(), next_value, props.get("step").cloned())
                    else {
                        ok = false;
                        continue;
                    };
                    if let Some(seq) =
                        mysql_sequence_capacity(name.clone(), props.get("table_name").cloned(), col_type, current, step)
                    {
                        record_provenance(&mut provenance, CatalogObjectRef::Sequence { name }, index, pos);
                        snapshot.operations.sequences.push(seq);
                    }
                }
                set_canonical_source(
                    snapshot,
                    source_id::SEQUENCE_CAPACITY,
                    snapshot.operations.sequences.is_empty(),
                    !ok,
                );
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
        let mut grouped: MysqlIndexGroup<'_> = BTreeMap::new();
        for (pos, row) in original.records().iter().enumerate() {
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
            grouped.entry((prop("table_name"), prop("index_name"))).or_default().push((seq, pos, native));
        }
        for ((table, name), mut parts) in grouped {
            parts.sort_by_key(|(seq, _, _)| *seq);
            let mut cols = Vec::new();
            for &(seq, pos, part) in &parts {
                record_provenance(
                    &mut provenance,
                    CatalogObjectRef::Index { schema: database.to_owned(), table: table.clone(), index: name.clone() },
                    index,
                    pos,
                );
                let col_name = part.catalog.properties.get("column_name").map(String::as_str);
                let sub_part = part.catalog.properties.get("sub_part").and_then(|s| s.parse::<u64>().ok());
                let collation = part.catalog.properties.get("collation").map(String::as_str);
                let uncollected_expr = format!("(expr:{name}:{seq})");
                cols.push(mysql_index_key_part(col_name, None, sub_part, collation, &uncollected_expr));
            }
            let first = &parts[0].2.catalog;
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
    Ok(NonMssqlProjection { admitted_inputs, projected_datasets, issues, provenance })
}
