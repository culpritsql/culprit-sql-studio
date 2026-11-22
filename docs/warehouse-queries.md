# Published diagnoses in ADX and ClickHouse

Culprit emits the same incident, finding and change contracts to both warehouses. These helper assets select those
recorded answers; they contain no SQL/KQL diagnostic rules and never query an application database.

Generate one binding for the existing evidence configuration and exact interpretation policy:

```sh
culprit collect queries --config archive.json --name Pilot \
  --history-from 2026-11-01T00:00:00Z --rules-version "$CULPRIT_RULES_VERSION" \
  --analyzer-version "$CULPRIT_ANALYZER_VERSION" --format text > query-helpers.txt
```

Use the actual collector rules/package/build identity; `--analyzer-revision` selects a recorded build revision when
present. There is no lexical “latest rules” policy. A mismatching interpretation is unavailable, rather than an
older active incident. `--history-from` is a retained-publication lower bound, not a complete-history assertion.

Generation opens the configured original-evidence root through the existing bounded evidence session and validates
the actual namespace. Contract, schema and store IDs are embedded in the functions/views; operators do not enter
those values for each query. No deployment is performed. ClickHouse helpers explicitly use SQL SECURITY INVOKER; existing underlying read permissions remain required. Hidden binding values are convenience, not an authorization boundary. Apply the returned script explicitly to the same existing
warehouse database using its deployment permissions, after the normal `collect schema` assets. JSON output also
contains the binding, store-owned executable script, commands and eight example queries. Only the named helper functions/views are replaced by explicit deployment. No landing table, cloud service or telemetry source is added.

The namespace view is dynamic. New published resources become visible without regenerating a database roster.
Its publication, batch membership, exact row-count, conflict, owner and reference checks use the same renderer as
the existing resource-scoped views. Existing scalar-resource queries keep their policy. These server-side views
preserve logical eligibility; the EvidenceService remains the owner of cryptographic original/EOF authentication
when a person or agent opens an incident.

## Browse by an approved recorded label

ADX:

```kusto
PilotResources(now())
PilotStatus("Orders production", now(), 30m)
PilotIncidents("Orders production", ago(1h), now(), now())
PilotFindings("Orders production", ago(1h), now(), now())
PilotChanges("Orders production", ago(1h), now(), now())
PilotIncidentHistory("Orders production", ago(1d), now(), now())
```

ClickHouse:

```sql
SELECT * FROM PilotResources(asof=now64(9));
SELECT * FROM PilotStatus(resource='Orders production', asof=now64(9), max_age_seconds=1800);
SELECT * FROM PilotIncidents(resource='Orders production', from=now64(9)-INTERVAL 1 HOUR,
                            until=now64(9), asof=now64(9));
SELECT * FROM PilotFindings(resource='Orders production', from=now64(9)-INTERVAL 1 HOUR,
                           until=now64(9), asof=now64(9));
SELECT * FROM PilotChanges(resource='Orders production', from=now64(9)-INTERVAL 1 HOUR,
                          until=now64(9), asof=now64(9));
SELECT * FROM PilotIncidentHistory(resource='Orders production', from=now64(9)-INTERVAL 1 DAY,
                                  until=now64(9), asof=now64(9));
```

The resource argument is an exact recorded connection alias, or an explicitly approved database display name.
It must select exactly one native resource in this namespace/history. Reused or ambiguous labels return
`ambiguous-label` in Status and no selected incident/alert. Empty Gold outputs still expose the canonical header's
approved alias/environment/optional database name and output coverage. An absent name remains unknown; it is never
filled from a target, endpoint or current connection. Resource IDs are internal selection keys; source/member/epoch
scope remains independent in returned rows. Caller labels are database-supplied claims, not service/team ownership.

Incidents first select the greatest exact controller revision within each original target/source/scope/episode and
controller version, across all interpretation versions. The configured analyzer/rules then select its interpretation
of that exact revision. Only an unambiguous latest interpretation body is selected; synthesis time chooses wording
of the same original revision, never an execution or causal edge. Missing newer interpretation does not revive an
older active row. Missing incident rows in an already published capture also withhold heads rather than reviving an
older active row. Resolved heads remain readable, and IncidentHistory retains original revisions.

Window filters use explicit original UTC capture/interval/controller-recorded bounds, not native offset-free
first/last labels or synthesis arrival. Time selects candidates; it never establishes a cause. Findings and changes
retain independent original populations, scopes, windows and interpretations; they are not pooled into database
rates or a single cross-capture finding lifetime. Full typed Details accompanies the convenient developer fields.
Victims, candidates and queries with a proven native causal role remain separate. Missing mechanism, caller, query
text or fix remains unavailable; string presence columns preserve native-null semantics in ADX.

## Alerts over recorded answers

A new-recorded-incident query joins original revision1's collector record time to the latest canonical head. It
suppresses resolved/uncertain heads, incomplete selected inputs, unavailable incident evaluation and incomplete
published output membership. The returned cause/action may therefore be enriched without creating a new alert:

```kusto
PilotNewIncidents("Orders production", ago(5m), now(), now())
```

```sql
SELECT * FROM PilotNewIncidents(resource='Orders production', from=now64(9)-INTERVAL 5 MINUTE,
                               until=now64(9), asof=now64(9));
```

Configure the existing alert system to retain state/deduplicate by
`TargetId, OriginalScopeId, OriginalSource, EpisodeId, ControllerVersion`. Use `FirstRecordedObservation` as the
original observation timestamp. Do not dimension by AnalysisId, synthesis RecordedAt, rule version or each new
Revision. A stateless webhook over overlapping windows can repeat any query result; the query is not an alert
acknowledgement store. First collector observation is not proven outage onset. Replay/backfill of an old original
revision keeps its old timestamp and cannot become a fresh outage just because synthesis/ingestion happened now.
Before applying time or interpretation filters, the same original-consistency helper checks each retained revision's
identity, reference, lifecycle and collector-recorded timestamp. Conflicting revision1 interpretations withhold novelty;
a later unique active head cannot choose a convenient first clock or receipt. `first-observation-unavailable` and
`UnavailableFirstObservations` expose an absent, unknown or conflicting first observation through Status, while a
consistent current head remains separately readable. If revision1 is no longer retained, no novelty is manufactured.
No query-only finding is promoted to an outage.

Run a separate evidence-availability alert:

```kusto
PilotStatus("Orders production", now(), 30m)
| where State != "observed" or not(InputReadComplete) or array_length(ContextIssues) > 0
```

```sql
SELECT * FROM PilotStatus(resource='Orders production', asof=now64(9), max_age_seconds=1800)
WHERE State!='observed' OR NOT InputReadComplete OR length(JSONExtractArrayRaw(ContextIssues))>0;
```

Status distinguishes unknown/ambiguous labels, no published analysis, stale original input acquisition bounds, missing configured rules,
incomplete output membership, partial inputs/context/evaluation, and an unavailable current interpretation.
Freshness uses the original header’s selected-input UTC bound, never output ingestion or reanalysis time. It does not establish native workload freshness across independently clocked inputs. `observed` means the selected published interpretation was observed; it is not native health or complete fleet
coverage. OutputCoverage, per-family Counts, ContextIssues and independent acquisition bounds remain visible even
when all output counts are zero. No rows from a finding/incident query is never a healthy verdict. Pipeline outages,
permission failures and retention gaps require their own availability handling. Ingestion/batching latency and
warehouse read/alert cost need deployment-specific validation; these examples assert no production SLO.

Primary syntax: [Kusto stored functions](https://learn.microsoft.com/en-us/kusto/query/functions/user-defined-functions),
[ClickHouse parameterized views](https://clickhouse.com/docs/sql-reference/statements/create/view#parameterized-view),
and [ClickHouse window functions](https://clickhouse.com/docs/sql-reference/window-functions). Deployment/runtime
validation is separate from asset generation; no actual warehouse execution is implied by this documentation.
