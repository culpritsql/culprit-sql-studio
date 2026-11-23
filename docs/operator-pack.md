# Operator dashboard and alerts

The existing `collect queries` command generates a namespace-bound script and import documents for an existing
Grafana installation. The ADX and ClickHouse packs read the same eligible published incident, finding and change
contracts as the normal warehouse workflow. No source database, new warehouse table, detector, deployment, contact
point or remediation is added. [Warehouse queries](warehouse-queries.md) describes the exact publication and
original-revision guards.

## Generate for one existing namespace

Use an existing evidence configuration, the actual analyzer build identity and the installed collector rules policy:

```sh
culprit collect queries --config archive.json --name Pilot \
  --history-from 2026-11-01T00:00:00Z --rules-version "$CULPRIT_RULES_VERSION" \
  --analyzer-version "$CULPRIT_ANALYZER_VERSION" \
  --analyzer-revision "$CULPRIT_ANALYZER_REVISION" --format json > operator-pack.json
jq -r '.script' operator-pack.json > query-helpers.txt
jq '.operator.dashboard' operator-pack.json > culprit-dashboard.json
jq '.operator.alerts' operator-pack.json > culprit-alerts.paused.json
```

Omit `--analyzer-revision` only when the recorded analyzer genuinely has no revision. Generation validates the
selected recovery namespace through the existing bounded EvidenceService session. Contract/schema/store IDs are
bound once in the helper script; operators enter an approved name and time window, not receipt hashes. Keep the
configuration and generated bindings in the deployment's existing access boundary. Hidden bindings do not replace
warehouse authorization.

Review and apply `query-helpers.txt` explicitly to the same existing warehouse database, after the normal schema
assets. This creates or replaces helper functions/views only. It dynamically discovers eligible publications;
new resources do not require regenerating a roster. Resource-scoped eligibility and interpretation policies remain
the same. Server-side eligibility is not raw original/EOF authentication: open an incident or artifact in Culprit
for that existing proof path.

## Import the dashboard

Import `culprit-dashboard.json` through Grafana's dashboard import and select the existing data source for that
exact warehouse database. Configure its default database (and ADX cluster) beforehand. Neither the pack nor
Culprit supplies Grafana credentials. Use the installed plugin's read-only identity and existing row/access policy.
Background alerts need a separately approved datasource authentication mode that works without an interactive user;
do not silently substitute a broader service identity.

The pack uses the classic dashboard JSON model and eight table panels: published resource labels, source
acquisitions, diagnosis availability, incident heads, original revision history, high-severity actionable findings,
changes and native deadlock events. Every panel is a bounded sample of at most 501 retained rows. Table filters
operate on that sample, not a complete fleet or time-window query. Use the ordinary parameterized resource/window
helpers or Culprit's paged warehouse workflow for a selected resource. No untrusted resource text is interpolated
into the dashboard's SQL. Native resource/source/member generations remain separate.

Incident tables keep victim summary, distinct affected/candidate/culprit queries, attribution status, mechanism,
developer action, verification and DB-supplied caller claims. Missing fields stay unavailable. Native deadlock
counts are graph metadata, not query attribution or a Culprit cause/action; use the original graph and incident
analysis for those. Findings use the recorded `high` severity, actionable state and inactive flag; they are not
promoted to a new outage. Changes retain native time labels and comparison bounds; proximity is not causality.
Resolved originals remain in incident history. Resource, acquisition and availability panels use the current warehouse
selection clock; the time picker bounds the incident/history/finding/change/native-event panels. A window before the
configured retained-publication floor is unavailable, not a healthy empty period.

Set a warehouse/data-source timeout appropriate to the deployment (the ClickHouse models also request a 30-second
server limit). Query results are bounded to 8 MiB. Table responses are capped at 501 rows; alert results at 10,000
instances. Limit errors are errors, not a silent subset of alert instances. These limits do not establish a measured
warehouse cost or ingestion latency. Review the query plans/cost under the deployment's real retention and fleet.

The [ADX plugin](https://grafana.com/docs/plugins/grafana-azure-data-explorer-datasource/latest/) currently requires
Grafana Enterprise or an eligible Grafana Cloud plan. The [ClickHouse plugin](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/)
has its own Grafana/plugin version requirements. Follow those supported combinations. The generated model uses each
plugin's native raw query fields; deployment validation must include an actual import/query against the installed
plugin version. See the [classic JSON model](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/view-dashboard-json-model/).

## Provision paused alerts

`culprit-alerts.paused.json` is a Grafana file-provisioning document for a self-managed installation. Replace
`REPLACE_WITH_EXISTING_DATASOURCE_UID` with the same existing datasource UID, and choose the existing Grafana
organization/folder as appropriate. Review it before copying it into that installation's `provisioning/alerting`
directory. Every rule starts paused. The file creates no contact points or notification policies. File-provisioned
rules must be maintained through their provisioning owner; Grafana Cloud requires its own supported alert
configuration workflow rather than copying this file into a directory. See [Grafana file provisioning](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/).

The three examples are deliberately different:

- **Newly recorded incident:** a consistent original revision-one collector timestamp falls in the last five minutes,
  and the latest canonical head is active, input-complete and incident-evaluated. Latest head selection precedes rules
  and window filtering. The instance labels contain only resource/target/original-scope/source/episode/controller
  identities. Reanalysis, new wording, rules or revisions do not rename the instance. Cause/action enrichment is on the
  linked dashboard. First collector observation is not proven outage onset.
- **Native deadlock event:** the native UTC event timestamp falls in the last five minutes. Exact native event/source
  identity is the instance key. This is an event notification, not a continuing-outage assertion. Aging out of the
  window, missing data or query failure is not native recovery.
- **Evidence availability:** original selected-input clocks, output membership/evaluation, current-rule availability,
  and independently published source acquisition clocks are checked separately. A namespace without publications,
  an acquisition-only resource without Gold, or Gold without retained source acquisitions is unavailable. `observed`
  and `observed-acquisition` are limited positive observations, never healthy database/full-fleet verdicts. Detailed
  coverage stays in the tables; a failed or absent collector not represented in publications cannot be enumerated.

The event rules treat an empty query as no selected event; the separate availability rule treats missing data as
alerting. All execution errors remain errors. Results are numeric tables with one value and unique stable string
labels, consumed directly by a threshold; see [Grafana tabular alerts](https://grafana.com/docs/grafana/latest/alerting/examples/table-data/).
The examples use one-minute evaluation, five-minute event windows and a thirty-minute age policy. Adjust those only
after observing source cadences and end-to-end publication behavior. Late delivery outside an original event/record
window can be missed; an arrival clock is not substituted to manufacture novelty. Restore/restart/notification
routing also needs the alert system's existing deduplication policy; this is not an exactly-once acknowledgement log.

Native `EventAt`, controller `OriginalRecordedAt`, selected-input `OriginalObservedFrom/Until`, independent original
Query Store intervals, and `InterpretationRecordedAt` are separate. Source freshness uses published raw acquisition
`CollectedUntil`, excluding local diagnostics/derived outputs. Historical Gold aliases attached to an exact resource
are retained labels, not a current source identity. Publication membership has no publication timestamp;
`PublicationTimeKnown=false` and configured-fleet/native-history coverage remain unknown. Zero findings alone never
means complete evaluated coverage or recovery.

## Optional ADX queued-ingestion batching

No batching policy is in the default helper script. An ADX operator with the existing Table Admin permission may
review the separate `opt-in-adx-queued-batching` example in the ADX JSON assets for the three Gold tables and their publication table:

```kusto
.alter-merge tables (CulpritIncidentsV1,CulpritFindingsV1,CulpritChangesV1,CulpritPublishedRangesV1)
policy ingestionbatching '{"MaximumBatchingTimeSpan":"00:00:30"}'
```

This is an explicit opt-in change to the customer's queued-ingestion policy, not a ClickHouse or source database
setting. `alter-merge` retains unspecified batching settings. It does not change the upstream raw tables, collector
cadence, publication membership gate or native budgets; the publication row is still usable only after the existing
receipt/membership conditions. Lower batching delay can increase cost and does not establish actual publication
latency or an SLA. Inspect existing policies, cost and actual workload before applying; retain the prior policy for
the deployment's own rollback. See [ADX batching policy](https://learn.microsoft.com/en-us/kusto/management/batching-policy)
and [alter-merge table policy](https://learn.microsoft.com/en-us/kusto/management/alter-merge-table-ingestion-batching-policy).
This document and asset generation perform no warehouse/cloud mutation.
