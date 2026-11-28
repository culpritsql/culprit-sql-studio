import { useState } from "react";
import type { ClickHouseObservation } from "../gen/ClickHouseObservation";
import type { Snapshot } from "../gen/Snapshot";
import type { ClickHouseFailure } from "../gen/ClickHouseFailure";
import type { DatabaseIdentity } from "../gen/DatabaseIdentity";
import { copyAndTell } from "../lib/clipboard";
import { count } from "../lib/format";
import { t } from "../lib/i18n";

const sources = {
  failure: "system.query_log.exceptions",
  execution: "system.query_log.execution",
  "part-event": "system.part_log",
  "query-family": "system.query_log.families",
  parts: "system.parts",
  merge: "system.merges",
  mutation: "system.mutations",
  replica: "system.replicas",
  "replication-task": "system.replication_queue",
  "distributed-queue": "system.distribution_queue",
  "async-insert": "system.asynchronous_insert_log",
} satisfies Record<ClickHouseObservation["kind"], string>;

const pageSize = 10;

function recordLabel(point: ClickHouseObservation): string {
  switch (point.kind) {
    case "failure": return `${point.observation.exception_name} · ${point.observation.query_id}`;
    case "execution": return point.observation.focus.query_id;
    case "query-family": return point.observation.family;
    default: return `${point.observation.database}.${point.observation.table}`;
  }
}

/** Field names are native evidence identifiers. Values remain text even when supplied by a database client. */
function fieldValue(value: unknown): string {
  if (Array.isArray(value)) return value.map(fieldValue).join(", ");
  if (typeof value === "object" && value !== null) {
    return Object.entries(value).map(([key, item]) => `${key} = ${fieldValue(item)}`).join("\n");
  }
  return value == null ? "—" : String(value);
}

function ExecutionFollowup({ failure, target }: { failure: ClickHouseFailure; target: DatabaseIdentity | null }) {
  const [table, setTable] = useState("");
  if (!target?.incarnation || target.location.kind !== "server" || target.location.instance !== failure.node
    || target.database !== failure.database || !Number.isSafeInteger(Number(failure.started_us))
    || !Number.isSafeInteger(Number(failure.ended_us))) return null;
  const prefix = target.database + ".";
  const tables = failure.tables.filter(name => name.startsWith(prefix)).map(name => name.slice(prefix.length));
  const selection = { kind: "click-house-investigation", focus: { target, table: table || null, request: {
    node: failure.node, database: failure.database, query_id: failure.query_id,
    started_us: Number(failure.started_us), ended_us: Number(failure.ended_us),
  } } };
  return <div className="actions">
    {tables.length > 0 && <label>{t("provider.clickhouse.part_context")} <select value={table} onChange={event => setTable(event.target.value)}>
      <option value="">{t("provider.clickhouse.execution_only")}</option>
      {tables.map(name => <option key={name} value={name}>{name}</option>)}
    </select></label>}
    <button type="button" className="btn ghost" onClick={() => void copyAndTell(JSON.stringify(selection, null, 2), t("provider.clickhouse.copied"))}>
      {t("provider.clickhouse.copy_selection")}
    </button>
  </div>;
}

function SourceRecords({ rows, target }: { rows: ClickHouseObservation[]; target: DatabaseIdentity | null }) {
  const [page, setPage] = useState(0);
  return <>
    {rows.slice(page * pageSize, (page + 1) * pageSize).map((point, index) =>
      <details key={page * pageSize + index}>
        <summary>{recordLabel(point)}</summary>
        <dl className="obs-fields">
          {Object.entries(point.observation).map(([name, value]) => <div key={name}>
            <dt><code>{name}</code></dt><dd style={{ overflowWrap: "anywhere", whiteSpace: "pre-wrap" }}>{fieldValue(value)}</dd>
          </div>)}
        </dl>
        {point.kind === "failure" && <ExecutionFollowup failure={point.observation} target={target} />}
      </details>)}
    {rows.length > pageSize && <div className="actions">
      <button type="button" disabled={page === 0} onClick={() => setPage(page - 1)}>{t("ui.common.previous")}</button>
      <button type="button" disabled={(page + 1) * pageSize >= rows.length} onClick={() => setPage(page + 1)}>{t("ui.common.next")}</button>
    </div>}
  </>;
}

export function ClickHouseHealth({ snapshot }: { snapshot: Snapshot }) {
  const rows = snapshot.operations.clickhouse ?? [];
  const outcomes = (snapshot.sources ?? []).filter(outcome => Object.values(sources).some(name => name === outcome.source));
  return <section className="operational-card">
    <h3>{t("provider.clickhouse.health_title")}</h3>
    <p className="muted">{t("provider.clickhouse.health_scope")}</p>
    {outcomes.map(outcome => {
      const group = rows.filter(row => sources[row.kind] === outcome.source);
      return <details key={outcome.source}>
        <summary><code>{outcome.source}</code> · {t(`ui.replication.source.${outcome.state.replaceAll("-", "_")}`)} · {count(group.length)}</summary>
        {outcome.reason && <p className="muted">{outcome.reason.text}</p>}
        <SourceRecords rows={group} target={snapshot.identity ?? null} />
      </details>;
    })}
  </section>;
}
