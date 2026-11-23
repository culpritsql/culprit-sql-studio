import type { ImportedEventDeadlock } from "../gen/ImportedEventDeadlock";
import type { MssqlExtendedEventObservation } from "../gen/MssqlExtendedEventObservation";
import { contextPreview } from "../lib/context-preview";
import { digits } from "../lib/format";
import { DeadlockGraph } from "./DeadlockGraph";
import type { MysqlErrorLogObservation } from "../gen/MysqlErrorLogObservation";
import type { NativeFailureRecord } from "../gen/NativeFailureRecord";
import type { NativeFailureSnapshot } from "../gen/NativeFailureSnapshot";
import type { PostgresLogFailureObservation } from "../gen/PostgresLogFailureObservation";
import type { RetainedFailureExplanation } from "../gen/RetainedFailureExplanation";
import type { RetainedFailureReport } from "../gen/RetainedFailureReport";
import type { SourceOutcome } from "../gen/SourceOutcome";
import { t } from "../lib/i18n";
import { ObservationList, ObservationSourceIssue, observationReadIssue } from "./ObservationList";

export function RetainedNativeFailures({ evidence, report, importedDeadlocks = [] }: { evidence: NativeFailureSnapshot; report: RetainedFailureReport; importedDeadlocks?: ImportedEventDeadlock[] }) {
  const observations = new Map(evidence.observations.map((row) => [row.observation.position, row]));
  return <ObservationList collapseEntries source={evidence.source} time={t("ui.memory_failures.read_between", { from: evidence.observed_from, until: evidence.observed_until })} report={report} scope={t(evidence.resource?.kind === "instance" ? "ui.memory_failures.scope.server" : "ui.memory_failures.scope.unqualified")} renderEntry={(entry) => {
    const observation = observations.get(entry.position);
    return observation
      ? <NativeFailureRow key={entry.position} row={observation} explanation={entry} deadlocks={importedDeadlocks.filter((graph) => graph.position === entry.position)} />
      : <div className="operational-card" key={entry.position}><p>{t("ui.memory_failures.missing_row")}</p></div>;
  }}><p className="muted">{report.incident_input_explanation}</p></ObservationList>;
}

export function nativeFailureReadIssue(source: string | undefined, sources: SourceOutcome[], requested = false): SourceOutcome | undefined {
  return observationReadIssue(source ?? "native failures", sources, requested);
}

export function NativeFailureSourceIssue({ source }: { source: SourceOutcome }) {
  return <ObservationSourceIssue source={source} title={t("view.native_failures.title")} />;
}

function NativeFailureRow({ row, explanation, deadlocks }: { row: NativeFailureRecord; explanation: RetainedFailureExplanation; deadlocks: ImportedEventDeadlock[] }) {
  return <article className="operational-card">
    <h4>{explanation.title}</h4>
    <p>{explanation.explanation}</p>
    <NativeEventTime row={row} />
    <details>
      <summary>{t("ui.memory_failures.investigation")}</summary>
      <p>{explanation.investigation}</p>
      <p>{explanation.verify}</p>
      {row.native === "mssql-extended-event" && <MssqlMetadata row={row.observation} />}
      {row.native === "mysql-error-log" && <MysqlMetadata row={row.observation} />}
      {row.native === "postgres-log" && <PostgresMetadata row={row.observation} />}
      <p className="muted">{t("ui.native_failures.message_omitted")}</p>
    </details>
    {deadlocks.map(({ graph }, index) => <details key={`${graph.id}:${index}`}><summary>{t("native_events.deadlock.graph")}</summary><DeadlockGraph deadlock={graph} /></details>)}
  </article>;
}

function NativeEventTime({ row }: { row: NativeFailureRecord }) {
  switch (row.native) {
    case "mssql-extended-event": return <p className="muted">{row.observation.timestamp_utc ? t("ui.native_failures.event_time") : t("ui.native_failures.local_event_time")} <code>{contextPreview(row.observation.timestamp_utc ?? row.observation.timestamp ?? t("ui.common.unknown")).text}</code></p>;
    case "mysql-error-log": return <p className="muted">{t("ui.native_failures.event_time")} <code>{row.observation.logged_at}</code></p>;
    case "postgres-log": return <p className="muted">{row.observation.timestamp_utc ? t("ui.native_failures.event_time") : t("ui.native_failures.local_event_time")} <code>{row.observation.timestamp_utc ?? row.observation.timestamp}</code></p>;
  }
}

function MysqlMetadata({ row }: { row: MysqlErrorLogObservation }) {
  return <>
    <dl>
      <div><dt>{t("ui.native_failures.priority")}</dt><dd><code>{row.priority}</code></dd></div>
      <div><dt>{t("ui.native_failures.error_code")}</dt><dd><code>{row.error_code ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.subsystem")}</dt><dd><code>{row.subsystem ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.thread")}</dt><dd><code>{row.thread_id ?? t("ui.common.unknown")}</code></dd></div>
    </dl>
    <p className="muted">{t("ui.native_failures.thread_note")}</p>
  </>;
}

function PostgresMetadata({ row }: { row: PostgresLogFailureObservation }) {
  return <>
    <dl>
      <div><dt>{t("ui.native_failures.sql_state")}</dt><dd><code>{row.sql_state}</code></dd></div>
      <div><dt>{t("ui.native_failures.severity")}</dt><dd><code>{row.severity ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.database_context")}</dt><dd><code>{row.database ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.process_id")}</dt><dd><code>{row.process_id ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.session_id")}</dt><dd><code>{row.session_id ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.session_start")}</dt><dd><code>{row.session_start ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.session_line")}</dt><dd><code>{row.session_line_number ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.query_id")}</dt><dd><code>{row.query_id ?? t("ui.common.unknown")}</code></dd></div>
      <div><dt>{t("ui.native_failures.backend_type")}</dt><dd><code>{row.backend_type ?? t("ui.common.unknown")}</code></dd></div>
    </dl>
    <p className="muted">{t("ui.native_failures.postgres_context_note")}</p>
    {row.statement.state === "available" && <details>
      <summary>{t("ui.native_failures.reported_statement")}</summary>
      <pre className="mono" style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere", maxHeight: "18rem", overflow: "auto" }}>{row.statement.text}</pre>
    </details>}
  </>;
}

function MssqlMetadata({ row }: { row: MssqlExtendedEventObservation }) {
  const text = (value: string | null) => value === null ? t("ui.common.unknown") : contextPreview(value).text;
  return <>
    <dl>
      <div><dt>{t("ui.native_failures.database_context")}</dt><dd>{text(row.database ?? row.database_id)}</dd></div>
      <div><dt>{t("ui.native_failures.session_id")}</dt><dd>{text(row.session_id)}</dd></div>
      <div><dt>{t("native_events.field.request")}</dt><dd>{text(row.request_id)}</dd></div>
      <div><dt>{t("native_events.field.application")}</dt><dd>{text(row.client?.application ?? null)}</dd></div>
      <div><dt>{t("native_events.field.user")}</dt><dd>{text(row.client?.user ?? null)}</dd></div>
      <div><dt>{t("native_events.field.host")}</dt><dd>{text(row.client?.host ?? null)}</dd></div>
      {row.kind.event === "error-reported" && <><div><dt>{t("ui.native_failures.error_code")}</dt><dd>{digits(String(row.kind.number))}</dd></div><div><dt>{t("ui.native_failures.severity")}</dt><dd>{digits(String(row.kind.severity))}</dd></div></>}
    </dl>
    <p className="muted">{t("native_events.context")}</p>
    {row.statement.state === "available" && <details><summary>{t("native_events.statement")}</summary><pre className="mono" style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere", maxHeight: "18rem", overflow: "auto" }}>{row.statement.text}</pre></details>}
  </>;
}
