import type { JobExecution } from "../gen/JobExecution";
import type { JobsReport } from "../gen/JobsReport";
import type { JobsSnapshot } from "../gen/JobsSnapshot";
import { findingMetric } from "../lib/format";
import { t } from "../lib/i18n";
import { observationRowsBySource } from "../lib/observation-rows";
import { NativeValue } from "./NativeObservationFields";
import { ObservationList } from "./ObservationList";

export const MYSQL_EVENTS_SOURCE = "mysql-scheduled-events";
export const AGENT_JOB_SOURCES = ["mssql-agent-jobs", "mssql-agent-job-steps", "mssql-agent-job-history", "mssql-agent-context"] as const;
export const JOBS_SOURCES = ["timescale-jobs", "timescale-job-runs", ...AGENT_JOB_SOURCES, "timescale-continuous-aggregates", MYSQL_EVENTS_SOURCE] as const;

/** Rust words the native conditions; references locate only the exact original row, never a current worker. */
export function Jobs({ evidence, report }: { evidence: JobsSnapshot; report: JobsReport }) {
  const native = evidence.snapshot;
  const sources = observationRowsBySource(native.observations, (row) => row.key);
  const sameRead = sameJobRead(evidence, report);
  return <ObservationList collapseEntries source={evidence.kind === "mysql-events" ? MYSQL_EVENTS_SOURCE : evidence.kind === "mssql-agent" ? AGENT_JOB_SOURCES[0] : JOBS_SOURCES[0]} report={report}
    scope={evidence.kind === "mysql-events" ? t("view.mysql_events.ui.scope") : evidence.kind === "mssql-agent" ? t("view.mssql_agent_jobs.ui.scope") : t("ui.jobs.scope")} entriesLabel={t("ui.jobs.inspect")}
    time={<>{t("ui.memory_failures.read_between", { from: native.context.observed_from, until: native.context.observed_until })} {evidence.kind === "timescale" && t("ui.jobs.window", { from: evidence.snapshot.metadata.run_window.from, until: evidence.snapshot.metadata.run_window.until })}</>}
    renderEntry={(entry) => {
      const row = sources.get(entry.source)?.get(entry.observation_key);
      const matched = sameRead && row && ((entry.kind === "state" && ["timescale-job-state", "mssql-agent-job", "mssql-agent-step", "mysql-scheduled-event"].includes(row.fact.kind))
        || (entry.kind === "retained-run" && (row.fact.kind === "timescale-job-run" || (row.fact.kind === "mssql-agent-history" && row.fact.data.step_id === "0")))
        || (entry.kind === "retained-step" && row.fact.kind === "mssql-agent-history" && row.fact.data.step_id !== "0")
        || (entry.kind === "agent-context" && row.fact.kind === "mssql-agent-context")
        || (entry.kind === "continuous-aggregate" && row.fact.kind === "timescale-continuous-aggregate"))
        && ("job_id" in row.fact.data ? row.fact.data.job_id === entry.job_id : entry.job_id === null);
      const execution = sameRead ? report.executions.entries.find((value) => value.reference.source === entry.source && value.reference.observation_key === entry.observation_key && value.reference.acquisition.observed_from === native.context.observed_from && value.reference.acquisition.observed_until === native.context.observed_until) : undefined;
      return matched ? <article className="operational-card" key={`${entry.source}:${entry.observation_key}`}>
        <h4>{entry.title}</h4><p>{entry.explanation}</p>
        {execution && <ExecutionFields entry={execution} />}
        <dl>{entry.metrics.map((metric, index) => <NativeValue key={`${metric.key}:${index}`} label={metric.label} value={findingMetric(metric)} />)}</dl>
        <details><summary>{t("ui.memory_failures.investigation")}</summary><p>{entry.investigation}</p><p>{entry.verify}</p></details>
      </article> : <div className="operational-card" key={`${entry.source}:${entry.observation_key}`}><p>{t("ui.memory_failures.missing_row")}</p></div>;
    }} />;
}

/** Native local fields stay literal. Only the backend's qualified UTC record enters an actual timeline. */
function ExecutionFields({ entry }: { entry: JobExecution }) {
  const time = entry.time;
  return <details><summary>{t("ui.jobs.execution.inspect")}</summary>
    <p>{t("ui.jobs.execution.status", { status: entry.status === "in-progress" ? "progress" : entry.status })}</p>
    <p>{entry.placement === "source-qualified-window" ? t("ui.jobs.execution.window_placement") : t("ui.jobs.execution.placement", { placement: entry.placement === "native-local" ? "local" : entry.placement === "no-native-run-identity" ? "unidentified" : entry.placement === "ambiguous-native-identity" ? "ambiguous" : "utc" })}</p>
    <dl>{time.kind === "utc" ? <>
      <NativeValue label={t("view.jobs.metric.started_at")} value={time.started_at} />
      <NativeValue label={t("view.jobs.metric.finished_at")} value={time.reported_until ?? t("view.mysql_events.unknown")} />
    </> : time.kind === "mssql-agent-active" ? <>
      {time.observed_from && <NativeValue label={t("ui.jobs.execution.observed_from")} value={time.observed_from} />}
      {time.observed_until && <NativeValue label={t("ui.jobs.execution.observed_until")} value={time.observed_until} />}
      <NativeValue label={t("view.mssql_agent_jobs.field.observed_at")} value={time.observed_at} />
      <NativeValue label={t("view.mssql_agent_jobs.field.started")} value={time.started_without_offset} />
      {time.last_executed_step_id && <NativeValue label={t("view.mssql_agent_jobs.field.last_step")} value={time.last_executed_step_id} />}
      {time.last_executed_step_without_offset && <NativeValue label={t("view.mssql_agent_jobs.field.last_step_time")} value={time.last_executed_step_without_offset} />}
    </> : time.kind === "mssql-local" ? <>
      <NativeValue label={t("view.mssql_agent_jobs.field.run_date")} value={time.run_date} />
      <NativeValue label={t("view.mssql_agent_jobs.field.run_time")} value={time.run_time} />
      <NativeValue label={t("view.mssql_agent_jobs.field.duration")} value={time.run_duration} />
      <NativeValue label={t("ui.jobs.execution.local_role")} value={t("ui.jobs.execution.local_time_role", { role: time.history_written_time ? "written" : "start" })} />
    </> : <>
      <NativeValue label={t("view.mysql_events.field.last_started")} value={time.started_without_offset} />
      <NativeValue label={t("view.mysql_events.field.time_zone")} value={time.event_time_zone ?? t("view.mysql_events.unknown")} />
    </>}</dl>
  </details>;
}

/** Context retained beside a UTC comparison, without assigning native-local rows to either period. */
export function JobComparisonContext({ evidence, report }: { evidence?: JobsSnapshot | null; report?: JobsReport | null }) {
  if (!report) return null;
  const assessment = report.executions.assessment;
  const unplaced = assessment.unplaced_records > 0;
  const incomplete = !assessment.read_complete || assessment.omitted_entries > 0 || assessment.sources.some((source) => !source.complete_scope || (source.state !== "collected" && source.state !== "empty"));
  if (!unplaced && !incomplete) return null;
  const matched = evidence != null && sameJobRead(evidence, report);
  return <details className="wc-group" aria-label={t("ui.what_changed.jobs_context.title")}>
    <summary>{t("ui.what_changed.jobs_context.title")}</summary>
    {unplaced && <p className="muted">{t("ui.what_changed.jobs_context.clock")}</p>}
    {incomplete && <p className="muted">{t("ui.what_changed.jobs_context.coverage")}</p>}
    {matched ? <>
      <p className="muted">{t("ui.what_changed.jobs_context.scope", { scope: report.context.target.scope })}</p>
      <Jobs evidence={evidence} report={report} />
    </> : <p>{t("ui.memory_failures.missing_row")}</p>}
  </details>;
}

function sameJobRead(evidence: JobsSnapshot, report: JobsReport): boolean {
  return JSON.stringify(evidence.snapshot.context) === JSON.stringify(report.context);
}
