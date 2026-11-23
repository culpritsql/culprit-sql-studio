import type { InspectArtifacts } from "../../lib/archive-artifacts";
import { PlanView } from "../../components/PlanView";
import type { WarehouseWindowReport } from "../../gen/WarehouseWindowReport";
import type { WindowPublishedFinding } from "../../gen/WindowPublishedFinding";
import type { WindowPublishedChange } from "../../gen/WindowPublishedChange";
import type { CollectedWindowSection } from "../../gen/CollectedWindowSection";
import { ObservationList } from "../../components/ObservationList";
import { ChangesSummary } from "../../components/WhatChanged";
import { NativeValue, nativeTextPreview } from "../../components/NativeObservationFields";
import { count, duration } from "../../lib/format";
import { t } from "../../lib/i18n";
import { ArchiveIncident } from "./ArchiveIncident";
const text = (value: string) => nativeTextPreview(value, 2000);
/** A reader of canonical window results. It never assembles a current Snapshot or authorizes stored fixes. */
export function ArchiveWindow({ value, onArtifacts }: { value: WarehouseWindowReport; onArtifacts?: InspectArtifacts }) {
  return <section aria-label={t("ui.window.title")}>
    <h3>{t("ui.window.title")}</h3>
    <p>{t("window.cli.scope", { from: value.request.window.from, until: value.request.window.until, as_of: value.request.as_of })}</p>
    {value.request.view === "published-answers" && <p className="muted">{t("ui.window.page_scope")}</p>}
    {value.qualifications.map((reason, index) => <p className="muted" key={index}>{text(reason)}</p>)}
    {value.incidents.incidents.length > 0 || value.incidents.history.entries.length > 0 ?
      <ArchiveIncident value={{ report: value.incidents, analysis_limitations: value.incident_limitations }} onArtifacts={onArtifacts} /> : null}
    {value.published.map((analysis) => <section key={analysis.context.capture.capture.observation_id}>
      <h4>{text(analysis.context.description.display.connection_alias)}</h4>
      <p className="muted">{t("ui.archives.original_bounds", { from: analysis.context.description.observed_from, until: analysis.context.description.observed_until })}</p>
      {analysis.limitations.map((reason, index) => <p className="muted" key={index}>{text(reason)}</p>)}
      <ObservationList source={t("ui.archives.published")} scope={t("ui.archives.original_scope")}
        report={{ title: t("ui.window.findings"), summary: t("window.reader.historical_guidance"), limitations: [], sources: [], entries: analysis.findings }}
        renderEntry={(row) => <Finding key={row.finding.finding.id} row={row} onArtifacts={onArtifacts} />} />
      <ObservationList source={t("ui.archives.published")} scope={t("ui.archives.original_scope")}
        report={{ title: t("ui.window.changes"), summary: t("ui.window.change_scope"), limitations: [], sources: [], entries: analysis.changes }}
        renderEntry={(row) => <Change key={row.change.change.id} row={row} onArtifacts={onArtifacts} />} />
    </section>)}
    {value.workload.kind === "not-requested" ? <p className="muted">{t("window.reader.workload_not_requested")}</p> : <>
      <h4>{t("ui.window.native_sections")}</h4><p className="muted">{t("ui.window.native_scope")}</p>
      {value.workload.analysis.sections.map((section) => <NativeSection key={section.analysis_id} section={section} />)}
    </>}
    {value.next && <p role="status">{t("window.reader.more")}</p>}
  </section>;
}
function Finding({ row, onArtifacts }: { row: WindowPublishedFinding; onArtifacts?: InspectArtifacts }) {
  const original = row.finding, finding = original.finding;
  return <article className="operational-card">
    <h4>{text(finding.title)}</h4><p>{text(finding.summary)}</p>
    <p className="muted">{text(row.placement.qualification)}</p>
    {original.query && <pre>{original.query.sql.state === "available" ? text(original.query.sql.text) : t("ui.archives.sql_unknown")}</pre>}
    {onArtifacts && (original.query || original.inputs.length > 0) && <button type="button" className="btn" onClick={() => onArtifacts({ references: original.query ? [original.query.reference] : original.inputs, sql: original.query?.sql ?? null })}>{t("artifact.ui.open")}</button>}
    {original.cause && <p>{text(original.cause.summary)}</p>}
    {finding.fix && <><h5>{t("ui.archives.action")}</h5><p>{text(finding.fix.summary)}</p>{finding.fix.sql && <pre>{text(finding.fix.sql)}</pre>}</>}
    {finding.verify.length > 0 && <><h5>{t("ui.archives.verification")}</h5>{finding.verify.map((verify, index) => <p key={index}>{text(verify)}</p>)}</>}
    <p className="muted">{t("window.reader.historical_guidance")}</p>
  </article>;
}
function Change({ row, onArtifacts }: { row: WindowPublishedChange; onArtifacts?: InspectArtifacts }) {
  const event = row.change.change;
  return <article className="operational-card"><h4>{text(event.title.text)}</h4>
    {event.detail && <p>{text(event.detail.text)}</p>}
    <p className="muted">{text(row.placement.qualification)}</p>
    {onArtifacts && row.change.inputs.length > 0 && <button type="button" className="btn" onClick={() => onArtifacts({ references: row.change.inputs, sql: null })}>{t("artifact.ui.open")}</button>}
    <dl><NativeValue label={t("ui.window.event_at")} value={text(event.at)} />
      <NativeValue label={t("ui.window.event_after")} value={event.after == null ? null : text(event.after)} />
      <NativeValue label={t("ui.window.event_until")} value={event.until == null ? null : text(event.until)} /></dl>
  </article>;
}
function NativeSection({ section }: { section: CollectedWindowSection }) {
  const totals = section.analysis.runtime_totals, workload = section.analysis.workload;
  return <article className="operational-card">
    <p>{t("window.cli.native_section", { from: section.observed.from, until: section.observed.until })}</p>
    {totals && <dl><NativeValue label={t("common.metric.runs")} value={totals.executions} />
      <NativeValue label={t("ui.what_changed.queries")} value={totals.query_identities} />
      <NativeValue label={t("ui.what_changed.database_time")} value={totals.duration.kind === "complete" ? duration(totals.duration.total_ms) : null} /></dl>}
    <ObservationList source={t("ui.window.native_sections")} scope={t("ui.archives.original_scope")}
      report={{ title: t("ui.common.view.findings"), summary: t("ui.window.native_scope"), limitations: [], sources: [], entries: section.analysis.findings }}
      renderEntry={(finding) => <div key={finding.id}><h5>{text(finding.title)}</h5><p>{text(finding.summary)}</p></div>} />
    {section.comparison.changes && <>
      <ChangesSummary changes={section.comparison.changes} />
      <p>{text(section.comparison.changes.summary)}</p>
      <ObservationList source={t("ui.window.native_sections")} scope={t("ui.archives.original_scope")}
        report={{ title: t("ui.what_changed.queries"), summary: section.comparison.changes.summary, limitations: [], sources: [], entries: section.comparison.changes.queries }}
        renderEntry={(query) => <div key={query.query_id}><p>{text(query.headline)}</p>{query.reason && <p className="muted">{text(query.reason)}</p>}<pre>{text(query.text)}</pre></div>} />
    </>}
    {workload && <ObservationList source={t("ui.window.native_sections")} scope={t("ui.archives.original_scope")}
      report={{ title: t("ui.window.queries"), summary: t("ui.window.native_scope"), limitations: [], sources: [], entries: workload.queries }}
      renderEntry={(query) => <div key={query.id}><pre>{text(query.text)}</pre><p>{t("ui.window.query_cost", { runs: count(query.executions), mean: duration(query.mean_time_ms) })}</p><details><summary>{t("artifact.ui.retained")}</summary><pre>{query.text}</pre>{query.plan && <PlanView plan={query.plan} query={query} />}</details></div>} />}
  </article>;
}
