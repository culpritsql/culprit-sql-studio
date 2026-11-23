import type { InspectArtifacts } from "../../lib/archive-artifacts";
import type { WarehouseIncidentView } from "../../gen/WarehouseIncidentView";
import type { DerivedIncident } from "../../gen/DerivedIncident";
import type { DerivedIncidentQuery } from "../../gen/DerivedIncidentQuery";
import type { DerivedCaller } from "../../gen/DerivedCaller";
import { IncidentHistory } from "../../components/IncidentHistory";
import { ObservationList } from "../../components/ObservationList";
import { NativeValue, nativeTextPreview } from "../../components/NativeObservationFields";
import { t } from "../../lib/i18n";
const text = (value: string) => nativeTextPreview(value, 2000);
export function ArchiveIncident({ value, onArtifacts }: { value: WarehouseIncidentView; onArtifacts?: InspectArtifacts }) {
  const { report } = value;
  return <section aria-label={t("ui.archives.incident")}>
    <p className="muted">{t("ui.archives.incident_scope", { from: report.from, until: report.until })}</p>
    {!report.discovery_complete && <p role="status">{t("ui.archives.partial")}</p>}
    {value.analysis_limitations.length > 0 && <details>
      <summary>{t("ui.memory_failures.limitations")}</summary>
      {value.analysis_limitations.map((limits, index) => <div key={index}>
        <p className="muted">{t("ui.archives.original_bounds", { from: limits.observed_from, until: limits.observed_until })}</p>
        {limits.reasons.map((reason, position) => <p className="muted" key={position}>{text(reason)}</p>)}
      </div>)}
    </details>}
    {report.incidents.length === 0 && <p>{t("ui.archives.no_incidents")}</p>}
    <ObservationList source={t("ui.archives.published")} scope={t("ui.archives.original_scope")} report={{ title: t("ui.archives.incident"), summary: t("ui.archives.caller_limit"), limitations: [], sources: [], entries: report.incidents }} renderEntry={(incident) => <Incident key={`${incident.analysis_id}:${incident.entry.revision_id}`} incident={incident} onArtifacts={onArtifacts} />} />
    <details><summary>{t("ui.archives.history")}</summary><IncidentHistory report={report.history} /></details>
  </section>;
}
function Incident({ incident, onArtifacts }: { incident: DerivedIncident; onArtifacts?: InspectArtifacts }) {
  return <article className="operational-card">
    <h3>{text(incident.entry.title)}</h3><p>{text(incident.victim_summary)}</p><p className="muted">{text(incident.entry.scope_label)}</p><p>{text(incident.entry.phase)}</p>
    <Queries title={t("ui.archives.affected")} entries={incident.affected_queries} onArtifacts={onArtifacts} />
    <Queries title={t("ui.archives.culprits")} entries={incident.culprit_queries} onArtifacts={onArtifacts} />
    {incident.culprit_queries.length === 0 && <p>{t("ui.archives.culprit_unknown")}</p>}
    <Queries title={t("ui.archives.candidates")} entries={incident.candidate_queries} onArtifacts={onArtifacts} />
    <Caller caller={incident.caller} />
    <h4>{t("ui.archives.mechanism")}</h4><p>{incident.mechanism == null ? t("ui.archives.mechanism_unknown") : text(incident.mechanism)}</p>
    <h4>{t("ui.archives.action")}</h4><p>{text(incident.developer_action)}</p>
    <h4>{t("ui.archives.verification")}</h4><p>{text(incident.verification)}</p>
  </article>;
}
function Queries({ title, entries, onArtifacts }: { title: string; entries: DerivedIncidentQuery[]; onArtifacts?: InspectArtifacts }) {
  if (entries.length === 0) return null;
  return <details><summary>{title}</summary><ObservationList source={t("ui.archives.published")} scope={t("ui.archives.original_scope")} report={{ title, summary: t("ui.archives.query_scope"), limitations: [], sources: [], entries: entries.map((query, position) => ({ query, position })) }} renderEntry={({ query, position }) => <div key={position}>
    <pre>{query.sql.state === "available" ? text(query.sql.text) : t("ui.archives.sql_unknown")}</pre>
    {query.identity?.kind === "shared-activity" && <p className="muted">{query.identity.data.database == null
      ? t("view.synthesis.shared_database_unknown")
      : t("view.synthesis.shared_database", { database: text(query.identity.data.database) })}</p>}
    {query.causal_role && <p>{text(query.causal_role.explanation.text)}</p>}
    <Caller caller={query.caller} />
    {onArtifacts && query.evidence.length > 0 && <button type="button" className="btn" onClick={() => onArtifacts({ references: query.evidence, sql: query.sql })}>{t("artifact.ui.open")}</button>}
  </div>} /></details>;
}
function Caller({ caller }: { caller: DerivedCaller | null }) {
  if (caller == null) return <p className="muted">{t("ui.archives.caller_unknown")}</p>;
  return <dl>
    <NativeValue label={t("view.session_census.field.application")} value={caller.client?.application == null ? null : text(caller.client.application)} />
    <NativeValue label={t("view.session_census.field.login")} value={caller.client?.user == null ? null : text(caller.client.user)} />
    <NativeValue label={t("view.session_census.field.host")} value={caller.client?.host == null ? null : text(caller.client.host)} />
    <NativeValue label={t("view.session_census.field.library")} value={caller.session_context?.client_library == null ? null : text(caller.session_context.client_library)} />
    <NativeValue label={t("view.session_census.field.client_process")} value={caller.session_context?.client_process_id == null ? null : text(caller.session_context.client_process_id)} />
  </dl>;
}
