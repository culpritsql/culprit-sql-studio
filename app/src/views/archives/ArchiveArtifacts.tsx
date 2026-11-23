import type { WarehouseArtifactReport } from "../../gen/WarehouseArtifactReport";
import { PlanView } from "../../components/PlanView";
import { nativeTextPreview } from "../../components/NativeObservationFields";
import { ObservationList } from "../../components/ObservationList";
import { t } from "../../lib/i18n";
const label = (value: string) => nativeTextPreview(value, 2000);
/** Complete bodies render only as text, under the explicit detail action. A chunk is never a complete plan. */
export function ArchiveArtifacts({ value }: { value: WarehouseArtifactReport }) {
  return <ObservationList source={t("artifact.ui.title")} scope={t("ui.archives.original_scope")}
      report={{ title: t("artifact.ui.title"), summary: t("artifact.ui.intro"), limitations: [], sources: [], entries: value.records }}
      renderEntry={(record) => <article className="operational-card" key={`${record.reference.batch_id}:${record.reference.row_ordinal}`}>
        <p>{record.scope.kind === "shared" ? t("artifact.ui.shared") : label(record.scope.label)}</p>
        <details><summary>{t("artifact.ui.clocks")}</summary><p className="muted">{t("ui.archives.original_bounds", { from: record.collected_from, until: record.collected_until })}</p>
        {record.native_clock.kind === "unknown" ? <p>{t("ui.archives.native_clock_unknown")}</p> : <p>{t("ui.archives.native_bounds", { from: record.native_clock.from, until: record.native_clock.until })}</p>}
        </details>
        {record.artifacts.map((artifact) => <div key={artifact.location}>
          <h4>{label(artifact.label)}</h4>

          {artifact.payload.kind === "complete" ? <>
            {artifact.payload.payload.state === "available" ? <pre className="archive-artifact-text">{artifact.payload.payload.text}</pre> : <p>{t("artifact.ui.unavailable")}</p>}
            {artifact.plan && <details><summary>{t("artifact.kind.plan")}</summary><PlanView plan={artifact.plan} /></details>}
          </> : <p>{t("artifact.ui.slice")}</p>}
          {artifact.limitations.map((reason, index) => <p className="muted" key={index}>{label(reason)}</p>)}
        </div>)}
      </article>}>
    <details><summary>{t("artifact.ui.evidence")}</summary>
      {value.qualifications.map((reason, index) => <p className="muted" key={index}>{label(reason)}</p>)}
      <p>{t("artifact.ui.record_scope")}</p><p>{t("artifact.ui.content_match")}</p>
    </details>
    {value.gaps.length > 0 && <p role="status">{t("artifact.reader.partial")}</p>}
  </ObservationList>;
}
