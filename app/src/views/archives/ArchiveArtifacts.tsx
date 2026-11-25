import { useEffect, useMemo, useState } from "react";
import { nativeTextPreview } from "../../components/NativeObservationFields";
import { ObservationList } from "../../components/ObservationList";
import { PlanView } from "../../components/PlanView";
import type { PlanDiff } from "../../gen/PlanDiff";
import type { WarehouseArtifactReport } from "../../gen/WarehouseArtifactReport";
import { api, errorMessage } from "../../lib/api";
import { type ArchivedComparablePlan, extractComparablePlans } from "../../lib/archive-plan-compare";
import { t, useLanguage } from "../../lib/i18n";
import { compare as openInPlanCompare } from "../../lib/plans";

const label = (value: string) => nativeTextPreview(value, 2000);

/** Complete bodies render only as text, under the explicit detail action. A chunk is never a complete plan. */
export function ArchiveArtifacts({ value }: { value: WarehouseArtifactReport }) {
  const language = useLanguage();
  const extracted = useMemo(() => extractComparablePlans(value), [value]);
  const [pinA, setPinA] = useState<ArchivedComparablePlan | null>(null);
  const [pinB, setPinB] = useState<ArchivedComparablePlan | null>(null);
  const [diffState, setDiffState] = useState<
    { status: "idle" | "loading" } | { status: "ready"; diff: PlanDiff } | { status: "error"; message: string }
  >({ status: "idle" });

  useEffect(() => {
    if (!pinA || !pinB) {
      setDiffState({ status: "idle" });
      return;
    }
    let cancelled = false;
    setDiffState({ status: "loading" });
    api()
      .then((client) => client.diffPlans(pinA.plan, pinB.plan, pinA.label, pinB.label, null))
      .then((diff) => {
        if (!cancelled) setDiffState({ status: "ready", diff });
      })
      .catch((failure) => {
        if (!cancelled) setDiffState({ status: "error", message: errorMessage(failure).message });
      });
    return () => {
      cancelled = true;
    };
  }, [pinA, pinB, language]);

  const byKey = useMemo(() => new Map(extracted.complete.map((item) => [item.key, item])), [extracted.complete]);

  return (
    <ObservationList
      source={t("artifact.ui.title")}
      scope={t("ui.archives.original_scope")}
      report={{ title: t("artifact.ui.title"), summary: t("artifact.ui.intro"), limitations: [], sources: [], entries: value.records }}
      renderEntry={(record) => (
        <article className="operational-card" key={`${record.reference.batch_id}:${record.reference.row_ordinal}`}>
          <p>{record.scope.kind === "shared" ? t("artifact.ui.shared") : label(record.scope.label)}</p>
          <details>
            <summary>{t("artifact.ui.clocks")}</summary>
            <p className="muted">{t("ui.archives.original_bounds", { from: record.collected_from, until: record.collected_until })}</p>
            {record.native_clock.kind === "unknown" ? (
              <p>{t("ui.archives.native_clock_unknown")}</p>
            ) : (
              <p>{t("ui.archives.native_bounds", { from: record.native_clock.from, until: record.native_clock.until })}</p>
            )}
          </details>
          {record.artifacts.map((artifact) => {
            const itemKey = `${record.reference.capture.observation_id}:${record.reference.batch_id}:${record.reference.row_ordinal}:${artifact.location}`;
            const comparable = byKey.get(itemKey) ?? null;
            return (
              <div key={artifact.location}>
                <h4>{label(artifact.label)}</h4>
                {artifact.payload.kind === "complete" ? (
                  <>
                    {artifact.payload.payload.state === "available" ? (
                      <pre className="archive-artifact-text">{artifact.payload.payload.text}</pre>
                    ) : (
                      <p>{t("artifact.ui.unavailable")}</p>
                    )}
                    {artifact.plan && (
                      <>
                        {comparable && (
                          <div className="actions">
                            <button
                              type="button"
                              className={`btn ${pinA?.key === comparable.key ? "primary" : ""}`}
                              onClick={() => setPinA(comparable)}
                            >
                              {pinA?.key === comparable.key ? t("ui.archives.plan_compare.pinned_a") : t("ui.archives.plan_compare.pin_a")}
                            </button>
                            <button
                              type="button"
                              className={`btn ${pinB?.key === comparable.key ? "primary" : ""}`}
                              onClick={() => setPinB(comparable)}
                            >
                              {pinB?.key === comparable.key ? t("ui.archives.plan_compare.pinned_b") : t("ui.archives.plan_compare.pin_b")}
                            </button>
                          </div>
                        )}
                        <details>
                          <summary>{t("artifact.kind.plan")}</summary>
                          <PlanView plan={artifact.plan} />
                        </details>
                      </>
                    )}
                  </>
                ) : (
                  <p>{t("artifact.ui.slice")}</p>
                )}
                {artifact.limitations.map((reason, index) => (
                  <p className="muted" key={index}>
                    {label(reason)}
                  </p>
                ))}
              </div>
            );
          })}
        </article>
      )}
    >
      {extracted.refusedSlices > 0 && <p className="muted">{t("ui.archives.plan_compare.slice_refused")}</p>}
      {(pinA || pinB) && (
        <section className="operational-card archive-plan-compare" aria-label={t("ui.archives.plan_compare.title")}>
          <h4>{t("ui.archives.plan_compare.title")}</h4>
          <div className="actions">
            {pinA && pinB && (
              <button
                type="button"
                className="btn primary"
                onClick={() =>
                  openInPlanCompare(
                    { plan: pinA.plan, name: pinA.label, origin: pinA.sourceLabel },
                    { plan: pinB.plan, name: pinB.label, origin: pinB.sourceLabel },
                  )
                }
              >
                {t("ui.archives.plan_compare.open_full")}
              </button>
            )}
            <button
              type="button"
              className="btn"
              onClick={() => {
                setPinA(null);
                setPinB(null);
              }}
            >
              {t("ui.archives.plan_compare.clear")}
            </button>
          </div>
          {(!pinA || !pinB) && <p className="muted">{t("ui.archives.plan_compare.pick_second")}</p>}
          {diffState.status === "loading" && <p role="status">{t("ui.archives.reading")}</p>}
          {diffState.status === "error" && <p role="alert">{label(diffState.message)}</p>}
          {diffState.status === "ready" && (
            <div>
              {diffState.diff.headlines.map((headline, idx) => (
                <p key={idx}>
                  <strong>{label(headline)}</strong>
                </p>
              ))}
              {diffState.diff.changes.length > 0 && (
                <ul>
                  {diffState.diff.changes.map((change, idx) => (
                    <li key={idx}>
                      {label(change.title)}
                      {change.detail ? ` — ${label(change.detail)}` : ""}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </section>
      )}
      <details>
        <summary>{t("artifact.ui.evidence")}</summary>
        {value.qualifications.map((reason, index) => (
          <p className="muted" key={index}>
            {label(reason)}
          </p>
        ))}
        <p>{t("artifact.ui.record_scope")}</p>
        <p>{t("artifact.ui.content_match")}</p>
      </details>
      {value.gaps.length > 0 && <p role="status">{t("artifact.reader.partial")}</p>}
    </ObservationList>
  );
}
