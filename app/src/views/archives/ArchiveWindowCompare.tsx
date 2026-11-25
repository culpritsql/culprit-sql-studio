import { nativeTextPreview } from "../../components/NativeObservationFields";
import type { WarehouseWindowReport } from "../../gen/WarehouseWindowReport";
import { compareArchivedWindows } from "../../lib/archive-window-compare";
import { duration } from "../../lib/format";
import { t } from "../../lib/i18n";

const text = (value: string) => nativeTextPreview(value, 2000);

export function ArchiveWindowCompare({
  baseline,
  current,
  onClear,
}: {
  baseline: WarehouseWindowReport;
  current: WarehouseWindowReport;
  onClear: () => void;
}) {
  const diff = compareArchivedWindows(baseline, current);
  if (!diff) return null;

  return (
    <section className="operational-card archive-window-compare" aria-label={t("ui.archives.window_compare.title")}>
      <div className="archive-fleet-card-head">
        <h4>{t("ui.archives.window_compare.title")}</h4>
        <span className="muted">
          {t("ui.archives.window_compare.pinned_baseline", {
            from: baseline.request.window.from,
            until: baseline.request.window.until,
          })}
        </span>
        <button type="button" className="btn" onClick={onClear}>
          {t("ui.archives.window_compare.clear_baseline")}
        </button>
      </div>
      <p>
        {t("ui.archives.window_compare.summary", {
          added: diff.newFindings.length,
          resolved: diff.resolvedFindings.length,
          slower: diff.slowerQueries.length,
        })}
      </p>
      {diff.newFindings.length > 0 && (
        <div>
          <h5>{t("ui.archives.window_compare.new_findings")}</h5>
          <ul>
            {diff.newFindings.map((item) => (
              <li key={item.finding.id}>
                <strong>{text(item.finding.title)}</strong> — {text(item.finding.summary)}
              </li>
            ))}
          </ul>
        </div>
      )}
      {diff.resolvedFindings.length > 0 && (
        <div>
          <h5>{t("ui.archives.window_compare.resolved_findings")}</h5>
          <ul>
            {diff.resolvedFindings.map((item) => (
              <li key={item.finding.id}>{text(item.finding.title)}</li>
            ))}
          </ul>
        </div>
      )}
      {diff.slowerQueries.length > 0 && (
        <div>
          <h5>{t("ui.archives.window_compare.slower_queries")}</h5>
          <ul>
            {diff.slowerQueries.map((query) => (
              <li key={query.queryId}>
                <strong>{`${query.ratio.toFixed(1)}×`}</strong> ({duration(query.baselineMeanMs)} → {duration(query.currentMeanMs)})
                <pre>{text(query.text)}</pre>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}
