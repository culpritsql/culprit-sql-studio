import { Copy, FileCode } from "lucide-react";
import { useMemo } from "react";
import { copyAndTell, exact, planEngineName, t } from "./host.js";
import { Sql } from "./Sql.js";
import type { Plan } from "./types.js";

function rawFormatLabel(raw: string): string {
  const trimmed = raw.trimStart();
  if (trimmed.startsWith("<")) return "XML";
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) return "JSON";
  return "TEXT";
}

export function PlanRaw({ plan }: { plan: Plan }) {
  const raw = plan.raw;
  const formatted = useMemo(() => {
    if (!raw) return null;
    const trimmed = raw.trim();
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        return JSON.stringify(JSON.parse(trimmed), null, 2);
      } catch {
        return raw;
      }
    }
    return raw;
  }, [raw]);

  if (!formatted) {
    return (
      <div className="culprit-pv plan plan-raw">
        <div className="plan-head">
          <FileCode size={13} className="muted" />
          <span className="tag">{planEngineName(plan)}</span>
          <span className="muted">{t("ui.plan_raw.unavailable")}</span>
        </div>
        {plan.statement && (
          <div className="plan-raw-sql">
            <Sql text={plan.statement} />
          </div>
        )}
      </div>
    );
  }

  const lineCount = formatted.split("\n").length;

  return (
    <div className="culprit-pv plan plan-raw">
      <div className="plan-head">
        <span className="tag">{rawFormatLabel(formatted)}</span>
        <span className="muted">
          {planEngineName(plan)} · {t("ui.plan_raw.lines", { n: lineCount, count: exact(lineCount) })}
        </span>
        <span className="spacer" />
        <button
          type="button"
          className="btn ghost small"
          onClick={() => copyAndTell(formatted, t("ui.plan_raw.copied"))}
        >
          <Copy size={12} /> {t("ui.plan_raw.copy")}
        </button>
      </div>
      <pre className="plan-raw-code selectable">{formatted}</pre>
    </div>
  );
}
