import { Copy } from "lucide-react";
import { useRef, type ReactNode } from "react";
import { DeadlockGraph } from "./DeadlockGraph.js";
import { copyAndTell, dateLocale, t } from "./host.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import { Sql } from "./Sql.js";
import type { Deadlock, DeadlockFix, DeadlockPattern, DeadlockProcess } from "./types.js";

export function deadlockPatternLabel(pattern: DeadlockPattern): string {
  switch (pattern) {
    case "key-lookup":
      return t("ui.deadlocks.pattern.key_lookup");
    case "lock-conversion":
      return t("ui.deadlocks.pattern.lock_conversion");
    case "opposite-order":
      return t("ui.deadlocks.pattern.opposite_order");
    case "lock-escalation":
      return t("ui.deadlocks.pattern.lock_escalation");
    case "parallelism":
      return t("ui.deadlocks.pattern.parallelism");
    case "cycle":
      return t("ui.deadlocks.pattern.cycle");
  }
}

export function deadlockWhen(iso: string | null): { short: string; full: string } {
  if (!iso) return { short: t("ui.deadlocks.time_unknown"), full: t("ui.deadlocks.time_unknown_full") };
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return { short: iso, full: iso };
  const today = new Date();
  const sameDay = d.toDateString() === today.toDateString();
  const loc = dateLocale();
  const time = d.toLocaleTimeString(loc, { hour: "numeric", minute: "2-digit" });
  const short = sameDay
    ? t("ui.deadlocks.today", { time })
    : t("ui.format.when.this_year", { date: d.toLocaleDateString(loc, { month: "short", day: "numeric" }), time });
  return { short, full: d.toLocaleString(loc) };
}

export type DeadlockViewerProps = {
  deadlock: Deadlock;
  /** Optional renderer for linking a deadlock fix to a host finding card. */
  renderRelatedFinding?: (fix: DeadlockFix) => ReactNode;
  theme?: PlanViewerTheme;
  className?: string;
};

/**
 * Complete deadlock investigation view: pattern classification, timestamp, interactive lock-cycle SVG graph,
 * plain-language explanation, numbered SQL fixes, and involved session/process cards.
 */
export function DeadlockViewer({ deadlock, renderRelatedFinding, theme, className }: DeadlockViewerProps) {
  const rootRef = useRef<HTMLElement>(null);
  const at = deadlockWhen(deadlock.at);

  const handlePickProcess = (p: DeadlockProcess) => {
    const reduce = typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    rootRef.current?.querySelector(`[data-proc-id="${p.id}"]`)?.scrollIntoView({ block: "nearest", behavior: reduce ? "auto" : "smooth" });
  };

  return (
    <article
      ref={rootRef}
      className={`culprit-pv dl-detail ${className ?? ""}`}
      data-theme={theme && theme !== "system" ? theme : undefined}
    >
      <div className="dl-detail-meta">
        <span className="dl-pattern">{deadlockPatternLabel(deadlock.pattern)}</span>
        <span className="muted" title={at.full}>
          {deadlock.source === "pasted"
            ? deadlock.at
              ? `${at.short} · ${t("ui.deadlocks.pasted")}`
              : t("ui.deadlocks.pasted")
            : `${at.short} · ${deadlock.source}`}
        </span>
      </div>
      <h2 className="dl-headline">{deadlock.headline}</h2>
      <DeadlockGraph deadlock={deadlock} onPick={handlePickProcess} />
      <p className="dl-explain">{deadlock.explanation}</p>

      {deadlock.fixes.length > 0 && (
        <>
          <h3 className="dl-section">{t("ui.deadlocks.how_to_fix")}</h3>
          <ol className="dl-fixes">
            {deadlock.fixes.map((f, i) => (
              <li key={i} className="dl-fix">
                <div className="dl-fix-title">{f.title}</div>
                <p>{f.detail}</p>
                {f.sql && (
                  <div className="dl-fix-sql">
                    <Sql text={f.sql} pretty={false} />
                    <button
                      type="button"
                      className="btn ghost small"
                      onClick={() => copyAndTell(f.sql!, t("ui.common.copied"))}
                      aria-label={t("ui.deadlocks.copy_sql")}
                    >
                      <Copy size={12} /> {t("ui.common.copy")}
                    </button>
                  </div>
                )}
                {renderRelatedFinding?.(f)}
              </li>
            ))}
          </ol>
        </>
      )}

      <h3 className="dl-section">{t("ui.deadlocks.involved")}</h3>
      <div className="dl-procs">
        {deadlock.processes.map((p) => (
          <div key={p.id} id={`dl-proc-${p.id}`} data-proc-id={p.id} className={`dl-proc-card ${p.victim ? "victim" : ""}`}>
            <div className="dl-proc-head">
              <strong>{p.session ? t("ui.deadlocks.session", { session: p.session }) : t("ui.deadlocks.process", { id: p.id })}</strong>
              {p.victim && <span className="pill victim">{t("ui.deadlocks.rolled_back")}</span>}
            </div>
            <div className="dl-proc-facts muted">
              {[
                p.application,
                p.login && p.host ? `${p.login}@${p.host}` : (p.login ?? p.host),
                p.isolation,
                p.wait_mode ? t("ui.deadlocks.wanted", { mode: p.wait_mode }) : null,
                p.procedure ? t("ui.deadlocks.in_procedure", { procedure: p.procedure }) : null,
              ]
                .filter(Boolean)
                .join(" · ")}
            </div>
            {p.statement ? <Sql text={p.statement} className="dl-proc-sql" /> : <p className="muted">{t("ui.deadlocks.no_statement")}</p>}
          </div>
        ))}
      </div>
    </article>
  );
}
