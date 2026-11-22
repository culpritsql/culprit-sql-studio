import { useState, type ReactNode } from "react";
import { planEngineName, planKindLabel } from "./host.js";
import { LookToggle, PlanSurface, type PlanLook } from "./PlanSurface.js";
import { Sql } from "./Sql.js";
import type { Plan, QueryStat } from "./types.js";

export type PlanViewerTheme = "light" | "dark" | "system";

export type PlanViewerProps = {
  plan: Plan;
  query?: QueryStat | null;
  focus?: number | null;
  /** Controlled view mode ("diagram" | "tree" | "operations"). */
  look?: PlanLook;
  /** Initial view mode when uncontrolled. Defaults to "diagram". */
  defaultLook?: PlanLook;
  onLookChange?: (look: PlanLook) => void;
  /** Render the built-in header bar with engine/kind metadata and the Diagram · Tree · Operations switcher. Defaults to true. */
  showToolbar?: boolean;
  /** Show the parsed SQL statement above the plan when available. Defaults to true. */
  showStatement?: boolean;
  /** Optional title displayed in the toolbar. */
  title?: string;
  /** Optional slot rendered at the end of the toolbar (for export/share buttons). */
  toolbarActions?: ReactNode;
  /** Optional explicit color theme ("light" | "dark" | "system"). */
  theme?: PlanViewerTheme;
  className?: string;
};

/**
 * Drop-in execution plan viewer: bottleneck insights, measured-run summary, cost/time strip,
 * interactive Diagram / Tree / Operations views, and operator property inspector.
 */
export function PlanViewer({
  plan,
  query,
  focus,
  look: controlledLook,
  defaultLook = "diagram",
  onLookChange,
  showToolbar = true,
  showStatement = true,
  title,
  toolbarActions,
  theme,
  className,
}: PlanViewerProps) {
  const [internalLook, setInternalLook] = useState<PlanLook>(defaultLook);
  const [sqlOpen, setSqlOpen] = useState(false);
  const look = controlledLook ?? internalLook;

  const handleLookChange = (next: PlanLook) => {
    setInternalLook(next);
    onLookChange?.(next);
  };

  return (
    <div className={`culprit-pv pv-root ${className ?? ""}`} data-theme={theme && theme !== "system" ? theme : undefined}>
      {showToolbar && (
        <div className="pv-toolbar">
          <div className="pv-meta">
            {title && <strong className="pv-title">{title}</strong>}
            <span className="tag">{planEngineName(plan)}</span>
            <span className="muted">{planKindLabel(plan)}</span>
          </div>
          <span className="spacer" />
          <LookToggle look={look} onChange={handleLookChange} />
          {toolbarActions}
        </div>
      )}
      {showStatement && plan.statement && (
        <div
          className={`plans-sql ${sqlOpen ? "open" : ""}`}
          onClick={() => setSqlOpen((v) => !v)}
          title={sqlOpen ? "Click to collapse SQL" : "Click to expand SQL"}
        >
          <Sql text={plan.statement} />
        </div>
      )}
      <PlanSurface plan={plan} query={query} focus={focus} look={look} />
    </div>
  );
}
