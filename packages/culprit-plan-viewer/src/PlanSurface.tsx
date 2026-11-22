import { Database, FileCode, GitBranch, ListTree, Network, TableProperties } from "lucide-react";
import { useMemo, useState } from "react";
import { DocdbWork } from "./DocdbWork.js";
import {
  allNodes,
  canPlanEstimates,
  cost,
  findingNoShareReason,
  genericPlanContextKey,
  insightsByNode,
  noShareReason,
  planKindLabel,
  planShares,
  remember,
  rememberedChoice,
  t,
  tabListKeys,
} from "./host.js";
import { NodeDetails } from "./NodeDetails.js";
import { PlanAssessmentNotes } from "./PlanAssessmentNotes.js";
import { PlanDiagram } from "./PlanDiagram.js";
import { PlanInsights } from "./PlanInsights.js";
import { PlanObjects } from "./PlanObjects.js";
import { PlanOperations } from "./PlanOperations.js";
import { PlanRaw } from "./PlanRaw.js";
import { PlanTypeSchema } from "./PlanTypeSchema.js";
import { PlanView } from "./PlanView.js";
import type { Plan, QueryStat } from "./types.js";

export type PlanLook = "diagram" | "tree" | "operations" | "objects" | "raw";

const LOOK_KEY = "culprit.plan.look";
const ALL_LOOKS: readonly PlanLook[] = ["diagram", "tree", "operations", "objects", "raw"];

export function rememberedLook(): PlanLook {
  return rememberedChoice<PlanLook>(LOOK_KEY, "diagram", ALL_LOOKS);
}

export function rememberLook(look: PlanLook): void {
  remember(LOOK_KEY, look);
}

const LOOKS: { look: PlanLook; label: () => string; icon: typeof Network; hint: () => string }[] = [
  { look: "diagram", label: () => t("ui.plan_surface.diagram"), icon: Network, hint: () => t("ui.plan_surface.diagram_hint") },
  { look: "tree", label: () => t("ui.plan_surface.tree"), icon: ListTree, hint: () => t("ui.plan_surface.tree_hint") },
  { look: "operations", label: () => t("ui.plan_surface.operations"), icon: TableProperties, hint: () => t("ui.plan_surface.operations_hint") },
  { look: "objects", label: () => t("ui.plan_surface.objects"), icon: Database, hint: () => t("ui.plan_surface.objects_hint") },
  { look: "raw", label: () => t("ui.plan_surface.raw"), icon: FileCode, hint: () => t("ui.plan_surface.raw_hint") },
];

/** Diagram · Tree · Operations · Tables · Raw. */
export function LookToggle({
  look,
  onChange,
  hasRaw = true,
}: {
  look: PlanLook;
  onChange: (look: PlanLook) => void;
  hasRaw?: boolean;
}) {
  const visibleLooks = hasRaw ? LOOKS : LOOKS.filter((item) => item.look !== "raw");
  return (
    <span className="seg look-toggle" role="tablist" aria-label={t("ui.plan_surface.label")} onKeyDown={tabListKeys}>
      {visibleLooks.map((l) => {
        const Icon = l.icon;
        return (
          <button
            type="button"
            key={l.look}
            role="tab"
            aria-selected={look === l.look}
            tabIndex={look === l.look ? 0 : -1}
            className={look === l.look ? "on" : ""}
            title={l.hint()}
            onClick={() => onChange(l.look)}
          >
            <Icon size={12} />
            {l.label()}
          </button>
        );
      })}
    </span>
  );
}

/**
 * A plan with everything Culprit knows about it: the insights first (what's wrong, how to fix it), then the plan in the
 * chosen view. Picking an insight selects its operator in whichever view is showing, and back.
 */
export function PlanSurface({
  plan,
  query,
  focus,
  look,
  badge,
}: {
  plan: Plan;
  query?: QueryStat | null;
  focus?: number | null;
  look: PlanLook;
  badge?: string;
}) {
  const [selected, setSelected] = useState<number | null>(null);
  return (
    <div className="culprit-pv plan-surface">
      {plan.facts.constants_redacted && <p className="notice">{t("ui.plan_surface.redacted")}</p>}
      {!plan.insights?.note && plan.kind === "generic" && (
        <PlanAssessmentNotes
          note={[t("ui.plan_surface.generic"), t(genericPlanContextKey(plan.engine)), t("ui.plan_surface.generic_history")].filter(Boolean).join(" ")}
        />
      )}
      <PlanInsights plan={plan} selectedNode={selected} onSelectNode={setSelected} />
      <DocdbWork metrics={plan.facts.docdb} basis="execution" />
      <PlanTypeSchema plan={plan} />
      {look === "diagram" && <PlanDiagram plan={plan} query={query} focus={focus} selectedId={selected} onSelect={setSelected} badge={badge} />}
      {look === "tree" && <PlanView plan={plan} query={query} focus={focus} selectedId={selected} onSelect={setSelected} />}
      {look === "operations" && <OperationsPanel plan={plan} query={query} focus={focus} selected={selected ?? focus ?? null} onSelect={setSelected} />}
      {look === "objects" && <ObjectsPanel plan={plan} query={query} focus={focus} selected={selected ?? focus ?? null} onSelect={setSelected} />}
      {look === "raw" && <PlanRaw plan={plan} />}
    </div>
  );
}

function OperationsPanel({
  plan,
  query,
  focus,
  selected,
  onSelect,
}: {
  plan: Plan;
  query?: QueryStat | null;
  focus?: number | null;
  selected: number | null;
  onSelect: (id: number) => void;
}) {
  const shares = useMemo(() => planShares(plan, query), [plan, query]);
  const nodes = useMemo(() => allNodes(plan.root), [plan]);
  const flagged = useMemo(() => insightsByNode(plan), [plan]);
  const shareOf = (id: number) => shares.stated.get(id) ?? null;
  const start = shares.hottest ?? plan.root;
  const current = nodes.find((n) => n.id === (selected ?? start.id)) ?? null;
  return (
    <div className="plan">
      <div className="plan-head">
        <span className="tag">{planKindLabel(plan)}</span>
        <span>
          {t("ui.common.plan_operators", { count: nodes.length })}
          {canPlanEstimates(plan.engine) &&
            ` · ${
              plan.facts.estimated_cost_unavailable
                ? t("ui.common.plan_cost_unavailable")
                : t("ui.common.plan_cost", { cost: cost(plan.total_cost || plan.root.subtree_cost) })
            }`}
        </span>
        <span className="spacer" />
        <GitBranch size={12} className="muted" />
        <span className="muted">{t("ui.plan_surface.click_row")}</span>
      </div>
      <PlanOperations plan={plan} query={query} focus={focus} selectedId={current?.id ?? null} onSelect={onSelect} />
      {current && (
        <NodeDetails
          plan={plan}
          estimates={canPlanEstimates(plan.engine)}
          actual={plan.kind === "actual"}
          node={current}
          share={shareOf(current.id)}
          subtreeShare={shares.statedSubtree.get(current.id) ?? null}
          noShare={shares.basis === "cost" ? (current.id === focus ? findingNoShareReason(plan, current, query) : noShareReason(plan, current, query)) : null}
          insights={flagged.get(current.id)}
        />
      )}
    </div>
  );
}

function ObjectsPanel({
  plan,
  query,
  focus,
  selected,
  onSelect,
}: {
  plan: Plan;
  query?: QueryStat | null;
  focus?: number | null;
  selected: number | null;
  onSelect: (id: number) => void;
}) {
  const shares = useMemo(() => planShares(plan, query), [plan, query]);
  const nodes = useMemo(() => allNodes(plan.root), [plan]);
  const flagged = useMemo(() => insightsByNode(plan), [plan]);
  const shareOf = (id: number) => shares.stated.get(id) ?? null;
  const firstWithObject = nodes.find((n) => n.object != null) ?? shares.hottest ?? plan.root;
  const current = nodes.find((n) => n.id === (selected ?? firstWithObject.id)) ?? null;
  return (
    <div className="plan">
      <div className="plan-head">
        <span className="tag">{planKindLabel(plan)}</span>
        <span>{t("ui.plan_surface.objects_hint")}</span>
        <span className="spacer" />
        <GitBranch size={12} className="muted" />
        <span className="muted">{t("ui.plan_surface.click_row")}</span>
      </div>
      <PlanObjects plan={plan} query={query} selectedId={current?.id ?? null} onSelect={onSelect} />
      {current && (
        <NodeDetails
          plan={plan}
          estimates={canPlanEstimates(plan.engine)}
          actual={plan.kind === "actual"}
          node={current}
          share={shareOf(current.id)}
          subtreeShare={shares.statedSubtree.get(current.id) ?? null}
          noShare={shares.basis === "cost" ? (current.id === focus ? findingNoShareReason(plan, current, query) : noShareReason(plan, current, query)) : null}
          insights={flagged.get(current.id)}
        />
      )}
    </div>
  );
}
