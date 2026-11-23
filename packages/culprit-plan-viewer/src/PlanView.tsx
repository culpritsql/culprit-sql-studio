import { ChevronDown, ChevronRight, ChevronUp, Flame, Search, TriangleAlert, X } from "lucide-react";
import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { ActualSummary } from "./ActualSummary.js";
import {
  canPlanEstimates,
  count,
  estRows,
  estRowsForRuns,
  findingNoShareReason,
  insightsByNode,
  misestimate,
  noShareReason,
  percent,
  planKindLabel,
  planShares,
  predicateLabel,
  rows as rowsText,
  shortPredicate,
  t,
} from "./host.js";
import { NodeDetails } from "./NodeDetails.js";
import { OpIcon } from "./OpIcon.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import type { Plan, PlanInsight, PlanNode, QueryStat } from "./types.js";

export { planKindLabel, predicateLabel, shortPredicate };

type Props = {
  plan: Plan;
  query?: QueryStat | null;
  focus?: number | null;
  compact?: boolean;
  selectedId?: number | null;
  onSelect?: (id: number) => void;
  showHeader?: boolean;
  showActualSummary?: boolean;
  showProps?: boolean;
  showInsights?: boolean;
  theme?: PlanViewerTheme;
  className?: string;
};

const WARNING_LABEL: Record<string, () => string> = {
  "implicit-conversion": () => t("ui.plan_view.warning.implicit_conversion"),
  spill: () => t("ui.plan_view.warning.spill"),
  "no-join-predicate": () => t("ui.plan_view.warning.no_join_predicate"),
  "missing-statistics": () => t("ui.plan_view.warning.missing_statistics"),
  "memory-grant": () => t("ui.plan_view.warning.memory_grant"),
  misestimate: () => t("ui.plan_view.warning.misestimate"),
  "residual-predicate": () => t("ui.plan_view.warning.residual_predicate"),
};

const warningLabel = (w: string) => WARNING_LABEL[w]?.() ?? w;

function walk(node: PlanNode, out: PlanNode[] = []): PlanNode[] {
  out.push(node);
  node.children.forEach((c) => walk(c, out));
  return out;
}

function pathTo(node: PlanNode, id: number): number[] | null {
  if (node.id === id) return [node.id];
  for (const child of node.children) {
    const path = pathTo(child, id);
    if (path) return [node.id, ...path];
  }
  return null;
}

function heatVar(share: number): string {
  if (share >= 0.3) return "var(--heat-4)";
  if (share >= 0.15) return "var(--heat-3)";
  if (share >= 0.05) return "var(--heat-2)";
  if (share >= 0.01) return "var(--heat-1)";
  return "var(--heat-0)";
}

export function PlanView({
  plan,
  query,
  focus,
  compact,
  selectedId,
  onSelect,
  showHeader = true,
  showActualSummary = true,
  showProps = true,
  showInsights = true,
  theme,
  className,
}: Props) {
  const nodes = useMemo(() => walk(plan.root), [plan]);
  const flagged = useMemo(() => (showInsights ? insightsByNode(plan) : new Map<number, PlanInsight[]>()), [plan, showInsights]);
  const shares = useMemo(() => planShares(plan, query), [plan, query]);
  const reason = (n: PlanNode) => (focus === n.id ? findingNoShareReason(plan, n, query) : noShareReason(plan, n, query));
  const focused = nodes.find((n) => n.id === focus);
  const note = shares.basis === "cost" && focused ? reason(focused) ?? shares.note : shares.note;
  const shareOf = (n: PlanNode) => shares.stated.get(n.id) ?? null;
  const hottest = shares.hottest;
  const target = focus ?? hottest?.id ?? plan.root.id;
  const hotPath = useMemo(() => new Set(pathTo(plan.root, target) ?? []), [plan.root, target]);

  const [expanded, setExpanded] = useState<Set<number>>(() => {
    const open = new Set<number>(hotPath);
    nodes.forEach((n) => {
      if ((shares.subtree.get(n.id) ?? 0) >= 0.05) open.add(n.id);
    });
    return open;
  });
  const [selected, setSelected] = useState<number | null>(selectedId ?? target);
  const [searchText, setSearchText] = useState("");
  const [matchCursor, setMatchCursor] = useState(0);
  const trimmedSearch = searchText.trim().toLowerCase();
  const matchedIds = useMemo(() => {
    if (!trimmedSearch) return [];
    return nodes
      .filter((n) => `${n.op} ${n.object ?? ""} ${n.index ?? ""} ${n.predicate ?? ""} ${n.seek_predicate ?? ""}`.toLowerCase().includes(trimmedSearch))
      .map((n) => n.id);
  }, [nodes, trimmedSearch]);
  const matchedSet = useMemo(() => new Set(matchedIds), [matchedIds]);

  useEffect(() => {
    if (matchedIds.length === 0) return;
    setMatchCursor(0);
    setExpanded((prev) => {
      const next = new Set(prev);
      matchedIds.forEach((id) => (pathTo(plan.root, id) ?? []).forEach((anc) => next.add(anc)));
      return next;
    });
  }, [matchedIds, plan.root]);

  const selectedNode = nodes.find((n) => n.id === selected) ?? null;

  useEffect(() => {
    if (selectedId == null) return;
    setSelected(selectedId);
    const path = pathTo(plan.root, selectedId) ?? [];
    setExpanded((prev) => new Set([...prev, ...path]));
  }, [selectedId, plan.root]);

  const hasActuals = nodes.some((n) => n.act_rows != null);
  const weighed = canPlanEstimates(plan.engine) || hasActuals;

  const toggle = (id: number) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const stepMatch = (delta: number) => {
    if (matchedIds.length === 0) return;
    const nextIdx = (matchCursor + delta + matchedIds.length) % matchedIds.length;
    setMatchCursor(nextIdx);
    const id = matchedIds[nextIdx];
    if (id != null) {
      setSelected(id);
      onSelect?.(id);
    }
  };

  const rows: { node: PlanNode; depth: number }[] = [];
  const visit = (node: PlanNode, depth: number) => {
    rows.push({ node, depth });
    if (expanded.has(node.id)) node.children.forEach((c) => visit(c, depth + 1));
  };
  visit(plan.root, 0);

  return (
    <div className={`culprit-pv plan ${className ?? ""}`} data-theme={theme && theme !== "system" ? theme : undefined}>
      {showHeader && (
        <div className="plan-head">
        <span className="tag">{planKindLabel(plan)}</span>
        <span>
          {t("ui.common.plan_operators", { count: nodes.length })}
          {weighed &&
            ` · ${
              plan.facts.estimated_cost_unavailable
                ? t("ui.common.plan_cost_unavailable")
                : t("ui.common.plan_cost", { cost: (plan.total_cost || plan.root.subtree_cost).toFixed((plan.total_cost || plan.root.subtree_cost) < 10 ? 3 : 1) })
            }`}
        </span>
        {plan.warnings.length > 0 && (
          <span className="warn-chip">
            <TriangleAlert size={11} />
            {plan.warnings.map(warningLabel).join(", ")}
          </span>
        )}
        <span className="spacer" />
        {!compact && (
          <div className="search plan-search">
            <Search size={12} />
            <input
              value={searchText}
              placeholder={t("ui.plan_diagram.search_placeholder")}
              onChange={(e) => setSearchText(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  stepMatch(e.shiftKey ? -1 : 1);
                } else if (e.key === "Escape" && searchText) {
                  e.preventDefault();
                  setSearchText("");
                }
              }}
            />
            {trimmedSearch && (
              <>
                <span className="plan-search-count num">
                  {matchedIds.length > 0
                    ? t("ui.plan_diagram.search_matches", { current: String(matchCursor + 1), total: String(matchedIds.length) })
                    : t("ui.plan_diagram.search_none")}
                </span>
                {matchedIds.length > 1 && (
                  <>
                    <button type="button" className="plan-search-btn" title={t("ui.plan_diagram.search_prev")} onClick={() => stepMatch(-1)}>
                      <ChevronUp size={12} />
                    </button>
                    <button type="button" className="plan-search-btn" title={t("ui.plan_diagram.search_next")} onClick={() => stepMatch(1)}>
                      <ChevronDown size={12} />
                    </button>
                  </>
                )}
                <button type="button" className="plan-search-btn" title={t("ui.plan_diagram.search_clear")} onClick={() => setSearchText("")}>
                  <X size={12} />
                </button>
              </>
            )}
          </div>
        )}
      </div>
      )}
      {showActualSummary && plan.kind === "actual" && <ActualSummary plan={plan} onPick={setSelected} />}
      <div className="plan-cols">
        <span>{t("ui.plan_view.operator")}</span>
        <span>{hasActuals ? t("ui.plan_view.rows_est_actual") : weighed ? t("ui.plan_view.est_rows") : ""}</span>
        <span>{shares.basis === "time" ? t("ui.plan_view.own_time") : weighed ? t("ui.plan_view.self_cost") : ""}</span>
      </div>
      <div style={compact ? { maxHeight: 340, overflowY: "auto" } : undefined}>
        {rows.map(({ node, depth }) => {
          const share = shareOf(node);
          const mis = node.warnings.includes("misestimate") ? misestimate(plan, node) : null;
          return (
            <button
              type="button"
              key={node.id}
              className={`pnode ${selected === node.id ? "sel" : ""} ${hotPath.has(node.id) ? "hot" : ""} ${matchedSet.has(node.id) ? "match" : ""}`}
              onClick={() => {
                setSelected(node.id);
                onSelect?.(node.id);
              }}
              onDoubleClick={() => toggle(node.id)}
              aria-expanded={node.children.length > 0 ? expanded.has(node.id) : undefined}
              onKeyDown={(e) => {
                const open = expanded.has(node.id);
                if (node.children.length > 0 && ((e.key === "ArrowRight" && !open) || (e.key === "ArrowLeft" && open))) {
                  e.preventDefault();
                  toggle(node.id);
                }
              }}
            >
              <span className="label">
                <span className="indent" style={{ "--depth": Math.min(depth, 12) } as CSSProperties} />
                <span
                  className="twisty"
                  aria-hidden
                  onClick={(e) => {
                    e.stopPropagation();
                    toggle(node.id);
                  }}
                >
                  {node.children.length > 0 ? expanded.has(node.id) ? <ChevronDown size={14} /> : <ChevronRight size={14} /> : null}
                </span>
                <span className="lines">
                  <span className="line1">
                    <span className="op-icon">
                      <OpIcon node={node} size={13} />
                    </span>
                    {node.id === hottest?.id && (share ?? 0) >= 0.05 && <Flame size={13} color="var(--heat-4)" className="flame" />}
                    <span className="opname" title={node.op}>
                      {node.op}
                    </span>
                    {flagged.get(node.id)?.[0] && (
                      <span className={`tree-flag ${flagged.get(node.id)![0]!.severity}`} title={flagged.get(node.id)!.map((i) => i.title).join("\n")}>
                        {flagged.get(node.id)!.length}
                      </span>
                    )}
                    {node.object && (
                      <span className="obj" title={node.index ? `${node.object} · ${node.index}` : node.object}>
                        {node.object}
                        {node.index ? ` · ${node.index}` : ""}
                      </span>
                    )}
                  </span>
                  {(node.warnings.length > 0 || node.predicate || node.seek_predicate) && (
                    <span className="line2">
                      {node.warnings.map((w) => (
                        <span key={w} className="warn-chip">
                          {warningLabel(w)}
                        </span>
                      ))}
                      {(node.seek_predicate || node.predicate) && (
                        <span className="pred">{node.seek_predicate ? predicateLabel("seek", node.seek_predicate) : predicateLabel("filter", node.predicate ?? "")}</span>
                      )}
                    </span>
                  )}
                </span>
              </span>
              <span className="rows">
                {node.act_rows != null ? (
                  <>
                    <span className="rows-pair">
                      {rowsText(estRowsForRuns(plan, node))} <span className="arrow">→</span> {rowsText(node.act_rows)}
                    </span>
                    {mis != null && (
                      <span className="mis">
                        {mis < 0 ? t("ui.plan_view.over", { times: count(Math.abs(mis)) }) : t("ui.plan_view.under", { times: count(Math.abs(mis)) })}
                      </span>
                    )}
                  </>
                ) : (
                  weighed && rowsText(estRows(plan, node))
                )}
              </span>
              <span className="cost">
                {weighed &&
                  (share != null ? (
                    <>
                      <span className="num">{percent(share)}</span>
                      <span className="heat">
                        <span style={{ width: `${Math.max(2, Math.min(100, share * 100))}%`, background: heatVar(share) }} />
                      </span>
                    </>
                  ) : (
                    <span className="num muted" title={reason(node) ?? undefined}>
                      -
                    </span>
                  ))}
              </span>
            </button>
          );
        })}
      </div>
      {weighed && note && <div className="muted" style={{ fontSize: 12, padding: "6px 10px" }}>{note}</div>}
      {selectedNode && showProps && (
        <NodeDetails
          plan={plan}
          estimates={weighed}
          actual={plan.kind === "actual"}
          node={selectedNode}
          share={shareOf(selectedNode)}
          subtreeShare={shares.statedSubtree.get(selectedNode.id) ?? null}
          noShare={shares.basis === "cost" ? reason(selectedNode) : null}
          insights={flagged.get(selectedNode.id)}
        />
      )}
    </div>
  );
}

/** Alias of PlanView for ergonomic granular imports alongside PlanCanvas. */
export const PlanTree = PlanView;

/** The operator detail panel; kept under this name for existing callers. */
export function NodeProps({ node, share, insights }: { node: PlanNode; share: number | null; insights?: PlanInsight[] }) {
  return <NodeDetails node={node} share={share} insights={insights} />;
}
