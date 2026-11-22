import {
  ArrowRightLeft,
  CircleCheck,
  EqualNot,
  FileDiff,
  Flag,
  Gauge,
  KeyRound,
  Link2,
  MemoryStick,
  Ruler,
  Split,
  Unlink2,
  Variable,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState, type ReactNode, type RefObject } from "react";
import { canPlanEstimates, count, duration, estRows, percent, planEngineName, stepShare, t, tabListKeys } from "./host.js";
import { PlanDiagram, type Mark } from "./PlanDiagram.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import { PlanView } from "./PlanView.js";
import type { ChangeKind, Plan, PlanDiff, PlanNode, QueryStat, ScenarioKind } from "./types.js";

export type PlanDiffLook = "diagram" | "tree";
type DiffTab = "changes" | "why" | "props";

const CHANGE_TONE: Record<ChangeKind, string> = {
  access: "warn",
  operator: "warn",
  added: "good",
  removed: "bad",
  cost: "info",
  rows: "info",
  warning: "warn",
};

function changeLabel(kind: ChangeKind): string {
  switch (kind) {
    case "access":
      return t("ui.plan_compare.change.access");
    case "operator":
      return t("ui.plan_compare.change.operator");
    case "added":
      return t("ui.plan_compare.change.added");
    case "removed":
      return t("ui.plan_compare.change.removed");
    case "cost":
      return t("ui.plan_compare.change.cost");
    case "rows":
      return t("ui.plan_compare.change.rows");
    case "warning":
      return t("ui.plan_compare.change.warning");
  }
}

const SCENARIO_ICON: Record<ScenarioKind, ReactNode> = {
  statement: <FileDiff size={15} />,
  "plan-kind": <Flag size={15} />,
  estimator: <Gauge size={15} />,
  parameters: <Variable size={15} />,
  estimates: <Ruler size={15} />,
  index: <KeyRound size={15} />,
  conversion: <ArrowRightLeft size={15} />,
  parallelism: <Split size={15} />,
  memory: <MemoryStick size={15} />,
};

function scenarioLabel(kind: ScenarioKind): string {
  switch (kind) {
    case "statement":
      return t("ui.plan_compare.scenario.statement");
    case "plan-kind":
      return t("ui.plan_compare.scenario.plan_kind");
    case "estimator":
      return t("ui.plan_compare.scenario.estimator");
    case "parameters":
      return t("ui.plan_compare.scenario.parameters");
    case "estimates":
      return t("ui.plan_compare.scenario.estimates");
    case "index":
      return t("ui.plan_compare.scenario.index");
    case "conversion":
      return t("ui.plan_compare.scenario.conversion");
    case "parallelism":
      return t("ui.plan_compare.scenario.parallelism");
    case "memory":
      return t("ui.plan_compare.scenario.memory");
  }
}

function formatCost(value: number): string {
  if (value < 10) return value.toFixed(3);
  if (value < 100) return value.toFixed(1);
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
  return value >= 1e4 ? `${Math.round(value / 1e3)}K` : value.toFixed(0);
}

function nodesOf(plan: Plan): PlanNode[] {
  const out: PlanNode[] = [];
  const walk = (n: PlanNode) => {
    out.push(n);
    n.children.forEach(walk);
  };
  walk(plan.root);
  return out;
}

function verdict(diff: PlanDiff): { tone: "worse" | "better" | "same"; text: string } {
  const timed = diff.time_a_ms != null && diff.time_b_ms != null && diff.time_a_ms > 0 && diff.time_b_ms > 0;
  const r = timed ? diff.time_b_ms! / diff.time_a_ms! : diff.cost_ratio;
  if (r == null) return { tone: "same", text: diff.comparable ? t("ui.plan_compare.verdict.no_costs") : t("ui.plan_compare.verdict.different_engines") };
  const x = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(1).replace(/\.0$/, "")}K×` : v >= 10 ? `${Math.round(v)}×` : `${v.toFixed(1)}×`);
  if (r >= 1.25) return { tone: "worse", text: timed ? t("ui.plan_compare.verdict.slower", { ratio: x(r) }) : t("ui.plan_compare.verdict.costlier", { ratio: x(r) }) };
  if (r <= 0.8) return { tone: "better", text: timed ? t("ui.plan_compare.verdict.faster", { ratio: x(1 / r) }) : t("ui.plan_compare.verdict.cheaper", { ratio: x(1 / r) }) };
  return { tone: "same", text: t("ui.plan_compare.verdict.same") };
}

export type PlanDiffViewerProps = {
  a: Plan;
  b: Plan;
  diff: PlanDiff;
  nameA?: string;
  nameB?: string;
  queryA?: QueryStat | null;
  queryB?: QueryStat | null;
  defaultLook?: PlanDiffLook;
  defaultSync?: boolean;
  toolbarStart?: ReactNode;
  toolbarActions?: ReactNode;
  theme?: PlanViewerTheme;
  className?: string;
};

/**
 * Side-by-side execution plan comparison viewer: verdict summary, structural & access-path changes,
 * root-cause scenarios, operator property diff table, and synchronized dual-plan pan/zoom.
 */
export function PlanDiffViewer({
  a,
  b,
  diff,
  nameA,
  nameB,
  queryA,
  queryB,
  defaultLook = "diagram",
  defaultSync = true,
  toolbarStart,
  toolbarActions,
  theme,
  className,
}: PlanDiffViewerProps) {
  const [pickA, setPickA] = useState<number | null>(null);
  const [pickB, setPickB] = useState<number | null>(null);
  const [active, setActive] = useState<number | null>(null);
  const [tab, setTab] = useState<DiffTab>("changes");
  const [look, setLook] = useState<PlanDiffLook>(defaultLook);
  const [sync, setSync] = useState(defaultSync);
  const [zoom, setZoom] = useState(0.8);
  const sideA = useRef<HTMLDivElement>(null);
  const sideB = useRef<HTMLDivElement>(null);

  const labelA = nameA ?? t("ui.plan_compare.plan_a");
  const labelB = nameB ?? t("ui.plan_compare.plan_b");
  const names: [string, string] = labelA === labelB ? [t("ui.plan_compare.plan_a"), t("ui.plan_compare.plan_b")] : [labelA, labelB];

  useEffect(() => {
    setActive(null);
    setPickA(null);
    setPickB(null);
  }, [a, b, diff]);

  const view = useMemo(() => {
    const marksA = new Map<number, Mark>();
    const marksB = new Map<number, Mark>();
    const tintsA = new Map<number, number>();
    const tintsB = new Map<number, number>();
    const aToB = new Map<number, number>();
    const bToA = new Map<number, number>();
    diff.matches.forEach((m) => {
      aToB.set(m.a, m.b);
      bToA.set(m.b, m.a);
    });
    diff.regions.forEach((r, i) => {
      r.a.forEach((id) => tintsA.set(id, i % 6));
      r.b.forEach((id) => tintsB.set(id, i % 6));
    });
    diff.changes.forEach((c) => {
      if (c.kind === "access" || c.kind === "operator") {
        if (c.a != null) marksA.set(c.a, "changed");
        if (c.b != null) marksB.set(c.b, "changed");
      }
    });
    diff.only_a.forEach((id) => marksA.set(id, "removed"));
    diff.only_b.forEach((id) => marksB.set(id, "added"));
    return { marksA, marksB, tintsA, tintsB, aToB, bToA };
  }, [diff]);

  useEffect(() => {
    const reduce = typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    const show = (side: HTMLDivElement | null, id: number | null) => {
      if (id == null || !side) return;
      requestAnimationFrame(() =>
        side.querySelector(`[data-node="${id}"]`)?.scrollIntoView({ block: "nearest", inline: "center", behavior: reduce ? "auto" : "smooth" }),
      );
    };
    show(sideA.current, pickA);
    show(sideB.current, pickB);
  }, [pickA, pickB]);

  useEffect(() => {
    if (!sync || look !== "diagram") return;
    const boxes = [sideA.current, sideB.current].map((s) => s?.querySelector<HTMLElement>(".diagram-scroll") ?? null);
    if (!boxes[0] || !boxes[1]) return;
    let mirroring = false;
    const handlers = boxes.map((from, i) => () => {
      const to = boxes[1 - i];
      if (!from || !to || mirroring) return;
      mirroring = true;
      const fx = from.scrollWidth - from.clientWidth;
      const fy = from.scrollHeight - from.clientHeight;
      to.scrollLeft = fx > 0 ? (from.scrollLeft / fx) * (to.scrollWidth - to.clientWidth) : 0;
      to.scrollTop = fy > 0 ? (from.scrollTop / fy) * (to.scrollHeight - to.clientHeight) : 0;
      requestAnimationFrame(() => (mirroring = false));
    });
    boxes.forEach((box, i) => box?.addEventListener("scroll", handlers[i]!, { passive: true }));
    return () => boxes.forEach((box, i) => box?.removeEventListener("scroll", handlers[i]!));
  }, [sync, look, diff, a, b]);

  const pickChange = (i: number) => {
    const c = diff.changes[i];
    if (!c) return;
    setActive(i);
    setPickA(c.a ?? (c.b != null ? view.bToA.get(c.b) ?? null : null));
    setPickB(c.b ?? (c.a != null ? view.aToB.get(c.a) ?? null : null));
  };

  const nodeA = pickA != null ? nodesOf(a).find((n) => n.id === pickA) ?? null : null;
  const nodeB = pickB != null ? nodesOf(b).find((n) => n.id === pickB) ?? null : null;
  const v = verdict(diff);

  return (
    <div className={`culprit-pv pv-diff cmp ${className ?? ""}`} data-theme={theme && theme !== "system" ? theme : undefined}>
      <div className="compare-pickers">
        {toolbarStart ?? (
          <div className="pv-meta">
            <span className="side-badge">A</span> <b>{names[0]}</b>
            <span className="muted">vs</span>
            <span className="side-badge">B</span> <b>{names[1]}</b>
          </div>
        )}
        <span className="spacer" />
        <span className="seg" role="group" aria-label={t("ui.plan_compare.plan_view")}>
          <button type="button" className={look === "diagram" ? "on" : ""} onClick={() => setLook("diagram")}>
            {t("ui.plan_compare.diagram")}
          </button>
          <button type="button" className={look === "tree" ? "on" : ""} onClick={() => setLook("tree")}>
            {t("ui.plan_compare.tree")}
          </button>
        </span>
        {look === "diagram" && (
          <button
            type="button"
            className={`btn ghost cmp-sync ${sync ? "on" : ""}`}
            aria-pressed={sync}
            title={sync ? t("ui.plan_compare.sync_on") : t("ui.plan_compare.sync_off")}
            onClick={() => setSync(!sync)}
          >
            {sync ? <Link2 size={13} /> : <Unlink2 size={13} />} {t("ui.plan_compare.sync")}
          </button>
        )}
        {toolbarActions}
      </div>

      <section className="cmp-verdict" aria-live="polite">
        <p className="cmp-lead">
          <span className={`cmp-pill ${v.tone}`}>{v.text}</span>
          {diff.headlines[0]}
        </p>
        {diff.headlines.length > 1 && (
          <ul className="cmp-more">
            {diff.headlines.slice(1).map((h, i) => (
              <li key={i}>{h}</li>
            ))}
          </ul>
        )}
        <div className="cmp-figures">
          <Figure label={t("ui.plan_compare.figure.cost", { plan: names[0] })} value={diff.cost_a == null ? t("ui.plan_compare.unavailable") : formatCost(diff.cost_a)} />
          <Figure label={t("ui.plan_compare.figure.cost", { plan: names[1] })} value={diff.cost_b == null ? t("ui.plan_compare.unavailable") : formatCost(diff.cost_b)} tone={v.tone} />
          {diff.time_a_ms != null && diff.time_b_ms != null && (
            <>
              <Figure label={t("ui.plan_compare.figure.time", { plan: names[0] })} value={duration(diff.time_a_ms)} />
              <Figure label={t("ui.plan_compare.figure.time", { plan: names[1] })} value={duration(diff.time_b_ms)} tone={v.tone} />
            </>
          )}
          <Figure label={t("ui.plan_compare.figure.operators")} value={`${nodesOf(a).length} → ${nodesOf(b).length}`} />
          <Figure label={t("ui.plan_compare.figure.regions")} value={String(diff.regions.length)} />
        </div>
      </section>

      <section className="cmp-panel">
        <div className="cmp-tabs" role="tablist" aria-label={t("ui.plan_compare.details")} onKeyDown={tabListKeys}>
          <TabButton id="changes" tab={tab} onPick={setTab} label={t("ui.plan_compare.tab.changes")} count={diff.changes.length} />
          <TabButton id="why" tab={tab} onPick={setTab} label={t("ui.plan_compare.tab.why")} count={diff.scenarios.length} />
          <TabButton id="props" tab={tab} onPick={setTab} label={t("ui.plan_compare.tab.props")} />
        </div>
        <div className="cmp-body" role="tabpanel">
          {tab === "changes" &&
            (diff.changes.length === 0 ? (
              <div className="cmp-empty">
                <CircleCheck size={18} />
                <div>
                  <b>{diff.same_shape ? t("ui.plan_compare.same_steps") : t("ui.plan_compare.minor_steps")}</b>
                  <div className="muted">{t("ui.plan_compare.same_steps_detail")}</div>
                </div>
              </div>
            ) : (
              <ul className="cmp-changes">
                {diff.changes.map((c, i) => (
                  <li key={i}>
                    <button type="button" className={`cmp-change ${active === i ? "on" : ""}`} onClick={() => pickChange(i)}>
                      <span className={`cmp-tag ${CHANGE_TONE[c.kind]}`}>{changeLabel(c.kind)}</span>
                      <span className="cmp-change-text">
                        <span className="cmp-title">{c.title}</span>
                        {c.detail && <span className="cmp-detail">{c.detail}</span>}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            ))}
          {tab === "why" &&
            (diff.scenarios.length === 0 ? (
              <div className="cmp-empty">
                <CircleCheck size={18} />
                <div>
                  <b>{t("ui.plan_compare.same_conditions")}</b>
                  <div className="muted">{t("ui.plan_compare.same_conditions_detail")}</div>
                </div>
              </div>
            ) : (
              <ul className="cmp-scenarios">
                {diff.scenarios.map((s, i) => (
                  <li key={i} className="cmp-scenario">
                    <span className="cmp-scenario-icon" aria-hidden>
                      {SCENARIO_ICON[s.kind]}
                    </span>
                    <div>
                      <div className="cmp-scenario-kind">{scenarioLabel(s.kind)}</div>
                      <div className="cmp-title">{s.title}</div>
                      {s.detail && <div className="cmp-detail wrap">{s.detail}</div>}
                      {(s.a != null || s.b != null) && (
                        <button
                          type="button"
                          className="link"
                          onClick={() => {
                            setPickA(s.a ?? (s.b != null ? view.bToA.get(s.b) ?? null : null));
                            setPickB(s.b ?? (s.a != null ? view.aToB.get(s.a) ?? null : null));
                            setTab("props");
                          }}
                        >
                          {t("ui.plan_compare.show_step")}
                        </button>
                      )}
                    </div>
                  </li>
                ))}
              </ul>
            ))}
          {tab === "props" && <PropsCompare a={nodeA} b={nodeB} planA={a} planB={b} names={names} />}
        </div>
      </section>

      <div className="cmp-legend" aria-hidden>
        <span className="cmp-key">
          <i className="tint" /> {t("ui.plan_compare.legend.same")}
        </span>
        <span className="cmp-key">
          <i className="changed" /> {t("ui.plan_compare.legend.changed")}
        </span>
        <span className="cmp-key">
          <i className="only" /> {t("ui.plan_compare.legend.only")}
        </span>
      </div>

      <div className="compare-sides">
        <Side label="A" name={names[0]} plan={a} innerRef={sideA}>
          {look === "diagram" ? (
            <PlanDiagram
              plan={a}
              query={queryA}
              marks={view.marksA}
              tints={view.tintsA}
              selectedId={pickA}
              initialZoom={0.8}
              zoom={sync ? zoom : undefined}
              onZoomChange={setZoom}
              showProps={false}
              onSelect={(id) => {
                setActive(null);
                setPickA(id);
                setPickB(view.aToB.get(id) ?? null);
                setTab("props");
              }}
            />
          ) : (
            <PlanView plan={a} query={queryA} selectedId={pickA} />
          )}
        </Side>
        <Side label="B" name={names[1]} plan={b} innerRef={sideB}>
          {look === "diagram" ? (
            <PlanDiagram
              plan={b}
              query={queryB}
              marks={view.marksB}
              tints={view.tintsB}
              selectedId={pickB}
              initialZoom={0.8}
              zoom={sync ? zoom : undefined}
              onZoomChange={setZoom}
              showProps={false}
              onSelect={(id) => {
                setActive(null);
                setPickB(id);
                setPickA(view.bToA.get(id) ?? null);
                setTab("props");
              }}
            />
          ) : (
            <PlanView plan={b} query={queryB} selectedId={pickB} />
          )}
        </Side>
      </div>
    </div>
  );
}

function TabButton({ id, tab, onPick, label, count: n }: { id: DiffTab; tab: DiffTab; onPick: (t: DiffTab) => void; label: string; count?: number }) {
  return (
    <button type="button" role="tab" aria-selected={tab === id} tabIndex={tab === id ? 0 : -1} className={`cmp-tab ${tab === id ? "on" : ""}`} onClick={() => onPick(id)}>
      {label}
      {n != null && <span className="cmp-count">{n}</span>}
    </button>
  );
}

function Figure({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className={`cmp-figure ${tone === "worse" || tone === "better" ? tone : ""}`}>
      <div className="value">{value}</div>
      <div className="label" title={label}>
        {label}
      </div>
    </div>
  );
}

function Side({ label, name, plan, innerRef, children }: { label: string; name: string; plan: Plan; innerRef: RefObject<HTMLDivElement | null>; children: ReactNode }) {
  return (
    <div className="compare-side" ref={innerRef}>
      <div className="compare-side-head">
        <span className="side-badge">{label}</span>
        <b>{name}</b>
        <span className="muted">{t("ui.plan_compare.side_meta", { engine: planEngineName(plan), kind: plan.kind })}</span>
      </div>
      {children}
    </div>
  );
}

function norm(value: string): string {
  return value.replace(/\[[^\]]+\]\.(?=\[[^\]]+\]\.\[[^\]]+\]\.\[[^\]]+\])/g, "").replace(/\s+/g, " ").trim();
}

type PropRow = { label: string; a: string | null; b: string | null };
type PropKey =
  | "operator"
  | "table"
  | "index"
  | "seek"
  | "filter"
  | "est_rows"
  | "est_executions"
  | "act_rows"
  | "rows_read"
  | "table_rows"
  | "act_time"
  | "cost_share"
  | "subtree_cost"
  | "warnings";

function rowsFor(node: PlanNode | null, plan: Plan): Partial<Record<PropKey, string | null>> {
  if (!node) return {};
  const est = estRows(plan, node);
  const weighed = canPlanEstimates(plan.engine) || plan.kind === "actual";
  return {
    operator: node.op,
    table: node.object,
    index: node.index,
    seek: node.seek_predicate ? norm(node.seek_predicate) : null,
    filter: node.predicate ? norm(node.predicate) : null,
    est_rows: weighed && est != null ? count(est) : null,
    est_executions: weighed && est != null ? count(node.est_executions) : null,
    act_rows: node.act_rows != null ? count(node.act_rows) : null,
    rows_read: node.rows_read != null ? count(node.rows_read) : null,
    table_rows: node.table_rows != null ? count(node.table_rows) : null,
    act_time: node.act_time_ms != null ? duration(node.act_time_ms) : null,
    cost_share: weighed ? percent(stepShare(plan, node)) : null,
    subtree_cost: weighed && !plan.facts.estimated_cost_unavailable ? formatCost(node.subtree_cost) : null,
    warnings: node.warnings.length ? node.warnings.map((w) => w.replace(/-/g, " ")).join(", ") : null,
  };
}

function propLabel(prop: PropKey): string {
  switch (prop) {
    case "operator":
      return t("ui.plan_compare.prop.operator");
    case "table":
      return t("ui.plan_compare.prop.table");
    case "index":
      return t("ui.plan_compare.prop.index");
    case "seek":
      return t("ui.plan_compare.prop.seek");
    case "filter":
      return t("ui.plan_compare.prop.filter");
    case "est_rows":
      return t("ui.plan_compare.prop.est_rows");
    case "est_executions":
      return t("ui.plan_compare.prop.est_executions");
    case "act_rows":
      return t("ui.plan_compare.prop.act_rows");
    case "rows_read":
      return t("ui.plan_compare.prop.rows_read");
    case "table_rows":
      return t("ui.plan_compare.prop.table_rows");
    case "act_time":
      return t("ui.plan_compare.prop.act_time");
    case "cost_share":
      return t("ui.plan_compare.prop.cost_share");
    case "subtree_cost":
      return t("ui.plan_compare.prop.subtree_cost");
    case "warnings":
      return t("ui.plan_compare.prop.warnings");
  }
}

const GROUPS: { group: "does" | "rows" | "cost" | "warnings"; props: PropKey[] }[] = [
  { group: "does", props: ["operator", "table", "index", "seek", "filter"] },
  { group: "rows", props: ["est_rows", "est_executions", "act_rows", "rows_read", "table_rows"] },
  { group: "cost", props: ["cost_share", "subtree_cost", "act_time"] },
  { group: "warnings", props: ["warnings"] },
];

function groupTitle(group: "does" | "rows" | "cost" | "warnings"): string {
  switch (group) {
    case "does":
      return t("ui.plan_compare.group.does");
    case "rows":
      return t("ui.plan_compare.group.rows");
    case "cost":
      return t("ui.plan_compare.group.cost");
    case "warnings":
      return t("ui.plan_compare.group.warnings");
  }
}

export function PropsCompare({
  a,
  b,
  planA,
  planB,
  names,
}: {
  a: PlanNode | null;
  b: PlanNode | null;
  planA: Plan;
  planB: Plan;
  names: [string, string];
}) {
  const [showSame, setShowSame] = useState(false);
  if (!a && !b) {
    return (
      <div className="cmp-empty">
        <EqualNot size={18} />
        <div>
          <b>{t("ui.plan_compare.select_step")}</b>
          <div className="muted">{t("ui.plan_compare.select_step_detail")}</div>
        </div>
      </div>
    );
  }
  const ra = rowsFor(a, planA);
  const rb = rowsFor(b, planB);
  const label = (n: PlanNode | null) => (n ? `${n.op}${n.object ? ` · ${n.object.split(".").pop()}` : ""}` : "—");
  const groups = GROUPS.map((g) => ({
    title: groupTitle(g.group),
    rows: g.props
      .map((k): PropRow => ({ label: propLabel(k), a: ra[k] ?? null, b: rb[k] ?? null }))
      .filter((r) => r.a != null || r.b != null),
  })).filter((g) => g.rows.length > 0);
  const keys = [...new Set([...Object.keys(a?.props ?? {}), ...Object.keys(b?.props ?? {})])].sort();
  const other: PropRow[] = keys.map((k) => ({
    label: k,
    a: a?.props[k] != null ? norm(a.props[k]!) : null,
    b: b?.props[k] != null ? norm(b.props[k]!) : null,
  }));
  const differs = (r: PropRow) => (r.a ?? "") !== (r.b ?? "");
  const changed = other.filter(differs);
  const same = other.filter((r) => !differs(r));

  const line = (r: PropRow) => (
    <div key={r.label} className={`cmp-prop ${differs(r) ? "diff" : ""}`}>
      <span className="cmp-prop-label" title={r.label}>
        {r.label}
      </span>
      <span className={`cmp-prop-value ${r.a == null ? "none" : ""}`}>{r.a ?? "—"}</span>
      <span className="cmp-prop-sign" role="img" aria-label={differs(r) ? t("ui.plan_compare.differs") : t("ui.plan_compare.same")}>
        {differs(r) ? "≠" : ""}
      </span>
      <span className={`cmp-prop-value ${r.b == null ? "none" : ""}`}>{r.b ?? "—"}</span>
    </div>
  );

  return (
    <div className="cmp-props">
      <div className="cmp-prop head">
        <span />
        <span>
          <span className="side-badge">A</span> {label(a)}
          {!a && <span className="muted"> · {t("ui.plan_compare.only_in", { plan: names[1] })}</span>}
        </span>
        <span />
        <span>
          <span className="side-badge">B</span> {label(b)}
          {!b && <span className="muted"> · {t("ui.plan_compare.only_in", { plan: names[0] })}</span>}
        </span>
      </div>
      {groups.map((g) => (
        <div key={g.title} className="cmp-prop-group">
          <div className="cmp-prop-title">{g.title}</div>
          {g.rows.map(line)}
        </div>
      ))}
      {other.length > 0 && (
        <div className="cmp-prop-group">
          <div className="cmp-prop-title">
            {t("ui.plan_compare.engine_props")} <span className="muted">· {t("ui.plan_compare.differ_count", { n: changed.length, count: String(changed.length) })}</span>
          </div>
          {changed.map(line)}
          {same.length > 0 && (
            <button type="button" className="link cmp-prop-more" onClick={() => setShowSame((v) => !v)}>
              {showSame
                ? t("ui.plan_compare.hide_identical", { count: String(same.length) })
                : t("ui.plan_compare.show_identical", { count: String(same.length) })}
            </button>
          )}
          {showSame && same.map(line)}
        </div>
      )}
    </div>
  );
}
