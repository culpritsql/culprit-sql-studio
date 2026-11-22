import { Database, KeyRound } from "lucide-react";
import { useMemo } from "react";
import { allNodes, canPlanEstimates, insightsByNode, percent, planShares, rows, rowsOut, t } from "./host.js";
import type { Plan, PlanNode, QueryStat, Severity } from "./types.js";

type TableRollup = {
  object: string;
  shortName: string;
  schema: string | null;
  operators: PlanNode[];
  share: number;
  hasShare: boolean;
  scans: number;
  seeks: number;
  lookups: number;
  indexes: string[];
  directTableScan: boolean;
  rowsProduced: number | null;
  reads: number | null;
  hitRatio: number | null;
  issues: number;
  worst: Severity | null;
};

const SEV_RANK: Record<Severity, number> = { high: 3, medium: 2, low: 1 };

export function PlanObjects({
  plan,
  query,
  selectedId,
  onSelect,
}: {
  plan: Plan;
  query?: QueryStat | null;
  selectedId: number | null;
  onSelect: (id: number) => void;
}) {
  const shares = useMemo(() => planShares(plan, query), [plan, query]);
  const flagged = useMemo(() => insightsByNode(plan), [plan]);
  const weighed = canPlanEstimates(plan.engine) || plan.kind === "actual";

  const rollups = useMemo(() => {
    const byObject = new Map<string, PlanNode[]>();
    for (const node of allNodes(plan.root)) {
      if (!node.object) continue;
      const list = byObject.get(node.object) ?? [];
      list.push(node);
      byObject.set(node.object, list);
    }

    const items: TableRollup[] = [];
    for (const [object, nodes] of byObject) {
      const parts = object.split(".");
      const shortName = parts[parts.length - 1] ?? object;
      const schema = parts.length > 1 ? parts.slice(0, -1).join(".") : null;

      let share = 0;
      let hasShare = false;
      let scans = 0;
      let seeks = 0;
      let lookups = 0;
      let directTableScan = false;
      const indexSet = new Set<string>();
      let rowsProduced = 0;
      let hasRows = false;
      let logicalReads = 0;
      let hasLogical = false;
      let physicalReads = 0;
      let hasPhysical = false;
      let rowsRead = 0;
      let hasRowsRead = false;
      const seenInsightRules = new Set<string>();
      let warningCount = 0;
      let worst: Severity | null = null;

      const sortedOps = [...nodes].sort((a, b) => (shares.stated.get(b.id) ?? 0) - (shares.stated.get(a.id) ?? 0));

      for (const n of sortedOps) {
        const s = shares.stated.get(n.id);
        if (s != null) {
          share += s;
          hasShare = true;
        }
        if (n.kind === "scan") {
          scans++;
          if (!n.index) directTableScan = true;
        } else if (n.kind === "seek") {
          seeks++;
        } else if (n.kind === "lookup") {
          lookups++;
        }
        if (n.index) indexSet.add(n.index);

        const out = rowsOut(plan, n);
        if (out != null) {
          rowsProduced += out;
          hasRows = true;
        }
        if (n.runtime?.logical_reads != null) {
          logicalReads += n.runtime.logical_reads;
          hasLogical = true;
        }
        if (n.runtime?.physical_reads != null) {
          physicalReads += n.runtime.physical_reads;
          hasPhysical = true;
        }
        if (n.rows_read != null) {
          rowsRead += n.rows_read;
          hasRowsRead = true;
        }
        warningCount += n.warnings.length;
        if (n.warnings.length > 0 && !worst) worst = "low";
        for (const ins of flagged.get(n.id) ?? []) {
          seenInsightRules.add(`${ins.rule}:${ins.nodes.join(",")}`);
          if (!worst || SEV_RANK[ins.severity] > SEV_RANK[worst]) worst = ins.severity;
        }
      }

      const hitRatio =
        hasLogical && logicalReads > 0 && hasPhysical
          ? Math.max(0, Math.min(1, (logicalReads - physicalReads) / logicalReads))
          : null;

      items.push({
        object,
        shortName,
        schema,
        operators: sortedOps,
        share: Math.min(1, share),
        hasShare: weighed && hasShare,
        scans,
        seeks,
        lookups,
        indexes: [...indexSet],
        directTableScan,
        rowsProduced: hasRows ? rowsProduced : null,
        reads: hasLogical ? logicalReads : hasRowsRead ? rowsRead : null,
        hitRatio,
        issues: seenInsightRules.size + warningCount,
        worst,
      });
    }

    return items.sort((a, b) => b.share - a.share || (b.reads ?? 0) - (a.reads ?? 0) || a.object.localeCompare(b.object));
  }, [plan, shares, flagged, weighed]);

  if (rollups.length === 0) {
    return (
      <div className="culprit-pv ops">
        <div className="empty">{t("ui.plan_objects.none")}</div>
      </div>
    );
  }

  const hasReads = rollups.some((r) => r.reads != null);
  const hasHits = rollups.some((r) => r.hitRatio != null);
  const hasIssues = rollups.some((r) => r.issues > 0);

  return (
    <div className="culprit-pv ops plan-objects">
      <div className="ops-scroll">
        <table className="ops-table">
          <thead>
            <tr>
              <th>{t("ui.plan_objects.table")}</th>
              <th>{t("ui.plan_objects.access")}</th>
              <th>{t("ui.plan_objects.indexes")}</th>
              {weighed && <th className="r">{t("ui.plan_objects.share")}</th>}
              <th className="r">{t("ui.plan_objects.rows_out")}</th>
              {hasReads && <th className="r">{t("ui.plan_objects.reads")}</th>}
              {hasHits && <th className="r">{t("ui.plan_objects.cache_hit")}</th>}
              {hasIssues && <th className="r">{t("ui.plan_objects.issues")}</th>}
            </tr>
          </thead>
          <tbody>
            {rollups.map((item) => {
              const isSelected = item.operators.some((n) => n.id === selectedId);
              const accessParts: string[] = [];
              if (item.scans > 0) accessParts.push(t("ui.plan_objects.scans_count", { n: item.scans, count: String(item.scans) }));
              if (item.seeks > 0) accessParts.push(t("ui.plan_objects.seeks_count", { n: item.seeks, count: String(item.seeks) }));
              if (item.lookups > 0) accessParts.push(t("ui.plan_objects.lookups_count", { n: item.lookups, count: String(item.lookups) }));
              if (accessParts.length === 0) {
                accessParts.push(t("ui.plan_objects.ops_count", { n: item.operators.length, count: String(item.operators.length) }));
              }
              return (
                <tr
                  key={item.object}
                  className={isSelected ? "sel" : ""}
                  onClick={() => {
                    if (item.operators[0]) onSelect(item.operators[0].id);
                  }}
                >
                  <td>
                    <span className="ops-name">
                      <span className="ops-icon">
                        <Database size={13} />
                      </span>
                      <span className="ops-text">
                        <b className="mono">{item.shortName}</b>
                        {item.schema && <span className="mono muted">{item.schema}</span>}
                      </span>
                    </span>
                  </td>
                  <td>
                    <div className="po-access">
                      <span>{accessParts.join(" · ")}</span>
                      {item.operators.length > 1 && (
                        <span className="po-step-pills">
                          {item.operators.map((op) => (
                            <button
                              type="button"
                              key={op.id}
                              className={`po-step-pill ${op.id === selectedId ? "on" : ""}`}
                              onClick={(e) => {
                                e.stopPropagation();
                                onSelect(op.id);
                              }}
                            >
                              {op.op}
                            </button>
                          ))}
                        </span>
                      )}
                    </div>
                  </td>
                  <td>
                    <div className="po-indexes">
                      {item.indexes.map((ix) => (
                        <span key={ix} className="tag mono">
                          <KeyRound size={10} /> {ix}
                        </span>
                      ))}
                      {item.directTableScan && <span className="tag medium">{t("ui.plan_objects.direct_table")}</span>}
                    </div>
                  </td>
                  {weighed && (
                    <td className="r num">
                      {item.hasShare ? (
                        <span className="ops-cost">
                          <span className="ops-cost-bar">
                            <span style={{ width: `${Math.min(100, item.share * 100)}%` }} />
                          </span>
                          {percent(item.share)}
                        </span>
                      ) : (
                        <span className="muted">-</span>
                      )}
                    </td>
                  )}
                  <td className="r num">{item.rowsProduced != null ? rows(item.rowsProduced) : <span className="muted">-</span>}</td>
                  {hasReads && <td className="r num">{item.reads != null ? rows(item.reads) : <span className="muted">-</span>}</td>}
                  {hasHits && (
                    <td className="r num">
                      {item.hitRatio != null ? (
                        <span className={item.hitRatio < 0.8 ? "ops-err bad" : ""}>{percent(item.hitRatio)}</span>
                      ) : (
                        <span className="muted">-</span>
                      )}
                    </td>
                  )}
                  {hasIssues && (
                    <td className="r num">
                      {item.issues > 0 ? <span className={`ops-issues ${item.worst ?? ""}`}>{item.issues}</span> : <span className="muted">-</span>}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
