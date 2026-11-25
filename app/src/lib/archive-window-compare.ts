import type { DerivedFinding } from "../gen/DerivedFinding";
import type { WarehouseWindowReport } from "../gen/WarehouseWindowReport";

export type ArchivedQueryRegression = {
  queryId: string;
  text: string;
  baselineMeanMs: number;
  currentMeanMs: number;
  ratio: number;
  currentExecutions: number;
};

export type ArchivedWindowDiff = {
  newFindings: DerivedFinding[];
  resolvedFindings: DerivedFinding[];
  slowerQueries: ArchivedQueryRegression[];
};

function collectActiveFindings(report: WarehouseWindowReport): Map<string, DerivedFinding> {
  const map = new Map<string, DerivedFinding>();
  for (const analysis of report.published) {
    for (const row of analysis.findings) {
      if (!row.finding.finding.inactive) {
        map.set(row.finding.finding.id, row.finding);
      }
    }
  }
  return map;
}

function sameResource(a: WarehouseWindowReport, b: WarehouseWindowReport): boolean {
  return (
    a.request.resource.namespace_id === b.request.resource.namespace_id &&
    a.request.resource.database === b.request.resource.database
  );
}

/** Pure comparison of two authenticated window reports for the same database resource. */
export function compareArchivedWindows(
  baseline: WarehouseWindowReport,
  current: WarehouseWindowReport,
): ArchivedWindowDiff | null {
  if (!sameResource(baseline, current)) return null;

  const baseFindings = collectActiveFindings(baseline);
  const currFindings = collectActiveFindings(current);

  const newFindings: DerivedFinding[] = [];
  for (const [id, finding] of currFindings) {
    if (!baseFindings.has(id)) newFindings.push(finding);
  }

  const resolvedFindings: DerivedFinding[] = [];
  for (const [id, finding] of baseFindings) {
    if (!currFindings.has(id)) resolvedFindings.push(finding);
  }

  const slowerQueries: ArchivedQueryRegression[] = [];
  if (baseline.workload.kind === "evaluated" && current.workload.kind === "evaluated") {
    const baseQueries = new Map<string, { mean: number; runs: number }>();
    for (const section of baseline.workload.analysis.sections) {
      for (const q of section.analysis.workload?.queries ?? []) {
        if (q.executions >= 5 && q.mean_time_ms > 0) {
          baseQueries.set(q.id, { mean: q.mean_time_ms, runs: q.executions });
        }
      }
    }
    const seen = new Set<string>();
    for (const section of current.workload.analysis.sections) {
      for (const q of section.analysis.workload?.queries ?? []) {
        if (seen.has(q.id) || q.executions < 5 || q.mean_time_ms <= 0) continue;
        const prior = baseQueries.get(q.id);
        if (prior && q.mean_time_ms >= prior.mean * 1.25) {
          seen.add(q.id);
          slowerQueries.push({
            queryId: q.id,
            text: q.text,
            baselineMeanMs: prior.mean,
            currentMeanMs: q.mean_time_ms,
            ratio: q.mean_time_ms / prior.mean,
            currentExecutions: q.executions,
          });
        }
      }
    }
    slowerQueries.sort((a, b) => b.currentMeanMs * b.currentExecutions - a.currentMeanMs * a.currentExecutions);
  }

  return { newFindings, resolvedFindings, slowerQueries };
}
