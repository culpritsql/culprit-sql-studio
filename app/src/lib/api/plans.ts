import type { ExplainMode } from "../../gen/ExplainMode";
import type { ExportFile } from "../../gen/ExportFile";
import type { ExportFormat } from "../../gen/ExportFormat";
import type { ExportOptions } from "../../gen/ExportOptions";
import type { Plan } from "../../gen/Plan";
import type { QueryProgress } from "../../gen/QueryProgress";
import type { PlanDiff } from "../../gen/PlanDiff";
import type { Invoke } from "../owned-requests";

export interface PlansApi {
  explain(name: string, sql: string, mode: ExplainMode, confirmed: boolean): Promise<Plan>;
  queryPlan(name: string, queryId: string, planId: string | null, reportId?: string): Promise<Plan>;
  /** The actual plan of the query's last run as the engine kept it (SQL Server 2019+); never runs the query. */
  lastActualPlan(name: string, queryId: string, planId: string | null, reportId?: string): Promise<Plan>;
  /** How far along a session's running statement is; `withPlan` asks for the plan too. */
  queryProgress(name: string, session: string, withPlan: boolean): Promise<QueryProgress>;
  parsePlan(text: string): Promise<Plan>;
  /** `connection`: where the plans came from, so scenarios can use its latest scan's statistics. */
  diffPlans(a: Plan, b: Plan, nameA: string, nameB: string, connection?: string | null): Promise<PlanDiff>;
  exportPlan(plan: Plan, title: string, format: ExportFormat, options: ExportOptions): Promise<ExportFile>;
  exportDiff(a: Plan, b: Plan, nameA: string, nameB: string, format: ExportFormat, options: ExportOptions, connection?: string | null): Promise<ExportFile>;
  /** A plan of a query from the latest scan, with its original text when the engine keeps it (.sqlplan). */
  exportQueryPlan(name: string, reportId: string, queryId: string, planId: string | null, format: ExportFormat, options: ExportOptions): Promise<ExportFile>;
}

export function desktopPlans(invoke: Invoke): PlansApi {
  return {
    explain: (name, sql, mode, confirmed) => invoke("explain", { name, sql, mode, confirmed }),
    queryPlan: (name, queryId, planId, reportId) => invoke("query_plan", { name, queryId, planId, reportId }),
    lastActualPlan: (name, queryId, planId, reportId) => invoke("last_actual_plan", { name, queryId, planId, reportId }),
    queryProgress: (name, session, withPlan) => invoke("query_progress", { name, session, withPlan }),
    parsePlan: (text) => invoke("parse_plan", { text }),
    diffPlans: (a, b, nameA, nameB, connection) =>
      invoke("diff_plans", { a: { ...a, raw: null }, b: { ...b, raw: null }, nameA, nameB, connection: connection ?? null }),
    exportPlan: (plan, title, format, options) => invoke("export_plan", { plan: format === "native" ? plan : { ...plan, raw: null }, title, format, options }),
    exportDiff: (a, b, nameA, nameB, format, options, connection) =>
      invoke("export_diff", { request: { a: { ...a, raw: null }, b: { ...b, raw: null }, nameA, nameB, format, options, connection: connection ?? null } }),
    exportQueryPlan: (name, reportId, queryId, planId, format, options) => invoke("export_query_plan", { name, reportId, queryId, planId, format, options }),
  };
}
