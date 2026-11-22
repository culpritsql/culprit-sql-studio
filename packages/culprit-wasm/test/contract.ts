import {
  init, parsePlan, diffPlans, parseDeadlock, exportPlan, ParserError,
  type CompareContext, type Deadlock, type Engine, type CoreError,
  type ExportFile, type ExportOptions, type Plan, type PlanDiff,
} from "../pkg/index.js";

// Type-only consumer compiled with NodeNext. No app, Tauri, provider or generated model copy is required at runtime.
export async function portableContract(source: string, engine: Engine): Promise<ExportFile> {
  await init();
  const plan: Plan = parsePlan(source, engine);
  const context: CompareContext = { statistics: [], taken_at: null };
  const diff: PlanDiff = diffPlans(plan, "before", plan, "after", context);
  const deadlocks: Deadlock[] = parseDeadlock(source);
  const options: ExportOptions = { redact: true, notes: [] };
  void diff; void deadlocks;
  try {
    return exportPlan(plan, "portable", "markdown", options);
  } catch (error) {
    if (error instanceof ParserError) {
      const original: CoreError = error.error;
      void original.code; void original.hint; void original.detail;
    }
    throw error;
  }
}
