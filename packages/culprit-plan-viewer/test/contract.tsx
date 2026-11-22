import type { ReactElement } from "react";
import {
  DeadlockGraph,
  DeadlockViewer,
  PlanDiagram,
  PlanDiffViewer,
  PlanInsights,
  PlanOperations,
  PlanSurface,
  PlanView,
  PlanViewer,
  type Deadlock,
  type Plan,
  type PlanDiff,
} from "../pkg/index.js";

// Type-only NodeNext consumer contract check: verifies closed generated declarations without app/ or Tauri.
export function verifyViewerContract(plan: Plan, diff: PlanDiff, deadlock: Deadlock): ReactElement[] {
  return [
    <PlanViewer key="pv" plan={plan} defaultLook="diagram" theme="dark" />,
    <PlanDiffViewer key="pdv" a={plan} b={plan} diff={diff} nameA="Before" nameB="After" />,
    <DeadlockViewer key="dv" deadlock={deadlock} />,
    <PlanSurface key="ps" plan={plan} look="tree" />,
    <PlanDiagram key="pd" plan={plan} />,
    <PlanView key="pt" plan={plan} />,
    <PlanOperations key="po" plan={plan} selectedId={null} onSelect={() => {}} />,
    <PlanInsights key="pi" plan={plan} onSelectNode={() => {}} />,
    <DeadlockGraph key="dg" deadlock={deadlock} />,
  ];
}
