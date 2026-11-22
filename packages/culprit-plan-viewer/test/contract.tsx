import type { ReactElement } from "react";
import {
  DeadlockGraph,
  DeadlockViewer,
  NodeDetails,
  PlanCanvas,
  PlanDiagram,
  PlanDiffViewer,
  PlanInsights,
  PlanObjects,
  PlanOperations,
  PlanRaw,
  PlanSurface,
  PlanTree,
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
    <PlanCanvas key="pc" plan={plan} lens="time" showInsights={false} height={480} theme="dark" />,
    <PlanDiffViewer key="pdv" a={plan} b={plan} diff={diff} nameA="Before" nameB="After" />,
    <DeadlockViewer key="dv" deadlock={deadlock} />,
    <PlanSurface key="ps" plan={plan} look="objects" />,
    <PlanDiagram key="pd" plan={plan} showInsights={false} showProps={false} />,
    <PlanTree key="ptree" plan={plan} showHeader={false} showActualSummary={false} showProps={false} showInsights={false} />,
    <PlanView key="pt" plan={plan} />,
    <PlanOperations key="po" plan={plan} selectedId={null} onSelect={() => {}} />,
    <PlanObjects key="pobj" plan={plan} selectedId={null} onSelect={() => {}} />,
    <PlanRaw key="praw" plan={plan} />,
    <NodeDetails key="nd" plan={plan} node={plan.root} share={1} theme="dark" />,
    <PlanInsights key="pi" plan={plan} onSelectNode={() => {}} />,
    <DeadlockGraph key="dg" deadlock={deadlock} showLegend={false} theme="dark" />,
  ];
}
