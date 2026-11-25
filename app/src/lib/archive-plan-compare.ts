import type { Plan } from "../gen/Plan";
import type { WarehouseArtifactReport } from "../gen/WarehouseArtifactReport";

export type ArchivedComparablePlan = {
  key: string;
  label: string;
  sourceLabel: string;
  plan: Plan;
};

export type ExtractedArchivePlans = {
  complete: ArchivedComparablePlan[];
  refusedSlices: number;
};

/** Extract only complete, authenticated plan artifacts. Text slices and metadata stubs are never promoted to plans. */
export function extractComparablePlans(report: WarehouseArtifactReport): ExtractedArchivePlans {
  const complete: ArchivedComparablePlan[] = [];
  let refusedSlices = 0;
  for (const record of report.records) {
    const scopeLabel = record.scope.kind === "shared" ? record.scope.member_id.slice(0, 12) : record.scope.label;
    for (const artifact of record.artifacts) {
      if (artifact.payload.kind === "complete") {
        if (artifact.payload.payload.state === "available" && artifact.plan != null) {
          complete.push({
            key: `${record.reference.capture.observation_id}:${record.reference.batch_id}:${record.reference.row_ordinal}:${artifact.location}`,
            label: artifact.label,
            sourceLabel: `${scopeLabel} · ${record.collected_until}`,
            plan: artifact.plan,
          });
        }
      } else if (artifact.kind === "plan" || artifact.plan != null) {
        refusedSlices += 1;
      }
    }
  }
  return { complete, refusedSlices };
}
