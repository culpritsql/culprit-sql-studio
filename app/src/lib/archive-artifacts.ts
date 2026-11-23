import type { DiagnosticEvidenceRef } from "../gen/DiagnosticEvidenceRef";
import type { DefinitionPayload } from "../gen/DefinitionPayload";
import type { PublishedResourceSelector } from "../gen/PublishedResourceSelector";
import type { WarehouseArtifactQuery } from "../gen/WarehouseArtifactQuery";
import type { WarehouseArtifactReport } from "../gen/WarehouseArtifactReport";
import { t } from "./i18n";
// Same named Core MAX_WAREHOUSE_ARTIFACT_REFERENCES admission; the service remains authoritative.
const MAX_ORIGINAL_REFERENCES = 16;
export type ArtifactSelection = { references: DiagnosticEvidenceRef[]; sql: DefinitionPayload | null };
export type InspectArtifacts = (selection: ArtifactSelection) => void;
/** UI staleness only. The service reopens the exact immutable originals and source-owned payload inventory. */
function sameData(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (left == null || right == null || typeof left !== "object" || typeof right !== "object") return false;
  if (Array.isArray(left) || Array.isArray(right)) return Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((value, index) => sameData(value, right[index]));
  const a = left as Record<string, unknown>, b = right as Record<string, unknown>, keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((key) => Object.hasOwn(b, key) && sameData(a[key], b[key]));
}
export function artifactRequest(resource: PublishedResourceSelector, selection: ArtifactSelection): WarehouseArtifactQuery {
  const references = selection.references.filter((value, index, all) => all.findIndex((other) => sameData(value, other)) === index);
  if (references.length === 0 || references.length > MAX_ORIGINAL_REFERENCES) throw new Error(t("artifact.ui.selection_unavailable"));
  return { resource, references, full_payloads: true, text: { kind: "complete" }, expected_sql_sha256: selection.sql?.state === "available" ? selection.sql.sha256 : null };
}
export function matchesArtifactRequest(report: WarehouseArtifactReport, request: WarehouseArtifactQuery): boolean {
  return sameData(report.query, request);
}
