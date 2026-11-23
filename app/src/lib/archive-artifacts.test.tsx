import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { artifactRequest, matchesArtifactRequest } from "./archive-artifacts";
import { ArchiveArtifacts } from "../views/archives/ArchiveArtifacts";
import { warehouseArtifactCase } from "../test/warehouse-artifacts";
import { warehouseIncidentCase } from "../test/warehouse-incidents";
const resource = { namespace_id: "a".repeat(64), database: "b".repeat(64) };
function selection() { const query = warehouseIncidentCase().report.incidents[0]!.affected_queries[0]!; return { references: query.evidence, sql: query.sql }; }
test("artifact selection preserves original roles and makes complete recovery authorization explicit without a native route", () => {
  const query = artifactRequest(resource, selection()); expect(query.references).toEqual(selection().references);
  expect(query.full_payloads).toBe(true); expect(query.text).toEqual({ kind: "complete" });
  const payload = selection().sql;
  expect(query.expected_sql_sha256).toBe(payload.state === "available" ? payload.sha256 : null);
  expect(() => artifactRequest(resource, { references: [], sql: null })).toThrow();
});
test("a cloned reordered IPC query matches but another namespace native ref or payload mode cannot replace the selection", () => {
  const request = artifactRequest(resource, selection()), report = warehouseArtifactCase(request);
  report.query = { expected_sql_sha256: request.expected_sql_sha256, text: request.text, references: structuredClone(request.references), full_payloads: request.full_payloads, resource: { database: resource.database, namespace_id: resource.namespace_id } };
  expect(matchesArtifactRequest(report, request)).toBe(true);
  report.query.resource.namespace_id = "c".repeat(64); expect(matchesArtifactRequest(report, request)).toBe(false);
  report.query = structuredClone(request); report.query.references[0]!.row_ordinal++; expect(matchesArtifactRequest(report, request)).toBe(false);
  report.query = structuredClone(request); report.query.text = { kind: "inventory", chunk_chars: 128 }; expect(matchesArtifactRequest(report, request)).toBe(false);
});
test("full original SQL remains complete escaped text with concise expandable evidence and no private locator display", () => {
  const report = warehouseArtifactCase(artifactRequest(resource, selection()));
  const body = report.records[0]!.artifacts[0]!.payload;
  if (body.kind !== "complete" || body.payload.state !== "available") throw new Error("Expected complete presentation payload");
  const html = renderToStaticMarkup(createElement(ArchiveArtifacts, { value: report }));
  expect(html).toContain("COMPLETE_RETAINED_BODY"); expect(html).toContain("&lt;script&gt;retained-data&lt;/script&gt;");
  expect(html).toContain(body.payload.text.slice(-80).replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
  expect(html).not.toContain("<script>"); expect(html).not.toContain(report.records[0]!.reference.manifest_sha256);
  expect(html).toContain("<details>"); expect(html).not.toContain("/record/data/statement");
});
