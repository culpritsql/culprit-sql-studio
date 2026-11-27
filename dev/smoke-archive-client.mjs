import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const scratch = mkdtempSync(join(tmpdir(), "culprit-archive-consumer-"));
try {
  const [tarball] = JSON.parse(execFileSync("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", scratch], {
    cwd: join(root, "packages/culprit-archive-client"), encoding: "utf8",
  }));
  const installed = join(scratch, "node_modules/@culpritsql/archive-client");
  mkdirSync(installed, { recursive: true });
  execFileSync("tar", ["-xf", join(scratch, tarball.filename), "--strip-components=1", "-C", installed]);
  writeFileSync(join(scratch, "consumer.mts"), [
    'import { createArchiveClient, type WarehouseIncidentView } from "@culpritsql/archive-client";',
    'const read = createArchiveClient().warehouseIncident;',
    'export const incident = (...args: Parameters<typeof read>): Promise<WarehouseIncidentView> => read(...args);',
  ].join("\n"));
  execFileSync(process.execPath, [join(root, "app/node_modules/typescript/bin/tsc"), "--strict", "--module", "NodeNext", "--target", "ES2022", "--noEmit", "consumer.mts"], { cwd: scratch, stdio: "inherit" });
  const { createArchiveClient } = await import(join(installed, "pkg/index.js"));
  const signal = new AbortController().signal;
  const connection = { name: "analytics", revision: "v1" };
  const request = { page_rows: 10 };
  const resource = { resource_key: "orders" };
  const around = "2026-01-01T00:00:00Z";
  const expected = [
    ["GET", "connections", undefined], ["POST", "fleet", { connection, request }],
    ["POST", "resources", { connection, request }], ["POST", "incident", { connection, resource, around }],
    ["POST", "window", { connection, request }], ["POST", "artifacts", { connection, request }],
  ];
  const reader = createArchiveClient(async (method, path, body, receivedSignal) => {
    const [verb, route, envelope] = expected.shift();
    assert.deepEqual([method, path, body], [verb, "/api/archive/" + route, envelope]);
    assert.equal(receivedSignal, signal);
    return route;
  });
  await reader.warehouseConnections(signal);
  await reader.warehouseFleet(connection, request, signal);
  await reader.warehouseResources(connection, request, signal);
  assert.equal(await reader.warehouseIncident(connection, resource, around, signal), "incident");
  await reader.warehouseWindow(connection, request, signal);
  await reader.warehouseArtifacts(connection, request, signal);
  assert.equal(expected.length, 0);
  console.log("Verified the packed archive client, its consumer types and all six transport routes.");
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
