import { expect, test } from "bun:test";
import { desktopWarehouses } from "./api/warehouses";
import type { Invoke } from "./owned-requests";
import type { PublishedResources } from "../gen/PublishedResources";
const connection = { name: "archive-only", revision: "a".repeat(64) };
const request = { as_of: "2026-11-22T12:00:00Z", window: null, after: null, page_rows: 32 };
const resource = { namespace_id: "b".repeat(64), database: "c".repeat(64) };
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>((yes) => { resolve = yes; }); return { promise, resolve }; }
async function until(check: () => boolean) { for (let i = 0; i < 100; i++) { if (check()) return; await Promise.resolve(); } throw new Error("Expected warehouse request did not dispatch"); }
test("archive reads register before dispatch and aborted registration opens no native or warehouse work", async () => {
  const begin = deferred<void>(); const calls: string[] = [];
  const invoke: Invoke = async <T>(command: string) => { calls.push(command); if (command === "begin_warehouse_read") await begin.promise; return undefined as T; };
  const api = desktopWarehouses(invoke); const abort = new AbortController();
  const pending = api.warehouseResources(connection, request, abort.signal).then(() => false, () => true);
  await until(() => calls.length === 1); abort.abort(); begin.resolve(); expect(await pending).toBe(true);
  expect(calls).toEqual(["begin_warehouse_read", "cancel_warehouse_read"]);
});
test("resources and incidents share ordered ownership and a late aborted result cannot become a new selection", async () => {
  const result = deferred<PublishedResources>(); const calls: Array<{ command: string; args?: Record<string, unknown> }> = [];
  const invoke: Invoke = async <T>(command: string, args?: Record<string, unknown>) => {
    calls.push({ command, args }); if (command === "warehouse_resources") return await result.promise as T;
    return { report: { database: resource.database }, analysis_limitations: [] } as T;
  };
  const api = desktopWarehouses(invoke); const abort = new AbortController();
  const old = api.warehouseResources(connection, request, abort.signal).then(() => false, () => true);
  await until(() => calls.some((call) => call.command === "warehouse_resources")); abort.abort();
  await api.warehouseIncident(connection, resource, "2026-11-22T10:00:00Z");
  result.resolve({} as PublishedResources); expect(await old).toBe(true);
  const begins = calls.filter((call) => call.command === "begin_warehouse_read"); expect(begins).toHaveLength(2);
  expect(begins[0]!.args?.requestId).not.toBe(begins[1]!.args?.requestId);
  const incident = calls.find((call) => call.command === "warehouse_incident")!;
  expect(incident.args?.connection).toEqual(connection); expect(incident.args?.resource).toEqual(resource);
  expect(calls.some((call) => ["scan", "connections", "test_connection", "read_file"].includes(call.command))).toBe(false);
});
test("archive metadata saves and removal carry typed scoped references rather than a production connection or file path", async () => {
  const calls: Array<{ command: string; args?: Record<string, unknown> }> = [];
  const invoke: Invoke = async <T>(command: string, args?: Record<string, unknown>) => { calls.push({ command, args }); return undefined as T; };
  const api = desktopWarehouses(invoke); await api.warehouseConnections(); await api.removeWarehouseConnection(connection);
  expect(calls).toEqual([{ command: "warehouse_connections", args: undefined }, { command: "remove_warehouse_connection", args: { connection } }]);
});

test("window reads use the same registered cancellation owner and a late aborted page cannot replace a newer action", async () => {
  const result = deferred<unknown>(); const calls: Array<{ command: string; args?: Record<string, unknown> }> = [];
  const invoke: Invoke = async <T>(command: string, args?: Record<string, unknown>) => {
    calls.push({ command, args }); if (command === "warehouse_window") return await result.promise as T; return undefined as T;
  };
  const api = desktopWarehouses(invoke), abort = new AbortController();
  const query = { resource, window: { from: "2026-11-22T09:00:00Z", until: "2026-11-22T11:00:00Z" }, as_of: request.as_of,
    view: "published-answers" as const, page_rows: 1, after: null };
  const pending = api.warehouseWindow(connection, query, abort.signal).then(() => false, () => true);
  await until(() => calls.some((call) => call.command === "warehouse_window")); abort.abort(); result.resolve({ request: query });
  expect(await pending).toBe(true);
  const read = calls.find((call) => call.command === "warehouse_window")!;
  expect(read.args?.connection).toEqual(connection);expect(read.args?.request).toEqual(query);
  expect(calls[0]!.command).toBe("begin_warehouse_read");expect(calls.some((call) => call.command === "cancel_warehouse_read")).toBe(true);
  expect(calls.some((call) => ["scan", "test_connection", "run_query", "read_file"].includes(call.command))).toBe(false);
});

test("artifact reads register the same owned action, carry explicit full-body opt-in and reject a late cancelled result", async () => {
  const result = deferred<unknown>(), calls: Array<{ command: string; args?: Record<string, unknown> }> = [];
  const invoke: Invoke = async <T>(command: string, args?: Record<string, unknown>) => { calls.push({ command, args }); if (command === "warehouse_artifacts") return await result.promise as T; return undefined as T; };
  const client = desktopWarehouses(invoke), abort = new AbortController();
  const query = { resource, references: [], full_payloads: true, text: { kind: "complete" as const }, expected_sql_sha256: null };
  // Presentation/lifetime transport test only; the backend independently refuses the empty original roster.
  const pending = client.warehouseArtifacts(connection, query, abort.signal).then(() => false, () => true);
  await until(() => calls.some((call) => call.command === "warehouse_artifacts"));
  abort.abort(); result.resolve({ query, records: [] }); expect(await pending).toBe(true);
  const read = calls.find((call) => call.command === "warehouse_artifacts")!;
  expect(read.args?.connection).toEqual(connection); expect(read.args?.request).toEqual(query);
  expect(calls[0]?.command).toBe("begin_warehouse_read"); expect(calls.some((call) => call.command === "cancel_warehouse_read")).toBe(true);
  expect(calls.some((call) => ["run_query", "test_connection", "read_file", "scan"].includes(call.command))).toBe(false);
});
