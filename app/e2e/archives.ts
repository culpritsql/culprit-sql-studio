import type { WarehouseArtifactQuery } from "../src/gen/WarehouseArtifactQuery";
import type { Plan } from "../src/gen/Plan";
import { expect, type Locator, type Page } from "@playwright/test";
import type { SaveWarehouseConnection } from "../src/gen/SaveWarehouseConnection";
import type { WarehouseConnectionInfo } from "../src/gen/WarehouseConnectionInfo";
import type { WarehouseConnectionRef } from "../src/gen/WarehouseConnectionRef";
import type { PublishedResourceRequest } from "../src/gen/PublishedResourceRequest";
import type { WarehouseWindowQuery } from "../src/gen/WarehouseWindowQuery";
import type { PublishedResourceSelector } from "../src/gen/PublishedResourceSelector";
import english from "../../locales/en.json" with { type: "json" };

export type HeldArchivePhase = "begin" | "resources" | "incident" | "window" | "artifacts" | "save";
export type ArchiveBrowserState = {
  commands: Array<{ command: string; requestId?: string }>;
  nativeCalls: string[];
  drafts: SaveWarehouseConnection[];
  resources: Array<{ connection: WarehouseConnectionRef; request: PublishedResourceRequest; requestId: string }>;
  incidents: Array<{ connection: WarehouseConnectionRef; resource: PublishedResourceSelector; around: string; requestId: string }>;
  windows: Array<{ connection: WarehouseConnectionRef; request: WarehouseWindowQuery; requestId: string }>;
  artifacts: Array<{ connection: WarehouseConnectionRef; request: WarehouseArtifactQuery; requestId: string }>;
  finished: Record<HeldArchivePhase, number>;
};
export async function installArchiveApi(page: Page, { saved = true, nativeView = false, artifactPlan = null }: { saved?: boolean; nativeView?: boolean; artifactPlan?: Plan | null } = {}) {
  await page.goto("/");
  await expect(page.getByRole("complementary").getByRole("button", { name: / mssql-demo \d+$/ })).toBeVisible();
  await page.locator(".language-select").selectOption("en");
  await page.evaluate(async ({ saved, nativeView, artifactPlan }) => {
    const apiPath = "/src/lib/api.ts", storePath = "/src/lib/store.ts";
    const adapterPath = "/src/lib/api/warehouses.ts", fixturePath = "/src/test/warehouse-incidents.ts";
    const { api } = await import(apiPath) as typeof import("../src/lib/api");
    const { setState, loadConnections } = await import(storePath) as typeof import("../src/lib/store");
    const { desktopWarehouses } = await import(adapterPath) as typeof import("../src/lib/api/warehouses");
    const { warehouseIncidentCase } = await import(fixturePath) as typeof import("../src/test/warehouse-incidents");
    const windowFixturePath = "/src/test/warehouse-windows.ts";
    const { warehouseWindowCase } = await import(windowFixturePath) as typeof import("../src/test/warehouse-windows");
    const artifactFixturePath = "/src/test/warehouse-artifacts.ts";
    const { warehouseArtifactCase } = await import(artifactFixturePath) as typeof import("../src/test/warehouse-artifacts");
    type Phase = "begin" | "resources" | "incident" | "window" | "artifacts" | "save";
    const state: ArchiveBrowserState = {
      commands: [], nativeCalls: [], drafts: [], resources: [], incidents: [], windows: [], artifacts: [],
      finished: { begin: 0, resources: 0, incident: 0, window: 0, artifacts: 0, save: 0 },
    };
    const held = new Set<Phase>();
    const pending = new Map<Phase, (marker?: string) => void>();
    function settings(): import("../src/gen/WarehouseConnectionSettings").WarehouseConnectionSettings {
      return {
        pipeline: "pipeline", shard: "shard-0",
        warehouse: { kind: "adx", endpoint: "https://fixture.westeurope.kusto.windows.net", database: "telemetry", authentication: { name: "warehouse-read", tenant: null } },
        recovery: { kind: "azure-blob", account: "ownedarchive", container: "originals", prefix: "pipeline/shard", authentication: { name: "recovery-read", tenant: null } },
      };
    }
    const connections: WarehouseConnectionInfo[] = saved ? [
      { reference: { name: "Archive Alpha", revision: "a".repeat(64) }, settings: settings(), password_saved: false },
      { reference: { name: "Archive Beta", revision: "b".repeat(64) }, settings: settings(), password_saved: false },
    ] : [];
    async function response<T>(phase: Phase, value: T, mark?: (value: T, marker: string) => void): Promise<T> {
      if (held.delete(phase)) {
        await new Promise<void>((resolve) => pending.set(phase, (marker) => {
          if (marker != null) mark?.(value, marker);
          pending.delete(phase); resolve();
        }));
      }
      state.finished[phase]++;
      // Every result owns new nested objects, as an actual IPC response does. No identity alias is a fixture shortcut.
      return structuredClone(value);
    }
    function resources(connection: WarehouseConnectionRef, request: PublishedResourceRequest): import("../src/gen/PublishedResources").PublishedResources {
      const beta = connection.name === "Archive Beta";
      return {
        as_of: request.as_of, window: request.window, next: null, discovery_complete: false, gaps: [], candidate_rows: 1,
        recovery_metadata_reads: 1,
        archive_work: { selected_rows: 1, queries: 1, pages: 1, returned_rows: 1, response_bytes: 2n, proof_bytes: 3n, recovery_objects: 1, recovery_bytes: 4n },
        resources: [{
          selector: { namespace_id: (beta ? "e" : "d").repeat(64), database: (beta ? "c" : "b").repeat(64) },
          position: { resource_id: "resource", source_id: "sessions", range_id: beta ? "beta-original" : "alpha-original" },
          logical_source_id: "sessions", engine: "postgres", database_name: beta ? "orders-beta" : "orders-alpha",
          connection_alias: null, source_name: "sessions", collection: "activity", continuity: "unverified",
          collected_from: "2026-11-22T10:00:00Z", collected_until: "2026-11-22T10:00:01Z", native_time: { kind: "unknown" },
          coverage: { kind: "activity", coverage: { source: "sessions", state: "partial", records: 1, complete_scope: false, error_code: null, stopped: null } },
        }],
      };
    }
    const invoke: import("../src/lib/owned-requests").Invoke = async <T>(command: string, args?: Record<string, unknown>) => {
      const requestId = typeof args?.requestId === "string" ? args.requestId : undefined;
      state.commands.push({ command, requestId });
      switch (command) {
        case "warehouse_connections": return structuredClone(connections) as T;
        case "add_warehouse_connection": {
          const input = args!.input as SaveWarehouseConnection;
          state.drafts.push(structuredClone(input));
          const value: WarehouseConnectionInfo = {
            reference: { name: input.name, revision: "f".repeat(64) }, settings: structuredClone(input.settings), password_saved: input.settings.warehouse.kind === "clickhouse",
          };
          connections.push(value);
          return await response("save", value) as T;
        }
        case "remove_warehouse_connection": {
          const reference = args!.connection as WarehouseConnectionRef;
          const index = connections.findIndex((item) => item.reference.revision === reference.revision);
          if (index >= 0) connections.splice(index, 1);
          return undefined as T;
        }
        case "begin_warehouse_read": return await response("begin", undefined) as T;
        case "cancel_warehouse_read": return undefined as T;
        case "warehouse_resources": {
          const connection = args!.connection as WarehouseConnectionRef;
          const request = args!.request as PublishedResourceRequest;
          state.resources.push({ connection: structuredClone(connection), request: structuredClone(request), requestId: requestId! });
          return await response("resources", resources(connection, request), (value, marker) => { value.resources[0]!.database_name = marker; }) as T;
        }
        case "warehouse_window": {
          const connection = args!.connection as WarehouseConnectionRef;
          const request = args!.request as WarehouseWindowQuery;
          state.windows.push({ connection: structuredClone(connection), request: structuredClone(request), requestId: requestId! });
          return await response("window", warehouseWindowCase(request, request.after == null ? "FIRST_WINDOW_PAGE" : "SECOND_WINDOW_PAGE"),
            (value, marker) => { value.published[0]!.findings[0]!.finding.finding.title = marker; }) as T;
        }
        case "warehouse_artifacts": {
          const connection = args!.connection as WarehouseConnectionRef;
          const request = args!.request as WarehouseArtifactQuery;
          state.artifacts.push({ connection: structuredClone(connection), request: structuredClone(request), requestId: requestId! });
          return await response("artifacts", warehouseArtifactCase(request, undefined, artifactPlan), (value, marker) => {
            const payload = value.records[0]!.artifacts[0]!.payload;
            if (payload.kind === "complete" && payload.payload.state === "available") payload.payload.text = marker;
          }) as T;
        }
        case "warehouse_incident": {
          const connection = args!.connection as WarehouseConnectionRef;
          const resource = args!.resource as PublishedResourceSelector;
          const around = args!.around as string;
          state.incidents.push({ connection: structuredClone(connection), resource: structuredClone(resource), around, requestId: requestId! });
          return await response("incident", warehouseIncidentCase(`SELECTED:${around}`, resource.database), (value, marker) => { value.report.incidents[0]!.victim_summary = marker; }) as T;
        }
        default: throw new Error(`Unexpected archive browser command: ${command}`);
      }
    };
    const client = await api();
    // This is the existing saved browser-demo report, never a source scan/login.
    const native = nativeView ? (await client.connections()).find((connection) => connection.name === "mssql-demo") : null;
    const report = native ? await client.latestReport(native.name, null, null) : null;
    if (nativeView && (!native || !report)) throw new Error("A saved native-view fixture is required");
    const selectedConnections = native ? [native] : [];
    const deniedNative = (name: string) => async () => { state.nativeCalls.push(name); throw new Error(`Archive flow attempted native ${name}`); };
    Object.assign(client, desktopWarehouses(invoke), {
      connections: async () => structuredClone(selectedConnections), scan: deniedNative("scan"), testConnection: deniedNative("testConnection"),
      live: deniedNative("live"), dashboardLive: deniedNative("dashboardLive"), run: deniedNative("run"),
      deadlocks: deniedNative("deadlocks"), waitSample: deniedNative("waitSample"), interruptSession: deniedNative("interruptSession"),
      testConnectionSettings: deniedNative("testConnectionSettings"), testSsh: deniedNative("testSsh"),
      previewTable: deniedNative("previewTable"), lintSql: deniedNative("lintSql"), catalog: deniedNative("catalog"),
      discoverAzure: deniedNative("discoverAzure"), discoverAws: deniedNative("discoverAws"),
    });
    await loadConnections();
    setState({
      connections: selectedConnections, active: native?.name ?? null, view: nativeView ? "overview" : "connections",
      report: report ? { status: "ready", report, scanning: false, scanError: null } : { status: "none" },
      saved: native ? [{ name: "Archive forbidden saved query", sql: "SELECT 1", connection: native.name, description: null, created_at: "2026-11-22T10:00:00Z", updated_at: "2026-11-22T10:00:00Z", last_run_at: null }] : [],
    });
    Object.assign(window, { archiveBrowser: {
      state,
      hold: (phase: Phase) => { held.add(phase); },
      release: (phase: Phase, marker?: string) => {
        const release = pending.get(phase);
        if (!release) throw new Error(`No pending archive ${phase}`);
        release(marker);
      },
    } });
  }, { saved, nativeView, artifactPlan });
}
export async function openArchives(page: Page) {
  await page.getByRole("complementary").getByRole("button", { name: english["ui.archives.title"], exact: true }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(english["ui.archives.title"]);
}
export async function archiveState(page: Page): Promise<ArchiveBrowserState> {
  return page.evaluate(() => (window as unknown as { archiveBrowser: { state: ArchiveBrowserState } }).archiveBrowser.state);
}
export async function holdArchivePhase(page: Page, phase: HeldArchivePhase) {
  await page.evaluate((phase) => (window as unknown as { archiveBrowser: { hold: (phase: HeldArchivePhase) => void } }).archiveBrowser.hold(phase), phase);
}
export async function releaseArchivePhase(page: Page, phase: HeldArchivePhase, marker?: string) {
  await page.evaluate(({ phase, marker }) => (window as unknown as { archiveBrowser: { release: (phase: HeldArchivePhase, marker?: string) => void } }).archiveBrowser.release(phase, marker), { phase, marker });
}
export async function selectArchive(page: Page, name = "Archive Alpha") {
  await page.locator(".archives .conn-grid").first().getByRole("button", { name: new RegExp(`^${name} ·`) }).click();
}
export async function selectArchiveResource(page: Page, name = "orders-alpha") {
  await page.locator(".archives").getByRole("button", { name: new RegExp(`^${name} ·`) }).click();
  await expect(page.locator(".archives").getByRole("heading", { level: 3, name, exact: true })).toBeVisible();
}
export function archiveIncident(page: Page): Locator {
  return page.getByRole("region", { name: english["ui.archives.incident"], exact: true }).first();
}
export async function fillArchiveForm(page: Page, name = "Typed archive") {
  const form = page.locator("form.archive-form");
  await form.getByLabel(english["ui.connection_card.name"], { exact: true }).fill(name);
  await form.getByRole("combobox", { name: english["ui.archives.warehouse"], exact: true }).selectOption("clickhouse");
  await form.getByLabel(english["ui.archives.endpoint"], { exact: true }).fill("https://warehouse.example");
  await form.getByLabel(english["ui.connection_settings.database"], { exact: true }).fill("telemetry");
  await form.getByLabel(english["ui.archives.warehouse_profile"], { exact: true }).fill("warehouse-reader");
  await form.getByLabel(english["ui.new_connection_form.user"], { exact: true }).fill("archive_reader");
  await form.getByLabel(english["ui.new_connection_form.password"], { exact: true }).fill("fixture-private-password");
  await form.getByLabel(english["ui.archives.account"], { exact: true }).fill("ownedarchive");
  await form.getByLabel(english["ui.archives.container"], { exact: true }).fill("originals");
  await form.getByLabel(english["ui.archives.prefix"], { exact: true }).fill("pipeline/shard");
  await form.getByLabel(english["ui.archives.recovery_profile"], { exact: true }).fill("original-reader");
  await form.getByLabel(english["ui.archives.tenant"], { exact: true }).fill("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb");
  await form.getByLabel(english["ui.archives.pipeline"], { exact: true }).fill("pipeline");
  await form.getByLabel(english["ui.archives.shard"], { exact: true }).fill("shard-0");
  return form;
}
export async function assertNoNativeArchiveWork(page: Page, connectionNames: string[] = []) {
  expect((await archiveState(page)).nativeCalls).toEqual([]);
  const saved = await page.evaluate(async () => {
    const path = "/src/lib/api.ts";
    const { api } = await import(path) as typeof import("../src/lib/api");
    return (await api()).connections();
  });
  expect(saved.map((connection) => connection.name)).toEqual(connectionNames);
}
