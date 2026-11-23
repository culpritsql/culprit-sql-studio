import { expect, type Page } from "@playwright/test";
import type { NativeLogAnalysis } from "../src/gen/NativeLogAnalysis";
import english from "../../locales/en.json" with { type: "json" };

type Phase = "begin" | "read";
export type EventBrowserState = {
  commands: Array<{ command: string; requestId: string; keys: string[] }>;
  reads: string[];
  finished: string[];
  nativeCalls: string[];
};
type Controls = {
  state: EventBrowserState;
  hold(phase: Phase): void;
  release(phase: Phase, marker?: string): void;
  cancelNext(): void;
};

/** Presentation/IPC timing only. Rust owns actual selected-file parsing, admission and CPU settlement controls. */
export async function installEventApi(page: Page) {
  await page.goto("/");
  await expect(page.getByRole("complementary").getByRole("button", { name: / mssql-demo \d+$/ })).toBeVisible();
  await page.locator(".language-select").selectOption("en");
  await page.evaluate(async () => {
    const apiPath = "/src/lib/api.ts", filesPath = "/src/lib/api/files.ts", storePath = "/src/lib/store.ts";
    const fixturePath = "/src/test/native-events.ts";
    const { api } = await import(apiPath) as typeof import("../src/lib/api");
    const { desktopFiles } = await import(filesPath) as typeof import("../src/lib/api/files");
    const { setState } = await import(storePath) as typeof import("../src/lib/store");
    const { nativeEventArtifact } = await import(fixturePath) as typeof import("../src/test/native-events");
    const state: EventBrowserState = { commands: [], reads: [], finished: [], nativeCalls: [] };
    const held = new Set<Phase>();
    const pending = new Map<Phase, (marker?: string) => void>();
    let cancelNext = false;
    async function pause(phase: Phase, value?: NativeLogAnalysis | null) {
      if (held.delete(phase)) await new Promise<void>((resolve) => pending.set(phase, (marker) => {
        if (marker !== undefined && value != null) value.report.summary = marker;
        pending.delete(phase);
        resolve();
      }));
    }
    const invoke: import("../src/lib/owned-requests").Invoke = async <T>(command: string, args: Record<string, unknown> = {}) => {
      const requestId = String(args.requestId);
      state.commands.push({ command, requestId, keys: Object.keys(args) });
      switch (command) {
        case "begin_native_events": await pause("begin"); return undefined as T;
        case "cancel_native_events": return undefined as T;
        case "open_native_events": {
          state.reads.push(requestId);
          const value = cancelNext ? null : nativeEventArtifact(`SELECTED_EVENT_${state.reads.length}`);
          cancelNext = false;
          await pause("read", value);
          state.finished.push(requestId);
          return structuredClone(value) as T;
        }
        default: throw new Error(`Unexpected event import command: ${command}`);
      }
    };
    const denied = (method: string) => async () => { state.nativeCalls.push(method); throw new Error(`Event artifact attempted native ${method}`); };
    Object.assign(await api(), desktopFiles(invoke), {
      connections: async () => [], latestReport: denied("latestReport"), scan: denied("scan"),
      testConnection: denied("testConnection"), live: denied("live"), dashboardLive: denied("dashboardLive"),
      run: denied("run"), deadlocks: denied("deadlocks"), waitSample: denied("waitSample"),
      interruptSession: denied("interruptSession"), queryPlan: denied("queryPlan"), explain: denied("explain"),
    });
    setState({ connections: [], active: null, view: "plans", report: { status: "none" }, saved: [] });
    Object.assign(window, { eventBrowser: {
      state, hold: (phase: Phase) => { held.add(phase); },
      release: (phase: Phase, marker?: string) => {
        const release = pending.get(phase);
        if (!release) throw new Error(`No pending event ${phase}`);
        release(marker);
      }, cancelNext: () => { cancelNext = true; },
    } });
  });
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(english["ui.palette.view.plans"]);
}
export function eventOpener(page: Page) {
  return page.getByRole("button", { name: english["native_events.import.open"], exact: true });
}
export function eventDialog(page: Page) {
  return page.getByRole("dialog", { name: english["native_events.import.title"], exact: true });
}
export function eventState(page: Page): Promise<EventBrowserState> {
  return page.evaluate(() => (window as unknown as { eventBrowser: Controls }).eventBrowser.state);
}
export async function holdEvent(page: Page, phase: Phase) {
  await page.evaluate((phase) => (window as unknown as { eventBrowser: Controls }).eventBrowser.hold(phase), phase);
}
export async function releaseEvent(page: Page, phase: Phase, marker?: string) {
  await page.evaluate(({ phase, marker }) => (window as unknown as { eventBrowser: Controls }).eventBrowser.release(phase, marker), { phase, marker });
}
export async function cancelNextEvent(page: Page) {
  await page.evaluate(() => (window as unknown as { eventBrowser: Controls }).eventBrowser.cancelNext());
}
export async function assertSourceFreeEvents(page: Page) {
  const state = await eventState(page);
  expect(state.nativeCalls).toEqual([]);
  expect(state.commands.every((call) => call.keys.length === 1 && call.keys[0] === "requestId")).toBe(true);
}
