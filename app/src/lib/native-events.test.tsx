import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { NativeEventsImport, NativeEventsRead } from "../components/NativeEvents";
import type { NativeLogAnalysis } from "../gen/NativeLogAnalysis";
import { nativeEventArtifact as artifact } from "../test/native-events";
import { desktopFiles } from "./api/files";
import { t } from "./i18n";


test("offline entry requires no database and renders no provisional imported evidence", () => {
  const entry = renderToStaticMarkup(<NativeEventsImport />);
  expect(entry).toContain(t("native_events.import.open"));
  expect(entry).not.toContain("role=\"dialog\"");
  expect(entry).not.toContain("<article");
});

test("imported event clocks and DB caller labels stay escaped, contextual and independent from intake time", () => {
  const html = renderToStaticMarkup(<NativeEventsRead analysis={artifact()} />);
  expect(html).toContain("2020-01-01T12:00:00Z");
  expect(html).toContain("2026-10-25T03:30:00");
  expect(html).toContain(t("ui.native_failures.local_event_time"));
  expect(html).toContain(t("native_events.context"));
  expect(html).toContain(t("native_events.statement"));
  expect(html).toContain("&lt;script&gt;");
  expect(html).toContain("&lt;img");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("<img");
  expect(html).not.toContain("2026-10-25T03:30:00Z");
  expect(html).not.toContain("session_action");
  expect(html).not.toContain("<button");
});

test("partial malformed and unplaced artifacts keep the existing limitation and paging owner", () => {
  const analysis = artifact();
  analysis.input.complete = false;
  analysis.stopped = { kind: "input-limit" };
  analysis.source.state = "partial";
  analysis.report.source_state = "partial";
  const original = analysis.evidence.observations[0]!;
  if (original.native !== "mssql-extended-event") throw new Error("Expected presentation event");
  analysis.evidence.observations = Array.from({ length: 11 }, (_, position) => ({ ...original, observation: { ...original.observation, position } }));
  const explanation = analysis.report.entries[0]!;
  analysis.report.entries = Array.from({ length: 11 }, (_, position) => ({ ...explanation, position }));
  const html = renderToStaticMarkup(<NativeEventsRead analysis={analysis} />);
  expect(html).toContain(t("native_logs.cli.incomplete"));
  expect(html).toContain(analysis.report.limitations[0]!);
  expect(html.match(/<article/g)).toHaveLength(10);
  expect(html).toContain(t("ui.termination_history.next"));
});

test("the desktop API delegates no path or connection and rejects an aborted late result independently of its replacement", async () => {
  let finish!: (value: NativeLogAnalysis | null) => void;
  const first = new Promise<NativeLogAnalysis | null>((resolve) => { finish = resolve; });
  const calls: Array<{ command: string; args: Record<string, unknown> }> = [];
  let reads = 0;
  const files = desktopFiles(async <T,>(command: string, args: Record<string, unknown> = {}) => {
    calls.push({ command, args });
    if (command === "open_native_events") return (reads++ === 0 ? first : Promise.resolve(null)) as Promise<T>;
    return undefined as T;
  });
  const previous = new AbortController();
  const old = files.openNativeEvents(previous.signal);
  for (let i = 0; reads === 0 && i < 100; i++) await Promise.resolve();
  expect(reads).toBe(1);
  previous.abort();
  expect(await files.openNativeEvents()).toBeNull();
  finish(artifact());
  await expect(old).rejects.toMatchObject({ name: "AbortError" });
  const started = calls.filter((call) => call.command === "begin_native_events");
  expect(started).toHaveLength(2);
  expect(started[0]!.args.requestId).not.toBe(started[1]!.args.requestId);
  expect(calls.filter((call) => call.command === "open_native_events").every((call) => Object.keys(call.args).join() === "requestId")).toBe(true);
  expect(calls.some((call) => call.command === "cancel_native_events" && call.args.requestId === started[0]!.args.requestId)).toBe(true);
});
