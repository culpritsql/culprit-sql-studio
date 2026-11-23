import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { ChangeEvent } from "../gen/ChangeEvent";
import type { Changes } from "../gen/Changes";
import { AroundThen, PeriodStrip, chartEvents } from "../components/ChangeTimeline";
import { partial } from "../test/fixtures";
import { t } from "./i18n";

function recorded(): ChangeEvent {
  return {
    id: "mark:native-event:2026-11-21T10:00:00Z", kind: "mark", source: "mark", action: null,
    at: "2026-11-21T10:00:00Z", after: null, until: null,
    title: { text: "raw <script>event-canary()</script>" }, detail: { text: "native <detail>" },
    table: null, object: null, minor: false, by: "native-owner", job_execution: null,
  };
}
function changes(events: ChangeEvent[]): Changes {
  return partial<Changes>({
    connection: "controlled", events, queries: [],
    baseline: { start: "2026-11-21T09:00:00Z", end: "2026-11-21T10:00:00Z" },
    recent: { start: "2026-11-21T10:00:00Z", end: "2026-11-21T11:00:00Z" },
  });
}

test("saved event text renders as plain text and never guesses raw catalog keys or renders the source object", () => {
  const raw = recorded();
  const catalog = { ...recorded(), id: "restart:native:2026-11-21T10:00:00Z", kind: "restart" as const,
    source: "database" as const, title: { text: "Titre enregistré", source: { kind: "catalog" as const,
      key: "engine.history.restarted", arguments: {} } }, detail: { text: "" } };
  const html = renderToStaticMarkup(<AroundThen changes={changes([raw, catalog])} onFocusQuery={() => {}} />);
  expect(html).toContain("raw &lt;script&gt;event-canary()&lt;/script&gt;");
  expect(html).toContain("native &lt;detail&gt;");
  expect(html).toContain("Titre enregistré");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("[object Object]");
  expect(html).not.toContain("engine.history.restarted");
  const key = { ...raw, title: { text: "engine.history.restarted" } };
  expect(chartEvents([key])[0]!.title).toBe("engine.history.restarted");
});

test("chart and period labels preserve saved event keys and native bounds independently of display text", () => {
  const raw = recorded();
  const marker = chartEvents([raw])[0]!;
  expect(marker.id).toBe(raw.id);
  expect(marker.title).toBe(raw.title.text);
  expect(marker.from).toBe(Date.parse(raw.at));
  expect(marker.to).toBe(Date.parse(raw.at));
  expect(marker.exact).toBe(true);
  const html = renderToStaticMarkup(<PeriodStrip changes={changes([raw])} />);
  expect(html).toContain("raw &lt;script&gt;event-canary()&lt;/script&gt;");
  expect(html).not.toContain("[object Object]");
});


test("imported native XML events retain explicit artifact provenance and plain text without cloud or database authority", () => {
  const event: ChangeEvent = { ...recorded(), id: "native-event:artifact:0:2026-11-21T10:00:00Z", kind: "native-event", source: "imported-artifact", by: null };
  const html = renderToStaticMarkup(<AroundThen changes={changes([event])} onFocusQuery={() => {}} />);
  expect(html).toContain(t("ui.change_timeline.imported_artifact"));
  expect(html).not.toContain(t("ui.change_timeline.from_cloud"));
  expect(html).not.toContain(t("ui.change_timeline.dated_by_database"));
  expect(html).toContain("raw &lt;script&gt;event-canary()&lt;/script&gt;");
  expect(chartEvents([event])[0]!.kind).toBe("native-event");
});
