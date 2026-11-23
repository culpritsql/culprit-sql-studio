import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { ChangesSummary } from "../components/WhatChanged";
import type { Changes } from "../gen/Changes";
import { partial } from "../test/fixtures";
import { t } from "./i18n";

function comparison(source: Changes["source"]): Changes {
  return partial<Changes>({
    source,
    baseline: { start: "2026-11-21T06:00:00Z", end: "2026-11-21T07:00:00Z" },
    recent: { start: "2026-11-21T07:00:00Z", end: "2026-11-21T08:00:00Z" },
    baseline_total: { executions: 50, total_ms: 9500, mean_ms: 190, min_ms: null, max_ms: null },
    recent_total: { executions: 50, total_ms: 19000, mean_ms: 380, min_ms: null, max_ms: null },
    baseline_coverage: 1, queries: [],
  });
}

test("native period comparisons retain their source label without claiming scan deltas or Query Store", () => {
  const html = renderToStaticMarkup(<ChangesSummary changes={comparison("native-intervals")} />);
  expect(html).toContain(t("ui.what_changed.source_native_intervals"));
  expect(html).not.toContain(t("ui.what_changed.source_scans"));
  expect(html).not.toContain(renderToStaticMarkup(t("ui.what_changed.source_query_store")));
});

test("the existing Query Store comparison retains its reader wording", () => {
  const html = renderToStaticMarkup(<ChangesSummary changes={comparison("query-store")} />);
  expect(html).toContain(renderToStaticMarkup(t("ui.what_changed.source_query_store")));
  expect(html).not.toContain(t("ui.what_changed.source_native_intervals"));
});
