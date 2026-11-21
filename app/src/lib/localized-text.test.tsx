import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { LocalizedText } from "../gen/LocalizedText";
import type { NodeResources } from "../gen/NodeResources";
import type { TerminationHistory } from "../gen/TerminationHistory";
import type { Workload } from "../gen/Workload";
import type { TableStat } from "../gen/TableStat";
import { Columns } from "../views/schema-table/Columns";
import type { TableRelations } from "./schema-model";
import { NodeResourcesView } from "../components/NodeResources";
import { TerminationHistoryView } from "../components/TerminationHistory";
import { healthReportFixture } from "../test/health-report";
import { partial } from "../test/fixtures";
import { workloadSource } from "./rates";

const notes: LocalizedText[] = [
  { text: "raw <script>native-canary()</script>" },
  { text: "Saved catalog wording", source: { kind: "catalog", key: "provider.yugabyte.activity.weights", arguments: {} } },
  { text: "provider.yugabyte.activity.weights" },
];

test("saved node labels and mixed notes render their explicit text in order and escape raw input", () => {
  const resources = partial<NodeResources>({
    source: { text: "Native resource display" },
    observed_from: "2026-11-10T00:00:00Z", observed_until: "2026-11-10T00:00:01Z",
    nodes_expected: 1, partial: false, notes,
    nodes: [{ node: "node-1", available: true, cpu_user_fraction: null, cpu_system_fraction: null,
      host_memory_total_bytes: null, host_memory_available_bytes: null, host_memory_free_bytes: null,
      process_memory: { name: { text: "native <tracker>" }, tracked_bytes: null, hard_limit: null, soft_limit: null } }],
  });
  const html = renderToStaticMarkup(<NodeResourcesView resources={resources} />);
  expect(html).toContain("Native resource display");
  expect(html).toContain('title="native &lt;tracker&gt;"');
  expect(html).toContain("raw &lt;script&gt;native-canary()&lt;/script&gt;");
  expect(html.indexOf("raw &lt;script&gt;")).toBeLessThan(html.indexOf("Saved catalog wording"));
  expect(html.indexOf("Saved catalog wording")).toBeLessThan(html.indexOf("provider.yugabyte.activity.weights"));
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("[object Object]");
});

test("termination source visibility and mixed notes remain explicit saved text", () => {
  const history = partial<TerminationHistory>({
    source: { text: "Native retained source" }, visibility: { text: "Observed role <42>" }, notes,
    requested_start: "2026-11-10T00:00:00Z", requested_end: "2026-11-10T00:00:01Z",
    events: [], partial: true,
  });
  const html = renderToStaticMarkup(<TerminationHistoryView history={history} report={healthReportFixture({})} />);
  expect(html).toContain("Native retained source");
  expect(html).toContain("Observed role &lt;42&gt;");
  expect(html).toContain("Saved catalog wording");
  expect(html).toContain("provider.yugabyte.activity.weights");
  expect(html).not.toContain("[object Object]");
  expect(html).not.toContain("<script>");
});

test("workload display labels never replace stable native counter identity or interpret raw keys", () => {
  const workload = partial<Workload>({ source: "stable-native-source", source_label: { text: "Reader display" } });
  expect(workloadSource(workload)).toBe("Reader display");
  expect(workload.source).toBe("stable-native-source");
  workload.source_label = { text: "provider.yugabyte.activity.weights" };
  expect(workloadSource(workload)).toBe("provider.yugabyte.activity.weights");
  delete workload.source_label;
  expect(workloadSource(workload)).toBe("stable-native-source");
});

test("saved collection notes use their text at the schema metadata boundary", () => {
  const table = partial<TableStat>({ schema: "dbo", name: "Hidden" });
  const report = healthReportFixture({ snapshot: { columns: [], notes: [{ collector: "catalog", table: "dbo.Hidden", message: notes[0]! }] } });
  const html = renderToStaticMarkup(<Columns report={report} table={table} indexes={[]} links={partial<TableRelations>({})} onOpenTable={() => {}} />);
  expect(html).toContain("raw &lt;script&gt;native-canary()&lt;/script&gt;");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("[object Object]");
});
