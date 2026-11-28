import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { ClickHouseHealth } from "../components/ClickHouseHealth";
import type { Snapshot } from "../gen/Snapshot";
import { fromRust } from "../test/fixtures";
import capture from "../../../fixtures/clickhouse-demo.json";

test("Health exposes the failed insert, native fields and coverage independently for each ClickHouse source", () => {
  const snapshot = fromRust<Snapshot>(structuredClone(capture));
  const failure = snapshot.operations.clickhouse?.find(row => row.kind === "failure");
  expect(failure).toBeDefined();
  if (failure?.kind !== "failure") throw new Error("missing fixture failure");
  failure.observation.query_id = "<script>native-query-id</script>";
  const source = snapshot.sources?.find(source => source.source === "system.replicas");
  if (!source) throw new Error("missing replica coverage");
  source.state = "unavailable";
  source.reason = { text: "permission denied by source" };
  const html = renderToStaticMarkup(<ClickHouseHealth snapshot={snapshot} />);
  expect(html).toContain("TOO_MANY_PARTS");
  expect(html).toContain("system.replicas");
  expect(html).toContain("permission denied by source");
  expect(html).toContain("Copy collector selection");
  expect(html).toContain("&lt;script&gt;native-query-id&lt;/script&gt;");
  expect(html).not.toContain("<script>native-query-id");
});
