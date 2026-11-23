import { expect, test } from "@playwright/test";
import english from "../../locales/en.json" with { type: "json" };
import demo from "../../fixtures/mssql-demo.report.json" with { type: "json" };
import type { Plan } from "../src/gen/Plan";
import type { Report } from "../src/gen/Report";
import { fromRust } from "../src/test/fixtures";
import { installArchiveApi, openArchives, selectArchive, selectArchiveResource, archiveState, holdArchivePhase, releaseArchivePhase, assertNoNativeArchiveWork } from "./archives";
async function openQuery(page: import("@playwright/test").Page, artifactPlan?: Plan) {
  await installArchiveApi(page, { artifactPlan }); await openArchives(page); await selectArchive(page); await selectArchiveResource(page);
  const root = page.locator(".archives"); await root.getByRole("button", { name: english["ui.archives.investigate"], exact: true }).click();
  const incident = root.getByRole("region", { name: english["ui.archives.incident"], exact: true }).first();
  await incident.locator("summary").filter({ hasText: english["ui.archives.affected"] }).click();
  return incident.getByRole("button", { name: english["artifact.ui.open"], exact: true }).first();
}
test("full query artifact click uses returned originals and renders complete safe text without source work", async ({ page }) => {
  const button = await openQuery(page); await button.click();
  const artifacts = page.getByRole("region", { name: english["artifact.ui.title"], exact: true });
  await expect(artifacts).toContainText("COMPLETE_RETAINED_BODY");
  await expect(artifacts).toContainText("<script>retained-data</script>");
  await expect(artifacts.locator("script, img")).toHaveCount(0);
  await expect(artifacts.locator("pre")).toContainText("retained_column,".repeat(200));
  const action = (await archiveState(page)).artifacts[0]!;
  expect(action.request.full_payloads).toBe(true); expect(action.request.text).toEqual({ kind: "complete" });
  expect(action.request.references[0]!.stream).toBe("activity-observations");
  expect(action.request.references[0]!.capture.observation_id).toBe("presentation-activity");
  await expect(artifacts).not.toContainText(action.request.references[0]!.manifest_sha256);
  await expect(artifacts.getByRole("button", { name: /execute|apply|interrupt/i })).toHaveCount(0);
  await assertNoNativeArchiveWork(page);
});
test("a returned original plan opens the shared interactive tree without source calls or execution controls", async ({ page }) => {
  const plan = fromRust<Report>(demo).snapshot.workload?.queries.find((query) => query.plan?.raw)?.plan;
  if (!plan?.raw) throw new Error("Expected the Rust-generated retained plan fixture");
  const button = await openQuery(page, plan); await button.click();
  const artifacts = page.getByRole("region", { name: english["artifact.ui.title"], exact: true });
  await expect(artifacts.locator("pre").filter({ hasText: plan.raw })).toHaveCount(1);
  await artifacts.locator("summary").filter({ hasText: /^Plan$/ }).click();
  const scan = artifacts.getByRole("button", { name: /^Index Scan / });
  await expect(scan).toBeVisible(); await scan.click();
  await expect(artifacts.getByRole("heading", { name: "What it does", exact: true })).toBeVisible();
  await expect(artifacts.getByRole("button", { name: /execute|apply|interrupt/i })).toHaveCount(0);
  await assertNoNativeArchiveWork(page);
});
test("changing archive cancels a started artifact read and rejects late old text without replacing selection", async ({ page }) => {
  const button = await openQuery(page); await holdArchivePhase(page, "artifacts"); await button.click();
  await expect.poll(async () => (await archiveState(page)).artifacts.length).toBe(1);
  const original = (await archiveState(page)).artifacts[0]!;
  await selectArchive(page, "Archive Beta");
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === original.requestId)).toBe(true);
  await releaseArchivePhase(page, "artifacts", "STALE_PRIVATE_ARTIFACT");
  await expect(page.locator(".archives")).not.toContainText("STALE_PRIVATE_ARTIFACT");
  await expect(page.getByRole("region", { name: english["artifact.ui.title"], exact: true })).toHaveCount(0);
  expect((await archiveState(page)).artifacts).toHaveLength(1);
  await assertNoNativeArchiveWork(page);
});
