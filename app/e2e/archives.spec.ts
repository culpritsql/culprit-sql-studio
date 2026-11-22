import { expect, test } from "@playwright/test";
import english from "../../locales/en.json" with { type: "json" };
import {
  archiveIncident, archiveState, assertNoNativeArchiveWork, fillArchiveForm, holdArchivePhase,
  installArchiveApi, openArchives, releaseArchivePhase, selectArchive, selectArchiveResource,
} from "./archives";

test("an archive-only user saves typed roles and investigates published originals with escaped causal roles and retained history", async ({ page }, info) => {
  await installArchiveApi(page, { saved: false });
  await openArchives(page);
  await expect(page.locator(".side-connections")).toHaveCount(0);
  await page.locator(".archives").getByRole("button", { name: english["ui.archives.add"], exact: true }).click();
  const form = await fillArchiveForm(page);
  await expect(form.locator("textarea")).toHaveCount(0);
  await form.getByRole("button", { name: english["ui.common.save"], exact: true }).click();
  await expect(form).toBeHidden();
  await expect.poll(async () => (await archiveState(page)).resources.length).toBe(1);
  const draft = (await archiveState(page)).drafts[0]!;
  expect(draft).toEqual({
    name: "Typed archive", password: "fixture-private-password",
    settings: {
      pipeline: "pipeline", shard: "shard-0",
      warehouse: { kind: "clickhouse", endpoint: "https://warehouse.example", database: "telemetry", username: "archive_reader", credential_name: "warehouse-reader" },
      recovery: { kind: "azure-blob", account: "ownedarchive", container: "originals", prefix: "pipeline/shard", authentication: { name: "original-reader", tenant: "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb" } },
    },
  });
  await selectArchiveResource(page);
  expect(await page.locator(".archives").getByRole("button", { name: /^orders-alpha ·/ }).evaluate((node) => node.scrollHeight <= node.clientHeight + 1)).toBe(true);
  await expect(page.locator(".archives")).toContainText(english["ui.archives.native_clock_unknown"]);
  await expect(page.locator(".archives")).toContainText(english["ui.common.read_partial"]);
  await page.locator(".archives").getByRole("button", { name: english["ui.archives.investigate"], exact: true }).click();
  const incident = archiveIncident(page);
  await expect(incident).toContainText("SELECTED:2026-11-22T10:00:00.000Z");
  await expect(incident).toContainText(english["ui.archives.caller_limit"]);
  await page.screenshot({ path: info.outputPath("archive-originals-default.png") });
  const limits = incident.locator(":scope > details > summary").filter({ hasText: english["ui.memory_failures.limitations"] }).locator("..");
  await expect(limits).toHaveJSProperty("open", false);
  await limits.locator(":scope > summary").focus();
  await page.keyboard.press("Enter");
  await expect(limits).toHaveJSProperty("open", true);
  await expect(limits).toContainText("Original schema context unavailable");
  for (const key of ["ui.archives.affected", "ui.archives.culprits", "ui.archives.candidates"] as const) {
    const entries = incident.locator("summary").filter({ hasText: english[key] }).first();
    await entries.focus();
    await page.keyboard.press("Enter");
    const group = incident.getByRole("region", { name: english[key], exact: true });
    await expect(group.locator("pre")).toBeVisible();
  }
  await expect(incident.getByRole("region", { name: english["ui.archives.affected"], exact: true })).toContainText("SELECT affected");
  const candidate = incident.getByRole("region", { name: english["ui.archives.candidates"], exact: true });
  await expect(candidate).toContainText("SELECT candidate");
  await expect(candidate).toContainText("<script>candidate</script>");
  const culprit = incident.getByRole("region", { name: english["ui.archives.culprits"], exact: true });
  await expect(culprit).toContainText(english["ui.archives.sql_unknown"]);
  await expect(culprit).not.toContainText("SELECT");
  await expect(culprit).toContainText("Presentation-only blocking-owner role; acquiring SQL is unavailable");
  await expect(incident).toContainText("<script>native-client</script>");
  await expect(incident.getByRole("region", { name: english["ui.archives.affected"], exact: true })).toContainText("<img src=x onerror=alert(1)>");
  await expect(incident.locator("img, script, button.danger")).toHaveCount(0);
  await expect(incident.getByRole("button", { name: /kill|interrupt/i })).toHaveCount(0);
  const history = incident.locator(":scope > details").filter({ hasText: english["ui.archives.history"] });
  await history.locator(":scope > summary").focus();
  await page.keyboard.press("Space");
  await expect(history).toHaveJSProperty("open", true);
  await expect(history.getByRole("region", { name: english["view.incident_history.title"], exact: true })).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.locator(".archives").evaluate((node) => node.scrollWidth <= node.clientWidth + 1)).toBe(true);
  await page.screenshot({ path: info.outputPath("archive-originals-narrow.png") });
  await assertNoNativeArchiveWork(page);
});

test("changing archive and around time retires delayed originals before their late results can replace the selection", async ({ page }) => {
  await installArchiveApi(page);
  await openArchives(page);
  await holdArchivePhase(page, "resources");
  await selectArchive(page);
  await expect.poll(async () => (await archiveState(page)).resources.length).toBe(1);
  const oldResource = (await archiveState(page)).resources[0]!;
  await selectArchive(page, "Archive Beta");
  await expect(page.locator(".archives").getByRole("button", { name: /^orders-beta ·/ })).toBeVisible();
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === oldResource.requestId)).toBe(true);
  await releaseArchivePhase(page, "resources", "STALE_RESOURCE");
  await expect.poll(async () => (await archiveState(page)).finished.resources).toBe(2);
  await expect(page.locator(".archives")).not.toContainText("STALE_RESOURCE");
  await selectArchiveResource(page, "orders-beta");
  await holdArchivePhase(page, "incident");
  const investigate = page.locator(".archives").getByRole("button", { name: english["ui.archives.investigate"], exact: true });
  await investigate.click();
  await expect.poll(async () => (await archiveState(page)).incidents.length).toBe(1);
  const oldIncident = (await archiveState(page)).incidents[0]!;
  await page.locator(".archives").getByLabel(english["ui.archives.around"], { exact: true }).fill("2026-11-22T10:01");
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === oldIncident.requestId)).toBe(true);
  await releaseArchivePhase(page, "incident", "STALE_AROUND");
  await expect(investigate).toBeEnabled();
  await expect(page.locator(".archives")).not.toContainText("STALE_AROUND");
  await investigate.click();
  await expect(archiveIncident(page)).toContainText("SELECTED:2026-11-22T10:01:00.000Z");
  const selected = (await archiveState(page)).incidents[1]!;
  expect(selected.connection.name).toBe("Archive Beta");
  expect(selected.resource.database).toBe("c".repeat(64));
  expect(selected.around).toBe("2026-11-22T10:01:00.000Z");
  await assertNoNativeArchiveWork(page);
});

test("Archives isolates a populated native palette and retires pending work when the user leaves", async ({ page }) => {
  await installArchiveApi(page, { nativeView: true });
  const palette = page.getByRole("dialog", { name: english["ui.palette.label"], exact: true });
  const runSaved = english["ui.palette.run_saved"].replace("{name}", "Archive forbidden saved query");
  await page.keyboard.press("ControlOrMeta+k");
  await expect(palette).toBeVisible();
  await expect(palette.getByRole("option", { name: english["ui.palette.scan_now"] })).toBeVisible();
  await expect(palette.getByRole("option", { name: english["ui.palette.view.live"], exact: true })).toBeVisible();
  await expect(palette.getByRole("option", { name: runSaved })).toBeVisible();
  await page.keyboard.press("Escape");
  await openArchives(page);
  await page.keyboard.press("ControlOrMeta+k");
  await expect(palette).toBeVisible();
  await expect(palette.getByRole("option", { name: english["ui.palette.scan_now"] })).toHaveCount(0);
  await expect(palette.getByRole("option", { name: english["ui.palette.view.live"], exact: true })).toHaveCount(0);
  await expect(palette.getByRole("option", { name: runSaved })).toHaveCount(0);
  await expect(palette.getByRole("option", { name: english["ui.palette.deadlocks"] })).toHaveCount(0);
  await expect(palette.getByRole("option", { name: "mssql-demo" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.locator(".scan-status")).toHaveCount(0);
  await selectArchive(page);
  await selectArchiveResource(page);
  await holdArchivePhase(page, "incident");
  await page.locator(".archives").getByRole("button", { name: english["ui.archives.investigate"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).incidents.length).toBe(1);
  const old = (await archiveState(page)).incidents[0]!;
  await page.locator(".conn-switch").click();
  await expect(page.locator(".archives")).toHaveCount(0);
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === old.requestId)).toBe(true);
  await releaseArchivePhase(page, "incident", "STALE_LEAVE");
  await expect.poll(async () => (await archiveState(page)).finished.incident).toBe(1);
  await expect(page.getByRole("main")).not.toContainText("STALE_LEAVE");
  await page.getByRole("complementary").getByRole("button", { name: english["ui.archives.title"], exact: true }).click();
  await holdArchivePhase(page, "save");
  await page.locator(".archives").getByRole("button", { name: english["ui.archives.add"], exact: true }).click();
  const form = await fillArchiveForm(page, "Late saved archive");
  await form.getByRole("button", { name: english["ui.common.save"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).drafts.length).toBe(1);
  const reads = (await archiveState(page)).resources.length;
  await page.locator(".conn-switch").click();
  await releaseArchivePhase(page, "save");
  await expect.poll(async () => (await archiveState(page)).finished.save).toBe(1);
  await page.getByRole("complementary").getByRole("button", { name: english["ui.archives.title"], exact: true }).click();
  await expect(page.locator(".archives").getByRole("button", { name: /^Late saved archive ·/ })).toBeVisible();
  expect((await archiveState(page)).resources.length).toBe(reads);
  await expect(archiveIncident(page)).toHaveCount(0);
  await assertNoNativeArchiveWork(page, ["mssql-demo"]);
});

test("cancel during pending registration dispatches no archive read and does not poison the next selection", async ({ page }) => {
  await installArchiveApi(page);
  await openArchives(page);
  await holdArchivePhase(page, "begin");
  await selectArchive(page);
  await expect.poll(async () => (await archiveState(page)).commands.filter((call) => call.command === "begin_warehouse_read").length).toBe(1);
  const before = await archiveState(page);
  const pending = before.commands.find((call) => call.command === "begin_warehouse_read")!;
  await page.locator(".conn-switch").click();
  await releaseArchivePhase(page, "begin");
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === pending.requestId)).toBe(true);
  expect((await archiveState(page)).resources).toHaveLength(0);
  expect((await archiveState(page)).incidents).toHaveLength(0);
  await page.getByRole("complementary").getByRole("button", { name: english["ui.archives.title"], exact: true }).click();
  await selectArchive(page, "Archive Beta");
  await expect(page.locator(".archives").getByRole("button", { name: /^orders-beta ·/ })).toBeVisible();
  const after = await archiveState(page);
  expect(after.resources).toHaveLength(1);
  expect(after.resources[0]!.connection.name).toBe("Archive Beta");
  expect(after.resources[0]!.requestId).not.toBe(pending.requestId);
  await assertNoNativeArchiveWork(page);
});
