import { expect, test } from "@playwright/test";
import english from "../../locales/en.json" with { type: "json" };
import { installArchiveApi, openArchives, selectArchive, selectArchiveResource, archiveState, holdArchivePhase,
  releaseArchivePhase, assertNoNativeArchiveWork } from "./archives";

test("archive resource and UTC window browse published findings changes and continuation without current source work", async ({ page }) => {
  await installArchiveApi(page); await openArchives(page); await selectArchive(page); await selectArchiveResource(page);
  const root = page.locator(".archives");
  await root.getByLabel(english["ui.window.from"], { exact: true }).fill("2026-11-22T09:00");
  await root.getByLabel(english["ui.window.until"], { exact: true }).fill("2026-11-22T11:00");
  await root.getByRole("button", { name: english["ui.window.answers"], exact: true }).click();
  const result = root.getByRole("region", { name: english["ui.window.title"], exact: true });
  await expect(result).toContainText(english["ui.window.page_scope"]);
  await expect(result).toContainText("FIRST_WINDOW_PAGE");
  await expect(result).toContainText("Recorded <script>finding</script> text");
  await expect(result.locator("script, img")).toHaveCount(0);
  await expect(result.getByRole("button", { name: /apply|execute|interrupt/i })).toHaveCount(0);
  const first = (await archiveState(page)).windows[0]!;
  expect(first.request.view).toBe("published-answers");expect(first.request.after).toBeNull();
  expect(first.request.window).toEqual({ from: "2026-11-22T09:00:00.000Z", until: "2026-11-22T11:00:00.000Z" });
  await root.getByRole("button", { name: english["ui.window.next"], exact: true }).click();
  await expect(result).toContainText("SECOND_WINDOW_PAGE");await expect(result).not.toContainText("FIRST_WINDOW_PAGE");
  const second = (await archiveState(page)).windows[1]!;
  expect(second.request.resource).toEqual(first.request.resource);expect(second.request.window).toEqual(first.request.window);
  expect(second.request.as_of).toBe(first.request.as_of);expect(second.request.after).not.toBeNull();
  await expect(root.getByRole("button", { name: english["ui.window.next"], exact: true })).toHaveCount(0);
  await assertNoNativeArchiveWork(page);
});

test("changing the UTC window or archive cancels and withholds a late published page without replacing selection", async ({ page }) => {
  await installArchiveApi(page); await openArchives(page); await selectArchive(page); await selectArchiveResource(page);
  const root = page.locator(".archives");
  await root.getByLabel(english["ui.window.from"], { exact: true }).fill("2026-11-22T09:00");
  await root.getByLabel(english["ui.window.until"], { exact: true }).fill("2026-11-22T11:00");
  await holdArchivePhase(page, "window");
  await root.getByRole("button", { name: english["ui.window.answers"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).windows.length).toBe(1);
  const first = (await archiveState(page)).windows[0]!;
  await root.getByLabel(english["ui.window.until"], { exact: true }).fill("2026-11-22T12:00");
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === first.requestId)).toBe(true);
  await releaseArchivePhase(page, "window", "STALE_WINDOW_PAGE");
  await expect(root).not.toContainText("STALE_WINDOW_PAGE");
  await root.getByRole("button", { name: english["ui.window.answers"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).windows.length).toBe(2);
  expect((await archiveState(page)).windows[1]!.request.window.until).toBe("2026-11-22T12:00:00.000Z");
  await expect(root.getByRole("region", { name: english["ui.window.title"], exact: true })).toContainText("FIRST_WINDOW_PAGE");
  await assertNoNativeArchiveWork(page);
});

test("1h 24h and 7d window presets bound the selected observation without current source work", async ({ page }) => {
  await installArchiveApi(page); await openArchives(page); await selectArchive(page); await selectArchiveResource(page);
  const root = page.locator(".archives");
  await root.getByLabel(english["ui.window.until"], { exact: true }).fill("2026-11-22T12:00");
  await root.getByRole("button", { name: english["ui.window_picker.short_24h"], exact: true }).click();
  await expect(root.getByLabel(english["ui.window.from"], { exact: true })).toHaveValue("2026-11-21T12:00");
  await expect(root.getByLabel(english["ui.window.until"], { exact: true })).toHaveValue("2026-11-22T12:00");
  await root.getByRole("button", { name: english["ui.window_picker.short_7d"], exact: true }).click();
  await expect(root.getByLabel(english["ui.window.from"], { exact: true })).toHaveValue("2026-11-15T12:00");
  await holdArchivePhase(page, "window");
  await root.getByRole("button", { name: english["ui.window.answers"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).windows.length).toBe(1);
  const pendingId = (await archiveState(page)).windows[0]!.requestId;
  await root.getByRole("button", { name: english["ui.window_picker.short_1h"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).commands.some((call) => call.command === "cancel_warehouse_read" && call.requestId === pendingId)).toBe(true);
  await releaseArchivePhase(page, "window", "STALE_PRESET_PAGE");
  await expect(root).not.toContainText("STALE_PRESET_PAGE");
  await expect(root.getByLabel(english["ui.window.from"], { exact: true })).toHaveValue("2026-11-22T11:00");
  await root.getByRole("button", { name: english["ui.window.answers"], exact: true }).click();
  await expect.poll(async () => (await archiveState(page)).windows.length).toBe(2);
  expect((await archiveState(page)).windows[1]!.request.window).toEqual({ from: "2026-11-22T11:00:00.000Z", until: "2026-11-22T12:00:00.000Z" });
  await assertNoNativeArchiveWork(page);
});
