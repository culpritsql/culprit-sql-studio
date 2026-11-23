import { expect, test } from "@playwright/test";
import english from "../../locales/en.json" with { type: "json" };
import { assertSourceFreeEvents, cancelNextEvent, eventDialog, eventOpener, eventState, holdEvent, installEventApi, releaseEvent } from "./native-events";

for (const populated of [false, true]) {
  test(`exported-event dialog owns Enter, focus, Tab and Escape from ${populated ? "populated" : "empty"} Plans`, async ({ page }) => {
    await installEventApi(page);
    if (populated) {
      await page.getByRole("button", { name: "PostgreSQL PostgreSQL", exact: true }).click();
      await page.getByRole("button", { name: english["ui.plans.one_plan"], exact: true }).click();
      await expect(page.locator(".plans-single")).toBeVisible();
      await expect(page.locator(".plans-list").getByRole("button", { name: english["native_events.import.open"], exact: true })).toBeVisible();
    } else await expect(page.locator(".plans-welcome")).toBeVisible();
    const opener = eventOpener(page), dialog = eventDialog(page);
    await opener.focus();
    await page.keyboard.press("Enter");
    await expect(dialog).toBeVisible();
    const choose = dialog.getByRole("button", { name: english["native_events.import.choose"], exact: true });
    const close = dialog.getByRole("button", { name: english["ui.common.close"], exact: true });
    await expect(choose).toBeFocused();
    await page.keyboard.press("Shift+Tab");
    await expect(close).toBeFocused();
    await page.keyboard.press("Tab");
    await expect(choose).toBeFocused();
    await close.focus();
    await page.keyboard.press("Tab");
    await expect(choose).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await expect(opener).toBeFocused();
    await page.keyboard.press("Space");
    await expect(dialog).toBeVisible();
    await expect(choose).toBeFocused();
    // Ambient focus cannot escape the same shared modal owner.
    await opener.evaluate((node: HTMLButtonElement) => node.focus());
    await expect(dialog).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await expect(opener).toBeFocused();
    expect((await eventState(page)).reads).toEqual([]);
    await assertSourceFreeEvents(page);
  });
}

test("choose via native Enter and cancel returns no provisional artifact or database fallback", async ({ page }) => {
  await installEventApi(page);
  await eventOpener(page).click();
  const dialog = eventDialog(page);
  await cancelNextEvent(page);
  await dialog.getByRole("button", { name: english["native_events.import.choose"], exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect.poll(async () => (await eventState(page)).finished.length).toBe(1);
  await expect(dialog).toBeVisible();
  await expect(dialog.locator("article")).toHaveCount(0);
  await expect(dialog.getByRole("alert")).toHaveCount(0);
  await dialog.getByRole("button", { name: english["native_events.import.choose"], exact: true }).click();
  await expect(dialog).toContainText("SELECTED_EVENT_2");
  const entries = dialog.locator(".observation-list > details").last();
  await entries.locator(":scope > summary").focus();
  await page.keyboard.press("Enter");
  await expect(dialog.locator("article")).toHaveCount(2);
  await expect(dialog).toContainText("2026-10-25T03:30:00");
  await expect(dialog.locator("img, script, button.danger")).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(eventOpener(page)).toBeFocused();
  await assertSourceFreeEvents(page);
});

test("Escape cancels an admitted import and late IPC cannot replace a newly selected artifact", async ({ page }) => {
  await installEventApi(page);
  await eventOpener(page).click();
  await holdEvent(page, "read");
  await eventDialog(page).getByRole("button", { name: english["native_events.import.choose"], exact: true }).click();
  await expect.poll(async () => (await eventState(page)).reads.length).toBe(1);
  const old = (await eventState(page)).reads[0]!;
  await page.keyboard.press("Escape");
  await expect(eventDialog(page)).toHaveCount(0);
  await expect(eventOpener(page)).toBeFocused();
  await expect.poll(async () => (await eventState(page)).commands.some((call) => call.command === "cancel_native_events" && call.requestId === old)).toBe(true);
  await page.keyboard.press("Enter");
  await eventDialog(page).getByRole("button", { name: english["native_events.import.choose"], exact: true }).click();
  await expect(eventDialog(page)).toContainText("SELECTED_EVENT_2");
  await releaseEvent(page, "read", "STALE_EVENT_MUST_NOT_APPEAR");
  await expect.poll(async () => (await eventState(page)).finished.length).toBe(2);
  await expect(eventDialog(page)).toContainText("SELECTED_EVENT_2");
  await expect(eventDialog(page)).not.toContainText("STALE_EVENT_MUST_NOT_APPEAR");
  await assertSourceFreeEvents(page);
});

test("Escape during registration starts no file read and a subsequent selection remains usable", async ({ page }) => {
  await installEventApi(page);
  await eventOpener(page).click();
  await holdEvent(page, "begin");
  await eventDialog(page).getByRole("button", { name: english["native_events.import.choose"], exact: true }).click();
  await expect.poll(async () => (await eventState(page)).commands.filter((call) => call.command === "begin_native_events").length).toBe(1);
  await page.keyboard.press("Escape");
  await expect(eventOpener(page)).toBeFocused();
  await releaseEvent(page, "begin");
  await expect.poll(async () => (await eventState(page)).commands.filter((call) => call.command === "cancel_native_events").length).toBe(1);
  expect((await eventState(page)).reads).toEqual([]);
  await page.keyboard.press("Enter");
  await eventDialog(page).getByRole("button", { name: english["native_events.import.choose"], exact: true }).click();
  await expect(eventDialog(page)).toContainText("SELECTED_EVENT_1");
  await assertSourceFreeEvents(page);
});
