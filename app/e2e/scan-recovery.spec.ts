import { expect, test } from "@playwright/test";
import english from "../../locales/en.json" with { type: "json" };
import { openConnection, waitForReport } from "./demo";

for (const saved of [false, true]) {
  test(`long scan errors keep the retry and the original response accessible ${saved ? "with" : "without"} a saved report`, async ({ page }) => {
    await page.setViewportSize({ width: 960, height: 600 });
    await openConnection(page, "mssql-demo");
    await waitForReport(page);
    const message = `Client IP denied.\n${"a".repeat(5000)}\nEnd of response.`;
    const hint = english["provider.mssql.error.firewall.hint"];
    await page.evaluate(async ({ saved, message, hint }) => {
      const storePath = "/src/lib/store.ts", apiPath = "/src/lib/api.ts";
      const { getState, setState } = await import(storePath) as typeof import("../src/lib/store");
      const { api } = await import(apiPath) as typeof import("../src/lib/api");
      const ready = getState().report;
      if (ready.status !== "ready") throw new Error("missing fixture report");
      const error = { code: "connection_failed", message, hint };
      (await api()).scan = async () => ready.report;
      setState({ report: saved ? { ...ready, scanError: error } : { status: "error", error } });
    }, { saved, message, hint });
    if (saved) {
      const notice = page.locator(".saved-scan-notice > summary");
      await expect(notice).toContainText(hint);
      await notice.click();
    }
    const panel = page.locator(".scan-failure");
    const retry = panel.getByRole("button", { name: english["ui.common.try_again"] });
    await expect(retry).toBeInViewport({ ratio: 1 });
    const details = panel.getByText(english["ui.scan_recovery.full_error"], { exact: true });
    await details.focus();
    await page.keyboard.press("Enter");
    const original = panel.getByRole("textbox", { name: english["ui.scan_recovery.full_error"] });
    await expect(original).toHaveValue(message);
    expect(await original.evaluate((el) => el.scrollWidth === el.clientWidth && el.scrollHeight > el.clientHeight)).toBe(true);
    await retry.click();
    await expect(panel).toHaveCount(0);
  });
}

for (const saved of [false, true]) {
  test(`a paused scan recovers the exact observation ${saved ? "alongside a saved report" : "without a saved report"}`, async ({ page }, info) => {
    await openConnection(page, "mssql-demo");
    await waitForReport(page);
    await page.evaluate(async (hasSaved) => {
      const apiPath = "/src/lib/api.ts", storePath = "/src/lib/store.ts", fixturePath = "/src/test/provider-acquisition.ts";
      const { api } = await import(apiPath) as typeof import("../src/lib/api");
      const { getState, setState } = await import(storePath) as typeof import("../src/lib/store");
      const { currentAcquisition } = await import(fixturePath) as typeof import("../src/test/provider-acquisition");
      const client = await api();
      const ready = getState().report;
      if (ready.status !== "ready") throw new Error("missing fixture report");
      const error = { code: "query_failed", message: "native failure", hint: "CLI-only hint", detail: { "collection-stopped": "cancellation-unconfirmed" as const } };
      const acquisition = currentAcquisition(0);
      let pending = "a".repeat(32);
      let acknowledged = false;
      const record = () => ({
        status: { observation_id: pending, source_quiescence_required: true, route: { kind: "saved-connection" as const, name: "original-connection" } },
        recorded_at_unix_seconds: 1795777200,
      });
      client.collectionAdmissionRecord = async () => record();
      client.acknowledgeCollectionQuiescence = async (id, verified) => {
        if (id !== pending || !verified) throw new Error("wrong observation");
        if (pending.startsWith("a")) {
          pending = "b".repeat(32);
          throw new Error("The pending observation changed.");
        }
        acknowledged = true;
        return { observation_id: null, route: null, source_quiescence_required: false };
      };
      client.scan = async () => {
        if (!acknowledged) throw new Error("scanned before acknowledgement");
        return ready.report;
      };
      setState({ report: hasSaved ? { ...ready, scanError: error, scanAcquisition: acquisition } : { status: "error", error, acquisition } });
    }, saved);
    if (saved) {
      await expect(page.locator('[data-tile="health"]')).toBeVisible();
      await page.locator(".saved-scan-notice > summary").click();
    }
    const panel = page.locator(".scan-failure");
    await expect(panel).toContainText("original-connection");
    await expect(panel).toContainText("a".repeat(32));
    await expect(panel).not.toContainText("CLI-only hint");
    await expect(panel.getByText(english["view.provider_acquisition.title"], { exact: true }).first()).not.toBeVisible();
    const resume = panel.getByRole("button", { name: english["ui.scan_recovery.resume"] });
    await expect(resume).toBeDisabled();
    await page.screenshot({ path: info.outputPath("scan-recovery.png") });
    await panel.getByRole("checkbox").check();
    await resume.click();
    await expect(panel.getByRole("alert")).toHaveText("The pending observation changed.");
    await panel.getByRole("button", { name: english["ui.scan_recovery.refresh"] }).click();
    await expect(panel).toContainText("b".repeat(32));
    await expect(resume).toBeDisabled();
    await panel.getByRole("checkbox").check();
    await resume.click();
    await waitForReport(page);
    await expect(panel).toHaveCount(0);
    await expect(page.locator('[data-tile="health"]')).toBeVisible();
  });
}
