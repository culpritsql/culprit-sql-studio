import { expect, test } from "bun:test";
import { archiveSearchWindowInputs, archiveUtcInput } from "./archive-time";

test("default search windows contain instantaneous and short originals instead of collapsing to the same minute", () => {
  for (const [from, until] of [
    ["2026-11-22T10:00:00Z", "2026-11-22T10:00:00Z"],
    ["2026-11-22T10:00:00Z", "2026-11-22T10:00:01Z"],
    ["2026-11-22T10:00:59.9999999Z", "2026-11-22T10:00:59.9999999Z"],
  ]) {
    const inputs = archiveSearchWindowInputs(from!, until!);
    expect(inputs).toEqual({ from: "2026-11-22T09:01", until: "2026-11-22T10:01" });
    const lower = archiveUtcInput(inputs!.from)!, upper = archiveUtcInput(inputs!.until)!;
    expect(Date.parse(lower)).toBeLessThanOrEqual(Date.parse(from!));
    expect(Date.parse(upper)).toBeGreaterThan(Date.parse(until!));
    expect(Date.parse(upper) - Date.parse(lower)).toBe(3_600_000);
  }
});

test("longer originals keep their complete extent, while invalid or unrepresentable search bounds stay unavailable", () => {
  expect(archiveSearchWindowInputs("2026-11-21T08:00:00.1234567Z", "2026-11-22T10:00:00Z"))
    .toEqual({ from: "2026-11-21T08:00", until: "2026-11-22T10:01" });
  for (const [from, until] of [
    ["unknown", "2026-11-22T10:00:00Z"],
    ["2026-11-22T10:00:00Z", "unknown"],
    ["2026-11-22T10:00:01Z", "2026-11-22T10:00:00Z"],
    ["9999-12-31T23:59:59Z", "9999-12-31T23:59:59Z"],
  ]) expect(archiveSearchWindowInputs(from!, until!)).toBeNull();
});
