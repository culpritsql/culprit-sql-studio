/** The form explicitly requests UTC. Offset-free native labels never pass through this UI input converter. */
export function archiveUtcInput(value: string): string | null {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value)) return null;
  const parsed = new Date(`${value}:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 16) === value ? parsed.toISOString() : null;
}
export function archiveTimeInput(utc: string): string {
  const at = new Date(utc);
  return Number.isFinite(at.getTime()) ? at.toISOString().slice(0, 16) : "";
}

const MINUTE_MS = 60_000;
const DEFAULT_SEARCH_WINDOW_MS = 60 * MINUTE_MS;

/** Human search bounds surround the selected observation; its original clocks remain unchanged. */
export function archiveSearchWindowInputs(from: string, until: string): { from: string; until: string } | null {
  const first = Date.parse(from), last = Date.parse(until);
  if (!Number.isFinite(first) || !Number.isFinite(last) || first > last) return null;
  // An exclusive upper bound must also contain an instantaneous or sub-minute observation.
  const upper = (Math.floor(last / MINUTE_MS) + 1) * MINUTE_MS;
  const lower = Math.min(Math.floor(first / MINUTE_MS) * MINUTE_MS, upper - DEFAULT_SEARCH_WINDOW_MS);
  if (!Number.isFinite(new Date(lower).getTime()) || !Number.isFinite(new Date(upper).getTime())) return null;
  const result = { from: archiveTimeInput(new Date(lower).toISOString()), until: archiveTimeInput(new Date(upper).toISOString()) };
  return archiveUtcInput(result.from) && archiveUtcInput(result.until) ? result : null;
}
