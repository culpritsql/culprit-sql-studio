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
