import initialize, * as native from "./native/culprit_wasm.js";
import type { ActivityClient } from "./models/ActivityClient.js";
import type { CompareContext } from "./models/CompareContext.js";
import type { Deadlock } from "./models/Deadlock.js";
import type { DefinitionPayload } from "./models/DefinitionPayload.js";
import type { Engine } from "./models/Engine.js";
import type { Error as CoreError } from "./models/Error.js";
import type { ExportFile } from "./models/ExportFile.js";
import type { ExportFormat } from "./models/ExportFormat.js";
import type { ExportOptions } from "./models/ExportOptions.js";
import type { ExtendedEventsBatch } from "./models/ExtendedEventsBatch.js";
import type { ExtendedEventsFormat } from "./models/ExtendedEventsFormat.js";
import type { ImportedEventDeadlock } from "./models/ImportedEventDeadlock.js";
import type { ImportedEventLocation } from "./models/ImportedEventLocation.js";
import type { MssqlExtendedEventKind } from "./models/MssqlExtendedEventKind.js";
import type { MssqlExtendedEventObservation } from "./models/MssqlExtendedEventObservation.js";
import type { NativeLogIssue } from "./models/NativeLogIssue.js";
import type { NativeLogLimits } from "./models/NativeLogLimits.js";
import type { NativeLogProblem } from "./models/NativeLogProblem.js";
import type { PayloadUnavailable } from "./models/PayloadUnavailable.js";
import type { Plan } from "./models/Plan.js";
import type { PlanDiff } from "./models/PlanDiff.js";

export type {
  ActivityClient,
  CompareContext,
  CoreError,
  Deadlock,
  DefinitionPayload,
  Engine,
  ExportFile,
  ExportFormat,
  ExportOptions,
  ExtendedEventsBatch,
  ExtendedEventsFormat,
  ImportedEventDeadlock,
  ImportedEventLocation,
  MssqlExtendedEventKind,
  MssqlExtendedEventObservation,
  NativeLogIssue,
  NativeLogLimits,
  NativeLogProblem,
  PayloadUnavailable,
  Plan,
  PlanDiff,
};

/** Initialize the local module once before calling its synchronous pure operations. */
export async function init(input?: Parameters<typeof initialize>[0]): Promise<void> {
  await initialize(input);
}

/** The exact Core error returned by the shared parser/export owner, without a second message catalogue. */
export class ParserError extends Error {
  readonly error: CoreError;

  constructor(error: CoreError) {
    super(error.message);
    this.name = "ParserError";
    this.error = error;
  }
}

// This is serde's Result transport. Domain values and their declarations remain Core-owned.
type JsonResult<T> = { Ok: T } | { Err: CoreError };

function result<T>(encoded: string): T {
  const decoded = JSON.parse(encoded) as JsonResult<T>;
  if ("Err" in decoded) throw new ParserError(decoded.Err);
  return decoded.Ok;
}

/** Parse only supplied text. An engine hint is checked by the existing strict native-format owner. */
export function parsePlan(input: string, engine?: Engine): Plan {
  return result<Plan>(native.parsePlanJson(input, JSON.stringify(engine ?? null)));
}

/** Compare supplied originals. Optional statistics are explicit; no database is opened or discovered. */
export function diffPlans(
  a: Plan,
  nameA: string,
  b: Plan,
  nameB: string,
  context: CompareContext = { statistics: [], taken_at: null },
): PlanDiff {
  return result<PlanDiff>(native.diffPlansJson(JSON.stringify(a), nameA, JSON.stringify(b), nameB, JSON.stringify(context)));
}

/** Parse native deadlock text into the existing Core observations; no source or clock is inferred. */
export function parseDeadlock(input: string): Deadlock[] {
  return result<Deadlock[]>(native.parseDeadlockJson(input));
}

const DEFAULT_EVENT_LIMITS: NativeLogLimits = {
  input_bytes: 8 * 1024 * 1024,
  record_bytes: 256 * 1024,
  field_bytes: 64 * 1024,
  records: 4096,
};

function resolveEventLimits(limits?: Partial<NativeLogLimits>): NativeLogLimits | null {
  if (!limits) return null;
  return { ...DEFAULT_EVENT_LIMITS, ...limits };
}

/** Decode a binary SQL Server Extended Events (.xel) buffer within explicit or default bounds. XML text is refused on this route. */
export function parseXel(input: Uint8Array, limits?: Partial<NativeLogLimits>): ExtendedEventsBatch {
  return result<ExtendedEventsBatch>(native.parseXelJson(input, JSON.stringify(resolveEventLimits(limits))));
}

/** Decode exported SQL Server Extended Events XML (`<events>`, `<RingBufferTarget>`, or `<event>`) within bounds. Binary .xel is refused. */
export function parseExtendedEventsXml(
  input: string | Uint8Array,
  limits?: Partial<NativeLogLimits>,
): ExtendedEventsBatch {
  const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  return result<ExtendedEventsBatch>(native.parseExtendedEventsXmlJson(bytes, JSON.stringify(resolveEventLimits(limits))));
}

/** Keep the existing Markdown/HTML/native redaction and file-naming policy. */
export function exportPlan(plan: Plan, title: string, format: ExportFormat, options: ExportOptions): ExportFile {
  const exportedAt = `${new Date().toISOString().slice(0, 16).replace("T", " ")} UTC`;
  return result<ExportFile>(native.exportPlanJson(JSON.stringify(plan), title, JSON.stringify(format), JSON.stringify(options), exportedAt));
}
