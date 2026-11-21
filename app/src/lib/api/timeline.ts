import type { ChangeRequest } from "../../gen/ChangeRequest";
import type { Changes } from "../../gen/Changes";
import type { CloudSignals } from "../../gen/CloudSignals";
import type { EventLog } from "../../gen/EventLog";
import type { EventsRequest } from "../../gen/EventsRequest";
import type { Mark } from "../../gen/Mark";
import type { Invoke } from "../owned-requests";

export interface TimelineApi {
  /** What changed between two periods: Query Store intervals, or Culprit's own scans. */
  changes(name: string, request: ChangeRequest): Promise<Changes>;
  /** What changed around a connection: marks, schema, settings, restarts and cloud throttling. */
  events(name: string, request: EventsRequest): Promise<EventLog>;
  /** What the cloud measured for the server (CloudWatch, Azure Monitor); `unsupported` for other hosts. */
  cloud(name: string, request: EventsRequest): Promise<CloudSignals>;
  /** Put a moment (a deploy, a migration) on the connection's timeline. */
  mark(name: string, text: string, at: string | null): Promise<Mark>;
  unmark(name: string, id: string): Promise<Mark>;
}

export function desktopTimeline(invoke: Invoke): TimelineApi {
  return {
    changes: (name, request) => invoke("changes", { name, request }),
    events: (name, request) => invoke("events", { name, request }),
    cloud: (name, request) => invoke("cloud", { name, request }),
    mark: (name, text, at) => invoke("mark", { name, text, at }),
    unmark: (name, id) => invoke("unmark", { name, id }),
  };
}
