import type { Deadlock } from "../../gen/Deadlock";
import type { Dashboard } from "../../gen/Dashboard";
import type { DashboardLive } from "../../gen/DashboardLive";
import type { DeadlockReport } from "../../gen/DeadlockReport";
import type { LiveState } from "../../gen/LiveState";
import type { WaitSample } from "../../gen/WaitSample";
import type { WaitsReport } from "../../gen/WaitsReport";
import type { SessionActionRequest } from "../../gen/SessionActionRequest";
import type { SessionActionOutcome } from "../../gen/SessionActionOutcome";
import type { Invoke } from "../owned-requests";

export interface ActivityApi {
  /** Refresh sessions and their assessment while retaining this report's selected window. */
  dashboardLive(name: string, dashboard: Dashboard): Promise<DashboardLive>;
  live(name: string): Promise<LiveState>;
  /** Where the latest scan's database time went, by wait category with the queries behind each. */
  waits(name: string, windowMinutes: number | null): Promise<WaitsReport | null>;
  /** What the server waits on right now, sampled for `seconds`. */
  waitSample(name: string, seconds: number): Promise<WaitSample>;
  interruptSession(request: SessionActionRequest): Promise<SessionActionOutcome>;
  /** Recent deadlocks on a connection, explained. */
  deadlocks(name: string, sinceHours: number): Promise<DeadlockReport>;
  /** Deadlocks in pasted text: SQL Server .xdl XML, InnoDB status, PostgreSQL log lines. */
  parseDeadlock(text: string): Promise<Deadlock[]>;
}

export function desktopActivity(invoke: Invoke): ActivityApi {
  return {
    dashboardLive: (name, dashboard) => invoke("dashboard_live", { name, dashboard }),
    live: (name) => invoke("live", { name }),
    waits: (name, windowMinutes) => invoke("waits", { name, windowMinutes }),
    waitSample: (name, seconds) => invoke("wait_sample", { name, seconds }),
    interruptSession: (request) => invoke("interrupt_session", { request }),
    deadlocks: (name, sinceHours) => invoke("deadlocks", { name, sinceHours }),
    parseDeadlock: (text) => invoke("parse_deadlock", { text }),
  };
}
