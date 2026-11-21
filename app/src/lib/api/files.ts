import type { IncidentHistoryReport } from "../../gen/IncidentHistoryReport";
import type { ConnectionInfo } from "../../gen/ConnectionInfo";
import { ownedRequests, type Invoke } from "../owned-requests";

/** Part of the window in CSS pixels, from its top left corner. */
export type ScreenArea = { x: number; y: number; width: number; height: number };

export interface FilesApi {
  /** Writes into the Downloads folder and returns the path; the browser demo downloads instead. */
  saveFile(name: string, contents: string): Promise<string>;
  /** Copies a PNG of `area`, exactly as the window shows it, to the clipboard. Desktop app on macOS only. */
  screenshot(area: ScreenArea): Promise<void>;
  /** Pick that log in the system's file dialog (desktop only); null when the person cancelled. */
  chooseQueryLog(name: string): Promise<ConnectionInfo | null>;
  /** Explicit local OperationalSamples JSONL, structurally checked and never source-authenticated. */
  openIncidentHistory(signal?: AbortSignal): Promise<IncidentHistoryReport | null>;
  /** Pick a SQLite file in the system's file dialog (desktop only): its path, for the new-connection input. */
  chooseDatabaseFile(): Promise<string | null>;
  chooseSshIdentity(): Promise<string | null>;
  chooseCaCertificate(): Promise<string | null>;
}

export function desktopFiles(invoke: Invoke): FilesApi {
  const history = ownedRequests(invoke, "begin_incident_history", "cancel_incident_history");
  return {
    saveFile: (name, contents) => invoke("save_file", { name, contents }),
    screenshot: (area) => invoke("screenshot", { area }),
    chooseQueryLog: (name) => invoke("choose_query_log", { name }),
    openIncidentHistory: (signal) => history("open_incident_history", {}, signal),
    chooseDatabaseFile: () => invoke("choose_database_file"),
    chooseSshIdentity: () => invoke("choose_ssh_identity"),
    chooseCaCertificate: () => invoke("choose_ca_certificate"),
  };
}
