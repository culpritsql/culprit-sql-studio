import type { ProviderAcquisitionReport } from "../../gen/ProviderAcquisitionReport";
import type { ObservedProbeAttempt } from "../../gen/ObservedProbeAttempt";
import type { ConnectionSettings } from "../../gen/ConnectionSettings";
import type { ConnectionInfo } from "../../gen/ConnectionInfo";
import type { ImportCandidate } from "../../gen/ImportCandidate";
import type { ImportSource } from "../../gen/ImportSource";
import type { NewConnection } from "../../gen/NewConnection";
import type { ParsedConnection } from "../../gen/ParsedConnection";
import type { SshConfig } from "../../gen/SshConfig";
import type { ServerInfo } from "../../gen/ServerInfo";
import type { Engine } from "../../gen/Engine";
import { ownedRequests, type Invoke } from "../owned-requests";
import { requireProbe } from "../provider-acquisition";

export interface ConnectionsApi {
  connections(): Promise<ConnectionInfo[]>;
  /** What `input` connects to, read as `engine` wherever the text leaves the engine open. */
  parseConnection(input: string, engine: Engine | null, ssh?: boolean): Promise<ParsedConnection>;
  testConnection(input: NewConnection, signal?: AbortSignal, acquisition?: (report: ProviderAcquisitionReport) => void): Promise<ServerInfo>;
  setConnectionSettings(name: string, settings: ConnectionSettings): Promise<ConnectionInfo>;
  testConnectionSettings(name: string, settings: ConnectionSettings, signal?: AbortSignal, acquisition?: (report: ProviderAcquisitionReport) => void): Promise<ServerInfo>;
  addConnection(input: NewConnection): Promise<ConnectionInfo>;
  removeConnection(name: string): Promise<void>;
  renameConnection(old: string, next: string): Promise<ConnectionInfo>;
  setPassword(name: string, password: string): Promise<ConnectionInfo>;
  setSsh(name: string, ssh: SshConfig | null): Promise<ConnectionInfo>;
  testSsh(name: string, ssh: SshConfig | null, signal?: AbortSignal, acquisition?: (report: ProviderAcquisitionReport) => void): Promise<ServerInfo>;
  importCandidates(source: ImportSource | null): Promise<ImportCandidate[]>;
  importConnections(items: ImportCandidate[]): Promise<ConnectionInfo[]>;
  /** The log scans read the statements from when the database keeps none (SQLite); null stops. */
  setQueryLog(name: string, path: string | null): Promise<ConnectionInfo>;
}

export function desktopConnections(invoke: Invoke): ConnectionsApi {
  const connectionTests = ownedRequests(invoke, "begin_connection_test", "cancel_connection_test");
  return {
    connections: () => invoke("connections"),
    parseConnection: (input, engine, ssh = false) => invoke("parse_connection", { input, engine, ssh }),
    testConnection: async (input, signal, acquisition) => requireProbe(
      await connectionTests<ObservedProbeAttempt>("test_connection", { input }, signal), acquisition,
    ),
    setConnectionSettings: (name, settings) => invoke("set_connection_settings", { name, settings }),
    testConnectionSettings: async (name, settings, signal, acquisition) => requireProbe(
      await connectionTests<ObservedProbeAttempt>("test_connection_settings", { name, settings }, signal), acquisition,
    ),
    addConnection: (input) => invoke("add_connection", { input }),
    removeConnection: (name) => invoke("remove_connection", { name }),
    renameConnection: (old, next) => invoke("rename_connection", { old, new: next }),
    setPassword: (name, password) => invoke("set_password", { name, password }),
    setSsh: (name, ssh) => invoke("set_ssh", { name, ssh }),
    testSsh: async (name, ssh, signal, acquisition) => requireProbe(
      await connectionTests<ObservedProbeAttempt>("test_ssh", { name, ssh }, signal), acquisition,
    ),
    importCandidates: (source) => invoke("import_candidates", { source }),
    importConnections: (items) => invoke("import_connections", { items }),
    setQueryLog: (name, path) => invoke("set_query_log", { name, path }),
  };
}
