import type {
  PublishedResourceRequest,
  PublishedResourceSelector,
  PublishedResources,
  WarehouseArtifactQuery,
  WarehouseArtifactReport,
  WarehouseConnectionInfo,
  WarehouseConnectionRef,
  WarehouseFleetReport,
  WarehouseFleetRequest,
  WarehouseIncidentView,
  WarehouseWindowQuery,
  WarehouseWindowReport,
} from "./types.js";
export type * from "./types.js";

export type ArchiveTransport = <T>(
  method: "GET" | "POST",
  path: string,
  body?: unknown,
  signal?: AbortSignal,
) => Promise<T>;
export interface ArchiveReader {
  warehouseConnections(
    signal?: AbortSignal,
  ): Promise<WarehouseConnectionInfo[]>;
  warehouseFleet(
    connection: WarehouseConnectionRef,
    request: WarehouseFleetRequest,
    signal?: AbortSignal,
  ): Promise<WarehouseFleetReport>;
  warehouseResources(
    connection: WarehouseConnectionRef,
    request: PublishedResourceRequest,
    signal?: AbortSignal,
  ): Promise<PublishedResources>;
  warehouseIncident(
    connection: WarehouseConnectionRef,
    resource: PublishedResourceSelector,
    around: string,
    signal?: AbortSignal,
  ): Promise<WarehouseIncidentView>;
  warehouseWindow(
    connection: WarehouseConnectionRef,
    request: WarehouseWindowQuery,
    signal?: AbortSignal,
  ): Promise<WarehouseWindowReport>;
  warehouseArtifacts(
    connection: WarehouseConnectionRef,
    request: WarehouseArtifactQuery,
    signal?: AbortSignal,
  ): Promise<WarehouseArtifactReport>;
}
async function sameOrigin<T>(
  method: "GET" | "POST",
  path: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(path, {
    method,
    signal,
    credentials: "same-origin",
    headers:
      body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json() as Promise<T>;
}
export function createArchiveClient(
  send: ArchiveTransport = sameOrigin,
): ArchiveReader {
  return {
    warehouseConnections: (signal) =>
      send("GET", "/api/archive/connections", undefined, signal),
    warehouseFleet: (connection, request, signal) =>
      send("POST", "/api/archive/fleet", { connection, request }, signal),
    warehouseResources: (connection, request, signal) =>
      send("POST", "/api/archive/resources", { connection, request }, signal),
    warehouseIncident: (connection, resource, around, signal) =>
      send(
        "POST",
        "/api/archive/incident",
        { connection, resource, around },
        signal,
      ),
    warehouseWindow: (connection, request, signal) =>
      send("POST", "/api/archive/window", { connection, request }, signal),
    warehouseArtifacts: (connection, request, signal) =>
      send("POST", "/api/archive/artifacts", { connection, request }, signal),
  };
}
