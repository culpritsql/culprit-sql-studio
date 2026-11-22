import type { WarehouseConnectionInfo } from "../../gen/WarehouseConnectionInfo";
import type { WarehouseConnectionRef } from "../../gen/WarehouseConnectionRef";
import type { SaveWarehouseConnection } from "../../gen/SaveWarehouseConnection";
import type { PublishedResourceRequest } from "../../gen/PublishedResourceRequest";
import type { PublishedResourceSelector } from "../../gen/PublishedResourceSelector";
import type { PublishedResources } from "../../gen/PublishedResources";
import type { WarehouseIncidentView } from "../../gen/WarehouseIncidentView";
import { ownedRequests, type Invoke } from "../owned-requests";
export interface WarehousesApi {
  warehouseConnections(): Promise<WarehouseConnectionInfo[]>;
  addWarehouseConnection(input: SaveWarehouseConnection): Promise<WarehouseConnectionInfo>;
  removeWarehouseConnection(connection: WarehouseConnectionRef): Promise<void>;
  warehouseResources(connection: WarehouseConnectionRef, request: PublishedResourceRequest, signal?: AbortSignal): Promise<PublishedResources>;
  warehouseIncident(connection: WarehouseConnectionRef, resource: PublishedResourceSelector, around: string, signal?: AbortSignal): Promise<WarehouseIncidentView>;
}
export function desktopWarehouses(invoke: Invoke): WarehousesApi {
  const read = ownedRequests(invoke, "begin_warehouse_read", "cancel_warehouse_read");
  return {
    warehouseConnections: () => invoke("warehouse_connections"),
    addWarehouseConnection: (input) => invoke("add_warehouse_connection", { input }),
    removeWarehouseConnection: (connection) => invoke("remove_warehouse_connection", { connection }),
    warehouseResources: (connection, request, signal) => read("warehouse_resources", { connection, request }, signal),
    warehouseIncident: (connection, resource, around, signal) => read("warehouse_incident", { connection, resource, around }, signal),
  };
}
