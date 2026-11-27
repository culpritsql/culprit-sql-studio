import type { ArchiveReader } from "../../../../packages/culprit-archive-client/src/index";
import type { SaveWarehouseConnection } from "../../gen/SaveWarehouseConnection";
import type { WarehouseConnectionInfo } from "../../gen/WarehouseConnectionInfo";
import type { WarehouseConnectionRef } from "../../gen/WarehouseConnectionRef";
import { ownedRequests, type Invoke } from "../owned-requests";
export interface WarehousesApi extends ArchiveReader {
  addWarehouseConnection(input: SaveWarehouseConnection): Promise<WarehouseConnectionInfo>;
  removeWarehouseConnection(connection: WarehouseConnectionRef): Promise<void>;
}
export function desktopWarehouses(invoke: Invoke): WarehousesApi {
  const read = ownedRequests(invoke, "begin_warehouse_read", "cancel_warehouse_read");
  return {
    warehouseConnections: () => invoke("warehouse_connections"),
    addWarehouseConnection: (input) => invoke("add_warehouse_connection", { input }),
    removeWarehouseConnection: (connection) => invoke("remove_warehouse_connection", { connection }),
    warehouseFleet: (connection, request, signal) => read("warehouse_fleet", { connection, request }, signal),
    warehouseResources: (connection, request, signal) => read("warehouse_resources", { connection, request }, signal),
    warehouseWindow: (connection, request, signal) => read("warehouse_window", { connection, request }, signal),
    warehouseArtifacts: (connection, request, signal) => read("warehouse_artifacts", { connection, request }, signal),
    warehouseIncident: (connection, resource, around, signal) => read("warehouse_incident", { connection, resource, around }, signal),
  };
}
