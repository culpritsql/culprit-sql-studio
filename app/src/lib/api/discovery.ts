import type { ConnectionInfo } from "../../gen/ConnectionInfo";
import type { DiscoveryResult } from "../../gen/DiscoveryResult";
import type { DiscoveredDatabase } from "../../gen/DiscoveredDatabase";
import type { Invoke } from "../owned-requests";
import { discoveryRequests } from "../discovery-requests";

export interface DiscoveryApi {
  discoverAzure(terms: string[], signal?: AbortSignal): Promise<DiscoveryResult>;
  discoverAws(terms: string[], regions: string[], signal?: AbortSignal): Promise<DiscoveryResult>;
  addDiscovered(items: DiscoveredDatabase[]): Promise<ConnectionInfo[]>;
}

export function desktopDiscovery(invoke: Invoke): DiscoveryApi {
  const discover = discoveryRequests(invoke);
  return {
    discoverAzure: (terms, signal) => discover("discover_azure", { terms }, signal),
    discoverAws: (terms, regions, signal) => discover("discover_aws", { terms, regions }, signal),
    addDiscovered: (items) => invoke("add_discovered", { items }),
  };
}
