import type { Report } from "../gen/Report";
import { type DeepPartial, partial } from "./fixtures";

/** Required Report collections shared by Health fixtures; each signal keeps its own source and evidence fields. */
export function healthReportFixture(fields: DeepPartial<Report>): Report {
  return partial<Report>({
    source_acquisitions: [],
    performance_counters: [],
    index_configuration: [],
    indexed_views: [],
    ...fields,
    change_capture: { ...fields.change_capture },
    snapshot: {
      ...fields.snapshot,
      operations: {
        source_acquisitions: [], process_cpu_originals: [], spaces: [], index_configuration: [], indexed_views: null, indexed_view_originals: [], counters: [], sequences: [], multixacts: [], sqlite: null, cluster: null, numa_nodes: [],
        ...fields.snapshot?.operations,
        change_capture: { cdc: [], change_feed: [], ...fields.snapshot?.operations?.change_capture },
      },
    },
  });
}
