import type { CollectionAdmissionRecord } from "../../gen/CollectionAdmissionRecord";
import type { CollectionAdmissionStatus } from "../../gen/CollectionAdmissionStatus";
import type { ProviderAcquisitionReport } from "../../gen/ProviderAcquisitionReport";
import type { ObservedScanAttempt } from "../../gen/ObservedScanAttempt";
import type { Dashboard } from "../../gen/Dashboard";
import type { Drift } from "../../gen/Drift";
import type { IndexAdvice } from "../../gen/IndexAdvice";
import type { ExportFile } from "../../gen/ExportFile";
import type { ExportFormat } from "../../gen/ExportFormat";
import type { ExportOptions } from "../../gen/ExportOptions";
import type { NativeIdentity } from "../../gen/NativeIdentity";
import type { Report } from "../../gen/Report";
import type { Verification } from "../../gen/Verification";
import type { Invoke } from "../owned-requests";
import { requireScan } from "../provider-acquisition";

export interface ReportsApi {
  collectionAdmissionRecord(): Promise<CollectionAdmissionRecord>;
  acknowledgeCollectionQuiescence(observationId: string, sourceQuiescent: true): Promise<CollectionAdmissionStatus>;
  latestReport(name: string, windowMinutes: number | null, population: NativeIdentity | null): Promise<Report | null>;
  reportAt(name: string, reportId: string): Promise<Report>;
  scan(name: string, windowMinutes: number | null, population: NativeIdentity | null, acquisition?: (report: ProviderAcquisitionReport) => void): Promise<Report>;
  brief(name: string, reportId: string, findingId: string): Promise<string>;
  /** What differs between the latest scans of two connections of the same engine. */
  drift(a: string, b: string): Promise<Drift>;
  /** The index advisor for the displayed report: indexes to add, to clean up, and tables to look at. */
  indexAdvice(name: string, reportId: string): Promise<IndexAdvice>;
  /** The dashboard for the latest report with this window (no live read; the app polls sessions itself). */
  dashboard(name: string, windowMinutes: number | null, reportId?: string): Promise<Dashboard>;
  /** Re-scan and compare with the displayed exact report, finding by finding. */
  verifyFindings(name: string, reportId: string, ids: string[]): Promise<Verification>;
  exportFinding(name: string, reportId: string, findingId: string, format: ExportFormat, options: ExportOptions): Promise<ExportFile>;
}

export function desktopReports(invoke: Invoke): ReportsApi {
  return {
    collectionAdmissionRecord: () => invoke("collection_admission_record"),
    acknowledgeCollectionQuiescence: (observationId, sourceQuiescent) => invoke("acknowledge_collection_quiescence", { observationId, sourceQuiescent }),
    latestReport: (name, windowMinutes, population) => invoke("latest_report", { name, windowMinutes, population }),
    reportAt: (name, reportId) => invoke("report_at", { name, reportId }),
    scan: async (name, windowMinutes, population, acquisition) => requireScan(
      await invoke<ObservedScanAttempt>("scan", { name, windowMinutes, population }), acquisition,
    ),
    brief: (name, reportId, findingId) => invoke("brief", { name, reportId, findingId }),
    drift: (a, b) => invoke("drift", { a, b }),
    indexAdvice: (name, reportId) => invoke("index_advice", { name, reportId }),
    dashboard: (name, windowMinutes, reportId) => invoke("dashboard", { name, windowMinutes, reportId }),
    verifyFindings: (name, reportId, ids) => invoke("verify_findings", { name, reportId, ids }),
    exportFinding: (name, reportId, findingId, format, options) => invoke("export_finding", { name, reportId, findingId, format, options }),
  };
}
