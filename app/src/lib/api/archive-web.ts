import type { Api, ApiError } from "../api";
import { demoApi } from "../demo-api";
import { language, t } from "../i18n";

function readOnly<T>(): Promise<T> {
  const err: ApiError = { code: "permission_denied", message: t("archive_web.http.read_only"), hint: null };
  return Promise.reject(err);
}

export async function archiveWebApi(): Promise<Api> {
  const fallback = await demoApi();
  let currentLang = language();

  async function requestJson<T>(method: "GET" | "POST", url: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, {
      method,
      headers: {
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
        "x-culprit-lang": currentLang,
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal,
    });
    const payload: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (typeof payload === "object" && payload !== null && "code" in payload && "message" in payload) {
        throw payload as ApiError;
      }
      const fallbackError: ApiError = {
        code: response.status === 401 || response.status === 403 ? "permission_denied" : "internal",
        message: `HTTP ${response.status}`,
        hint: null,
      };
      throw fallbackError;
    }
    return payload as T;
  }

  return {
    ...fallback,
    kind: "archive-web",
    connections: async () => [],
    addConnection: readOnly,
    removeConnection: readOnly,
    renameConnection: readOnly,
    setConnectionSettings: readOnly,
    setPassword: readOnly,
    setSsh: readOnly,
    importConnections: readOnly,
    addDiscovered: readOnly,
    addWarehouseConnection: readOnly,
    removeWarehouseConnection: readOnly,
    warehouseConnections: () => requestJson("GET", "/api/archive/connections"),
    warehouseFleet: (connection, request, signal) => requestJson("POST", "/api/archive/fleet", { connection, request }, signal),
    warehouseResources: (connection, request, signal) => requestJson("POST", "/api/archive/resources", { connection, request }, signal),
    warehouseIncident: (connection, resource, around, signal) =>
      requestJson("POST", "/api/archive/incident", { connection, resource, around }, signal),
    warehouseWindow: (connection, request, signal) => requestJson("POST", "/api/archive/window", { connection, request }, signal),
    warehouseArtifacts: (connection, request, signal) => requestJson("POST", "/api/archive/artifacts", { connection, request }, signal),
    parsePlan: (text) => requestJson("POST", "/api/plans/parse", { text }),
    diffPlans: (a, b, nameA, nameB) =>
      requestJson("POST", "/api/plans/diff", { a: { ...a, raw: null }, b: { ...b, raw: null }, nameA, nameB }),
    exportPlan: (plan, title, format, options) =>
      requestJson("POST", "/api/plans/export-plan", { plan: format === "native" ? plan : { ...plan, raw: null }, title, format, options }),
    exportDiff: (a, b, nameA, nameB, format, options) =>
      requestJson("POST", "/api/plans/export-diff", { a: { ...a, raw: null }, b: { ...b, raw: null }, nameA, nameB, format, options }),
    setLanguage: async (code) => {
      currentLang = code;
    },
  };
}
