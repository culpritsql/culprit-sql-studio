import { createArchiveClient, createArchivePlanClient } from "../../../../packages/culprit-archive-client/src/index";
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
    ...createArchiveClient(requestJson),
    ...createArchivePlanClient(requestJson),
    setLanguage: async (code) => {
      currentLang = code;
    },
  };
}
