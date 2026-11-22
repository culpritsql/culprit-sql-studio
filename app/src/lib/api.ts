import type { WarehousesApi } from "./api/warehouses";
import type { ConnectionsApi } from "./api/connections";
import type { DiscoveryApi } from "./api/discovery";
import type { AuthenticationApi } from "./api/authentication";
import type { ReportsApi } from "./api/reports";
import type { PlansApi } from "./api/plans";
import type { ActivityApi } from "./api/activity";
import type { TimelineApi } from "./api/timeline";
import type { EditorApi } from "./api/editor";
import type { FilesApi } from "./api/files";
import type { ApprovalsApi } from "./api/approvals";
import type { UpdatesApi } from "./api/updates";
import type { LocalizationApi } from "./api/localization";
export type { UpdateInfo, UpdateProgress } from "./api/updates";
export type { ScreenArea } from "./api/files";
import { lazyResource } from "./requests";
import type { ErrorDetail } from "../gen/ErrorDetail";

export type ApiError = { code: string; message: string; hint: string | null; detail?: ErrorDetail | null };

export interface Api extends WarehousesApi, ConnectionsApi, DiscoveryApi, AuthenticationApi, ReportsApi, PlansApi, ActivityApi, TimelineApi, EditorApi, FilesApi, ApprovalsApi, UpdatesApi, LocalizationApi {
  readonly kind: "desktop" | "demo";
}

export function isApiError(value: unknown): value is ApiError {
  return typeof value === "object" && value !== null && "message" in value && "code" in value;
}

export function errorMessage(value: unknown): ApiError {
  if (isApiError(value)) return value;
  if (value instanceof Error) return { code: "internal", message: value.message, hint: null };
  return { code: "internal", message: String(value), hint: null };
}

export function isDesktop(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

const loadApi = lazyResource<Api>(() => isDesktop() ? import("./api/desktop").then((m) => m.desktopApi()) : import("./demo-api").then((m) => m.demoApi()));
let languageApplication: Promise<void> = Promise.resolve();

/** Apply choices in order; every subsequent API read waits for the latest choice. */
export function applyApiLanguage(code: string): Promise<void> {
  languageApplication = languageApplication.catch(() => {}).then(async () => {
    const client = await loadApi();
    await client.setLanguage(code);
  });
  return languageApplication;
}

export async function api(): Promise<Api> {
  const client = await loadApi();
  for (;;) {
    const applying = languageApplication;
    await applying;
    if (applying === languageApplication) return client;
  }
}

export async function copyText(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text);
  } catch (e) {
    // The async clipboard refuses when the document isn't focused; the older copy command works within the click.
    const area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.cssText = "position:fixed;top:-1000px;opacity:0";
    document.body.appendChild(area);
    area.select();
    const copied = document.execCommand("copy");
    area.remove();
    if (!copied) throw e;
  }
}
