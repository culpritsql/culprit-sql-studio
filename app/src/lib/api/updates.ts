import type { Invoke } from "../owned-requests";

/** A newer published release than the running app. */
export type UpdateInfo = { version: string; current: string; notes: string | null };
export type UpdateProgress = { downloaded: number; total: number | null };

export interface UpdatesApi {
  /** A newer published release, or null. Asked by the app itself at most once a day, or when the person checks. */
  checkUpdate(): Promise<UpdateInfo | null>;
  /** Downloads, verifies and installs the update the last check found; the app restarts into it with restartApp. */
  installUpdate(): Promise<void>;
  onUpdateProgress(listener: (progress: UpdateProgress) => void): Promise<() => void>;
  restartApp(): Promise<void>;
}

export function desktopUpdates(invoke: Invoke): UpdatesApi {
  return {
    checkUpdate: () => invoke("check_update"),
    installUpdate: () => invoke("install_update"),
    onUpdateProgress: async (listener) => {
      const { listen } = await import("@tauri-apps/api/event");
      return listen<UpdateProgress>("update-progress", (e) => listener(e.payload));
    },
    restartApp: () => invoke("restart_app"),
  };
}
