import type { Api } from "../api";
import { language } from "../i18n";
import { desktopConnections } from "./connections";
import { desktopDiscovery } from "./discovery";
import { desktopAuthentication } from "./authentication";
import { desktopReports } from "./reports";
import { desktopPlans } from "./plans";
import { desktopActivity } from "./activity";
import { desktopTimeline } from "./timeline";
import { desktopEditor } from "./editor";
import { desktopFiles } from "./files";
import { desktopApprovals } from "./approvals";
import { desktopUpdates } from "./updates";
import { desktopLocalization } from "./localization";

export async function desktopApi(): Promise<Api> {
  const { invoke } = await import("@tauri-apps/api/core");
  const discovery = desktopDiscovery(invoke);
  const connections = desktopConnections(invoke);
  const editor = desktopEditor(invoke);
  const files = desktopFiles(invoke);
  // Before anything is read: the backend words what it reads in the app's language.
  await invoke("set_language", { code: language() });
  return {
    kind: "desktop",
    ...connections,
    ...discovery,
    ...desktopAuthentication(invoke),
    ...desktopReports(invoke),
    ...desktopPlans(invoke),
    ...desktopActivity(invoke),
    ...desktopTimeline(invoke),
    ...editor,
    ...files,
    ...desktopApprovals(invoke),
    ...desktopUpdates(invoke),
    ...desktopLocalization(invoke),
  };
}
