import type { Invoke } from "../owned-requests";

export interface LocalizationApi {
  /** Write what the desktop app words (findings, notes, plan insights) in `code`, the language the app is shown in. */
  setLanguage(code: string): Promise<void>;
}

export function desktopLocalization(invoke: Invoke): LocalizationApi {
  return {
    setLanguage: (code) => invoke("set_language", { code }),
  };
}
