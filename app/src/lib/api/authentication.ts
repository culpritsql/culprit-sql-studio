import type { SignInEvent } from "../../gen/SignInEvent";
import type { Invoke } from "../owned-requests";

export interface AuthenticationApi {
  /** Device-code sign-in progress for Microsoft Entra ID; resolves to an unsubscribe function. */
  onSignIn(listener: (event: SignInEvent) => void): Promise<() => void>;
  cancelSignIn(): Promise<void>;
  openSignInPage(): Promise<void>;
}

export function desktopAuthentication(invoke: Invoke): AuthenticationApi {
  return {
    onSignIn: async (listener) => {
      const { listen } = await import("@tauri-apps/api/event");
      return listen<SignInEvent>("entra-sign-in", (e) => listener(e.payload));
    },
    cancelSignIn: () => invoke("cancel_sign_in"),
    openSignInPage: () => invoke("open_sign_in_page"),
  };
}
