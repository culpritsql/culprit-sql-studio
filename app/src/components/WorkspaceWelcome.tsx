import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";

export function WorkspaceWelcome({ icon: Icon, heading, description, actions, children }: {
  icon: LucideIcon; heading: string; description: string; actions: ReactNode; children: ReactNode;
}) {
  return <div className="workspace-welcome">
    <div className="welcome-card">
      <Icon size={26} className="welcome-icon" aria-hidden />
      <h2>{heading}</h2>
      <p className="muted">{description}</p>
      <div className="welcome-actions">{actions}</div>
      {children}
    </div>
  </div>;
}
