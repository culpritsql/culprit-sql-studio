import type { AgentAccess } from "../../gen/AgentAccess";
import type { ApprovalRequest } from "../../gen/ApprovalRequest";
import type { ConnectionInfo } from "../../gen/ConnectionInfo";
import type { Invoke } from "../owned-requests";

export interface ApprovalsApi {
  setAgentAccess(name: string, access: AgentAccess): Promise<ConnectionInfo>;
  pendingApprovals(): Promise<ApprovalRequest[]>;
  decideApproval(id: string, hash: string, approved: boolean): Promise<void>;
}

export function desktopApprovals(invoke: Invoke): ApprovalsApi {
  return {
    setAgentAccess: (name, access) => invoke("set_agent_access", { name, access }),
    pendingApprovals: () => invoke("pending_approvals"),
    decideApproval: (id, hash, approved) => invoke("decide_approval", { id, hash, approved }),
  };
}
