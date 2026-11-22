import { useState, type FormEvent } from "react";
import type { SaveWarehouseConnection } from "../../gen/SaveWarehouseConnection";
import type { WarehouseConnectionInfo } from "../../gen/WarehouseConnectionInfo";
import type { WarehousesApi } from "../../lib/api/warehouses";
import { errorMessage } from "../../lib/api";
import { t } from "../../lib/i18n";

export function ArchiveConnectionForm({ client, onSaved, onCancel }: { client: () => Promise<WarehousesApi>; onSaved: (value: WarehouseConnectionInfo) => void; onCancel: () => void }) {
  const [name, setName] = useState("");
  const [kind, setKind] = useState<"adx" | "clickhouse">("adx");
  const [endpoint, setEndpoint] = useState("");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [warehouseProfile, setWarehouseProfile] = useState("warehouse-read");
  const [warehouseTenant, setWarehouseTenant] = useState("");
  const [recovery, setRecovery] = useState<"azure-blob" | "s3">("azure-blob");
  const [account, setAccount] = useState("");
  const [container, setContainer] = useState("");
  const [prefix, setPrefix] = useState("");
  const [region, setRegion] = useState("");
  const [owner, setOwner] = useState("");
  const [recoveryProfile, setRecoveryProfile] = useState("recovery-read");
  const [recoveryIdentity, setRecoveryIdentity] = useState("");
  const [pipeline, setPipeline] = useState("");
  const [shard, setShard] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function save(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError(null);
    const input: SaveWarehouseConnection = {
      name,
      settings: {
        pipeline, shard,
        warehouse: kind === "adx" ? { kind, endpoint, database, authentication: { name: warehouseProfile, tenant: warehouseTenant || null } }
          : { kind, endpoint, database, username, credential_name: warehouseProfile },
        recovery: recovery === "azure-blob" ? { kind: recovery, account, container, prefix, authentication: { name: recoveryProfile, tenant: recoveryIdentity || null } }
          : { kind: recovery, bucket: container, region, prefix, expected_owner: owner, authentication: { name: recoveryProfile, profile: recoveryIdentity || null } },
      },
      password: kind === "clickhouse" ? password : null,
    };
    try { const saved = await (await client()).addWarehouseConnection(input); setPassword(""); onSaved(saved); }
    catch (failure) { setError(errorMessage(failure).message); }
    finally { setPassword(""); setBusy(false); }
  }
  return <form className="conn-panel archive-form" onKeyDown={(event) => { if (event.key === "Escape" && !busy) { event.stopPropagation(); setPassword(""); onCancel(); } }} onSubmit={(event) => void save(event)}>
    <h2>{t("ui.archives.add")}</h2>
    <p className="muted">{t("ui.archives.authentication")}</p>
    {error && <p role="alert">{error}</p>}
    <fieldset disabled={busy}>
    <Field label={t("ui.connection_card.name")} value={name} set={setName} />
    <label className="field">{t("ui.archives.warehouse")}<select value={kind} onChange={(event) => { setKind(event.target.value as typeof kind); setPassword(""); }} disabled={busy}>
      <option value="adx">{t("ui.archives.adx")}</option><option value="clickhouse">{t("ui.archives.clickhouse")}</option>
    </select></label>
    <Field label={t("ui.archives.endpoint")} value={endpoint} set={setEndpoint} />
    <Field label={t("ui.connection_settings.database")} value={database} set={setDatabase} />
    <Field label={t("ui.archives.warehouse_profile")} value={warehouseProfile} set={setWarehouseProfile} />
    {kind === "adx" ? <Field label={t("ui.archives.tenant")} value={warehouseTenant} set={setWarehouseTenant} optional /> : <>
      <Field label={t("ui.new_connection_form.user")} value={username} set={setUsername} />
      <Field label={t("ui.new_connection_form.password")} value={password} set={setPassword} type="password" />
    </>}
    <label className="field">{t("ui.archives.recovery")}<select value={recovery} onChange={(event) => { setRecovery(event.target.value as typeof recovery); setRecoveryIdentity(""); }} disabled={busy}>
      <option value="azure-blob">{t("ui.archives.azure_blob")}</option><option value="s3">{t("ui.archives.s3")}</option>
    </select></label>
    {recovery === "azure-blob" && <Field label={t("ui.archives.account")} value={account} set={setAccount} />}
    <Field label={recovery === "azure-blob" ? t("ui.archives.container") : t("ui.archives.bucket")} value={container} set={setContainer} />
    <Field label={t("ui.archives.prefix")} value={prefix} set={setPrefix} />
    {recovery === "s3" && <><Field label={t("ui.archives.region")} value={region} set={setRegion} /><Field label={t("ui.archives.expected_owner")} value={owner} set={setOwner} /></>}
    <Field label={t("ui.archives.recovery_profile")} value={recoveryProfile} set={setRecoveryProfile} />
    <Field label={recovery === "azure-blob" ? t("ui.archives.tenant") : t("ui.archives.aws_profile")} value={recoveryIdentity} set={setRecoveryIdentity} optional />
    <Field label={t("ui.archives.pipeline")} value={pipeline} set={setPipeline} />
    <Field label={t("ui.archives.shard")} value={shard} set={setShard} />
    </fieldset>
    <div className="actions"><button className="btn primary" type="submit" disabled={busy} aria-busy={busy}>{t("ui.common.save")}</button><button className="btn" type="button" disabled={busy} onClick={() => { setPassword(""); onCancel(); }}>{t("ui.common.cancel")}</button></div>
  </form>;
}
function Field({ label, value, set, optional = false, type = "text" }: { label: string; value: string; set: (value: string) => void; optional?: boolean; type?: "text" | "password" }) {
  return <label className="field">{label}<input type={type} value={value} onChange={(event) => set(event.target.value)} required={!optional} autoComplete={type === "password" ? "new-password" : "off"} /></label>;
}
