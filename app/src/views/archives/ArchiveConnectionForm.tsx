import { Archive, Database, FolderOpen, type LucideIcon } from "lucide-react";
import { useState, type FormEvent, type ReactNode } from "react";
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
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError(null);
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
    try {
      const saved = await (await client()).addWarehouseConnection(input);
      onSaved(saved);
    } catch (failure) {
      setError(errorMessage(failure).message);
    } finally {
      setPassword("");
      setBusy(false);
    }
  }

  return (
    <form className="archive-form" aria-labelledby="archive-setup-title" onSubmit={save}
      onKeyDown={(event) => {
        if (event.key === "Escape" && !busy) {
          event.stopPropagation();
          onCancel();
        }
      }}>
      <h1 id="archive-setup-title">{t("ui.archives.add")}</h1>
      <Section title={t("ui.archives.form_identity")} hint={t("ui.archives.form_identity_hint")} icon={Archive} busy={busy}>
        <Field label={t("ui.connection_card.name")} value={name} set={setName} autoFocus wide />
        <Field label={t("ui.archives.pipeline")} value={pipeline} set={setPipeline} />
        <Field label={t("ui.archives.shard")} value={shard} set={setShard} />
      </Section>
      <Section title={t("ui.archives.form_warehouse")} hint={t("ui.archives.form_warehouse_hint")} icon={Database} busy={busy}>
        <label className="field">{t("ui.archives.warehouse")}
          <select value={kind} onChange={(event) => { setKind(event.target.value as typeof kind); setPassword(""); }}>
            <option value="adx">{t("ui.archives.adx")}</option>
            <option value="clickhouse">{t("ui.archives.clickhouse")}</option>
          </select>
        </label>
        <Field label={t("ui.connection_settings.database")} value={database} set={setDatabase} />
        <Field label={t("ui.archives.endpoint")} type="url" value={endpoint} set={setEndpoint} wide />
        <p className="archive-auth-hint">{kind === "adx" ? t("ui.archives.form_adx_auth") : t("ui.archives.form_clickhouse_auth")}</p>
        <Field label={t("ui.archives.warehouse_profile")} value={warehouseProfile} set={setWarehouseProfile} wide={kind === "clickhouse"} />
        {kind === "adx" ? <Field label={t("ui.archives.tenant")} value={warehouseTenant} set={setWarehouseTenant} optional /> : <>
          <Field label={t("ui.new_connection_form.user")} value={username} set={setUsername} />
          <Field label={t("ui.new_connection_form.password")} value={password} set={setPassword} type="password" />
        </>}
      </Section>
      <Section title={t("ui.archives.form_originals")} hint={t("ui.archives.form_originals_hint")} icon={FolderOpen} busy={busy}>
        <label className="field">{t("ui.archives.recovery")}
          <select value={recovery} onChange={(event) => { setRecovery(event.target.value as typeof recovery); setRecoveryIdentity(""); }}>
            <option value="azure-blob">{t("ui.archives.azure_blob")}</option>
            <option value="s3">{t("ui.archives.s3")}</option>
          </select>
        </label>
        {recovery === "azure-blob" ? <Field label={t("ui.archives.account")} value={account} set={setAccount} />
          : <Field label={t("ui.archives.region")} value={region} set={setRegion} />}
        <Field label={recovery === "azure-blob" ? t("ui.archives.container") : t("ui.archives.bucket")} value={container} set={setContainer} />
        <Field label={t("ui.archives.prefix")} value={prefix} set={setPrefix} />
        {recovery === "s3" && <Field label={t("ui.archives.expected_owner")} value={owner} set={setOwner} wide />}
        <p className="archive-auth-hint">{recovery === "azure-blob" ? t("ui.archives.form_blob_auth") : t("ui.archives.form_s3_auth")}</p>
        <Field label={t("ui.archives.recovery_profile")} value={recoveryProfile} set={setRecoveryProfile} />
        <Field label={recovery === "azure-blob" ? t("ui.archives.tenant") : t("ui.archives.aws_profile")} value={recoveryIdentity} set={setRecoveryIdentity} optional />
      </Section>
      <footer className="archive-form-footer">
        {error && <p role="alert">{error}</p>}
        <div className="actions">
          <button className="btn" type="button" disabled={busy} onClick={onCancel}>{t("ui.common.cancel")}</button>
          <button className="btn primary" type="submit" disabled={busy} aria-busy={busy}>{t("ui.common.save")}</button>
        </div>
      </footer>
    </form>
  );
}

function Section({ title, hint, icon: Icon, busy, children }: {
  title: string; hint: string; icon: LucideIcon; busy: boolean; children: ReactNode;
}) {
  return <fieldset disabled={busy}>
    <legend><Icon size={16} aria-hidden />{title}</legend>
    <p className="archive-section-hint">{hint}</p>
    <div className="archive-fields">{children}</div>
  </fieldset>;
}

function Field({ label, value, set, optional, type = "text", wide, autoFocus }: {
  label: string; value: string; set: (value: string) => void; optional?: boolean;
  type?: "text" | "url" | "password"; wide?: boolean; autoFocus?: boolean;
}) {
  return <label className={wide ? "field wide" : "field"}>{label}
    <input type={type} value={value} onChange={(event) => set(event.target.value)} required={!optional}
      autoFocus={autoFocus} autoComplete={type === "password" ? "new-password" : "off"} />
  </label>;
}
