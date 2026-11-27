import { useCallback, useEffect, useRef, useState } from "react";
import type { CollectionAdmissionRecord } from "../gen/CollectionAdmissionRecord";
import type { ProviderAcquisitionReport } from "../gen/ProviderAcquisitionReport";
import { api, type ApiError, errorMessage } from "../lib/api";
import { dateFormat } from "../lib/format";
import { t } from "../lib/i18n";
import { LatestRequest } from "../lib/requests";
import { scan } from "../lib/store";
import { ProviderAcquisition } from "./ProviderAcquisition";

export function scanNeedsVerification(error: ApiError) {
  return typeof error.detail === "object" && error.detail !== null
    && "collection-stopped" in error.detail && error.detail["collection-stopped"] === "cancellation-unconfirmed";
}

export function ScanFailure({ name, error, acquisition }: {
  name: string; error: ApiError; acquisition?: ProviderAcquisitionReport | null;
}) {
  const paused = scanNeedsVerification(error);
  return (
    <section className="scan-failure">
      <h2>{paused ? t("ui.scan_recovery.paused") : t("ui.app.scan_failed", { name })}</h2>
      {paused ? <ScanRecovery name={name} /> : <>
        <p className="selectable">{error.message}</p>
        {error.hint && <p className="muted">{error.hint}</p>}
        <button type="button" className="btn" onClick={() => scan(name)}>{t("ui.common.try_again")}</button>
      </>}
      {acquisition && <details><summary>{t("ui.scan_recovery.details")}</summary><ProviderAcquisition report={acquisition} /></details>}
    </section>
  );
}

function ScanRecovery({ name }: { name: string }) {
  const owner = useRef(new LatestRequest()).current;
  const [record, setRecord] = useState<CollectionAdmissionRecord | null>(null);
  const [verified, setVerified] = useState(false);
  const [busy, setBusy] = useState(true);
  const [failure, setFailure] = useState<string | null>(null);
  const inspect = useCallback(async () => {
    const current = owner.start();
    setBusy(true);
    setVerified(false);
    setFailure(null);
    setRecord(null);
    try {
      const record = await (await api()).collectionAdmissionRecord();
      if (current()) setRecord(record);
    } catch (error) {
      if (current()) setFailure(errorMessage(error).message);
    } finally {
      if (current()) setBusy(false);
    }
  }, [owner]);
  useEffect(() => {
    void inspect();
    return () => owner.invalidate();
  }, [inspect, owner]);

  const status = record?.status;
  const id = status?.observation_id;
  async function resume() {
    if (busy || !verified || !id || !status?.source_quiescence_required) return;
    const current = owner.start();
    setBusy(true);
    try {
      const client = await api();
      if (!current()) return;
      await client.acknowledgeCollectionQuiescence(id, true);
      if (current()) void scan(name);
    } catch (error) {
      if (!current()) return;
      setFailure(errorMessage(error).message);
      setRecord(null);
      setVerified(false);
      setBusy(false);
    }
  }

  const route = status?.route;
  const time = record?.recorded_at_unix_seconds;
  return <>
    <p>{t("ui.scan_recovery.reason")}</p>
    {status?.source_quiescence_required && id && route && <>
      <dl className="selectable">
        <dt>{t("ui.scan_recovery.request")}</dt>
        <dd>{route.kind === "saved-connection" ? route.name : route.kind === "saved-connection-test"
          ? t("ui.scan_recovery.saved_test", { name: route.name }) : t("ui.scan_recovery.unsaved_test")}</dd>
        {time != null && <><dt>{t("ui.scan_recovery.recorded")}</dt><dd>{dateFormat({ dateStyle: "medium", timeStyle: "medium" }).format(time * 1000)}</dd></>}
        <dt>{t("ui.scan_recovery.observation")}</dt><dd><code>{id}</code></dd>
      </dl>
      <p>{t("ui.scan_recovery.verify")}</p>
      <label><input type="checkbox" disabled={busy} checked={verified} onChange={(event) => setVerified(event.target.checked)} />{t("ui.scan_recovery.verified")}</label>
    </>}
    {status && !status.source_quiescence_required && <p>{t("ui.scan_recovery.clear")}</p>}
    {failure && <p role="alert">{failure}</p>}
    <div className="actions">
      {status?.source_quiescence_required && id && route && <button type="button" className="btn primary" disabled={!verified || busy} onClick={() => void resume()}>{t("ui.scan_recovery.resume")}</button>}
      {status && !status.source_quiescence_required && <button type="button" className="btn" onClick={() => scan(name)}>{t("ui.common.try_again")}</button>}
      <button type="button" className="btn" disabled={busy} onClick={() => void inspect()}>{t("ui.scan_recovery.refresh")}</button>
    </div>
  </>;
}
