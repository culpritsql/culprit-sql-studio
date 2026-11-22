import "./archives/archives.css";
import { useEffect, useRef, useState } from "react";
import type { WarehouseConnectionInfo } from "../gen/WarehouseConnectionInfo";
import type { PublishedDatabaseResource } from "../gen/PublishedDatabaseResource";
import type { PublishedResources } from "../gen/PublishedResources";
import type { WarehouseIncidentView } from "../gen/WarehouseIncidentView";
import type { WarehousesApi } from "../lib/api/warehouses";
import { api, errorMessage } from "../lib/api";
import { t, useLanguage } from "../lib/i18n";
import { archiveTimeInput, archiveUtcInput } from "../lib/archive-time";
import { nativeTextPreview } from "../components/NativeObservationFields";
import { ObservationReadQualifications } from "../components/ObservationList";
import { ArchiveConnectionForm } from "./archives/ArchiveConnectionForm";
import { ArchiveIncident } from "./archives/ArchiveIncident";
const client = (): Promise<WarehousesApi> => api();
const text = (value: string) => nativeTextPreview(value, 256);
const RESOURCE_PAGE_ROWS = 32;
export default function Archives({ load = client }: { load?: () => Promise<WarehousesApi> }) {
  const language = useLanguage();
  const [connections, setConnections] = useState<WarehouseConnectionInfo[]>([]);
  const [selected, setSelected] = useState<WarehouseConnectionInfo | null>(null);
  const [adding, setAdding] = useState(false);
  const [page, setPage] = useState<PublishedResources | null>(null);
  const [resource, setResource] = useState<PublishedDatabaseResource | null>(null);
  const [around, setAround] = useState("");
  const [acquiredFrom, setAcquiredFrom] = useState("");
  const [acquiredUntil, setAcquiredUntil] = useState("");
  const [incident, setIncident] = useState<WarehouseIncidentView | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [removing, setRemoving] = useState(false);
  const pending = useRef<AbortController | null>(null);
  const selection = useRef<string | null>(null);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    let current = true;
    pending.current?.abort(); setIncident(null);
    load().then((client) => client.warehouseConnections()).then((items) => { if (current) setConnections(items); })
      .catch((failure) => { if (current) setError(errorMessage(failure).message); });
    return () => { mounted.current = false; current = false; pending.current?.abort(); };
  }, [load, language]);
  function change(next: WarehouseConnectionInfo | null) {
    pending.current?.abort(); selection.current = next?.reference.revision ?? null;
    setSelected(next); setPage(null); setResource(null); setIncident(null); setError(null); setBusy(false); setRemoving(false);
  }
  async function read(next: WarehouseConnectionInfo, more = false) {
    pending.current?.abort(); const owner = new AbortController(); pending.current = owner;
    selection.current = next.reference.revision; setSelected(next); setResource(null); setIncident(null); setBusy(true); setError(null);
    try {
      const from = acquiredFrom ? archiveUtcInput(acquiredFrom) : null;
      const until = acquiredUntil ? archiveUtcInput(acquiredUntil) : null;
      if ((acquiredFrom || acquiredUntil) && (from == null || until == null || from >= until)) throw new Error(t("ui.archives.time_invalid"));
      const window = from && until ? { from, until } : null;
      const request = { as_of: more && page ? page.as_of : new Date().toISOString(), window: more && page ? page.window : window, after: more && page ? page.next : null, page_rows: RESOURCE_PAGE_ROWS };
      const result = await (await load()).warehouseResources(next.reference, request, owner.signal);
      if (!owner.signal.aborted && pending.current === owner && selection.current === next.reference.revision) setPage(result);
    } catch (failure) { if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message); }
    finally { if (pending.current === owner) { pending.current = null; setBusy(false); } }
  }
  async function investigate() {
    const at = archiveUtcInput(around);
    if (!selected || !resource || at == null) { setError(t("ui.archives.time_invalid")); return; }
    pending.current?.abort(); const owner = new AbortController(); pending.current = owner;
    const connection = selected, original = resource;
    setIncident(null); setBusy(true); setError(null);
    try {
      const result = await (await load()).warehouseIncident(connection.reference, original.selector, at, owner.signal);
      if (!owner.signal.aborted && pending.current === owner && selection.current === connection.reference.revision) {
        if (result.report.database !== original.selector.database) throw new Error(t("ui.archives.changed"));
        setIncident(result);
      }
    } catch (failure) { if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message); }
    finally { if (pending.current === owner) { pending.current = null; setBusy(false); } }
  }
  async function remove() {
    if (!selected) return;
    const original = selected; pending.current?.abort(); const owner = new AbortController(); pending.current = owner; setBusy(true); setError(null);
    try {
      await (await load()).removeWarehouseConnection(original.reference);
      setConnections((items) => items.filter((item) => item.reference.revision !== original.reference.revision));
      if (selection.current === original.reference.revision) change(null);
    } catch (failure) { if (selection.current === original.reference.revision) setError(errorMessage(failure).message); }
    finally { if (pending.current === owner) { pending.current = null; setBusy(false); } }
  }
  return <div className="connections archives"><div className="connections-inner">
    <h1>{t("ui.archives.title")}</h1><p>{t("ui.archives.intro")}</p>
    <button type="button" className="btn" onClick={() => { pending.current?.abort(); setAdding(!adding); }}>{t("ui.archives.add")}</button>
    {adding && <ArchiveConnectionForm client={load} onCancel={() => setAdding(false)} onSaved={(saved) => { if (!mounted.current) return; setAdding(false); setConnections((items) => [...items, saved]); change(saved); void read(saved); }} />}
    {error && <p role="alert">{text(error)}</p>}
    <div className="conn-grid">{connections.map((connection) => <button type="button" className="btn" key={connection.reference.revision} aria-pressed={selected?.reference.revision === connection.reference.revision} onClick={() => { change(connection); void read(connection); }}>
      {text(connection.reference.name)} · {connection.settings.warehouse.kind === "adx" ? t("ui.archives.adx") : t("ui.archives.clickhouse")}
    </button>)}</div>
    {connections.length === 0 && !adding && <p className="muted">{t("ui.archives.none")}</p>}
    {selected && <section>
      <h2>{text(selected.reference.name)}</h2><p className="muted">{t("ui.archives.read_only")}</p>
      <label className="field">{t("ui.archives.acquired_from")}<input type="datetime-local" value={acquiredFrom} onChange={(event) => { pending.current?.abort(); setAcquiredFrom(event.target.value); setPage(null); setResource(null); setIncident(null); }} /></label>
      <label className="field">{t("ui.archives.acquired_until")}<input type="datetime-local" value={acquiredUntil} onChange={(event) => { pending.current?.abort(); setAcquiredUntil(event.target.value); setPage(null); setResource(null); setIncident(null); }} /></label>
      <div className="actions"><button type="button" className="btn" onClick={() => void read(selected)} disabled={busy}>{t("ui.common.refresh")}</button><button type="button" className="btn" onClick={() => setRemoving(!removing)} disabled={busy}>{t("ui.common.remove")}</button></div>
      {removing && <div><p>{t("ui.archives.remove_confirm")}</p><button type="button" className="btn" onClick={() => void remove()} disabled={busy}>{t("ui.common.remove")}</button><button type="button" className="btn" onClick={() => setRemoving(false)}>{t("ui.common.cancel")}</button></div>}
      {busy && <p role="status">{t("ui.archives.reading")}</p>}
      {page && <>
        <p className="muted">{t("ui.archives.as_of", { at: page.as_of })}</p>
        {!page.discovery_complete && <p role="status">{t("ui.archives.partial")}</p>}
        {page.resources.length === 0 && <p>{t("ui.archives.no_resources")}</p>}
        <div className="conn-grid">{page.resources.map((original) => <button type="button" className="btn" key={`${original.position.resource_id}:${original.position.source_id}:${original.position.range_id}`} onClick={() => { pending.current?.abort(); setResource(original); setAround(archiveTimeInput(original.collected_until)); setIncident(null); setError(null); }}>
          {text(original.database_name)} · {text(original.source_name)}<br />{t("ui.archives.original_bounds", { from: original.collected_from, until: original.collected_until })}
        </button>)}</div>
        {page.next && <button type="button" className="btn" onClick={() => void read(selected, true)} disabled={busy}>{t("ui.common.next")}</button>}
      </>}
      {resource && <section>
        <h3>{text(resource.database_name)}</h3><p className="muted">{t("ui.archives.original_scope")}</p>
        <ObservationReadQualifications sources={resource.coverage.kind === "snapshot" ? resource.coverage.sources.map((source) => ({ source: source.source, state: source.state })) : resource.coverage.kind === "activity" ? [{ source: resource.coverage.coverage.source, state: resource.coverage.coverage.state }] : [{ source: resource.source_name, state: null }]} />
        {resource.native_time.kind === "utc" ? <p>{t("ui.archives.native_bounds", { from: resource.native_time.from, until: resource.native_time.until })}</p> : <p>{t("ui.archives.native_clock_unknown")}</p>}
        <label className="field">{t("ui.archives.around")}<input type="datetime-local" value={around} onChange={(event) => { pending.current?.abort(); setAround(event.target.value); setIncident(null); setError(null); }} /></label>
        <button type="button" className="btn primary" disabled={busy || archiveUtcInput(around) == null} onClick={() => void investigate()}>{t("ui.archives.investigate")}</button>
      </section>}
      {incident && <ArchiveIncident value={incident} />}
    </section>}
  </div></div>;
}
