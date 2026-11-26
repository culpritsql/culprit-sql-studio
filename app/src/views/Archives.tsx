import { useEffect, useRef, useState } from "react";
import { nativeTextPreview } from "../components/NativeObservationFields";
import { ObservationReadQualifications } from "../components/ObservationList";
import type { PublishedDatabaseResource } from "../gen/PublishedDatabaseResource";
import type { PublishedResources } from "../gen/PublishedResources";
import type { PublishedResourceSelector } from "../gen/PublishedResourceSelector";
import type { WarehouseArtifactReport } from "../gen/WarehouseArtifactReport";
import type { WarehouseConnectionInfo } from "../gen/WarehouseConnectionInfo";
import type { WarehouseFleetReport } from "../gen/WarehouseFleetReport";
import type { WarehouseFleetRequest } from "../gen/WarehouseFleetRequest";
import type { WarehouseFleetRow } from "../gen/WarehouseFleetRow";
import type { WarehouseIncidentView } from "../gen/WarehouseIncidentView";
import type { WarehouseWindowQuery } from "../gen/WarehouseWindowQuery";
import type { WarehouseWindowReport } from "../gen/WarehouseWindowReport";
import type { WarehouseWindowView } from "../gen/WarehouseWindowView";
import { api, errorMessage, isArchiveWeb } from "../lib/api";
import type { WarehousesApi } from "../lib/api/warehouses";
import { type ArtifactSelection, artifactRequest, matchesArtifactRequest } from "../lib/archive-artifacts";
import { matchesFleetRequest } from "../lib/archive-fleet";
import {
  ARCHIVE_WINDOW_PRESETS,
  type ArchiveWindowPresetMinutes,
  archivePresetWindowInputs,
  archiveSearchWindowInputs,
  archiveTimeInput,
  archiveUtcInput,
} from "../lib/archive-time";
import { matchesWindowRequest, nextWindowPage } from "../lib/archive-window";
import { t, useLanguage } from "../lib/i18n";
import { ArchiveArtifacts } from "./archives/ArchiveArtifacts";
import { ArchiveConnectionForm } from "./archives/ArchiveConnectionForm";
import { ArchiveFleet } from "./archives/ArchiveFleet";
import { ArchiveIncident } from "./archives/ArchiveIncident";
import { ArchiveWindow } from "./archives/ArchiveWindow";
import { ArchiveWindowCompare } from "./archives/ArchiveWindowCompare";
import "./archives/archives.css";

const client = (): Promise<WarehousesApi> => api();
const text = (value: string) => nativeTextPreview(value, 256);
const RESOURCE_PAGE_ROWS = 32;
const FLEET_PAGE_ROWS = 24;
const DEFAULT_FLEET_MAX_AGE_SECONDS = 1800;

type BrowseMode = "fleet" | "publications";

type SelectedTarget = {
  resourceId: string | null;
  selector: PublishedResourceSelector;
  databaseName: string;
  anchorUntil: string;
  rawResource: PublishedDatabaseResource | null;
};

export default function Archives({ load = client }: { load?: () => Promise<WarehousesApi> }) {
  const language = useLanguage();
  const webReadOnly = isArchiveWeb();
  const [connections, setConnections] = useState<WarehouseConnectionInfo[]>([]);
  const [selected, setSelected] = useState<WarehouseConnectionInfo | null>(null);
  const [adding, setAdding] = useState(false);
  const [mode, setMode] = useState<BrowseMode>(webReadOnly ? "fleet" : "publications");
  const [fleet, setFleet] = useState<WarehouseFleetReport | null>(null);
  const [page, setPage] = useState<PublishedResources | null>(null);
  const [target, setTarget] = useState<SelectedTarget | null>(null);
  const [around, setAround] = useState("");
  const [windowFrom, setWindowFrom] = useState("");
  const [windowUntil, setWindowUntil] = useState("");
  const [windowReport, setWindowReport] = useState<WarehouseWindowReport | null>(null);
  const [baselineWindow, setBaselineWindow] = useState<WarehouseWindowReport | null>(null);
  const [acquiredFrom, setAcquiredFrom] = useState("");
  const [acquiredUntil, setAcquiredUntil] = useState("");
  const [incident, setIncident] = useState<WarehouseIncidentView | null>(null);
  const [artifacts, setArtifacts] = useState<WarehouseArtifactReport | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [removing, setRemoving] = useState(false);
  const pending = useRef<AbortController | null>(null);
  const selection = useRef<string | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    let current = true;
    pending.current?.abort();
    setIncident(null);
    setWindowReport(null);
    setArtifacts(null);
    load()
      .then((c) => c.warehouseConnections())
      .then((items) => {
        if (current) setConnections(items);
      })
      .catch((failure) => {
        if (current) setError(errorMessage(failure).message);
      });
    return () => {
      mounted.current = false;
      current = false;
      pending.current?.abort();
    };
  }, [load, language]);

  function change(next: WarehouseConnectionInfo | null) {
    pending.current?.abort();
    selection.current = next?.reference.revision ?? null;
    setSelected(next);
    setFleet(null);
    setPage(null);
    setTarget(null);
    setIncident(null);
    setWindowReport(null);
    setBaselineWindow(null);
    setArtifacts(null);
    setError(null);
    setBusy(false);
    setRemoving(false);
  }

  function pickFleetRow(row: WarehouseFleetRow) {
    pending.current?.abort();
    const obsFrom = row.latest_observed_from ?? row.latest_collected_from;
    const obsUntil = row.latest_observed_until ?? row.latest_collected_until;
    setTarget({
      resourceId: row.resource_id,
      selector: row.selector,
      databaseName: row.database_name,
      anchorUntil: obsUntil,
      rawResource: null,
    });
    setAround(archiveTimeInput(obsUntil));
    const window = archiveSearchWindowInputs(obsFrom, obsUntil);
    setWindowFrom(window?.from ?? "");
    setWindowUntil(window?.until ?? "");
    setIncident(null);
    setWindowReport(null);
    setBaselineWindow(null);
    setArtifacts(null);
    setError(null);
  }

  function pickRawResource(original: PublishedDatabaseResource) {
    pending.current?.abort();
    const obsFrom = original.native_time.kind === "utc" ? original.native_time.from : original.collected_from;
    const obsUntil = original.native_time.kind === "utc" ? original.native_time.until : original.collected_until;
    setTarget({
      resourceId: original.position.resource_id,
      selector: original.selector,
      databaseName: original.database_name,
      anchorUntil: obsUntil,
      rawResource: original,
    });
    setAround(archiveTimeInput(original.collected_until));
    const window = archiveSearchWindowInputs(obsFrom, obsUntil);
    setWindowFrom(window?.from ?? "");
    setWindowUntil(window?.until ?? "");
    setIncident(null);
    setWindowReport(null);
    setBaselineWindow(null);
    setArtifacts(null);
    setError(null);
  }

  async function readBrowse(next: WarehouseConnectionInfo, activeMode: BrowseMode = mode, more = false) {
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    selection.current = next.reference.revision;
    setSelected(next);
    if (!more) {
      setTarget(null);
      setIncident(null);
      setWindowReport(null);
      setBaselineWindow(null);
      setArtifacts(null);
    }
    setBusy(true);
    setError(null);
    try {
      const from = acquiredFrom ? archiveUtcInput(acquiredFrom) : null;
      const until = acquiredUntil ? archiveUtcInput(acquiredUntil) : null;
      if ((acquiredFrom || acquiredUntil) && (from == null || until == null || from >= until)) {
        throw new Error(t("ui.archives.time_invalid"));
      }
      const window = from && until ? { from, until } : null;
      const apiClient = await load();
      if (activeMode === "fleet" && typeof apiClient.warehouseFleet === "function") {
        const request: WarehouseFleetRequest = {
          as_of: more && fleet ? fleet.request.as_of : new Date().toISOString(),
          window: more && fleet ? fleet.request.window : window,
          max_age_seconds: DEFAULT_FLEET_MAX_AGE_SECONDS,
          filter: { environment: null, engine: null, attention_only: false, shared_group: null },
          sort: "attention",
          page_rows: FLEET_PAGE_ROWS,
          after: more && fleet ? fleet.next : null,
        };
        const result = await apiClient.warehouseFleet(next.reference, request, owner.signal);
        if (!owner.signal.aborted && pending.current === owner && selection.current === next.reference.revision) {
          if (!matchesFleetRequest(result, request)) throw new Error(t("ui.archives.changed"));
          setFleet(result);
        }
      } else {
        const request = {
          as_of: more && page ? page.as_of : new Date().toISOString(),
          window: more && page ? page.window : window,
          after: more && page ? page.next : null,
          page_rows: RESOURCE_PAGE_ROWS,
        };
        const result = await apiClient.warehouseResources(next.reference, request, owner.signal);
        if (!owner.signal.aborted && pending.current === owner && selection.current === next.reference.revision) {
          setPage(result);
        }
      }
    } catch (failure) {
      if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }

  async function investigate() {
    const at = archiveUtcInput(around);
    if (!selected || !target || at == null) {
      setError(t("ui.archives.time_invalid"));
      return;
    }
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    const connection = selected;
    const original = target;
    setIncident(null);
    setWindowReport(null);
    setArtifacts(null);
    setBusy(true);
    setError(null);
    try {
      const result = await (await load()).warehouseIncident(connection.reference, original.selector, at, owner.signal);
      if (!owner.signal.aborted && pending.current === owner && selection.current === connection.reference.revision) {
        if (result.report.database !== original.selector.database) throw new Error(t("ui.archives.changed"));
        setIncident(result);
      }
    } catch (failure) {
      if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }

  async function readWindow(view: WarehouseWindowView = "published-answers", more = false) {
    if (!selected || !target) return;
    const connection = selected;
    const original = target;
    let request: WarehouseWindowQuery | null;
    if (more) {
      request = windowReport ? nextWindowPage(windowReport) : null;
    } else {
      const from = archiveUtcInput(windowFrom);
      const until = archiveUtcInput(windowUntil);
      if (from == null || until == null || from >= until) {
        setError(t("ui.archives.time_invalid"));
        return;
      }
      request = {
        resource: original.selector,
        window: { from, until },
        as_of: new Date().toISOString(),
        view,
        page_rows: 1,
        after: null,
      };
    }
    if (request == null) return;
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    setWindowReport(null);
    setArtifacts(null);
    setIncident(null);
    setBusy(true);
    setError(null);
    try {
      const result = await (await load()).warehouseWindow(connection.reference, request, owner.signal);
      if (!owner.signal.aborted && pending.current === owner && selection.current === connection.reference.revision) {
        if (!matchesWindowRequest(result, request)) throw new Error(t("ui.archives.changed"));
        setWindowReport(result);
      }
    } catch (failure) {
      if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }

  async function inspectArtifacts(selectionTarget: ArtifactSelection) {
    if (!selected || !target) return;
    const connection = selected;
    const original = target;
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    setArtifacts(null);
    setBusy(true);
    setError(null);
    try {
      const request = artifactRequest(original.selector, selectionTarget);
      const result = await (await load()).warehouseArtifacts(connection.reference, request, owner.signal);
      if (!owner.signal.aborted && pending.current === owner && selection.current === connection.reference.revision) {
        if (!matchesArtifactRequest(result, request)) throw new Error(t("ui.archives.changed"));
        setArtifacts(result);
      }
    } catch (failure) {
      if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }

  async function remove() {
    if (!selected) return;
    const original = selected;
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    setBusy(true);
    setError(null);
    try {
      await (await load()).removeWarehouseConnection(original.reference);
      setConnections((items) => items.filter((item) => item.reference.revision !== original.reference.revision));
      if (selection.current === original.reference.revision) change(null);
    } catch (failure) {
      if (selection.current === original.reference.revision) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }

  return (
    <div className="connections archives">
      <div className="connections-inner">
        <h1>{t("ui.archives.title")}</h1>
        <p>{t("ui.archives.intro")}</p>
        {!webReadOnly && (
          <button
            type="button"
            className="btn"
            onClick={() => {
              pending.current?.abort();
              setAdding(!adding);
            }}
          >
            {t("ui.archives.add")}
          </button>
        )}
        {adding && (
          <ArchiveConnectionForm
            client={load}
            onCancel={() => setAdding(false)}
            onSaved={(saved) => {
              if (!mounted.current) return;
              setAdding(false);
              setConnections((items) => [...items, saved]);
              change(saved);
              void readBrowse(saved, mode, false);
            }}
          />
        )}
        {error && <p role="alert">{text(error)}</p>}
        <div className="conn-grid">
          {connections.map((connection) => (
            <button
              type="button"
              className="btn"
              key={connection.reference.revision}
              aria-pressed={selected?.reference.revision === connection.reference.revision}
              onClick={() => {
                change(connection);
                void readBrowse(connection, mode, false);
              }}
            >
              {text(connection.reference.name)} ·{" "}
              {connection.settings.warehouse.kind === "adx" ? t("ui.archives.adx") : t("ui.archives.clickhouse")}
            </button>
          ))}
        </div>
        {connections.length === 0 && !adding && <p className="muted">{t("ui.archives.none")}</p>}
        {selected && (
          <section>
            <h2>{text(selected.reference.name)}</h2>
            <p className="muted">{t("ui.archives.read_only")}</p>
            <div className="actions">
              <span className="seg" role="group" aria-label={t("ui.archives.title")}>
                <button
                  type="button"
                  className={mode === "fleet" ? "on" : ""}
                  onClick={() => {
                    setMode("fleet");
                    void readBrowse(selected, "fleet", false);
                  }}
                >
                  {t("ui.archives.mode.fleet")}
                </button>
                <button
                  type="button"
                  className={mode === "publications" ? "on" : ""}
                  onClick={() => {
                    setMode("publications");
                    void readBrowse(selected, "publications", false);
                  }}
                >
                  {t("ui.archives.mode.publications")}
                </button>
              </span>
              {mode === "fleet" && target && (
                <button
                  type="button"
                  className="btn"
                  onClick={() => {
                    setTarget(null);
                    setIncident(null);
                    setWindowReport(null);
                    setArtifacts(null);
                    setBaselineWindow(null);
                  }}
                >
                  {t("ui.archives.fleet.back_to_fleet")}
                </button>
              )}
            </div>
            <div className="archive-time-range">
              <label className="field">
                {t("ui.archives.acquired_from")}
                <input
                  type="datetime-local"
                  value={acquiredFrom}
                  onChange={(event) => {
                    pending.current?.abort();
                    setAcquiredFrom(event.target.value);
                    setFleet(null);
                    setPage(null);
                    setTarget(null);
                    setIncident(null);
                    setWindowReport(null);
                    setArtifacts(null);
                  }}
                />
              </label>
              <label className="field">
                {t("ui.archives.acquired_until")}
                <input
                  type="datetime-local"
                  value={acquiredUntil}
                  onChange={(event) => {
                    pending.current?.abort();
                    setAcquiredUntil(event.target.value);
                    setFleet(null);
                    setPage(null);
                    setTarget(null);
                    setIncident(null);
                    setWindowReport(null);
                    setArtifacts(null);
                  }}
                />
              </label>
            </div>
            <div className="actions">
              <button type="button" className="btn" onClick={() => void readBrowse(selected, mode, false)} disabled={busy}>
                {t("ui.common.refresh")}
              </button>
              {!webReadOnly && (
                <button type="button" className="btn" onClick={() => setRemoving(!removing)} disabled={busy}>
                  {t("ui.common.remove")}
                </button>
              )}
            </div>
            {removing && (
              <div>
                <p>{t("ui.archives.remove_confirm")}</p>
                <button type="button" className="btn" onClick={() => void remove()} disabled={busy}>
                  {t("ui.common.remove")}
                </button>
                <button type="button" className="btn" onClick={() => setRemoving(false)}>
                  {t("ui.common.cancel")}
                </button>
              </div>
            )}
            {busy && <p role="status">{t("ui.archives.reading")}</p>}

            {mode === "fleet" && fleet && (
              <ArchiveFleet
                report={fleet}
                selectedResourceId={target?.resourceId ?? null}
                busy={busy}
                onSelect={pickFleetRow}
                onNext={() => void readBrowse(selected, "fleet", true)}
              />
            )}

            {mode === "publications" && page && (
              <>
                <p className="muted">{t("ui.archives.as_of", { at: page.as_of })}</p>
                {!page.discovery_complete && <p role="status">{t("ui.archives.partial")}</p>}
                {page.resources.length === 0 && <p>{t("ui.archives.no_resources")}</p>}
                <div className="conn-grid">
                  {page.resources.map((original) => (
                    <button
                      type="button"
                      className="btn"
                      key={`${original.position.resource_id}:${original.position.source_id}:${original.position.range_id}`}
                      onClick={() => pickRawResource(original)}
                    >
                      {text(original.database_name)} · {text(original.source_name)}
                      <br />
                      {t("ui.archives.original_bounds", { from: original.collected_from, until: original.collected_until })}
                    </button>
                  ))}
                </div>
                {page.next && (
                  <button type="button" className="btn" onClick={() => void readBrowse(selected, "publications", true)} disabled={busy}>
                    {t("ui.common.next")}
                  </button>
                )}
              </>
            )}

            {target && (
              <section className="archive-investigation">
                <h3>{text(target.databaseName)}</h3>
                {target.rawResource && (
                  <>
                    <ObservationReadQualifications
                      sources={
                        target.rawResource.coverage.kind === "snapshot"
                          ? target.rawResource.coverage.sources.map((source) => ({ source: source.source, state: source.state }))
                          : target.rawResource.coverage.kind === "activity"
                            ? [{ source: target.rawResource.coverage.coverage.source, state: target.rawResource.coverage.coverage.state }]
                            : [{ source: target.rawResource.source_name, state: null }]
                      }
                    />
                    <details>
                      <summary>{t("artifact.ui.evidence")}</summary>
                      <p className="muted">{t("ui.archives.original_scope")}</p>
                      {target.rawResource.native_time.kind === "utc" ? (
                        <p>
                          {t("ui.archives.native_bounds", {
                            from: target.rawResource.native_time.from,
                            until: target.rawResource.native_time.until,
                          })}
                        </p>
                      ) : (
                        <p>{t("ui.archives.native_clock_unknown")}</p>
                      )}
                    </details>
                  </>
                )}
                <h4>{t("ui.window.title")}</h4>
                <p className="muted">{t("ui.window.intro")}</p>
                <div className="archive-time-range">
                  <label className="field">
                    {t("ui.window.from")}
                    <input
                      type="datetime-local"
                      value={windowFrom}
                      onChange={(event) => {
                        pending.current?.abort();
                        setWindowFrom(event.target.value);
                        setWindowReport(null);
                        setArtifacts(null);
                        setError(null);
                      }}
                    />
                  </label>
                  <label className="field">
                    {t("ui.window.until")}
                    <input
                      type="datetime-local"
                      value={windowUntil}
                      onChange={(event) => {
                        pending.current?.abort();
                        setWindowUntil(event.target.value);
                        setWindowReport(null);
                        setArtifacts(null);
                        setError(null);
                      }}
                    />
                  </label>
                </div>
                <div className="actions">
                  {ARCHIVE_WINDOW_PRESETS.map((minutes: ArchiveWindowPresetMinutes) => {
                    const labelKey =
                      minutes === 60
                        ? "ui.window_picker.short_1h"
                        : minutes === 1440
                          ? "ui.window_picker.short_24h"
                          : "ui.window_picker.short_7d";
                    return (
                      <button
                        type="button"
                        className="btn"
                        key={minutes}
                        onClick={() => {
                          const anchor = archiveUtcInput(windowUntil) ? windowUntil : target.anchorUntil;
                          const preset = archivePresetWindowInputs(anchor, minutes);
                          if (!preset) {
                            setError(t("ui.archives.time_invalid"));
                            return;
                          }
                          pending.current?.abort();
                          pending.current = null;
                          setBusy(false);
                          setWindowFrom(preset.from);
                          setWindowUntil(preset.until);
                          setWindowReport(null);
                          setArtifacts(null);
                          setError(null);
                        }}
                      >
                        {t(labelKey)}
                      </button>
                    );
                  })}
                  <button type="button" className="btn primary" disabled={busy} onClick={() => void readWindow()}>
                    {t("ui.window.answers")}
                  </button>
                  <button type="button" className="btn" disabled={busy} onClick={() => void readWindow("compatible-workload")}>
                    {t("ui.window.workload")}
                  </button>
                  {windowReport && (
                    <button type="button" className="btn" onClick={() => setBaselineWindow(windowReport)}>
                      {t("ui.archives.window_compare.pin_baseline")}
                    </button>
                  )}
                </div>
                <label className="field">
                  {t("ui.archives.around")}
                  <input
                    type="datetime-local"
                    value={around}
                    onChange={(event) => {
                      pending.current?.abort();
                      setAround(event.target.value);
                      setIncident(null);
                      setWindowReport(null);
                      setArtifacts(null);
                      setError(null);
                    }}
                  />
                </label>
                <button type="button" className="btn primary" disabled={busy || archiveUtcInput(around) == null} onClick={() => void investigate()}>
                  {t("ui.archives.investigate")}
                </button>
              </section>
            )}
            {baselineWindow && windowReport && baselineWindow !== windowReport && (
              <ArchiveWindowCompare baseline={baselineWindow} current={windowReport} onClear={() => setBaselineWindow(null)} />
            )}
            {incident && <ArchiveIncident value={incident} onArtifacts={(selectionTarget) => void inspectArtifacts(selectionTarget)} />}
            {windowReport && (
              <>
                <ArchiveWindow value={windowReport} onArtifacts={(selectionTarget) => void inspectArtifacts(selectionTarget)} />
                {windowReport.next && (
                  <button type="button" className="btn" disabled={busy} onClick={() => void readWindow("published-answers", true)}>
                    {t("ui.window.next")}
                  </button>
                )}
              </>
            )}
            {artifacts && <ArchiveArtifacts value={artifacts} />}
          </section>
        )}
      </div>
    </div>
  );
}
