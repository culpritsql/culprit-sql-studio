import { useMemo, useRef, useState } from "react";
import { EngineBadge } from "../../components/EngineBadge";
import { nativeTextPreview } from "../../components/NativeObservationFields";
import type { Environment } from "../../gen/Environment";
import type { WarehouseFleetFreshness } from "../../gen/WarehouseFleetFreshness";
import type { WarehouseFleetReport } from "../../gen/WarehouseFleetReport";
import type { WarehouseFleetRow } from "../../gen/WarehouseFleetRow";
import { type FleetChip, filterFleetRows, groupFleetRows } from "../../lib/archive-fleet";
import { count } from "../../lib/format";
import { useHotkeys } from "../../lib/hotkeys";
import { t } from "../../lib/i18n";

const text = (value: string) => nativeTextPreview(value, 256);

const CHIPS: FleetChip[] = ["attention", "incidents", "regressions", "high-findings", "collector-health", "all"];

function chipLabel(chip: FleetChip): string {
  switch (chip) {
    case "attention":
      return t("ui.archives.fleet.filter.attention");
    case "incidents":
      return t("ui.archives.fleet.filter.incidents");
    case "regressions":
      return t("ui.archives.fleet.filter.regressions");
    case "high-findings":
      return t("ui.archives.fleet.filter.high_findings");
    case "collector-health":
      return t("ui.archives.fleet.filter.collector_health");
    case "all":
      return t("ui.archives.fleet.filter.all");
  }
}

function freshnessLabel(freshness: WarehouseFleetFreshness): string {
  switch (freshness) {
    case "observed":
      return t("ui.archives.fleet.freshness.observed");
    case "stale":
      return t("ui.archives.fleet.freshness.stale");
    case "partial":
      return t("ui.archives.fleet.freshness.partial");
    case "no-derived-diagnosis":
      return t("ui.archives.fleet.freshness.no_derived");
    case "clock-boundary":
      return t("ui.archives.fleet.freshness.clock_boundary");
    case "unknown-clock":
      return t("ui.archives.fleet.freshness.unknown_clock");
  }
}

function asEnvironment(value: string | null): Environment {
  if (value === "production" || value === "staging" || value === "development") return value;
  return "development";
}

export function ArchiveFleet({
  report,
  selectedResourceId,
  busy,
  onSelect,
  onNext,
}: {
  report: WarehouseFleetReport;
  selectedResourceId: string | null;
  busy: boolean;
  onSelect: (row: WarehouseFleetRow) => void;
  onNext: () => void;
}) {
  const [chip, setChip] = useState<FleetChip>("all");
  const [byShared, setByShared] = useState(false);
  const [search, setSearch] = useState("");
  const [cursorIndex, setCursorIndex] = useState(0);
  const searchRef = useRef<HTMLInputElement>(null);

  const visible = useMemo(() => filterFleetRows(report.rows, chip, search), [report.rows, chip, search]);
  const groups = useMemo(
    () => groupFleetRows(visible, byShared, t("ui.archives.fleet.standalone_group")),
    [visible, byShared],
  );

  useHotkeys({
    "/": () => searchRef.current?.focus(),
    j: () => {
      if (visible.length === 0) return;
      const next = Math.min(visible.length - 1, cursorIndex + 1);
      setCursorIndex(next);
      const row = visible[next];
      if (row) onSelect(row);
    },
    k: () => {
      if (visible.length === 0) return;
      const prev = Math.max(0, cursorIndex - 1);
      setCursorIndex(prev);
      const row = visible[prev];
      if (row) onSelect(row);
    },
  });

  const s = report.summary;

  return (
    <section className="archive-fleet" aria-label={t("ui.archives.mode.fleet")}>
      <div className="archive-fleet-summary" role="status">
        {t("ui.archives.fleet.summary_strip", {
          incidents: s.active_incidents,
          high: s.high_finding_resources,
          regressions: s.regression_resources,
          stale: s.stale_or_partial_resources,
        })}
      </div>

      <div className="archive-fleet-toolbar">
        <div className="archive-fleet-chips" role="toolbar" aria-label={t("ui.archives.mode.fleet")}>
          {CHIPS.map((item) => (
            <button
              type="button"
              key={item}
              className={`btn ${chip === item ? "primary" : ""}`}
              aria-pressed={chip === item}
              onClick={() => {
                setChip(item);
                setCursorIndex(0);
              }}
            >
              {chipLabel(item)}
            </button>
          ))}
          <button
            type="button"
            className={`btn ${byShared ? "primary" : ""}`}
            aria-pressed={byShared}
            onClick={() => setByShared(!byShared)}
          >
            {t("ui.archives.fleet.group_by_shared")}
          </button>
        </div>
        <label className="field archive-fleet-search">
          <input
            ref={searchRef}
            type="search"
            placeholder={t("ui.archives.fleet.search_placeholder")}
            value={search}
            onChange={(event) => {
              setSearch(event.target.value);
              setCursorIndex(0);
            }}
          />
        </label>
      </div>

      {report.qualifications.map((note, index) => (
        <p className="muted" key={index}>
          {text(note)}
        </p>
      ))}

      {visible.length === 0 ? (
        <p className="muted">{t("ui.archives.fleet.empty")}</p>
      ) : (
        groups.map((group) => (
          <div key={group.key} className="archive-fleet-group">
            {group.label && <h4 className="archive-fleet-group-title">{text(group.label)}</h4>}
            <div className="archive-fleet-grid">
              {group.rows.map((row) => {
                const isSelected = selectedResourceId === row.resource_id;
                const freshnessTone =
                  row.freshness === "observed" ? "good" : row.freshness === "stale" ? "bad" : "warn";
                return (
                  <button
                    type="button"
                    key={row.resource_id}
                    className={`archive-fleet-card ${isSelected ? "selected" : ""}`}
                    aria-pressed={isSelected}
                    onClick={() => onSelect(row)}
                  >
                    <div className="archive-fleet-card-head">
                      <EngineBadge engine={row.engine} environment={asEnvironment(row.environment)} />
                      <strong>{text(row.connection_alias ?? row.database_name)}</strong>
                      {row.connection_alias && row.connection_alias !== row.database_name && (
                        <span className="muted">· {text(row.database_name)}</span>
                      )}
                      <span className={`archive-pill ${freshnessTone}`}>{freshnessLabel(row.freshness)}</span>
                    </div>
                    {row.shared_group && <div className="muted">{text(row.shared_group.label)}</div>}
                    <div className="archive-fleet-metrics">
                      {row.active_incidents > 0 && (
                        <span className="archive-pill bad">
                          {t("ui.archives.fleet.metric.incidents", {
                            n: row.active_incidents,
                            count: count(row.active_incidents),
                          })}
                        </span>
                      )}
                      {row.uncertain_incidents > 0 && (
                        <span className="archive-pill warn">
                          {t("ui.archives.fleet.metric.uncertain", {
                            n: row.uncertain_incidents,
                            count: count(row.uncertain_incidents),
                          })}
                        </span>
                      )}
                      {row.high_findings > 0 && (
                        <span className="archive-pill bad">
                          {t("ui.archives.fleet.metric.high_findings", {
                            n: row.high_findings,
                            count: count(row.high_findings),
                          })}
                        </span>
                      )}
                      {row.actionable_findings > 0 && (
                        <span className="archive-pill info">
                          {t("ui.archives.fleet.metric.actionable", {
                            n: row.actionable_findings,
                            count: count(row.actionable_findings),
                          })}
                        </span>
                      )}
                      {row.recent_changes > 0 && (
                        <span className="archive-pill info">
                          {t("ui.archives.fleet.metric.changes", {
                            n: row.recent_changes,
                            count: count(row.recent_changes),
                          })}
                        </span>
                      )}
                    </div>
                    <div className="muted archive-fleet-bounds">
                      {t("ui.archives.original_bounds", {
                        from: row.latest_observed_from ?? row.latest_collected_from,
                        until: row.latest_observed_until ?? row.latest_collected_until,
                      })}{" "}
                      · {t("ui.archives.fleet.inspect")}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>
        ))
      )}

      {report.next && (
        <button type="button" className="btn" disabled={busy} onClick={onNext}>
          {t("ui.common.next")}
        </button>
      )}
    </section>
  );
}
