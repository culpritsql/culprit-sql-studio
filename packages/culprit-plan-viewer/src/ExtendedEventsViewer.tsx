import { AlertTriangle, Clock, Copy, Flame, Search, Zap } from "lucide-react";
import { useMemo, useState } from "react";
import { DeadlockViewer } from "./DeadlockViewer.js";
import { copyAndTell, count, digits, t, useLanguage } from "./host.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import { Sql } from "./Sql.js";
import type {
  ExtendedEventsBatch,
  ImportedEventDeadlock,
  ImportedEventLocation,
  MssqlExtendedEventObservation,
  NativeLogProblem,
} from "./types.js";

export type ExtendedEventsFilter = "all" | "deadlock-report" | "error-reported" | "attention";

export type ExtendedEventsViewerProps = {
  batch: ExtendedEventsBatch;
  title?: string;
  defaultFilter?: ExtendedEventsFilter;
  theme?: PlanViewerTheme;
  className?: string;
};

function problemReason(problem: NativeLogProblem): string {
  switch (problem.kind) {
    case "record-limit":
      return t("ui.extended_events.problem.record_limit");
    case "input-limit":
      return t("ui.extended_events.problem.input_limit");
    case "record-bytes":
      return t("ui.extended_events.problem.record_bytes");
    case "field-bytes":
      return t("ui.extended_events.problem.field_bytes");
    case "malformed-record":
      return t("ui.extended_events.problem.malformed");
    default:
      return t("ui.extended_events.problem.other");
  }
}

function formatHexOffset(location: ImportedEventLocation | undefined): string | null {
  if (!location) return null;
  const evHex = `0x${location.event_offset.toString(16)}`;
  if (location.buffer_offset !== null) {
    return `0x${location.buffer_offset.toString(16)} / ${evHex}`;
  }
  return evHex;
}

function eventTimeLabel(row: MssqlExtendedEventObservation): { text: string; title: string } {
  if (row.timestamp_utc) {
    return {
      text: t("ui.extended_events.time.utc", { time: row.timestamp_utc }),
      title: row.timestamp_utc,
    };
  }
  if (row.timestamp) {
    return {
      text: t("ui.extended_events.time.local", { time: row.timestamp }),
      title: row.timestamp,
    };
  }
  const unknown = t("ui.extended_events.time.unknown");
  return { text: unknown, title: unknown };
}

function matchesQuery(row: MssqlExtendedEventObservation, needle: string): boolean {
  if (!needle) return true;
  const parts: Array<string | null | undefined> = [
    row.database,
    row.database_id,
    row.session_id,
    row.request_id,
    row.event_sequence,
    row.client?.application,
    row.client?.user,
    row.client?.host,
    row.timestamp_utc,
    row.timestamp,
    row.statement.state === "available" ? row.statement.text : null,
  ];
  if (row.kind.event === "error-reported") {
    parts.push(String(row.kind.number), `sev ${row.kind.severity}`);
  }
  return parts.some((p) => p?.toLowerCase().includes(needle));
}

/**
 * Interactive SQL Server Extended Events (`.xel` and XML) inspector:
 * filters `xml_deadlock_report`, `error_reported`, and `attention` records, preserves UTC vs offset-free timestamps,
 * surfaces partial/truncated block limits honestly, and embeds `DeadlockViewer` for captured deadlock graphs.
 */
export function ExtendedEventsViewer({
  batch,
  title,
  defaultFilter = "all",
  theme,
  className,
}: ExtendedEventsViewerProps) {
  useLanguage();
  const [filter, setFilter] = useState<ExtendedEventsFilter>(defaultFilter);
  const [query, setQuery] = useState("");

  const locationsByPos = useMemo(() => {
    const map = new Map<number, ImportedEventLocation>();
    for (const loc of batch.locations) map.set(loc.position, loc);
    return map;
  }, [batch.locations]);

  const deadlocksByPos = useMemo(() => {
    const map = new Map<number, ImportedEventDeadlock[]>();
    for (const item of batch.deadlocks) {
      const list = map.get(item.position);
      if (list) list.push(item);
      else map.set(item.position, [item]);
    }
    return map;
  }, [batch.deadlocks]);

  const counts = useMemo(() => {
    let deadlocks = 0;
    let errors = 0;
    let attentions = 0;
    for (const row of batch.records) {
      if (row.kind.event === "deadlock-report") deadlocks++;
      else if (row.kind.event === "error-reported") errors++;
      else attentions++;
    }
    return { all: batch.records.length, deadlocks, errors, attentions };
  }, [batch.records]);

  const issueBreakdown = useMemo(() => {
    let skippedUnsupported = 0;
    let decodeIssues = 0;
    let firstProblem: NativeLogProblem | null = null;
    for (const issue of batch.issues) {
      if (issue.problem.kind === "unsupported-event") {
        skippedUnsupported++;
      } else {
        decodeIssues++;
        if (!firstProblem) firstProblem = issue.problem;
      }
    }
    return { skippedUnsupported, decodeIssues, firstProblem };
  }, [batch.issues]);

  const normalizedQuery = query.trim().toLowerCase();
  const filteredRecords = useMemo(
    () =>
      batch.records.filter(
        (row) => (filter === "all" || row.kind.event === filter) && matchesQuery(row, normalizedQuery),
      ),
    [batch.records, filter, normalizedQuery],
  );

  return (
    <section
      className={`culprit-pv xe-viewer ${className ?? ""}`}
      data-theme={theme && theme !== "system" ? theme : undefined}
    >
      <header className="xe-head">
        <div className="xe-head-top">
          <div className="xe-title-group">
            <span className="xe-format-badge">
              {batch.format === "xel" ? t("ui.extended_events.badge.xel") : t("ui.extended_events.badge.xml")}
            </span>
            <h2 className="xe-title">{title || t("ui.extended_events.title.default")}</h2>
          </div>
          <div className="xe-stats">
            <span>
              {t("ui.extended_events.summary.events", {
                n: batch.records.length,
                count: count(batch.records.length),
              })}
            </span>
            <span aria-hidden="true">·</span>
            <span>
              {t("ui.extended_events.summary.examined", {
                n: batch.records_examined,
                count: count(batch.records_examined),
              })}
            </span>
            <span aria-hidden="true">·</span>
            <span>
              {t("ui.extended_events.summary.deadlocks", {
                n: batch.deadlocks.length,
                count: count(batch.deadlocks.length),
              })}
            </span>
          </div>
        </div>
        <p className="xe-scope muted">{t("ui.extended_events.scope_note")}</p>

        {(batch.stopped !== null || issueBreakdown.decodeIssues > 0 || issueBreakdown.skippedUnsupported > 0) && (
          <div className="xe-banners" role="status">
            {batch.stopped !== null && (
              <div className="xe-banner warn">
                <AlertTriangle size={14} />
                <span>{t("ui.extended_events.banner.stopped", { reason: problemReason(batch.stopped) })}</span>
              </div>
            )}
            {issueBreakdown.decodeIssues > 0 && issueBreakdown.firstProblem && (
              <div className="xe-banner warn">
                <AlertTriangle size={14} />
                <span>
                  {t("ui.extended_events.banner.issues", {
                    n: issueBreakdown.decodeIssues,
                    count: count(issueBreakdown.decodeIssues),
                    reason: problemReason(issueBreakdown.firstProblem),
                  })}
                </span>
              </div>
            )}
            {issueBreakdown.skippedUnsupported > 0 && (
              <div className="xe-banner info">
                <span>
                  {t("ui.extended_events.banner.skipped_other", {
                    n: issueBreakdown.skippedUnsupported,
                    count: count(issueBreakdown.skippedUnsupported),
                  })}
                </span>
              </div>
            )}
          </div>
        )}

        <div className="xe-toolbar">
          <div className="seg xe-filters" role="tablist">
            <button
              type="button"
              role="tab"
              aria-selected={filter === "all"}
              className={filter === "all" ? "on" : ""}
              onClick={() => setFilter("all")}
            >
              {t("ui.extended_events.filter.all")} <span className="xe-pill-count">{count(counts.all)}</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={filter === "deadlock-report"}
              className={filter === "deadlock-report" ? "on" : ""}
              onClick={() => setFilter("deadlock-report")}
            >
              {t("ui.extended_events.filter.deadlocks")} <span className="xe-pill-count">{count(counts.deadlocks)}</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={filter === "error-reported"}
              className={filter === "error-reported" ? "on" : ""}
              onClick={() => setFilter("error-reported")}
            >
              {t("ui.extended_events.filter.errors")} <span className="xe-pill-count">{count(counts.errors)}</span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={filter === "attention"}
              className={filter === "attention" ? "on" : ""}
              onClick={() => setFilter("attention")}
            >
              {t("ui.extended_events.filter.attentions")}{" "}
              <span className="xe-pill-count">{count(counts.attentions)}</span>
            </button>
          </div>

          <label className="plan-search xe-search">
            <Search size={13} />
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("ui.extended_events.search.placeholder")}
              aria-label={t("ui.extended_events.search.label")}
            />
          </label>
        </div>
      </header>

      {filteredRecords.length === 0 ? (
        <div className="xe-empty muted">
          {batch.records.length === 0
            ? t("ui.extended_events.empty.artifact")
            : t("ui.extended_events.empty.filtered")}
        </div>
      ) : (
        <div className="xe-list">
          {filteredRecords.map((row) => (
            <ExtendedEventCard
              key={row.position}
              row={row}
              location={locationsByPos.get(row.position)}
              deadlocks={deadlocksByPos.get(row.position) ?? []}
              theme={theme}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function ExtendedEventCard({
  row,
  location,
  deadlocks,
  theme,
}: {
  row: MssqlExtendedEventObservation;
  location: ImportedEventLocation | undefined;
  deadlocks: ImportedEventDeadlock[];
  theme: PlanViewerTheme | undefined;
}) {
  const time = eventTimeLabel(row);
  const offset = formatHexOffset(location);

  let badgeClass = "attention";
  let badgeText = t("ui.extended_events.kind.attention");
  let cardTitle = t("ui.extended_events.card.attention_title");
  let icon = <Clock size={13} />;

  if (row.kind.event === "deadlock-report") {
    badgeClass = "deadlock";
    badgeText = t("ui.extended_events.kind.deadlock");
    cardTitle = t("ui.extended_events.card.deadlock_title", {
      n: row.kind.graphs,
      count: count(row.kind.graphs),
    });
    icon = <Flame size={13} />;
  } else if (row.kind.event === "error-reported") {
    badgeClass = row.kind.severity >= 16 ? "error-high" : "error";
    badgeText = t("ui.extended_events.kind.error", {
      number: String(row.kind.number),
      severity: digits(String(row.kind.severity)),
    });
    cardTitle = t("ui.extended_events.card.error_title", {
      number: String(row.kind.number),
      severity: digits(String(row.kind.severity)),
      state: digits(String(row.kind.state)),
    });
    icon = <Zap size={13} />;
  }

  const fields: Array<{ label: string; value: string }> = [];
  if (row.database ?? row.database_id) {
    fields.push({ label: t("ui.extended_events.field.database"), value: (row.database ?? row.database_id)! });
  }
  if (row.session_id) {
    fields.push({ label: t("ui.extended_events.field.session"), value: row.session_id });
  }
  if (row.request_id) {
    fields.push({ label: t("ui.extended_events.field.request"), value: row.request_id });
  }
  if (row.event_sequence) {
    fields.push({ label: t("ui.extended_events.field.sequence"), value: row.event_sequence });
  }
  if (row.client?.application) {
    fields.push({ label: t("ui.extended_events.field.application"), value: row.client.application });
  }
  if (row.client?.user) {
    fields.push({ label: t("ui.extended_events.field.user"), value: row.client.user });
  }
  if (row.client?.host) {
    fields.push({ label: t("ui.extended_events.field.host"), value: row.client.host });
  }
  if (row.kind.event === "attention" && row.kind.native_duration) {
    fields.push({ label: t("ui.extended_events.field.duration"), value: digits(row.kind.native_duration) });
  }
  if (row.kind.event === "error-reported") {
    if (row.kind.intercepted !== null) {
      fields.push({
        label: t("ui.extended_events.field.intercepted"),
        value: row.kind.intercepted ? t("ui.extended_events.value.yes") : t("ui.extended_events.value.no"),
      });
    }
    if (row.kind.user_defined !== null) {
      fields.push({
        label: t("ui.extended_events.field.user_defined"),
        value: row.kind.user_defined ? t("ui.extended_events.value.yes") : t("ui.extended_events.value.no"),
      });
    }
  }
  if (offset) {
    fields.push({ label: t("ui.extended_events.field.offset"), value: offset });
  }

  return (
    <article className="xe-card" data-event-kind={row.kind.event} data-position={row.position}>
      <div className="xe-card-head">
        <span className={`xe-kind-pill ${badgeClass}`}>
          {icon}
          <span>{badgeText}</span>
        </span>
        <span className="xe-time muted" title={time.title}>
          #{row.position} · {time.text}
        </span>
      </div>

      <h3 className="xe-card-title">{cardTitle}</h3>

      {row.kind.event === "attention" && (
        <p className="xe-card-note muted">{t("ui.extended_events.card.attention_note")}</p>
      )}

      {fields.length > 0 && (
        <dl className="xe-meta-grid">
          {fields.map((f) => (
            <div key={f.label} className="xe-meta-item">
              <dt>{f.label}</dt>
              <dd>{f.value}</dd>
            </div>
          ))}
        </dl>
      )}

      {row.statement.state === "available" && (
        <div className="xe-sql-block">
          <div className="xe-sql-head">
            <span className="muted">{t("ui.extended_events.statement.label")}</span>
            <button
              type="button"
              className="btn ghost small"
              onClick={() => copyAndTell(row.statement.state === "available" ? row.statement.text : "", t("ui.common.copied"))}
            >
              <Copy size={12} /> {t("ui.common.copy")}
            </button>
          </div>
          <Sql text={row.statement.text} className="xe-sql" />
        </div>
      )}

      {deadlocks.map(({ graph }, idx) => (
        <details key={graph.id || idx} className="xe-deadlock-details" open>
          <summary>{t("ui.extended_events.deadlock.toggle")}</summary>
          <div className="xe-deadlock-body">
            <DeadlockViewer deadlock={graph} theme={theme} />
          </div>
        </details>
      ))}
    </article>
  );
}
