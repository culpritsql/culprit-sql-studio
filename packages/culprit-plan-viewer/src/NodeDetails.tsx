import { ChevronRight, Search, TriangleAlert } from "lucide-react";
import { Fragment, useMemo, useState } from "react";
import { DocdbWork } from "./DocdbWork.js";
import { cost, count, duration, estRowsForRuns, exact, misestimate, operatorRuns, operatorView, percent, propValue, shortPredicate, t } from "./host.js";
import { OpIcon } from "./OpIcon.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import type { Engine, Plan, PlanInsight, PlanNode, PlanPageServerIoView } from "./types.js";

function warningLabel(warning: string): string {
  switch (warning) {
    case "implicit-conversion":
      return t("ui.node_details.warning.implicit_conversion");
    case "spill":
      return t("ui.node_details.warning.spill");
    case "no-join-predicate":
      return t("ui.node_details.warning.no_join_predicate");
    case "missing-statistics":
      return t("ui.node_details.warning.missing_statistics");
    case "memory-grant":
      return t("ui.node_details.warning.memory_grant");
    case "misestimate":
      return t("ui.node_details.warning.misestimate");
    case "residual-predicate":
      return t("ui.node_details.warning.residual_predicate");
    default:
      return warning;
  }
}

const SHOWN = new Set([
  "PhysicalOp",
  "LogicalOp",
  "EstimateRows",
  "EstimatedRowsRead",
  "TableCardinality",
  "EstimatedTotalSubtreeCost",
  "EstimateCPU",
  "EstimateIO",
  "ActualRows",
  "ActualExecutions",
  "ActualRowsRead",
  "ActualLogicalReads",
  "ActualPhysicalReads",
  "ActualExecutionMode",
  "EstimateRebinds",
  "EstimateRewinds",
  "OutputList",
  "Rows Removed by Filter",
  "Parent Relationship",
  "Shared Hit Blocks",
  "Shared Read Blocks",
  "Shared Dirtied Blocks",
  "Shared Written Blocks",
  "Local Hit Blocks",
  "Local Read Blocks",
  "Temp Read Blocks",
  "Temp Written Blocks",
  "I/O Read Time",
  "I/O Write Time",
  "WAL Bytes",
  "WAL Records",
  "Workers Planned",
  "Workers Launched",
  "Sort Space Used",
]);

const MAX_PAGE_SERVER_METRICS = 4;
const MAX_PAGE_SERVER_LIMITATIONS = 2;

function formatKb(kb: number): string {
  if (kb >= 1024 * 1024) return `${(kb / 1024 / 1024).toFixed(1)}\u00a0GB`;
  if (kb >= 1024) return `${(kb / 1024).toFixed(kb >= 10 * 1024 ? 0 : 1)}\u00a0MB`;
  return `${Math.round(kb)}\u00a0KB`;
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024 / 1024).toFixed(1)}\u00a0GB`;
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)}\u00a0MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)}\u00a0KB`;
  return `${Math.round(bytes)}\u00a0B`;
}

function uses8KbPages(engine?: Engine): boolean {
  return engine === "postgres" || engine === "mssql" || engine === "yugabyte";
}

function formatPages(pages: number, engine?: Engine): string {
  const base = exact(pages);
  if (!uses8KbPages(engine) || pages <= 0) return base;
  return `${base} (${formatKb(pages * 8)})`;
}

function optNumber(raw: string | undefined): number | null {
  if (raw == null || raw.trim() === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function PageServerIoDetails({ view }: { view: PlanPageServerIoView }) {
  return (
    <details className="nd-all nd-page-server">
      <summary>
        <ChevronRight size={13} className="chev" aria-hidden="true" /> {t("view.plan_page_server_io.title")}
      </summary>
      <div className="nd-card nd-page-server-body">
        <dl>
          {view.metrics.slice(0, MAX_PAGE_SERVER_METRICS).map((metric) => (
            <Fragment key={metric.key || metric.label}>
              <dt>{metric.label}</dt>
              <dd>{metric.text ?? t("view.plan_page_server_io.unknown")}</dd>
            </Fragment>
          ))}
        </dl>
        {view.limitations.slice(0, MAX_PAGE_SERVER_LIMITATIONS).map((limitation) => (
          <p className="nd-page-server-limit muted" key={limitation}>
            {limitation}
          </p>
        ))}
      </div>
    </details>
  );
}

function ratio(plan: Plan | undefined, n: PlanNode) {
  const factor = misestimate(plan, n);
  if (factor == null || Math.abs(factor) < 2) return null;
  return (
    <span className={`nd-ratio ${n.warnings.includes("misestimate") ? "bad" : ""}`}>
      {factor > 0
        ? t("ui.node_details.more_than_expected", { factor: exact(Math.round(Math.abs(factor))) })
        : t("ui.node_details.fewer_than_expected", { factor: exact(Math.round(Math.abs(factor))) })}
    </span>
  );
}

/** One operator, explained: what it does, its rows, cost, buffers/I/O, parallelism, warnings, then raw properties on demand. */
export function NodeDetails({
  plan,
  node,
  share,
  subtreeShare,
  noShare,
  insights = [],
  estimates = true,
  actual = false,
  theme,
  className,
}: {
  plan?: Plan;
  node: PlanNode;
  share: number | null;
  subtreeShare?: number | null;
  noShare?: string | null;
  insights?: PlanInsight[];
  estimates?: boolean;
  actual?: boolean;
  theme?: PlanViewerTheme;
  className?: string;
}) {
  const [filter, setFilter] = useState("");
  const props = useMemo(() => {
    const q = filter.trim().toLowerCase();
    return Object.entries(node.props)
      .filter(([k, v]) => v != null && v !== "" && !SHOWN.has(k))
      .filter(([k, v]) => !q || k.toLowerCase().includes(q) || v.toLowerCase().includes(q))
      .sort(([a], [b]) => a.localeCompare(b));
  }, [node, filter]);
  const cpu = Number(node.props.EstimateCPU ?? NaN);
  const io = Number(node.props.EstimateIO ?? NaN);
  const split = Number.isFinite(cpu) && Number.isFinite(io) && cpu + io > 0 ? cpu / (cpu + io) : null;
  const removed = node.props["Rows Removed by Filter"];
  const executions = operatorRuns(plan, node) ?? 0;
  const pageServerIo = operatorView(plan, node)?.page_server_io;

  const rt = node.runtime;
  const logicalReads = rt?.logical_reads ?? null;
  const physicalReads = rt?.physical_reads ?? null;
  const cachedReads = logicalReads != null && physicalReads != null ? Math.max(0, logicalReads - physicalReads) : null;
  const hitShare = logicalReads != null && logicalReads > 0 && cachedReads != null ? cachedReads / logicalReads : null;
  const spillPages = rt?.spill_pages != null && rt.spill_pages > 0 ? rt.spill_pages : null;
  const cpuMs = rt?.cpu_ms ?? null;
  const heapFetches = node.facts.heap_fetches != null && node.facts.heap_fetches > 0 ? node.facts.heap_fetches : null;
  const sortSpaceKb = node.facts.sort_space_kb != null && node.facts.sort_space_kb > 0 ? node.facts.sort_space_kb : null;
  const diskUsageKb = node.facts.disk_usage_kb != null && node.facts.disk_usage_kb > 0 ? node.facts.disk_usage_kb : null;
  const ioReadMs = optNumber(node.props["I/O Read Time"]);
  const ioWriteMs = optNumber(node.props["I/O Write Time"]);
  const walBytes = optNumber(node.props["WAL Bytes"]);
  const execMode = node.facts.actual_execution_mode ?? null;

  const hasIoCard =
    logicalReads != null ||
    physicalReads != null ||
    spillPages != null ||
    cpuMs != null ||
    heapFetches != null ||
    sortSpaceKb != null ||
    diskUsageKb != null ||
    ioReadMs != null ||
    ioWriteMs != null ||
    walBytes != null ||
    execMode != null;

  const workersPlanned = node.facts.workers_planned ?? null;
  const workersLaunched = node.facts.workers_launched ?? null;
  const threadRows = rt?.thread_rows ?? [];
  const maxWorkerRows = threadRows.length > 0 ? Math.max(1, ...threadRows) : 1;
  const hasParallelCard = workersPlanned != null || workersLaunched != null || threadRows.length > 1;

  return (
    <div className={`culprit-pv nd ${className ?? ""}`} data-theme={theme && theme !== "system" ? theme : undefined}>
      <div className="nd-head">
        <span className="nd-icon">
          <OpIcon node={node} size={16} />
        </span>
        <div className="nd-title">
          <div className="nd-op">
            {node.op}
            {node.detail && <span className="nd-detail">{node.detail}</span>}
          </div>
          {(node.object || node.index) && (
            <div className="nd-obj mono">
              {node.object}
              {node.index && <span className="muted"> · {node.index}</span>}
            </div>
          )}
        </div>
        {estimates &&
          (share != null ? (
            <span className="nd-share" title={t("ui.node_details.share_hint")}>
              {percent(share)}
              <span className="muted"> {t("ui.node_details.of_the_work")}</span>
            </span>
          ) : (
            <span className="nd-share muted" title={noShare ?? undefined}>
              {t("ui.node_details.no_share_of_work")}
            </span>
          ))}
      </div>

      {insights.length > 0 && (
        <ul className="nd-insights">
          {insights.map((i) => (
            <li key={i.rule} className={i.severity}>
              <i />
              {i.title}
            </li>
          ))}
        </ul>
      )}

      <DocdbWork metrics={node.facts.docdb} basis="loop" />
      <div className="nd-grid">
        {(node.seek_predicate || node.predicate) && (
          <section className="nd-card wide">
            <h4>{t("ui.node_details.what_it_does")}</h4>
            {node.seek_predicate && (
              <div className="nd-cond">
                <span className="nd-k">{node.kind === "seek" || node.kind === "lookup" ? t("ui.node_details.seeks_on") : t("ui.node_details.index_condition")}</span>
                <code>{shortPredicate(node.seek_predicate)}</code>
              </div>
            )}
            {node.predicate && (
              <div className="nd-cond">
                <span className="nd-k">{t("ui.node_details.then_filters")}</span>
                <code>{shortPredicate(node.predicate)}</code>
              </div>
            )}
          </section>
        )}
        {estimates && (
          <section className="nd-card">
            <h4>{t("ui.node_details.rows")}</h4>
            <dl>
              <dt>{t("ui.node_details.estimated")}</dt>
              <dd title={node.act_executions != null ? t("ui.node_details.estimated_hint") : undefined}>{exact(estRowsForRuns(plan, node))}</dd>
              {node.act_rows != null && (
                <>
                  <dt>{t("ui.node_details.actual")}</dt>
                  <dd>
                    {exact(node.act_rows)} {ratio(plan, node)}
                  </dd>
                </>
              )}
              {executions > 1 && (
                <>
                  <dt>{node.act_executions != null ? t("ui.node_details.ran") : t("ui.node_details.expected_runs")}</dt>
                  <dd>{t("ui.node_details.times", { n: executions, count: exact(executions) })}</dd>
                </>
              )}
              {node.rows_read != null && (
                <>
                  <dt>{actual ? t("ui.node_details.read") : t("ui.node_details.expected_to_read")}</dt>
                  <dd>{exact(node.rows_read)}</dd>
                </>
              )}
              {removed && (
                <>
                  <dt>{t("ui.node_details.filtered_out")}</dt>
                  <dd>{propValue(removed)}</dd>
                </>
              )}
              {node.table_rows != null && (
                <>
                  <dt>{t("ui.node_details.in_table")}</dt>
                  <dd>{exact(node.table_rows)}</dd>
                </>
              )}
            </dl>
          </section>
        )}
        {estimates && (
          <section className="nd-card">
            <h4>{t("ui.node_details.cost")}</h4>
            <dl>
              <dt>{t("ui.node_details.this_operator")}</dt>
              <dd>
                {share != null ? percent(share) : t("ui.node_details.no_share")}{" "}
                <span className="muted">
                  · {plan?.facts.estimated_cost_unavailable ? t("ui.node_details.cost_unavailable") : t("ui.node_details.units", { cost: cost(node.self_cost) })}
                </span>
              </dd>
              {subtreeShare != null && (
                <>
                  <dt>{t("ui.node_details.with_inputs")}</dt>
                  <dd>{percent(subtreeShare)}</dd>
                </>
              )}
              {node.act_time_ms != null && (
                <>
                  <dt>{t("ui.node_details.actual_time")}</dt>
                  <dd>{duration(node.act_time_ms)}</dd>
                </>
              )}
            </dl>
            {share == null && noShare && <p className="muted" style={{ margin: "6px 0 0", fontSize: 12 }}>{noShare}</p>}
            {split != null && (
              <div className="nd-split" title={t("ui.node_details.split_hint", { cpu: percent(split), io: percent(1 - split) })}>
                <div className="nd-split-bar">
                  <span className="cpu" style={{ width: `${split * 100}%` }} />
                  <span className="io" style={{ width: `${(1 - split) * 100}%` }} />
                </div>
                <div className="nd-split-legend">
                  <span>
                    <i className="cpu" />
                    {t("ui.node_details.cpu_share", { share: percent(split) })}
                  </span>
                  <span>
                    <i className="io" />
                    {t("ui.node_details.io_share", { share: percent(1 - split) })}
                  </span>
                </div>
              </div>
            )}
          </section>
        )}
        {hasIoCard && (
          <section className="nd-card">
            <h4>{t("ui.node_details.io_buffers")}</h4>
            <dl>
              {logicalReads != null && (
                <>
                  <dt>{t("ui.node_details.logical_reads")}</dt>
                  <dd>{formatPages(logicalReads, plan?.engine)}</dd>
                </>
              )}
              {cachedReads != null && cachedReads > 0 && (
                <>
                  <dt>{t("ui.node_details.from_cache")}</dt>
                  <dd>{formatPages(cachedReads, plan?.engine)}</dd>
                </>
              )}
              {physicalReads != null && physicalReads > 0 && (
                <>
                  <dt>{t("ui.node_details.from_disk")}</dt>
                  <dd>{formatPages(physicalReads, plan?.engine)}</dd>
                </>
              )}
              {spillPages != null && (
                <>
                  <dt>{t("ui.node_details.spill_pages")}</dt>
                  <dd>
                    <span className="nd-ratio bad">{formatPages(spillPages, plan?.engine)}</span>
                  </dd>
                </>
              )}
              {sortSpaceKb != null && (
                <>
                  <dt>{t("ui.node_details.sort_memory")}</dt>
                  <dd>{formatKb(sortSpaceKb)}</dd>
                </>
              )}
              {diskUsageKb != null && sortSpaceKb == null && (
                <>
                  <dt>{t("ui.node_details.work_memory")}</dt>
                  <dd>{formatKb(diskUsageKb)}</dd>
                </>
              )}
              {heapFetches != null && (
                <>
                  <dt>{t("ui.node_details.heap_fetches")}</dt>
                  <dd>{exact(heapFetches)}</dd>
                </>
              )}
              {cpuMs != null && (
                <>
                  <dt>{t("ui.node_details.cpu_time")}</dt>
                  <dd>{duration(cpuMs)}</dd>
                </>
              )}
              {ioReadMs != null && (
                <>
                  <dt>{t("ui.node_details.io_read_time")}</dt>
                  <dd>{duration(ioReadMs)}</dd>
                </>
              )}
              {ioWriteMs != null && (
                <>
                  <dt>{t("ui.node_details.io_write_time")}</dt>
                  <dd>{duration(ioWriteMs)}</dd>
                </>
              )}
              {walBytes != null && (
                <>
                  <dt>{t("ui.node_details.wal_bytes")}</dt>
                  <dd>{formatBytes(walBytes)}</dd>
                </>
              )}
              {execMode != null && (
                <>
                  <dt>{t("ui.node_details.execution_mode")}</dt>
                  <dd>
                    {execMode.kind === "batch"
                      ? t("ui.node_details.mode_batch")
                      : execMode.kind === "row"
                        ? t("ui.node_details.mode_row")
                        : execMode.value}
                  </dd>
                </>
              )}
            </dl>
            {hitShare != null && (
              <div className="nd-split">
                <div className="nd-split-bar">
                  <span className="hit" style={{ width: `${hitShare * 100}%` }} />
                  <span className="disk" style={{ width: `${(1 - hitShare) * 100}%` }} />
                </div>
                <div className="nd-split-legend">
                  <span>
                    <i className="hit" />
                    {t("ui.node_details.cache_hit_share", { share: percent(hitShare) })}
                  </span>
                  <span>
                    <i className="disk" />
                    {t("ui.node_details.disk_read_share", { share: percent(1 - hitShare) })}
                  </span>
                </div>
              </div>
            )}
          </section>
        )}
        {hasParallelCard && (
          <section className="nd-card">
            <h4>{t("ui.node_details.parallelism")}</h4>
            <dl>
              {(workersLaunched != null || workersPlanned != null) && (
                <>
                  <dt>{t("ui.node_details.workers_launched")}</dt>
                  <dd>
                    {workersLaunched != null && workersPlanned != null
                      ? t("ui.node_details.workers_of_planned", { launched: String(workersLaunched), planned: String(workersPlanned) })
                      : String(workersLaunched ?? workersPlanned)}
                  </dd>
                </>
              )}
            </dl>
            {threadRows.length > 1 && (
              <div className="nd-workers">
                {threadRows.slice(0, 8).map((wRows, idx) => (
                  <div key={idx} className="nd-worker-row">
                    <span className="muted">{t("ui.node_details.worker_rows", { worker: String(idx + 1) })}</span>
                    <span className="nd-worker-bar">
                      <span style={{ width: `${Math.max(3, (wRows / maxWorkerRows) * 100)}%` }} />
                    </span>
                    <span className="num">{count(wRows)}</span>
                  </div>
                ))}
              </div>
            )}
          </section>
        )}
        {node.warnings.length > 0 && (
          <section className="nd-card nd-warn">
            <h4>
              <TriangleAlert size={12} /> {t("ui.node_details.warnings")}
            </h4>
            <ul>
              {node.warnings.map((w) => (
                <li key={w}>{warningLabel(w)}</li>
              ))}
            </ul>
          </section>
        )}
      </div>

      {pageServerIo && <PageServerIoDetails key={node.id} view={pageServerIo} />}

      {Object.keys(node.props).length > 0 && (
        <details className="nd-all">
          <summary>
            <ChevronRight size={13} className="chev" /> {t("ui.node_details.all_properties")} <span className="muted">{props.length}</span>
          </summary>
          <label className="search nd-search">
            <Search size={12} />
            <input value={filter} placeholder={t("ui.node_details.find_property")} onChange={(e) => setFilter(e.target.value)} />
          </label>
          <dl className="nd-props">
            {props.map(([k, v]) => (
              <div key={k} className="nd-prop">
                <dt>{k}</dt>
                <dd title={v.length > 80 ? v : undefined}>{propValue(v)}</dd>
              </div>
            ))}
          </dl>
        </details>
      )}
    </div>
  );
}
