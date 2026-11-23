import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { JobComparisonContext, Jobs } from "../components/Jobs";
import { chartEvents } from "../components/ChangeTimeline";
import { HealthTab } from "../components/Health";
import type { OperationalObservation } from "../gen/OperationalObservation";
import { partial } from "../test/fixtures";
import { JOBS_SOURCE, jobPoint, jobsEvidence, jobsReport, agentJobEvidence, agentJobsReport, mysqlEventEvidence, mysqlEventsReport } from "../test/jobs";
import { t } from "./i18n";
import { healthReportFixture } from "../test/health-report";

test("continuous-aggregate settings render as exact folded text without becoming a job or refresh verdict", () => {
  const row = partial<OperationalObservation>({ source: "timescale-continuous-aggregates", key: "original-catalog-point", fact: { kind: "timescale-continuous-aggregate", data: {
    position: 1, observed_at: "2026-11-15T12:00:00.123456Z", view: { schema: "public", name: "hourly <script>aggregate-canary</script>" },
    source_hypertable: { schema: "public", name: "readings" }, materialization_hypertable: { schema: "_timescaledb_internal", name: "_materialized_hypertable_2" },
    owner: "native_catalog_owner", materialized_only: false, compression_enabled: true, granular_refresh_enabled: null,
  } } });
  const evidence = jobsEvidence([row]);
  const report = jobsReport([]);
  report.native_rows = 1; report.observed_rows = 1;
  report.entries = [{ source: row.source, observation_key: row.key, job_id: null, kind: "continuous-aggregate", title: "hourly <script>aggregate-canary</script>",
    explanation: t("view.jobs.continuous_aggregates.explanation"), metrics: [], investigation: t("view.jobs.continuous_aggregates.investigate"), verify: t("view.jobs.continuous_aggregates.verify") }];
  const html = renderToStaticMarkup(<Jobs evidence={evidence} report={report} />);
  expect(html).toContain("hourly &lt;script&gt;aggregate-canary&lt;/script&gt;");
  expect(html).not.toContain("<script>");
  expect(html).toContain(t("view.jobs.continuous_aggregates.explanation"));
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);
  report.entries[0]!.kind = "state";
  expect(renderToStaticMarkup(<Jobs evidence={evidence} report={report} />)).toContain(t("ui.memory_failures.missing_row"));
});

test("an unavailable continuous-aggregate catalog remains visible in Health without a raw point", () => {
  const report = healthReportFixture({ snapshot: { server: { engine: "postgres" }, sources: [{ source: "timescale-continuous-aggregates", state: "unavailable", reason: { text: "Native aggregate catalog permission denied." } }],
    operations: { spaces: [], counters: [], sequences: [], multixacts: [], sqlite: null, cluster: null } } });
  const html = renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="PostgreSQL" />);
  expect(html).toContain("Native aggregate catalog permission denied.");
  expect(html).not.toContain(t("ui.health.empty"));
});

test("job points keep exact native counts in collapsed paged text without rendering read-owner metadata", () => {
  const rows = Array.from({ length: 12 }, (_, index) => jobPoint(index));
  const html = renderToStaticMarkup(<Jobs evidence={jobsEvidence([...rows].reverse())} report={jobsReport(rows)} />);
  expect(html.match(/<article/g)).toHaveLength(10);
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);
  expect(html).toContain("9007199254740993");
  expect(html).toContain("2026-11-15T12:00:00.123456Z");
  expect(html).toContain("native_procedure&lt;script&gt;job-canary&lt;/script&gt;");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("private-read-host-canary");
  expect(html).not.toContain("private-read-database-canary");
  expect(html).not.toContain("sig-dot");
});

test("a missing, duplicate, foreign-source or different-job row cannot supply displayed job details", () => {
  const original = jobPoint();
  const otherSource = structuredClone(original); otherSource.source = "another-native-source";
  const otherJob = structuredClone(original);
  if (otherJob.fact.kind !== "timescale-job-state") throw new Error("Job state fixture required");
  otherJob.fact.data.job_id = "2000";
  for (const rows of [[], [original, original], [otherSource], [otherJob]]) {
    const html = renderToStaticMarkup(<Jobs evidence={jobsEvidence(rows)} report={jobsReport([original])} />);
    expect(html).toContain(t("ui.memory_failures.missing_row"));
    expect(html).not.toContain("9007199254740993");
    expect(html).not.toContain("native_procedure");
  }
});

test("failed scan acquisition stays visible under Health without a typed raw job wrapper", () => {
  const report = healthReportFixture({ snapshot: { server: { engine: "postgres" }, sources: [{ source: JOBS_SOURCE, state: "unavailable", reason: { text: "Native job permission denied." } }],
    operations: { spaces: [], counters: [], sequences: [], multixacts: [], sqlite: null, cluster: null } } });
  const html = renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="PostgreSQL" />);
  expect(html).toContain(t("view.jobs.title"));
  expect(html).toContain("Native job permission denied.");
  expect(html).not.toContain(t("ui.health.empty"));
});

test("a view from different native read bounds cannot authorize job detail presentation", () => {
  const evidence = jobsEvidence();
  const report = jobsReport();
  report.context = { ...report.context, observed_until: "2026-11-15T12:00:02.123456Z" };
  const html = renderToStaticMarkup(<Jobs evidence={evidence} report={report} />);
  expect(html).toContain(t("ui.memory_failures.missing_row"));
  expect(html).not.toContain("9007199254740993");
});

test("Jobs assesses the native-failure opt-in while an explicitly recorded log-source gap remains visible", () => {
  const report = healthReportFixture({ snapshot: { server: { engine: "postgres" }, options: { collect_native_failures: true },
    sources: [{ source: JOBS_SOURCE, state: "unavailable", reason: { text: "Native job permission denied." } }],
    operations: { spaces: [], counters: [], sequences: [], multixacts: [], sqlite: null, cluster: null } } });
  const render = () => renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="PostgreSQL" />);
  expect(render()).not.toContain(`aria-label="${t("view.native_failures.title")}"`);
  report.snapshot.sources!.push({ source: "native failures", state: "unavailable", reason: { text: "Original native-log permission failure." } });
  expect(render()).toContain("Original native-log permission failure.");
  expect(render()).toContain(`aria-label="${t("view.native_failures.title")}"`);
  report.snapshot.sources![1] = { source: "native failures", state: "not-applicable", reason: { text: "An explicit native-log coverage qualification." } };
  expect(render()).toContain("An explicit native-log coverage qualification.");
});

test("Agent history reuses folded exact text, local-clock limits and source binding without a Timescale window", () => {
  const evidence=agentJobEvidence();const report=agentJobsReport(evidence);
  const html=renderToStaticMarkup(<Jobs evidence={evidence} report={report}/>);
  expect(html).toContain("2147483647");expect(html).toContain("250101");expect(html).toContain("stored &lt;script&gt;agent-canary&lt;/script&gt;");expect(html).not.toContain("<script>");
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);expect(html).not.toContain("private-read-database-canary");expect(html).not.toContain("private-read-host-canary");
  expect(html).toContain("Job local times have no recorded offset.");
  report.entries[0]!.kind="state";
  const mismatch=renderToStaticMarkup(<Jobs evidence={evidence} report={report}/>);expect(mismatch).toContain(t("ui.memory_failures.missing_row"));expect(mismatch).not.toContain("2147483647");
});
test("Agent source-only gap cannot fall through to healthy Health emptiness", () => {
  const report=healthReportFixture({snapshot:{server:{engine:"mssql"},sources:[{source:"mssql-agent-jobs",state:"unavailable",reason:{ text: "Native msdb visibility denied." }}],operations:{spaces:[],counters:[],sequences:[],multixacts:[],sqlite:null,cluster:null}}});
  const html=renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="SQL Server"/>);
  expect(html).toContain(t("view.mssql_agent_jobs.title"));expect(html).toContain("Native msdb visibility denied.");expect(html).not.toContain(t("ui.health.empty"));
});

test("Agent not-applicable and acquired-without-view markers retain explicit Health coverage", () => {
  for (const state of ["not-applicable","collected"] as const) {
    const report=healthReportFixture({snapshot:{server:{engine:"mssql"},sources:[{source:"mssql-agent-jobs",state,reason:null}],operations:{spaces:[],counters:[],sequences:[],multixacts:[],sqlite:null,cluster:null}}});
    const html=renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="SQL Server"/>);
    expect(html).toContain(t("view.mssql_agent_jobs.title"));expect(html).not.toContain(t("ui.health.empty"));
    if (state==="collected") expect(html).toContain(t("view.jobs.invalid"));
  }
});



test("scheduled-event context stays folded plain text and requires its original source, row and read", () => {
  const evidence = mysqlEventEvidence(); const report = mysqlEventsReport(evidence);
  const html = renderToStaticMarkup(<Jobs evidence={evidence} report={report} />);
  expect(html).toContain("nightly &lt;script&gt;event-canary&lt;/script&gt;"); expect(html).not.toContain("<script>");
  expect(html).toContain(report.limitations[0]!); expect(html).not.toContain("private-native-event-host");
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);
  evidence.snapshot.context = structuredClone(evidence.snapshot.context); evidence.snapshot.context.observed_until = "2026-11-15T12:00:02.123456Z";
  const changed = renderToStaticMarkup(<Jobs evidence={evidence} report={report} />);
  expect(changed).toContain(t("ui.memory_failures.missing_row")); expect(changed).not.toContain("event-canary");
});

test("unavailable scheduled-event catalog keeps its own source title without Agent or empty-health labels", () => {
  const report = healthReportFixture({ snapshot: { server: { engine: "mysql" }, sources: [{ source: "mysql-scheduled-events", state: "unavailable", reason: { text: "Native event catalog permission denied." } }], operations: { spaces: [], counters: [], sequences: [], multixacts: [], sqlite: null, cluster: null } } });
  const html = renderToStaticMarkup(<HealthTab report={report} pressure={null} resources={[]} engineLabel="MySQL" />);
  expect(html).toContain(t("view.mysql_events.title")); expect(html).toContain("Native event catalog permission denied.");
  expect(html).not.toContain(t("view.mssql_agent_jobs.title")); expect(html).not.toContain(t("ui.health.empty"));
});


test("typed Agent execution stays unplaced and folded with literal DST-sensitive native clock fields", () => {
  const evidence=agentJobEvidence();const report=agentJobsReport(evidence);
  const execution=report.executions.entries[0]!;
  if(execution.time.kind!=="mssql-local")throw new Error("native local fixture");
  execution.time.run_date="20261025";execution.time.run_time="33000";execution.time.started_without_offset="2026-10-25T03:30:00";
  execution.time.history_written_time=true;
  const before=JSON.stringify(report);
  const html=renderToStaticMarkup(<Jobs evidence={evidence} report={report}/>);
  expect(html).toContain("20261025");expect(html).toContain("33000");
  expect(html).toContain(t("ui.jobs.execution.placement",{placement:"local"}));
  expect(html).toContain(t("ui.jobs.execution.local_time_role",{role:"written"}));
  expect(html).not.toContain("2026-10-25T03:30:00Z");expect(html).not.toContain("private-agent-machine");
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);
  expect(JSON.stringify(report)).toBe(before);
  report.context={...report.context,observed_until:"2026-11-15T12:00:02.123456Z"};
  const foreign=renderToStaticMarkup(<Jobs evidence={evidence} report={report}/>);
  expect(foreign).toContain(t("ui.memory_failures.missing_row"));
  expect(foreign).not.toContain("20261025");
});


test("an execution child from another read cannot supply typed execution details for a matching outer row", () => {
  const evidence=agentJobEvidence();const report=agentJobsReport(evidence);
  report.executions.entries[0]!.reference.acquisition.observed_until="2026-11-15T12:00:02.123456Z";
  const html=renderToStaticMarkup(<Jobs evidence={evidence} report={report}/>);
  expect(html).toContain("2147483647");
  expect(html).not.toContain(t("ui.jobs.execution.inspect"));
});


test("What Changed keeps unplaced Agent evidence folded with original scope/acquisition and no UTC window or causal marker", () => {
  const evidence = agentJobEvidence(); const report = agentJobsReport(evidence);
  const before = JSON.stringify({ evidence, report });
  const html = renderToStaticMarkup(<JobComparisonContext evidence={evidence} report={report} />);
  expect(html).toContain(t("ui.what_changed.jobs_context.clock"));
  expect(html).toContain(t("ui.what_changed.jobs_context.scope", { scope: "shared" }));
  expect(html).toContain(evidence.snapshot.context.observed_from);
  expect(html).toContain(evidence.snapshot.context.observed_until);
  expect(html).toContain(t("ui.jobs.execution.status", { status: "failed" }));
  expect(html).toContain("20261115"); expect(html).toContain("250101");
  expect(html).toContain("stored &lt;script&gt;agent-canary&lt;/script&gt;"); expect(html).not.toContain("<script>");
  expect(html).not.toMatch(/<details[^>]*\bopen(?:=|\s|>)/);
  expect(html).not.toContain("private-agent-machine"); expect(html).not.toContain("private-agent-instance");
  expect(chartEvents(report.executions.events)).toEqual([]);
  expect(JSON.stringify({ evidence, report })).toBe(before);
});

test("unplaced in-progress history retains the write-clock role without turning a DST local time into a start or UTC end", () => {
  const evidence = agentJobEvidence(); const report = agentJobsReport(evidence);
  const raw = evidence.snapshot.observations[0]!;
  if (raw.fact.kind !== "mssql-agent-history") throw new Error("Agent history fixture");
  raw.fact.data.run_status = 4; raw.fact.data.run_date = "20261025"; raw.fact.data.run_time = "33000";
  const entry = report.executions.entries[0]!;
  if (entry.time.kind !== "mssql-local" || entry.reference.native.kind !== "mssql-agent-history" || entry.diagnostics.kind !== "mssql") throw new Error("Agent execution fixture");
  entry.status = "in-progress"; entry.diagnostics.native_status = 4;
  entry.time.run_date = raw.fact.data.run_date; entry.reference.native.run_date = raw.fact.data.run_date;
  entry.time.run_time = raw.fact.data.run_time; entry.reference.native.run_time = raw.fact.data.run_time;
  entry.time.started_without_offset = "2026-10-25T03:30:00"; entry.time.history_written_time = true;
  report.entries[0]!.explanation = "This retained history write reports work in progress.";
  const html = renderToStaticMarkup(<JobComparisonContext evidence={evidence} report={report} />);
  expect(html).toContain(t("ui.jobs.execution.local_time_role", { role: "written" }));
  expect(html).toContain(t("ui.jobs.execution.status", { status: "progress" }));
  expect(html).toContain("20261025"); expect(html).toContain("33000");
  expect(html).not.toContain("2026-10-25T03:30:00Z");
  expect(chartEvents(report.executions.events)).toEqual([]);
});

test("a missing or different original read cannot supply native details beside a comparison", () => {
  for (const missing of [false, true]) {
    const evidence = agentJobEvidence(); const report = agentJobsReport(evidence);
    if (!missing) report.context = { ...report.context, observed_until: "2026-11-15T12:00:02.123456Z" };
    const html = renderToStaticMarkup(<JobComparisonContext evidence={missing ? null : evidence} report={report} />);
    expect(html).toContain(t("ui.memory_failures.missing_row"));
    expect(html).not.toContain("2147483647"); expect(html).not.toContain("250101");
    expect(html).not.toContain("agent-canary"); expect(html).not.toContain(t("ui.jobs.execution.inspect"));
  }
});

test("denied or partial retained coverage stays explicit without claiming that no job ran", () => {
  const evidence = agentJobEvidence(); evidence.snapshot.observations = [];
  evidence.read_complete = false; evidence.snapshot.sources[0]!.records = 0; evidence.snapshot.sources[0]!.state = "unavailable";
  const report = agentJobsReport(); report.entries = []; report.executions.entries = [];
  report.native_rows = 0; report.observed_rows = 0; report.summary = "Native history is unavailable.";
  report.executions.assessment.sources[0]!.native_rows = 0; report.executions.assessment.sources[0]!.observed_rows = 0;
  report.executions.assessment.unplaced_records = 0; report.executions.assessment.step_records = 0;
  report.executions.assessment.read_complete = false; report.executions.assessment.sources[0]!.state = "unavailable";
  report.read_complete = false; report.sources[0]!.state = "unavailable"; report.sources[0]!.reason = { text: "Original msdb visibility denied." };
  const html = renderToStaticMarkup(<JobComparisonContext evidence={evidence} report={report} />);
  expect(html).toContain(t("ui.what_changed.jobs_context.coverage"));
  expect(html).toContain("Original msdb visibility denied.");
  expect(html).not.toContain(t("ui.health.empty")); expect(html).not.toContain(t("ui.what_changed.jobs_context.clock"));
});

test("complete zero-unplaced records do not duplicate UTC job markers or create an unplaced section", () => {
  const evidence = jobsEvidence([]); const report = jobsReport([]);
  evidence.snapshot.sources.forEach((source) => { source.complete_scope = true; source.state = "empty"; });
  report.sources.forEach((source) => { source.state = "empty"; source.reason = null; });
  report.executions.assessment.sources.forEach((source) => { source.complete_scope = true; source.state = "empty"; });
  expect(renderToStaticMarkup(<JobComparisonContext evidence={evidence} report={report} />)).toBe("");
  expect(renderToStaticMarkup(<JobComparisonContext evidence={evidence} report={null} />)).toBe("");
});
