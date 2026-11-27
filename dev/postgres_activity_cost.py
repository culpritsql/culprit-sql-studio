"""Paired PostgreSQL activity sampling measurements on the smoke runner's disposable database."""
import json
import math
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


CLIENTS = 4
RATE = 200
WINDOW_SECONDS = 25
SAMPLE_OFFSETS = (2, 12, 22)


def collector_counters(database):
    _, value = database.sql("""
        SELECT row_to_json(counters) FROM (
            SELECT coalesce(sum(calls), 0) AS calls,
                   coalesce(sum(total_exec_time), 0) AS execution_ms,
                   coalesce(sum(shared_blks_hit), 0) AS shared_hits,
                   coalesce(sum(shared_blks_read), 0) AS shared_reads,
                   coalesce(sum(temp_blks_written), 0) AS temp_writes
            FROM pg_stat_statements
            WHERE userid = 'activity_reader'::regrole
              AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ) counters;
    """, tuples=True)
    return json.loads(value)


def capture(database, binary, environment, output):
    database.commands.run([binary, "collect", "once", "sample", "--collection", "activity", "--output", output],
                          env=environment, timeout=8)
    manifest = json.loads((output / "manifest.json").read_text())
    if manifest["status"]["state"] != "complete" or not manifest["source_queries"]:
        raise RuntimeError("The activity read did not return a complete native capture")
    return sum(file.stat().st_size for file in output.rglob("*") if file.is_file())


def pgbench(database, arguments):
    return database.commands.run(["docker", "exec", database.name, "pgbench", "-U", "postgres",
                                  "-d", database.fixture.database, *arguments], timeout=90)[1]


def workload_run(database):
    # A separate cgroup keeps client CPU and its transaction log outside the source counters.
    command = ["docker", "run", "--rm", "--network", f"container:{database.name}",
               "--env", "PGPASSWORD", "--env", "PGCONNECT_TIMEOUT=3", database.fixture.image,
               "sh", "-ec", 'timeout 65 pgbench "$@" && echo BEGIN_LATENCIES && cat /tmp/latency.*',
               "workload", "-h", "127.0.0.1", "-U", "postgres", "-d", database.fixture.database,
               "-n", "-S", "-c", str(CLIENTS), "-T", str(WINDOW_SECONDS), "-R", str(RATE),
               "--random-seed=17", "--log", "--log-prefix=/tmp/latency"]
    _, output = database.commands.run(command, env={"PGPASSWORD": database.password}, timeout=90)
    summary, transactions = output.split("BEGIN_LATENCIES\n", 1)
    latencies = sorted(int(line.split()[2]) for line in transactions.splitlines())
    reported = re.search(r"number of transactions actually processed: (\d+)", summary)
    if not latencies or not reported or len(latencies) != int(reported[1]):
        raise RuntimeError("The pgbench latency log does not cover the completed transactions")
    return latencies


def source_cgroup(database):
    _, cpu = database.commands.run(["docker", "exec", database.name, "cat", "/sys/fs/cgroup/cpu.stat"])
    _, io = database.commands.run(["docker", "exec", database.name, "cat", "/sys/fs/cgroup/io.stat"])
    usage = dict(line.split() for line in cpu.splitlines())
    devices = [dict(field.split("=") for field in row.split()[1:]) for row in io.splitlines()]
    return {"cpu_us": int(usage["usage_usec"]),
            "read_bytes": sum(int(device["rbytes"]) for device in devices) if devices else None,
            "written_bytes": sum(int(device["wbytes"]) for device in devices) if devices else None}


def window(database, binary, directory, environment, index, sampled):
    before = collector_counters(database)
    source_before = source_cgroup(database)
    sizes = []
    with ThreadPoolExecutor(max_workers=1) as worker:
        workload = worker.submit(workload_run, database)
        # Align the sample clock with connected workload clients. Apply the same probe in control windows.
        deadline = time.monotonic() + 5
        while True:
            _, count = database.sql("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pgbench'",
                                    tuples=True)
            if int(count) == CLIENTS:
                break
            if workload.done():
                workload.result()
                raise RuntimeError("The pgbench workload ended before sampling began")
            if time.monotonic() >= deadline:
                raise RuntimeError("The pgbench clients did not become ready")
            time.sleep(0.05)
        start = time.monotonic()
        if sampled:
            for offset in SAMPLE_OFFSETS:
                time.sleep(max(0, start + offset - time.monotonic()))
                if workload.done() or time.monotonic() - start - offset > 1:
                    raise RuntimeError("Missed the scheduled sample during the workload")
                sizes.append(capture(database, binary, environment, directory / f"capture-{index}-{offset}"))
            if workload.done():
                raise RuntimeError("The last sample did not finish inside the workload window")
        latencies = workload.result()

    source = {key: value - source_before[key] if value is not None and source_before[key] is not None else None
              for key, value in source_cgroup(database).items()}
    after = collector_counters(database)
    work = {key: after[key] - value for key, value in before.items()}
    if any(value < 0 for value in (*source.values(), *work.values()) if value is not None) or bool(work["calls"]) != sampled:
        raise RuntimeError("Collector role counters do not match the measured window")
    return {"sampled": sampled, "transactions": len(latencies),
            "workload_p95_ms": latencies[math.ceil(len(latencies) * 0.95) - 1] / 1000,
            "source_cgroup": source, "collector": work, "capture_bytes": sizes}


def measure_activity_cost(database, binary):
    database.sql("CREATE EXTENSION pg_stat_statements; "
                 f"CREATE ROLE activity_reader LOGIN PASSWORD '{database.limited_password}' IN ROLE pg_monitor;")
    pgbench(database, ["-i", "-s", "1"])
    pgbench(database, ["-n", "-S", "-c", str(CLIENTS), "-T", "3"])
    with tempfile.TemporaryDirectory(prefix="culprit-activity-cost-") as temp:
        directory = Path(temp)
        environment = {"CULPRIT_HOME": str(directory / "workspace"), "CULPRIT_KEYCHAIN": "off",
                       "CULPRIT_FIXTURE_READER": database.limited_password}
        url = f"postgres://activity_reader@127.0.0.1:{database.port}/{database.fixture.database}?sslmode=disable"
        database.commands.run([binary, "conn", "add", "sample", "--env", "local", "--url", url,
                               "--password-env", "CULPRIT_FIXTURE_READER", "--no-test"], env=environment)
        capture(database, binary, environment, directory / "warmup")
        for index, sampled in enumerate((False, True, True, False), start=1):
            result = window(database, binary, directory, environment, index, sampled)
            print(json.dumps({"window": index, "seconds": WINDOW_SECONDS, "clients": CLIENTS,
                              "target_tps": RATE, **result}), flush=True)
