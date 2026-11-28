#!/usr/bin/env python3
"""Exercise the source's actual SQL against an isolated ClickHouse server. Each run owns its container and its optional loopback port."""
import argparse
import json
import os
from datetime import datetime, timezone, timedelta
import subprocess
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

SQL = Path(__file__).resolve().parents[1] / "crates/providers/culprit-clickhouse/src/sql"


class Server:
    def __init__(self, container):
        self.container = container

    def run(self, statement, *options, check=True):
        return subprocess.run(
            ["docker", "exec", self.container, "clickhouse-client", *options, "--query", statement],
            capture_output=True, text=True, timeout=30, check=check,
        )

    def observe(self, source, **params):
        defaults = dict(since=self.since, until=self.until, row_limit=32, text_bytes=128, profile_events="UserTimeMicroseconds,SelectedParts")
        sql = (SQL / f"{source}.sql").read_text()
        args = [f"--param_{key}={value}" for key, value in (defaults | params).items()]
        reply = self.run(sql + " FORMAT JSONEachRow", "--database=orders", *args, check=False)
        if reply.returncode:
            raise AssertionError(f"{source}: {reply.stderr}")
        return [json.loads(row) for row in reply.stdout.splitlines()]


@contextmanager
def server(image, http):
    network = ["-p", "127.0.0.1::8123"] if http else ["--network", "none"]
    container = subprocess.check_output(
        ["docker", "create", "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", *network, "--memory", "2g", "--cpus", "2", image], text=True,
    ).strip()
    try:
        # Older releases grant implicit system-table access unless this server policy is enabled.
        with tempfile.NamedTemporaryFile("w", suffix=".xml") as settings:
            settings.write("<clickhouse><access_control_improvements>"
                           "<select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>"
                           "</access_control_improvements></clickhouse>")
            settings.flush()
            os.chmod(settings.name, 0o644)
            subprocess.run(["docker", "cp", settings.name, container + ":/etc/clickhouse-server/config.d/source-access.xml"], check=True)
        subprocess.run(["docker", "start", container], check=True, stdout=subprocess.DEVNULL)
        db = Server(container)
        for _ in range(60):
            if db.run("SELECT 1", check=False).returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("ClickHouse did not start")
        yield db
    finally:
        subprocess.run(["docker", "rm", "-fv", container], check=True, stdout=subprocess.DEVNULL)


def verify_cli(cli, port, password):
    with tempfile.TemporaryDirectory(prefix="culprit-clickhouse-") as directory:
        workspace = Path(directory)
        env = os.environ | {"CULPRIT_HOME": str(workspace / "workspace"), "CULPRIT_SOURCE_PASSWORD": password}

        def run(*args):
            result = subprocess.run([cli, *args, "--json"], env=env,capture_output=True,text=True,timeout=50)
            if result.returncode:
                raise RuntimeError(f"Culprit {args[0]} failed: {result.stderr}")
            return result.stdout

        run("conn", "add", "source", "--url", f"clickhouse://source_reader@127.0.0.1:{port}/orders?sslmode=disable",
            "--password-env", "CULPRIT_SOURCE_PASSWORD")
        scan = run("scan","source")
        assert "clickhouse-health" in scan and "TOO_MANY_PARTS" in scan
        run("collect","outbox","init",str(workspace / "outbox"))
        capture = run("collect","once","source","--collection","operational","--outbox",str(workspace / "outbox"))
        assert "clickhouse-system-tables-v1" in capture or "operational" in capture, capture[:300]

def record_faults(db):
    limited = db.run("SELECT n, count() FROM orders.events GROUP BY n SETTINGS max_rows_to_group_by=1, group_by_overflow_mode='throw'", "--query_id=bounded-group", check=False)
    assert "TOO_MANY_ROWS" in limited.stderr, limited.stderr
    malformed = db.run("""INSERT INTO orders.async_events SETTINGS async_insert=1,wait_for_async_insert=1 FORMAT JSONEachRow {"n":"not-an-integer"}""", check=False)
    assert malformed.returncode != 0
    db.run("CREATE TABLE orders.mutation_test(k UInt64, v String) ENGINE=MergeTree ORDER BY k")
    db.run("INSERT INTO orders.mutation_test VALUES (1, 'invalid')")
    db.run("ALTER TABLE orders.mutation_test UPDATE v=toString(toUInt64(v)) WHERE k=1")
    for _ in range(50):
        if db.run("SELECT count() FROM system.mutations WHERE database='orders' AND table='mutation_test' AND latest_fail_time > 0").stdout.strip() == "1":
            return
        time.sleep(0.2)
    raise AssertionError("the test mutation did not record a failed part")


def verify(image, provider_test, cli):
    with server(image, bool(provider_test or cli)) as db:
        db.run("CREATE DATABASE orders")
        db.run("CREATE TABLE orders.events(n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS parts_to_delay_insert=2, parts_to_throw_insert=3")
        db.run("SYSTEM STOP MERGES orders.events")
        db.since = db.run("SELECT toString(now64(6, 'UTC'))").stdout.strip()
        for value in range(3):
            db.run(f"INSERT INTO orders.events VALUES ({value})", f"--query_id=insert-{value}")
        refused = db.run("INSERT INTO orders.events VALUES (7)", "--query_id=refused", check=False)
        assert "TOO_MANY_PARTS" in refused.stderr, refused.stderr
        for value in (1, 2):
            db.run(f"SELECT sum(n) FROM orders.events WHERE n > {value}")
        db.run("SELECT countIf(toString(n) = 'fixture-private-value') FROM orders.events")
        db.run("/* culprit */ SELECT avg(n) FROM orders.events", "--query_id=probe-self")
        db.run("CREATE TABLE orders.async_events(n UInt64) ENGINE=MergeTree ORDER BY n")
        db.run("INSERT INTO orders.async_events SETTINGS async_insert=1,wait_for_async_insert=1 VALUES (2)")
        record_faults(db)
        db.run("SYSTEM FLUSH LOGS")
        db.until = db.run("SELECT toString(now64(6, 'UTC'))").stdout.strip()
        assert db.observe("identity")[0]["database"] == "orders"
        parts = db.observe("active_parts")
        assert any(row["table"] == "events" and int(row["active_parts"]) == 3 for row in parts)
        queries = db.observe("workload")
        assert any(int(q["executions"]) == 2 and "SELECT sum(n)" in q["sample_sql"] for q in queries)
        assert all("/* culprit */" not in q["sample_sql"] for q in queries)
        failed = db.observe("failures")
        rejection = next(row for row in failed if row["query_id"] == "refused")
        assert rejection["exception_name"] == "TOO_MANY_PARTS"
        assert any(row["exception_name"] == "TOO_MANY_ROWS" for row in failed), db.run("SELECT type, exception_code, databases, tables FROM system.query_log WHERE query_id='bounded-group'").stdout
        assert any(row["latest_failed_part"] for row in db.observe("mutations"))
        assert any(row["status"] in ("ParsingError","FlushError") for row in db.observe("async_inserts"))
        for source in ["merges", "replicas", "replication_queue", "distribution_queue", "live"]:
            db.observe(source)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        stamp = lambda value: (epoch + timedelta(microseconds=int(value))).strftime("%Y-%m-%d %H:%M:%S.%f")
        focus = dict(node=rejection["node"], query_id="refused", started=stamp(rejection["started_us"]), ended=stamp(rejection["ended_us"]))
        assert len(db.observe("execution", **focus)) == 1
        assert not db.observe("execution", **(focus | {"query_id": "not-the-request"}))
        actual = db.observe("part_history", table="events", node=rejection["node"])
        assert sum(row["event"] == "NewPart" for row in actual) == 3
        if provider_test or cli:
            password = uuid.uuid4().hex
            db.run(f"CREATE USER source_reader IDENTIFIED BY '{password}'")
            db.run("GRANT SELECT ON system.* TO source_reader")
            db.run("GRANT SELECT ON orders.* TO source_reader")
            db.run(f"CREATE USER catalog_reader IDENTIFIED BY '{password}'")
            db.run("GRANT SELECT ON orders.* TO catalog_reader")
            for system_table in ["databases", "tables", "columns"]:
                db.run(f"GRANT SELECT ON system.{system_table} TO catalog_reader")
            port = subprocess.check_output(["docker", "port", db.container, "8123/tcp"], text=True).strip().rsplit(":", 1)[1]
            env = os.environ | {"CULPRIT_TEST_CLICKHOUSE_PORT": port, "CULPRIT_TEST_CLICKHOUSE_PASSWORD": password}
            if cli:
                verify_cli(cli,port,password)
            if provider_test:
                subprocess.run([provider_test, "--ignored", "--exact", "tests::disposable_http_source", "--nocapture"], env=env, check=True)
        print(f"{image}: query families, rejected insert, failed mutation, async insert failure and result limit passed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="clickhouse/clickhouse-server:26.8")
    parser.add_argument("--provider-test", help="Path to the compiled culprit-clickhouse library test binary")
    parser.add_argument("--cli", help="Verify connection, scan and sealed outbox capture with this CLI binary")
    args = parser.parse_args()
    verify(args.image, args.provider_test, args.cli)
