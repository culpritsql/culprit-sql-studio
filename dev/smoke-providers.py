#!/usr/bin/env python3
"""Run real provider checks in owned, loopback-only, disposable databases."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, replace

from replication_fixtures import MySqlReplication, PostgresReplication, PostgresReadableRecovery
from mssql_replication_fixture import AG_WORKLOAD_TEST, SqlServerReplication
from mysql_replication_fixture import MySqlMovingReplication
from mysql_group_replication_fixture import MySqlGroupReplication
from mariadb_galera_fixture import MariaDbGalera


ROOT = Path(__file__).resolve().parent.parent
OWNER_LABEL = "dev.culprit.smoke"
MSSQL_SHORT_HOST_OWNER_LENGTH = 14
STARTUP_BUDGET = 240
COMMAND_BUDGET = 120
CONNECT_BUDGET = 3
TEST_BUDGET = 1200
HEARTBEAT = 30
TIMEOUT_OUTPUT_BYTES = 64 * 1024  # Per stream, after secret redaction; completed output remains unchanged.
TIMEOUT_OUTPUT_TRUNCATED = "\n[partial subprocess output truncated]\n"


@dataclass(frozen=True)
class Fixture:
    name: str
    image: str
    engine: str
    port: int
    database: str
    user: str
    memory: str = "1g"
    platform: str | None = None
    options: tuple[str, ...] = ()


FIXTURES = (
    Fixture("postgres", "postgres:18.6", "postgres", 5432, "culprit_smoke", "postgres"),
    Fixture("mysql", "mysql:8.4", "mysql", 3306, "culprit_smoke", "root", "2g"),
    Fixture("mysql-5.7", "mysql:5.7", "mysql", 3306, "culprit_smoke", "root", "2g", "linux/amd64",
            ("--master-info-repository=TABLE", "--relay-log-info-repository=TABLE")),
    *(Fixture(f"mariadb-{version}", f"mariadb:{version}", "mysql", 3306, "culprit_smoke", "root")
      for version in ("10.6", "10.11", "11.8")),
    Fixture("mssql", "mcr.microsoft.com/mssql/server:2022-latest", "mssql", 1433,
            "culprit_smoke", "sa", "3g", "linux/amd64"),
    Fixture("timescale", "timescale/timescaledb@sha256:d971568f9af05e218866181151117f517282f05f299a0e4010dc6a97704f6f2c",
            "postgres", 5432, "culprit_timescale", "postgres"),
    Fixture("yugabyte", "yugabytedb/yugabyte@sha256:e604d9e273a1fc3fccaf6ecbfe6ac6e3b93d2b4f06a075c461e3d73676d183fc",
            "yugabyte", 5433, "culprit_yugabyte", "yugabyte", "6g"),
)

REPLICATION_FIXTURES = (
    Fixture("mysql-replication", "mysql:8.4", "mysql", 3306, "culprit_smoke", "root", "2g"),
    Fixture("mysql-no-performance-schema", "mysql:8.4", "mysql", 3306, "culprit_smoke", "root", "2g",
            options=("--performance-schema=OFF",)),
    Fixture("mariadb-replication", "mariadb:11.8", "mysql", 3306, "culprit_smoke", "root"),
    Fixture("mariadb-no-performance-schema", "mariadb:11.8", "mysql", 3306, "culprit_smoke", "root",
            options=("--performance-schema=OFF",)),
)
POSTGRES_REPLICATION_IMAGES = {"postgres-replication": "postgres:18.6", "postgres-16-replication": "postgres:16"}
MSSQL_EMPTY_CATALOG_TEST = ("indexes::native_tests::configuration_scale::"
                            "owned_index_and_heap_partition_projection_records_small_and_medium_catalog_work")
MSSQL_DATABASE_COUNTER_TEST = ("telemetry::state::native_tests::"
                               "owned_selected_database_performance_counters_use_the_real_factory_and_sealed_original_outbox")
MSSQL_FULL_TEXT_TESTS = (
    "tables::native_tests::owned_full_text_enabled_and_disabled_rows_match_scan_and_stream_without_starting_a_population",
    "tables::native_tests::owned_full_text_runtime_population_retains_original_after_stop_with_supervisor_owned_background",
)
MSSQL_SQL2016_TEST = ("tables::native_tests::temporal::"
                     "owned_sql2016_temporal_retention_columns_remain_unsupported_without_erasing_native_roles")
OPTIONAL_NATIVE_MODULES = {
    "culprit-postgres": (
        "telemetry::native_tests::replication::", "jobs::", "maintenance_waits::", "metadata_queue::",
        "transaction_metadata::native_tests::",
    ),
    "culprit-mysql": (
        "telemetry::native_tests::replication::", "telemetry::native_tests::replication_movement::",
        "telemetry::native_tests::group_replication::", "telemetry::native_tests::galera::",
    ),
    "culprit-mssql": (
        "conn::native_tests::refused_connection::",
        "activity::native_tests::census::idle_connections::",
        "activity::native_tests::census::instance::",
        "activity::page_targets::native_tests::schema_queue::",
        "telemetry::replication::native_tests::", "jobs::",
        "an_owned_native_page_task_holder_requires_the_reported_pointer_and_exact_current_request",
        "local_native_writer_plans_keep_targeted_writes_separate_from_readers_and_other_resources",
        "exact_original_scalar_metadata_is_observed_without_memory_collection",
        "exact_original_scalar_metadata_is_observed_between_memory_source_phases",
        AG_WORKLOAD_TEST, MSSQL_EMPTY_CATALOG_TEST, MSSQL_DATABASE_COUNTER_TEST, *MSSQL_FULL_TEXT_TESTS, MSSQL_SQL2016_TEST,
    ),
}
POSTGRES_OUTPUT_CONSUMER_TEST = ("checks_synthesis::explanations::context_tests::native_output_blocking::"
                                 "owned_client_write_retains_a_relation_lock_and_exact_collateral_victim_through_canonical_gold_and_retirement")

NATIVE_TEST_FEATURES = {
    "culprit-engine": "collector",
    "culprit-postgres": "native-test-support",
}


@dataclass(frozen=True)
class MySqlAcceptancePolicy:
    name: str
    selection: str
    test_features: tuple[str, ...]
    seed_current_statements: bool
    startup_options: tuple[str, ...] = ()
    expected_slow_query_log: str | None = None

    def accepts(self, crate, selection):
        return crate == "culprit-engine" and selection.startswith(self.selection)


MYSQL_AUTOMATIC_BLOCKING = MySqlAcceptancePolicy(
    "automatic-blocking",
    "collector_service::monitoring::mysql_native_tests::normal_builtin_mysql_enrollment_",
    ("mysql-native-tests", "store-adx", "recovery-azure", "auth-azure"),
    seed_current_statements=True,
)
MYSQL_AUTOMATIC_RECOVERY = MySqlAcceptancePolicy(
    "automatic-recovery",
    "collector_service::monitoring::mysql_native_tests::normal_builtin_mysql_incident_resolves_",
    MYSQL_AUTOMATIC_BLOCKING.test_features,
    seed_current_statements=True,
)
MYSQL_METADATA_CONTEXT = MySqlAcceptancePolicy(
    "metadata-context",
    "checks_synthesis::explanations::context_tests::mysql_metadata_context::native::"
    "owned_metadata_ticket_context_retains_three_native_actors_canonical_eof_and_candidate_only_gold_with_ended_refusal",
    ("mysql-native-tests",),
    seed_current_statements=False,
    # Explicit startup prerequisites on this disposable fixture. Native code observes them without changing them.
    startup_options=(
        "--performance-schema-consumer-global-instrumentation=ON",
        "--performance-schema-consumer-thread-instrumentation=ON",
        "--performance-schema-consumer-events-statements-current=ON",
        "--performance-schema-instrument=wait/lock/metadata/sql/mdl=ON",
    ),
)
MYSQL_SETTINGS = MySqlAcceptancePolicy(
    "settings",
    "collector_service::monitoring::mysql_native_tests::settings::normal_builtin_mysql_settings_",
    MYSQL_AUTOMATIC_BLOCKING.test_features,
    seed_current_statements=False,
    startup_options=("--slow-query-log=OFF", "--long-query-time=0.2", "--log-output=FILE"),
    expected_slow_query_log="OFF",
)
MYSQL_SETTINGS_HEALTHY = MySqlAcceptancePolicy(
    "settings-healthy",
    MYSQL_SETTINGS.selection,
    MYSQL_SETTINGS.test_features,
    seed_current_statements=False,
    startup_options=("--slow-query-log=ON", "--long-query-time=0.2", "--log-output=FILE"),
    expected_slow_query_log="ON",
)
MYSQL_ACCEPTANCE_CASES = {
    "mysql-automatic-blocking": ("mysql", MYSQL_AUTOMATIC_BLOCKING),
    "mariadb-automatic-blocking": ("mariadb-11.8", MYSQL_AUTOMATIC_BLOCKING),
    "mysql-automatic-recovery": ("mysql", MYSQL_AUTOMATIC_RECOVERY),
    "mariadb-automatic-recovery": ("mariadb-11.8", MYSQL_AUTOMATIC_RECOVERY),
    "mysql-metadata-context": ("mysql", MYSQL_METADATA_CONTEXT),
    "mariadb-metadata-context": ("mariadb-11.8", MYSQL_METADATA_CONTEXT),
    "mysql-settings": ("mysql", MYSQL_SETTINGS),
    "mariadb-settings": ("mariadb-11.8", MYSQL_SETTINGS),
    "mysql-settings-healthy": ("mysql", MYSQL_SETTINGS_HEALTHY),
    "mariadb-settings-healthy": ("mariadb-11.8", MYSQL_SETTINGS_HEALTHY),
}


class Commands:
    def __init__(self, passwords):
        self.passwords = passwords

    def sanitized(self, text):
        for password in self.passwords:
            text = text.replace(password, "[redacted]")
        return text

    @staticmethod
    def output_text(output):
        return output.decode(errors="replace") if isinstance(output, bytes) else (output or "")

    def timeout_output(self, output):
        sanitized = self.sanitized(self.output_text(output))
        encoded = sanitized.encode("utf-8")
        if len(encoded) <= TIMEOUT_OUTPUT_BYTES:
            return sanitized
        prefix = encoded[:TIMEOUT_OUTPUT_BYTES - len(TIMEOUT_OUTPUT_TRUNCATED.encode("utf-8"))]
        return prefix.decode("utf-8", errors="ignore") + TIMEOUT_OUTPUT_TRUNCATED

    @staticmethod
    def stop(process):
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.wait()
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()

    def run(self, args, *, input=None, env=None, timeout=COMMAND_BUDGET, check=True, echo=False,
            separate_stderr=False):
        process = subprocess.Popen(args, cwd=ROOT, env={**os.environ, **(env or {})},
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE if separate_stderr else subprocess.STDOUT,
                                   text=True, start_new_session=True)
        started = time.monotonic()
        pending_input = input
        output = diagnostics = ""
        timed_out = False
        try:
            while True:
                remaining = timeout - (time.monotonic() - started)
                if remaining <= 0:
                    timed_out = True
                    break
                try:
                    output, diagnostics = process.communicate(pending_input, timeout=min(HEARTBEAT, remaining))
                    break
                except subprocess.TimeoutExpired as progress:
                    pending_input = None
                    # communicate's timeout snapshots are cumulative; appending would duplicate earlier native phases.
                    output, diagnostics = progress.output, progress.stderr
                    elapsed = time.monotonic() - started
                    if elapsed < timeout:
                        partial = self.sanitized(self.output_text(output))
                        tests = re.findall(r"^test ([A-Za-z0-9_:]+) \.\.\.", partial, re.MULTILINE)
                        current = f"; {tests[-1]}" if tests else ""
                        print(self.sanitized(f"  {args[0]} is still running ({int(elapsed)}s{current})"), flush=True)
        finally:
            self.stop(process)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
        if timed_out:
            error = TimeoutError(self.sanitized(f"{args[0]} exceeded its {timeout}s budget"))
            error.stdout, error.stderr = self.timeout_output(output), self.timeout_output(diagnostics)
            for text, stream in ((error.stdout, sys.stdout), (error.stderr, sys.stderr)):
                if text:
                    print(text, file=stream, end="" if text.endswith("\n") else "\n", flush=True)
            raise error
        output = self.sanitized(output)
        diagnostics = self.sanitized(diagnostics or "")
        if diagnostics:
            if process.returncode:
                output += diagnostics
            else:
                print(diagnostics, file=sys.stderr, end="" if diagnostics.endswith("\n") else "\n", flush=True)
        if echo or (check and process.returncode):
            print(output, end="" if output.endswith("\n") else "\n", flush=True)
        if check and process.returncode:
            raise RuntimeError(f"{args[0]} exited with status {process.returncode}")
        return process.returncode, output


def run_tests(commands, crate, selection, environment, *, library=True, skip=None, test_features=()):
    target = ["--lib"] if library else ["--test", "all"]
    print(f"  Testing {crate} {selection}", flush=True)
    skips = [skip] if isinstance(skip, str) else (skip or [])
    filters = [argument for selection in skips for argument in ("--skip", selection)]
    required_features = list(dict.fromkeys([*NATIVE_TEST_FEATURES.get(crate, "").split(","), *test_features]))
    required_features = [feature for feature in required_features if feature]
    features = ["--features", ",".join(required_features)] if required_features else []
    _, output = commands.run(["cargo", "test", "--locked", "-p", crate, *features, *target, selection,
                             "--", "--ignored", "--test-threads=1", *filters],
                            env=environment, timeout=TEST_BUDGET, echo=True)
    counts = re.findall(r"test result: ok\. (\d+) passed", output)
    if not counts or sum(map(int, counts)) == 0:
        raise RuntimeError(f"{crate} {selection} did not execute any live tests")


class Database:
    def __init__(self, fixture, *, launch_configuration=None):
        self.fixture = fixture
        self.owner = secrets.token_hex(12)
        self.name = f"culprit-smoke-{fixture.name}-{self.owner}"
        self.hostname = f"culprit-smoke-{fixture.engine}-{self.owner}"
        if fixture.engine == "mssql":
            # SQL Server on Linux supports physical hostnames of at most fifteen characters. The full
            # nonce remains in the native database name and Docker label, not a truncated member claim.
            self.hostname = "m" + self.owner[:MSSQL_SHORT_HOST_OWNER_LENGTH]
            self.fixture = replace(fixture, database=f"{fixture.database}_{self.owner}")
        self.password = f"Smoke!{secrets.token_hex(16)}Aa9"
        self.limited_password = f"Reader!{secrets.token_hex(16)}Aa9"
        self.commands = Commands([self.password, self.limited_password])
        self.port = None
        self.options = list(fixture.options)
        self.container_options = []
        self.launch_configuration = launch_configuration

    def __enter__(self):
        try:
            self.start()
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *exception):
        self.close()

    def published_ports(self):
        return (self.fixture.port,)

    def start(self):
        fixture = self.fixture
        print(f"Starting {fixture.name} in {self.name}", flush=True)
        found, _ = self.commands.run(["docker", "image", "inspect", fixture.image, "--format", "{{.Id}}"], check=False)
        if found:
            self.commands.run(["docker", "pull", fixture.image], timeout=600, echo=True)
        args = ["docker", "create", "--pull=never", "--name", self.name,
                "--hostname", self.hostname,
                "--label", f"{OWNER_LABEL}={self.owner}",
                "--cpus", "3" if fixture.engine == "yugabyte" else "2", "--memory", fixture.memory]
        for port in self.published_ports():
            args += ["-p", f"127.0.0.1::{port}"]
        if fixture.platform:
            args += ["--platform", fixture.platform]
        args += self.container_options
        environment, command = self.configuration()
        for name in environment:
            args += ["-e", name]
        self.commands.run([*args, fixture.image, *command, *self.options], env=environment)
        self.commands.run(["docker", "start", self.name])
        self.wait_until_ready()

    def wait_until_ready(self):
        _, port = self.commands.run(["docker", "port", self.name, str(self.fixture.port)])
        self.port = port.strip().removeprefix("127.0.0.1:")
        if not self.port.isdigit():
            raise RuntimeError("Docker did not publish exactly one loopback port")
        deadline = time.monotonic() + STARTUP_BUDGET
        while time.monotonic() < deadline:
            self.require_running()
            try:
                status, _ = self.sql(self.readiness_query(), database=self.admin_database(), check=False, timeout=5)
            except TimeoutError:
                status = 1
            if status == 0:
                print(f"  {self.fixture.name} ready at 127.0.0.1:{self.port}", flush=True)
                return
            time.sleep(2)
        self.commands.run(["docker", "logs", "--tail", "60", self.name], check=False, echo=True)
        raise TimeoutError(f"{self.fixture.name} did not become ready")

    def require_running(self):
        template = '{{index .Config.Labels "' + OWNER_LABEL + '"}}\t{{.State.Status}}\t{{.State.ExitCode}}'
        status, result = self.commands.run(
            ["docker", "inspect", "--format", template, self.name], check=False, timeout=5,
        )
        fields = result.strip().split("\t")
        if status or len(fields) != 3 or fields[0] != self.owner:
            raise RuntimeError("the exact owned fixture disappeared during readiness")
        _, state, exit_code = fields
        if state != "running":
            self.commands.run(["docker", "logs", "--tail", "60", self.name], check=False, echo=True)
            raise RuntimeError(f"{self.fixture.name} stopped during startup ({state}, exit {exit_code})")

    def readiness_query(self):
        return "SELECT 1;"

    def restart(self):
        if not self.owns_container():
            raise RuntimeError("the owned fixture disappeared before restart")
        self.commands.run(["docker", "restart", self.name])
        self.wait_until_ready()

    def configuration(self):
        if self.launch_configuration is not None:
            return self.launch_configuration
        fixture = self.fixture
        if fixture.engine == "postgres":
            preload = "timescaledb,pg_stat_statements" if fixture.name == "timescale" else "pg_stat_statements"
            command = ["-c", f"shared_preload_libraries={preload}", "-c", "track_io_timing=on"]
            if fixture.name in ("timescale", "postgres"):
                command += ["-c", f"cluster_name=culprit_owned_{self.owner}"]
            return ({"POSTGRES_PASSWORD": self.password, "POSTGRES_DB": fixture.database}, command)
        if fixture.engine == "mysql":
            explicit_options = {option.partition("=")[0] for option in self.options}
            defaults = [option for option in ("--performance-schema=ON", "--server-id=1")
                        if option.partition("=")[0] not in explicit_options]
            return ({"MYSQL_ROOT_PASSWORD": self.password, "MYSQL_ROOT_HOST": "%", "MYSQL_DATABASE": fixture.database},
                    defaults)
        if fixture.engine == "mssql":
            return ({"ACCEPT_EULA": "Y", "MSSQL_PID": "Developer", "MSSQL_SA_PASSWORD": self.password}, [])
        return ({}, ["bin/yugabyted", "start", "--background=false", "--callhome=false", "--ui=false",
                     # Bound native backfill speed so progress tests observe work, even on fast CI hosts.
                     "--tserver_flags=memory_limit_hard_bytes=2147483648,backfill_index_rate_rows_per_sec=1000",
                     "--master_flags=memory_limit_hard_bytes=536870912"])

    def admin_database(self):
        return {"postgres": "postgres", "yugabyte": "yugabyte", "mysql": "mysql", "mssql": "master"}[self.fixture.engine]

    def sql(self, sql, *, database=None, check=True, timeout=COMMAND_BUDGET, tuples=False,
            messages_to_stderr=False):
        fixture = self.fixture
        if messages_to_stderr and fixture.engine != "mssql":
            raise ValueError("SQL message routing requires the owned SQL Server fixture")
        database = database or fixture.database
        env = {}
        args = ["docker", "exec", "-i"]
        if fixture.engine in ("postgres", "yugabyte"):
            binary = "psql" if fixture.engine == "postgres" else "bin/ysqlsh"
            if fixture.engine == "postgres":
                env = {"PGPASSWORD": self.password, "PGCONNECT_TIMEOUT": str(CONNECT_BUDGET)}
                args += ["-e", "PGPASSWORD", "-e", "PGCONNECT_TIMEOUT"]
            host = self.name if fixture.engine == "yugabyte" else "127.0.0.1"
            args += [self.name, binary, "-X", "-v", "ON_ERROR_STOP=1", "-U", fixture.user,
                     "-d", database, "-h", host]
            if tuples:
                args += ["-A", "-t"]
        elif fixture.engine == "mysql":
            env = {"MYSQL_PWD": self.password}
            binary = "mariadb" if fixture.name.startswith("mariadb") else "mysql"
            args += ["-e", "MYSQL_PWD", self.name, binary, "--protocol=TCP", "-h", "127.0.0.1",
                     f"--connect-timeout={CONNECT_BUDGET}", "-u", fixture.user, database]
            if tuples:
                args += ["--batch", "--skip-column-names"]
        else:
            env = {"SQLCMDPASSWORD": self.password}
            args += ["-e", "SQLCMDPASSWORD", self.name, "/opt/mssql-tools18/bin/sqlcmd",
                     "-C", "-S", "localhost", "-U", fixture.user, "-d", database, "-b", "-l", str(CONNECT_BUDGET)]
            if messages_to_stderr:
                args += ["-r1"]
        return self.commands.run(args, input=sql, env=env, check=check, timeout=timeout, separate_stderr=True)

    def prepare_database(self):
        fixture = self.fixture
        if fixture.engine in ("yugabyte", "mssql"):
            self.sql(f"CREATE DATABASE {fixture.database};", database=self.admin_database())
        if fixture.engine == "mssql":
            self.sql(f"ALTER DATABASE {fixture.database} SET QUERY_STORE=ON (OPERATION_MODE=READ_WRITE, QUERY_CAPTURE_MODE=ALL);")

    def seed(self):
        self.prepare_database()
        fixture = self.fixture
        if fixture.name in ("timescale", "yugabyte"):
            self.sql((ROOT / "dev" / fixture.name / "seed.sql").read_text())
            self.sql((ROOT / "dev" / fixture.name / "workload.sql").read_text())
        elif fixture.engine == "postgres":
            self.sql("CREATE EXTENSION pg_stat_statements;")
            self.prepare_hypopg()
        schema = "dbo" if fixture.engine == "mssql" else (fixture.database if fixture.engine == "mysql" else "public")
        table = f"{schema}.smoke_rows"
        if fixture.engine == "mysql" and "--performance-schema=OFF" not in self.options:
            self.prepare_mysql_current_statements()
        values = ",".join(f"({i},{i * 3})" for i in range(1, 101))
        self.sql(f"CREATE TABLE {table} (id int PRIMARY KEY, value int NOT NULL); INSERT INTO {table} VALUES {values};")
        self.create_reader(table)
        self.sql("\n".join(f"SELECT sum(value) FROM {table} WHERE id > {i};" for i in range(20)))
        if fixture.engine == "mssql":
            self.sql("EXEC sys.sp_query_store_flush_db;")
        if fixture.name == "timescale":
            self.prepare_timescale_jobs()

    def prepare_mysql_current_statements(self):
        # Explicit setup on this owned fixture, never collector behavior. MariaDB leaves the current-statement
        # consumer off by default; provider tests separately verify that disabled instrumentation stays unknown.
        self.sql("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME IN "
                 "('global_instrumentation','thread_instrumentation','statements_digest','events_statements_current');")

    def prepare_timescale_jobs(self):
        if self.fixture.name != "timescale" or self.fixture.engine != "postgres":
            raise RuntimeError("job setup requires the dedicated Timescale fixture")
        if not re.fullmatch(r"[0-9a-f]{24}", self.owner):
            raise RuntimeError("job setup requires this run's fixture ownership nonce")
        if not self.owns_container():
            raise RuntimeError("the owned fixture disappeared before job setup")
        # The script independently checks the native cluster marker, database, extension and fresh job schema.
        self.sql((ROOT / "dev" / "timescale" / "jobs.sql").read_text())

    def prepare_hypopg(self):
        status, _ = self.commands.run(["docker", "exec", self.name, "test", "-f",
                                      "/usr/share/postgresql/18/extension/hypopg.control"], check=False)
        if status:
            # Only the owned fixture gets packages. Provider collection never installs extensions.
            if not self.owns_container():
                raise RuntimeError("the owned fixture disappeared before extension setup")
            self.commands.run(["docker", "exec", self.name, "apt-get", "update"], timeout=300)
            self.commands.run(["docker", "exec", self.name, "apt-get", "install", "-y", "--no-install-recommends",
                               "postgresql-18-hypopg"], timeout=300)
            # The package manager can replace server libraries and binaries with a newer minor release.
            # Restart before loading any extension so the running server and its libraries agree.
            self.restart()
            _, version = self.sql("SHOW server_version;", database=self.admin_database())
            print(f"  PostgreSQL after extension package setup:\n{version.strip()}", flush=True)
        self.sql("CREATE DATABASE culprit_hypopg;", database="postgres")

    def create_reader(self, table):
        engine = self.fixture.engine
        if engine in ("postgres", "yugabyte"):
            self.sql(f"CREATE ROLE smoke_reader LOGIN PASSWORD '{self.limited_password}'; "
                     f"GRANT USAGE ON SCHEMA public TO smoke_reader; GRANT SELECT ON {table} TO smoke_reader;")
        elif engine == "mysql":
            self.sql(f"CREATE USER 'smoke_reader'@'%' IDENTIFIED BY '{self.limited_password}'; "
                     f"GRANT SELECT ON {table} TO 'smoke_reader'@'%';")
        else:
            self.sql(f"CREATE LOGIN smoke_reader WITH PASSWORD='{self.limited_password}'; "
                     f"CREATE USER smoke_reader FOR LOGIN smoke_reader; GRANT SELECT ON {table} TO smoke_reader;")

    def test_environment(self):
        fixture = self.fixture
        return {"CULPRIT_KEYCHAIN": "off", "CULPRIT_LANG": "en", "CULPRIT_SMOKE_OWNER": self.owner,
                "CULPRIT_SMOKE_ENGINE": fixture.engine, "CULPRIT_SMOKE_PORT": self.port,
                "CULPRIT_SMOKE_DATABASE": fixture.database, "CULPRIT_SMOKE_USER": fixture.user,
                "CULPRIT_SMOKE_PASSWORD": self.password, "CULPRIT_SMOKE_LIMITED_USER": "smoke_reader",
                "CULPRIT_SMOKE_LIMITED_PASSWORD": self.limited_password,
                "CULPRIT_SMOKE_SCHEMA": "dbo" if fixture.engine == "mssql" else
                    (fixture.database if fixture.engine == "mysql" else "public"),
                "CULPRIT_PREVIEW_PG_PORT": self.port, "CULPRIT_PREVIEW_PG_DATABASE": fixture.database,
                "CULPRIT_PREVIEW_PG_PASSWORD": self.password, "CULPRIT_TIMESCALE_TEST_PORT": self.port,
                "CULPRIT_HYPOPG_PG_TEST_PORT": self.port, "CULPRIT_HYPOPG_PG_TEST_PASSWORD": self.password,
                "CULPRIT_TIMESCALE_TEST_PASSWORD": self.password,
                "CULPRIT_YSQL_TEST_PORT": self.port, "CULPRIT_MYSQL_TEST_PORT": self.port,
                "CULPRIT_MYSQL_TEST_PASSWORD": self.password,
                "CULPRIT_PREVIEW_MSSQL_PORT": self.port, "CULPRIT_PREVIEW_MSSQL_PASSWORD": self.password,
                "CULPRIT_MSSQL_SOURCE_TEST_PORT": self.port, "CULPRIT_MSSQL_SOURCE_TEST_PASSWORD": self.password,
                "CULPRIT_MSSQL_REPLICATION_EXPECT_EXPANDED_AG":
                    self.configuration()[0].get("MSSQL_ENABLE_HADR", "0")}

    def test(self, crate, selection, *, library=True, skip=None, environment=None, test_features=()):
        skips = [skip] if isinstance(skip, str) else list(skip or [])
        skips += [module for module in OPTIONAL_NATIVE_MODULES.get(crate, ()) if module not in selection]
        run_tests(self.commands, crate, selection, self.test_environment() | (environment or {}),
                  library=library, skip=list(dict.fromkeys(skips)), test_features=test_features)

    def verify(self, selected_tests=None):
        if selected_tests:
            for crate, selection in selected_tests:
                self.test(crate, selection, library=crate != "culprit-engine",
                          skip="cluster_tests::" if crate == "culprit-yugabyte" else None)
            return
        self.test("culprit-engine", "live_providers::live_registered_provider", library=False)
        if self.fixture.name != "mysql-5.7":
            self.test("culprit-engine", "live_providers::live_measured_plan_", library=False)
        fixture = self.fixture
        if fixture.engine == "postgres":
            self.test("culprit-engine", "live_providers::native_telemetry_", library=False)
            self.test("culprit-postgres", "read::native_tests::")
            self.test("culprit-postgres", "telemetry::native_tests::")
            self.test("culprit-postgres", "native::memory::tests::live_")
            self.test("culprit-postgres", "native::cleanup::tests::live_")
            self.test("culprit-postgres", "wire::tests::live_table_preview")
            if fixture.name == "timescale":
                self.test("culprit-postgres", "timescale::tests::live_")
                self.test("culprit-postgres", "telemetry::native_tests::jobs::compatibility::")
                self.test("culprit-postgres", "telemetry::native_tests::jobs::limits::")
            else:
                self.test("culprit-postgres", "wire::tests::live_postgres_hypothetical_")
                if fixture.name == "postgres":
                    self.test("culprit-postgres", "telemetry::native_tests::jobs::absent::")
        elif fixture.engine == "mysql":
            self.test("culprit-engine", "live_providers::native_telemetry_", library=False)
            self.test("culprit-mysql", "telemetry::native_tests::")
            self.test("culprit-mysql", "read::native_tests::")
            self.test("culprit-mysql", "preview::tests::live_")
        elif fixture.engine == "mssql":
            self.test("culprit-mssql", "statement::tests::live_")
            self.test("culprit-mssql", "memory_clerks::tests::live_")
            # Includes scheduler fields, telemetry and protocol/cancellation native tests, without a duplicate run.
            self.test("culprit-mssql", "native_tests::")
            # This exact owned history route is outside native_tests::; use the same fixture once.
            self.test("culprit-mssql", "telemetry::native_history::owned_boxed_history_keeps_native_acquisition_separate_from_data_identity_and_replays_canonically")
        else:
            self.test("culprit-engine", "live_providers::native_telemetry_", library=False)
            self.test("culprit-yugabyte", "telemetry::native_tests::")
            self.test("culprit-yugabyte", "tests::live_", skip="cluster_tests::")
            self.test("culprit-postgres", "wire::tests::live_ysql_")
            self.test("culprit-engine", "services::live_native_", library=False)

    def owns_container(self, *, timeout=COMMAND_BUDGET):
        status, owner = self.commands.run(["docker", "inspect", "--format",
                                          '{{index .Config.Labels "' + OWNER_LABEL + '"}}', self.name],
                                          check=False, timeout=timeout)
        if status:
            return False
        if owner.strip() != self.owner:
            raise RuntimeError("refusing to change a container without this run's ownership label")
        return True

    def close(self):
        # A cancelled create can succeed before its CLI returns. Verify the exact ownership label even then.
        if not self.owns_container():
            return
        self.commands.run(["docker", "unpause", self.name], check=False)
        self.commands.run(["docker", "rm", "--force", "--volumes", self.name])
        print(f"Removed owned {self.fixture.name} container and anonymous volumes", flush=True)


class PostgresOutputConsumerDatabase(Database):
    """Explicit real ClientWrite plus retained-lock acceptance on the existing disposable PG18 owner."""
    def seed(self):
        # The Rust fixture owns its finite schema/workload. No unrelated catalog or HypoPG setup is required.
        self.prepare_database()

    def verify(self, selected_tests=None):
        if self.fixture.name != "postgres" or self.fixture.image != "postgres:18.6":
            raise RuntimeError("output-consumer acceptance requires the existing PostgreSQL18 fixture")
        tests = selected_tests or [("culprit-engine", POSTGRES_OUTPUT_CONSUMER_TEST)]
        if tests != [("culprit-engine", POSTGRES_OUTPUT_CONSUMER_TEST)]:
            raise RuntimeError("output-consumer fixture accepts only its explicit native library selector")
        directory = os.environ.get("CULPRIT_NATIVE_SAFETY_RECEIPT_DIR")
        if directory is None:
            directory = tempfile.mkdtemp(prefix="culprit-postgres-output-consumer-")
        if not directory:
            raise RuntimeError("native receipt directory must not be empty")
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(self.commands.sanitized(f"  Native output-consumer receipts: {directory}"), flush=True)
        self.test("culprit-engine", POSTGRES_OUTPUT_CONSUMER_TEST, library=True,
                  test_features=("postgres-native-tests",),
                  environment={"CULPRIT_NATIVE_SAFETY_RECEIPT_DIR": directory})


class MySqlAcceptanceDatabase(Database):
    """One supervisor for the explicit automatic and deep native library policies."""
    RECEIPT_ENV = "CULPRIT_NATIVE_SAFETY_RECEIPT_DIR"

    def __init__(self, fixture, policy):
        super().__init__(fixture)
        self.policy = policy
        self.options.extend(policy.startup_options)

    def prepare_mysql_current_statements(self):
        if self.policy.seed_current_statements:
            super().prepare_mysql_current_statements()

    def receipt_environment(self):
        directory = os.environ.get(self.RECEIPT_ENV)
        if directory is None:
            directory = tempfile.mkdtemp(prefix=f"culprit-{self.fixture.name}-{self.policy.name}-")
        if not directory:
            raise RuntimeError("native safety receipt directory must not be empty")
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{self.policy.name}-fixture.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump({"fixture": self.fixture.name, "image": self.fixture.image,
                       "selection": self.policy.selection, "startup_prerequisites": self.policy.startup_options,
                       "seed_current_statement_setup": self.policy.seed_current_statements,
                       "native_prerequisite_observation_required": True, "native_acceptance": "not-yet-run"},
                      output, indent=2)
            output.write("\n")
        print(self.commands.sanitized(f"  Native safety receipts: {directory}"), flush=True)
        environment = {self.RECEIPT_ENV: directory}
        if self.policy.expected_slow_query_log is not None:
            if self.policy.expected_slow_query_log not in ("OFF", "ON"):
                raise RuntimeError("Settings acceptance requires an explicit OFF/ON startup expectation")
            environment["CULPRIT_NATIVE_MYSQL_SETTINGS_EXPECTED_SLOW_QUERY_LOG"] = self.policy.expected_slow_query_log
        return environment

    def verify(self, selected_tests=None):
        if self.fixture.name not in ("mysql", "mariadb-11.8"):
            raise RuntimeError("native safety acceptance requires exactly MySQL8.4 or MariaDB11.8")
        tests = selected_tests or [("culprit-engine", self.policy.selection)]
        if any(not self.policy.accepts(crate, selection) for crate, selection in tests):
            raise RuntimeError("native fixture accepts only its explicit library selector")
        environment = self.receipt_environment()
        for crate, selection in tests:
            self.test(crate, selection, library=True, test_features=self.policy.test_features, environment=environment)


FULL_TEXT_INSTALL_BUDGET = 300
FULL_TEXT_INSTALL = r"""set -eu
server_version="$1"
test -n "$server_version"
binary_sha=$(sha256sum /opt/mssql/bin/sqlservr)
sed 's|/prod|/mssql-server-2022|' /etc/apt/sources.list.d/microsoft-prod.list > /etc/apt/sources.list.d/mssql-server-2022.list
apt-get update
tmpdir=$(mktemp -d)
chmod 0755 "$tmpdir"
trap 'rm -rf "$tmpdir"' EXIT
cd "$tmpdir"
apt-get download "mssql-server-fts=${server_version}-*"
deb=$(ls mssql-server-fts_*.deb)
pkg_version=$(dpkg-deb -f "$deb" Version)
case "$pkg_version" in
  "${server_version}-"*) ;;
  *) echo "unexpected FTS package version: $pkg_version (expected ${server_version}-*)" >&2; exit 1 ;;
esac
dpkg-deb -x "$deb" /
chmod 0644 /opt/mssql/lib/sqlservr.fts.sfp
test -s /opt/mssql/lib/sqlservr.fts.sfp
test "$(sha256sum /opt/mssql/bin/sqlservr)" = "$binary_sha"
"""


class EmptyCatalogDatabase(Database):
    def seed(self):
        # Exact native ownership and Query Store preparation are shared; no smoke table/workload exists here.
        self.prepare_database()


class FullTextDatabase(Database):
    def start(self):
        super().start()
        if not self.owns_container():
            raise RuntimeError("the exact owned SQL fixture disappeared before its optional FTS installation")
        # This is an explicit disposable fixture prerequisite, never a provider operation. Match the installed
        # server build so native FTS support cannot silently replace the tested engine binary.
        _, raw_version = self.sql(
            "/* culprit fixture */ SET NOCOUNT ON; SELECT CONVERT(varchar(64), SERVERPROPERTY('ProductVersion'));",
            database="master",
            timeout=5,
        )
        versions = [line.strip() for line in raw_version.splitlines() if re.fullmatch(r"\d+(?:\.\d+){2,3}", line.strip())]
        if len(versions) != 1:
            raise RuntimeError(f"could not determine exact native SQL Server version: {raw_version!r}")
        server_version = versions[0]
        self.commands.run(
            ["docker", "exec", "--user", "0", "-i", self.name, "sh", "-ec", FULL_TEXT_INSTALL, "fts-install", server_version],
            timeout=FULL_TEXT_INSTALL_BUDGET,
        )
        self.restart()
        self.sql(f"/* culprit fixture */ IF CONVERT(varchar(64),SERVERPROPERTY('ProductVersion'))<>N'{server_version}' "
                 "OR FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') IS NULL "
                 "OR FULLTEXTSERVICEPROPERTY('IsFullTextInstalled')<>1 "
                 "THROW 51000,N'Owned FTS installation did not establish its native prerequisite.',1; SELECT 1;",
                 database="master", timeout=5)


def selected_mssql_cases(selected_tests, selectors):
    expected = [("culprit-mssql", selection) for selection in selectors]
    tests = [tuple(test) for test in selected_tests] if selected_tests else expected
    if not tests or len(set(tests)) != len(tests) or any(test not in expected for test in tests):
        raise ValueError("this dedicated owned fixture accepts only its exact native library cases")
    return tests


MSSQL_DEDICATED_CASES = {
    "mssql-empty-catalog": (EmptyCatalogDatabase, (MSSQL_EMPTY_CATALOG_TEST,)),
    "mssql-database-counters": (Database, (MSSQL_DATABASE_COUNTER_TEST,)),
    "mssql-fulltext": (FullTextDatabase, MSSQL_FULL_TEXT_TESTS),
}


class AgentDatabase(Database):
    def configuration(self):
        environment, command = super().configuration()
        environment["MSSQL_AGENT_ENABLED"] = "true"
        return environment, command

    def readiness_query(self):
        # A reported Running service can precede Agent's own initialized database connection.
        return """IF NOT EXISTS (
 SELECT 1 FROM sys.dm_exec_sessions WHERE program_name=N'SQLAgent - Generic Refresher'
) THROW 51000, N'Owned Agent fixture is not ready.', 1;
SELECT 1;"""


class HadrDatabase(Database):
    def configuration(self):
        environment, command = super().configuration()
        environment["MSSQL_ENABLE_HADR"] = "1"
        return environment, command


class SshDatabase(Database):
    def __init__(self):
        super().__init__(Fixture("ssh", "postgres:18.6", "postgres", 5432, "ssh_fixture", "postgres"))
        self.directory = tempfile.TemporaryDirectory(prefix="culprit-ssh-")
        self.files = Path(self.directory.name)
        self.database_password = "fixture-only-password"
        self.commands.passwords.append(self.database_password)

    def published_ports(self):
        return (self.fixture.port, 22)

    def certificates(self):
        def path(name):
            return str(self.files / name)

        self.commands.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256",
                           "-keyout", path("ca.key"), "-out", path("ca.pem"), "-days", "1",
                           "-subj", "/CN=Culprit disposable fixture CA", "-addext", "basicConstraints=critical,CA:TRUE"])
        self.commands.run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256",
                           "-keyout", path("server.key"), "-out", path("server.csr"), "-subj", "/CN=localhost"])
        (self.files / "server.ext").write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n")
        self.commands.run(["openssl", "x509", "-req", "-in", path("server.csr"), "-CA", path("ca.pem"),
                           "-CAkey", path("ca.key"), "-CAcreateserial", "-days", "1", "-sha256",
                           "-extfile", path("server.ext"), "-out", path("server.crt")])

    def install_file(self, destination, content, owner="root", mode="600"):
        self.commands.run(["docker", "exec", "-i", self.name, "install", "-m", mode, "-o", owner,
                           "-g", owner, "/dev/stdin", destination], input=content)

    def seed(self):
        self.certificates()
        self.commands.run(["docker", "exec", self.name, "mkdir", "-p", "/tmp/culprit-ssl"])
        self.install_file("/tmp/culprit-ssl/server.key", (self.files / "server.key").read_text(), owner="postgres")
        self.install_file("/tmp/culprit-ssl/server.crt", (self.files / "server.crt").read_text(), owner="postgres")
        self.sql("ALTER SYSTEM SET ssl_cert_file='/tmp/culprit-ssl/server.crt';\n"
                 "ALTER SYSTEM SET ssl_key_file='/tmp/culprit-ssl/server.key';\n"
                 "ALTER SYSTEM SET ssl=on; SELECT pg_reload_conf();\n"
                 f"CREATE ROLE ssh_fixture LOGIN PASSWORD '{self.database_password}';")
        self.start_bastion()

    def start_bastion(self):
        status, _ = self.commands.run(["docker", "exec", self.name, "test", "-x", "/usr/sbin/sshd"], check=False)
        if status:
            self.commands.run(["docker", "exec", self.name, "apt-get", "update"], timeout=300)
            self.commands.run(["docker", "exec", self.name, "apt-get", "install", "-y", "--no-install-recommends",
                               "openssh-server"], timeout=300)
        self.commands.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.files / "client")])
        self.commands.run(["docker", "exec", self.name, "useradd", "--create-home", "--shell", "/bin/sh", "smoke_tunnel"])
        self.commands.run(["docker", "exec", "-i", self.name, "chpasswd"], input=f"smoke_tunnel:{self.password}\n")
        self.commands.run(["docker", "exec", self.name, "install", "-d", "-m", "700", "-o", "smoke_tunnel",
                           "-g", "smoke_tunnel", "/home/smoke_tunnel/.ssh"])
        self.install_file("/home/smoke_tunnel/.ssh/authorized_keys", (self.files / "client.pub").read_text(), owner="smoke_tunnel")
        self.commands.run(["docker", "exec", self.name, "ssh-keygen", "-A"])
        self.commands.run(["docker", "exec", self.name, "mkdir", "-p", "/run/sshd"])
        self.install_file("/tmp/culprit-sshd.conf", """Port 22
ListenAddress 0.0.0.0
HostKey /etc/ssh/ssh_host_ed25519_key
AuthorizedKeysFile .ssh/authorized_keys
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
AllowUsers smoke_tunnel
AllowTcpForwarding local
PermitOpen localhost:5432 127.0.0.1:5432
UsePAM no
PidFile /run/culprit-sshd.pid
""")
        self.commands.run(["docker", "exec", self.name, "/usr/sbin/sshd", "-f", "/tmp/culprit-sshd.conf",
                           "-E", "/tmp/culprit-sshd.log"])
        _, endpoint = self.commands.run(["docker", "port", self.name, "22"])
        port = endpoint.strip().removeprefix("127.0.0.1:")
        if not port.isdigit():
            raise RuntimeError("the SSH fixture must publish exactly one loopback port")
        _, host_key = self.commands.run(["docker", "exec", self.name, "cat", "/etc/ssh/ssh_host_ed25519_key.pub"])
        (self.files / "known_hosts").write_text(f"[127.0.0.1]:{port} {host_key.strip()}\n")
        (self.files / "config").write_text(f"""Host fixture-bastion
    HostName 127.0.0.1
    Port {port}
    User smoke_tunnel
    IdentityFile "{self.files / 'client'}"
    UserKnownHostsFile "{self.files / 'known_hosts'}"
    StrictHostKeyChecking yes
    IdentitiesOnly yes
    BatchMode yes
    HostKeyAlgorithms ssh-ed25519
""")
        self.commands.run(["ssh", "-F", str(self.files / "config"), "fixture-bastion", "true"])

    def verify(self, selected_tests=None):
        environment = {"CULPRIT_KEYCHAIN": "off", "CULPRIT_LANG": "en",
                       "CULPRIT_SSH_LIVE_CONFIG": str(self.files / "config"),
                       "CULPRIT_SSH_LIVE_CA": str(self.files / "ca.pem"),
                       "CULPRIT_SSH_LIVE_PG_PORT": str(self.fixture.port)}
        tests = selected_tests or [("culprit-engine", "ssh::tests::real_openssh_")]
        for crate, selection in tests:
            run_tests(self.commands, crate, selection, environment, library=True)

    def close(self):
        try:
            super().close()
        finally:
            self.directory.cleanup()


class YugabyteCluster:
    def __init__(self):
        self.owner = secrets.token_hex(12)
        self.project = f"culprit-smoke-cluster-{self.owner}"
        self.directory = tempfile.TemporaryDirectory(prefix=self.project + "-")
        self.config = Path(self.directory.name) / "compose.json"
        self.commands = Commands([])
        self.environment = {"CULPRIT_KEYCHAIN": "off", "CULPRIT_LANG": "en",
                            "CULPRIT_YSQL_CLUSTER_TEST": "1", "CULPRIT_YSQL_CLUSTER_PROJECT": self.project,
                            "CULPRIT_YSQL_CLUSTER_OWNER": self.owner}

    def compose(self, *args):
        return ["docker", "compose", "--project-name", self.project, "--file", str(self.config), *args]

    def __enter__(self):
        try:
            self.start()
            return self
        except BaseException:
            if self.config.exists():
                self.commands.run(self.compose("logs", "--no-color", "--tail", "80"), check=False, echo=True)
            self.close()
            raise

    def __exit__(self, *exception):
        self.close()

    def start(self):
        print(f"Starting owned three-node cluster {self.project}", flush=True)
        _, source = self.commands.run(["docker", "compose", "-f", "dev/yugabyte/cluster.compose.yml",
                                       "config", "--format", "json"])
        config = json.loads(source)
        config["name"] = self.project
        for service in config["services"].values():
            service.setdefault("labels", {})[OWNER_LABEL] = self.owner
            for port in service["ports"]:
                port["published"] = "0"
                port["host_ip"] = "127.0.0.1"
        # Compose's resolved default-network name contains the source project's name: give this run its own network.
        config["networks"]["default"]["name"] = self.project + "-network"
        config["networks"]["default"].setdefault("labels", {})[OWNER_LABEL] = self.owner
        self.config.write_text(json.dumps(config))
        self.commands.run(self.compose("up", "--detach", "--wait", "--wait-timeout", str(STARTUP_BUDGET)),
                          timeout=STARTUP_BUDGET + 60, echo=True)
        for index in range(1, 4):
            self.environment[f"CULPRIT_YSQL_CLUSTER_PORT_{index}"] = self.port(f"yb{index}", 5433)
        self.environment["CULPRIT_YSQL_CLUSTER_PORT_POOL"] = self.port("yb3", 5434)
        self.sql("CREATE DATABASE culprit_cluster;", database="yugabyte")
        self.sql((ROOT / "dev/yugabyte/cluster-seed.sql").read_text())

    def port(self, service, port):
        _, published = self.commands.run(self.compose("port", service, str(port)))
        value = published.strip().removeprefix("127.0.0.1:")
        if not value.isdigit():
            raise RuntimeError("the cluster must publish exactly one loopback endpoint per port")
        return value

    def sql(self, sql, *, database="culprit_cluster"):
        self.commands.run(self.compose("exec", "-T", "yb1", "bin/ysqlsh", "-h", "culprit-yb-cluster-1",
                                       "-X", "-v", "ON_ERROR_STOP=1", "-U", "yugabyte", "-d", database), input=sql)

    def verify(self, selected_tests=None):
        for crate, selection in selected_tests or [("culprit-yugabyte", "cluster_tests::")]:
            run_tests(self.commands, crate, selection, self.environment, library=crate != "culprit-engine")

    def close(self):
        try:
            if not self.config.exists():
                return
            _, containers = self.commands.run(["docker", "ps", "--all", "--quiet", "--filter",
                                                f"label=com.docker.compose.project={self.project}"])
            for container in containers.split():
                _, owner = self.commands.run(["docker", "inspect", "--format",
                                              '{{index .Config.Labels "' + OWNER_LABEL + '"}}', container])
                if owner.strip() != self.owner:
                    raise RuntimeError("refusing to remove a cluster container without this run's ownership label")
                self.commands.run(["docker", "unpause", container], check=False)
            self.commands.run(self.compose("down", "--volumes", "--remove-orphans"), echo=True)
            print(f"Removed owned cluster {self.project}", flush=True)
        finally:
            self.directory.cleanup()


def interrupted(*_):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures", nargs="+", choices=[f.name for f in FIXTURES + REPLICATION_FIXTURES]
                        + list(POSTGRES_REPLICATION_IMAGES) + list(MYSQL_ACCEPTANCE_CASES) + list(MSSQL_DEDICATED_CASES)
                        + ["postgres-readable-secondary", "postgres-output-consumer", "mariadb-galera", "mssql-agent", "mssql-hadr", "mssql-replication", "mysql-moving-replication", "mysql-group-replication", "yugabyte-cluster", "ssh", "all"])
    parser.add_argument("--test", nargs=2, action="append", metavar=("CRATE", "FILTER"),
                        help="run only this live test selection; repeat for multiple commands in the same fixture")
    args = parser.parse_args()
    selected = FIXTURES if "all" in args.fixtures else [f for f in FIXTURES if f.name in args.fixtures]
    signal.signal(signal.SIGTERM, interrupted)
    for fixture in selected:
        with Database(fixture) as database:
            database.seed()
            database.verify(args.test)
    count = len(selected)
    for name, (fixture_type, selectors) in MSSQL_DEDICATED_CASES.items():
        if name in args.fixtures:
            tests = selected_mssql_cases(args.test, selectors)
            fixture = replace(next(fixture for fixture in FIXTURES if fixture.name == "mssql"), name=name)
            with fixture_type(fixture) as database:
                database.seed()
                database.verify(tests)
            count += 1
    for name, (source, policy) in MYSQL_ACCEPTANCE_CASES.items():
        if name in args.fixtures:
            fixture = next(fixture for fixture in FIXTURES if fixture.name == source)
            with MySqlAcceptanceDatabase(fixture, policy) as database:
                database.seed()
                database.verify(args.test)
            count += 1
    for fixture in REPLICATION_FIXTURES:
        if fixture.name in args.fixtures:
            with MySqlReplication(Database(fixture)) as database:
                database.verify(args.test)
            count += 1
    if "mariadb-galera" in args.fixtures:
        fixture = Fixture("mariadb-galera", "mariadb:11.8-ubi", "mysql", 3306, "culprit_smoke", "root")
        with MariaDbGalera(Database(fixture)) as database:
            database.verify(args.test)
        count += 1
    for name, image in POSTGRES_REPLICATION_IMAGES.items():
        if name in args.fixtures:
            primary = Database(Fixture("pg-repl-primary", image, "postgres", 5432, "culprit_replication", "postgres"))
            standby = Database(Fixture("pg-repl-standby", image, "postgres", 5432, "culprit_replication", "postgres"))
            with PostgresReplication(primary, standby, OWNER_LABEL) as database:
                database.verify(args.test)
            count += 1
    if "postgres-output-consumer" in args.fixtures:
        fixture = next(fixture for fixture in FIXTURES if fixture.name == "postgres")
        with PostgresOutputConsumerDatabase(fixture) as database:
            database.seed()
            database.verify(args.test)
        count += 1
    if "postgres-readable-secondary" in args.fixtures:
        primary = Database(Fixture("pg-recovery-primary", "postgres:18.6", "postgres", 5432, "culprit_replication", "postgres"))
        standby = Database(Fixture("pg-recovery-standby", "postgres:18.6", "postgres", 5432, "culprit_replication", "postgres"))
        with PostgresReadableRecovery(primary, standby, OWNER_LABEL) as database:
            database.verify(args.test)
        count += 1
    if "mssql-agent" in args.fixtures:
        fixture = replace(next(fixture for fixture in FIXTURES if fixture.name == "mssql"), name="mssql-agent")
        with AgentDatabase(fixture) as database:
            database.seed()
            database.verify(args.test or [("culprit-mssql", "jobs::native_tests::")])
        count += 1
    if "mssql-hadr" in args.fixtures:
        fixture = replace(next(fixture for fixture in FIXTURES if fixture.name == "mssql"), name="mssql-hadr")
        with HadrDatabase(fixture) as database:
            database.seed()
            database.verify(args.test or [("culprit-mssql", "telemetry::replication::native_tests::")])
        count += 1
    if "mssql-replication" in args.fixtures:
        fixture = next(fixture for fixture in FIXTURES if fixture.name == "mssql")
        primary = Database(replace(fixture, name="mssql-ag-primary"))
        secondary = Database(replace(fixture, name="mssql-ag-secondary"))
        with SqlServerReplication(primary, secondary, OWNER_LABEL) as database:
            database.verify(args.test)
        count += 1
    if "mysql-moving-replication" in args.fixtures:
        fixture = next(fixture for fixture in FIXTURES if fixture.name == "mysql")
        source = Database(replace(fixture, name="mysql-source"))
        replica = Database(replace(fixture, name="mysql-replica"))
        with MySqlMovingReplication(source, replica, OWNER_LABEL) as database:
            database.verify(args.test)
        count += 1
    if "mysql-group-replication" in args.fixtures:
        fixture = replace(next(fixture for fixture in FIXTURES if fixture.name == "mysql"), name="mysql-group-replication")
        with MySqlGroupReplication(Database(fixture)) as database:
            database.verify(args.test)
        count += 1
    if "yugabyte-cluster" in args.fixtures or "all" in args.fixtures:
        with YugabyteCluster() as cluster:
            cluster.verify(args.test)
        count += 1
    if "ssh" in args.fixtures or "all" in args.fixtures:
        with SshDatabase() as database:
            database.seed()
            database.verify(args.test)
        count += 1
    print(f"Verified {count} disposable provider fixtures", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, KeyboardInterrupt) as error:
        print(str(error) or "Provider smoke run interrupted", file=sys.stderr)
        sys.exit(1)
