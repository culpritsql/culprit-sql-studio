"""Owned native replication topologies; production readers never perform this setup."""

from contextlib import ExitStack
from ipaddress import IPv4Network
import re
import secrets
import time


RFC1918_NETWORKS = tuple(IPv4Network(cidr) for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))


def explicit_private_subnet(subnet):
    if subnet is None:
        return None
    message = "an explicit smoke subnet must be an aligned RFC1918 IPv4 CIDR"
    if not isinstance(subnet, str) or "/" not in subnet:
        raise ValueError(message)
    try:
        network = IPv4Network(subnet, strict=True)
    except ValueError as error:
        raise ValueError(message) from error
    if not any(network.subnet_of(private) for private in RFC1918_NETWORKS):
        raise ValueError(message)
    return str(network)


class OwnedNetwork:
    def __init__(self, commands, owner, label, *, subnet=None):
        self.subnet = explicit_private_subnet(subnet)
        self.commands = commands
        self.owner = owner
        self.label = label
        self.name = f"culprit-smoke-replication-{owner}"

    def __enter__(self):
        try:
            command = ["docker", "network", "create", "--label", f"{self.label}={self.owner}"]
            if self.subnet is not None:
                command += ["--subnet", self.subnet]
            self.commands.run([*command, self.name])
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *exception):
        self.close()

    def close(self):
        status, owner = self.commands.run(["docker", "network", "inspect", "--format",
                                           '{{index .Labels "' + self.label + '"}}', self.name], check=False)
        if status:
            return
        if owner.strip() != self.owner:
            raise RuntimeError("refusing to remove a replication network without this run's ownership label")
        self.commands.run(["docker", "network", "rm", self.name])


class PostgresReplication:
    """One physical stream, with resource cleanup delegated to the existing owned database supervisor."""

    def __init__(self, primary, standby, owner_label, *, standby_options=()):
        self.primary = primary
        self.standby = standby
        self.stack = ExitStack()
        self.nonce = primary.owner
        self.marker = f"culprit_replication_{self.nonce}"
        self.slot = f"culprit_slot_{self.nonce}"
        self.network = OwnedNetwork(primary.commands, self.nonce, owner_label)
        primary.container_options += ["--network", self.network.name]
        primary.options += ["-c", f"cluster_name={self.marker}", "-c", "max_wal_senders=8",
                            "-c", "max_replication_slots=8"]
        standby.password = primary.password
        standby.commands.passwords.append(primary.password)
        standby.container_options += ["--network", self.network.name, "--entrypoint", "bash", "--user", "postgres"]
        environment = {
            "PGHOST": primary.name, "PGPORT": "5432", "PGUSER": "postgres", "PGPASSWORD": primary.password,
            "PGCONNECT_TIMEOUT": "3", "PGDATA": "/tmp/culprit-standby", "PGAPPNAME": self.marker,
            "CULPRIT_REPLICATION_SLOT": self.slot, "CULPRIT_REPLICATION_MARKER": self.marker,
        }
        # pg_basebackup -R writes the original connection settings and standby.signal. The private container
        # holds the temporary credentials; no workspace, host data directory or external endpoint is mounted.
        bootstrap = '''pg_basebackup --pgdata="$PGDATA" --wal-method=stream --write-recovery-conf \
  --create-slot --slot="$CULPRIT_REPLICATION_SLOT" --checkpoint=fast --no-password
exec postgres -D "$PGDATA" -c "cluster_name=$CULPRIT_REPLICATION_MARKER" "$@"
'''
        standby.launch_configuration = (environment, ["-euc", bootstrap, "--", *standby_options])

    def __enter__(self):
        try:
            self.stack.enter_context(self.network)
            self.stack.enter_context(self.primary)
            if not self.primary.owns_container():
                raise RuntimeError("the owned primary disappeared before replication setup")
            self.primary.commands.run(["docker", "exec", "--user", "postgres", "-i", self.primary.name,
                                       "sh", "-c", 'cat >> "$PGDATA/pg_hba.conf"'],
                                      input="\nhost replication postgres all scram-sha-256\n")
            self.primary.sql("SELECT pg_reload_conf();", database="postgres")
            self.stack.enter_context(self.standby)
            self.wait_for_stream()
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *exception):
        self.stack.close()

    def wait_for_stream(self):
        deadline = time.monotonic() + 30
        statement = ("SELECT EXISTS (SELECT FROM pg_catalog.pg_stat_replication "
                     f"WHERE application_name='{self.marker}' AND state='streaming');")
        while time.monotonic() < deadline:
            _, result = self.primary.sql(statement, tuples=True, timeout=min(3, max(0.1, deadline - time.monotonic())))
            if result.strip() == "t":
                return
            time.sleep(0.2)
        raise TimeoutError("the owned standby did not establish its identified native stream")

    def verify(self, selected_tests=None):
        environment = {
            "CULPRIT_PG_REPLICATION_PRIMARY_PORT": self.primary.port,
            "CULPRIT_PG_REPLICATION_STANDBY_PORT": self.standby.port,
            "CULPRIT_PG_REPLICATION_NONCE": self.nonce,
            "CULPRIT_PG_REPLICATION_PASSWORD": self.primary.password,
        }
        for crate, selection in selected_tests or [("culprit-postgres", "telemetry::native_tests::replication::")]:
            self.primary.test(crate, selection, library=crate != "culprit-engine", environment=environment)


class PostgresReadableRecovery(PostgresReplication):
    """The same disposable pair, with explicit fixture-only recovery delay and lock-table capacity."""

    def __init__(self, primary, standby, owner_label):
        capacity = ("-c", "max_connections=128", "-c", "max_locks_per_transaction=256",
                    "-c", "max_prepared_transactions=16")
        primary.options += list(capacity)
        super().__init__(primary, standby, owner_label, standby_options=(*capacity,
                         "-c", "max_standby_streaming_delay=120s", "-c", "max_standby_archive_delay=120s"))

    def verify(self, selected_tests=None):
        environment = {
            "CULPRIT_PG_REPLICATION_PRIMARY_PORT": self.primary.port,
            "CULPRIT_PG_REPLICATION_STANDBY_PORT": self.standby.port,
            "CULPRIT_PG_REPLICATION_NONCE": self.nonce,
            "CULPRIT_PG_REPLICATION_PASSWORD": self.primary.password,
            "CULPRIT_PG_RECOVERY_FIXTURE": "owned-physical-standby",
            "CULPRIT_PREVIEW_PG_DATABASE": self.standby.fixture.database,
            "CULPRIT_PREVIEW_PG_PORT": self.standby.port,
            "CULPRIT_PREVIEW_PG_PASSWORD": self.primary.password,
        }
        tests = selected_tests or [
            ("culprit-engine", "collector_service::monitoring::native_tests::readable_secondary::"),
        ]
        for crate, selection in tests:
            self.primary.test(crate, selection, library=True, environment=environment,
                              test_features=("postgres-native-tests", "store-adx", "recovery-azure", "auth-azure")
                              if crate == "culprit-engine" else ())


class MySqlReplication:
    """Two known local channels and separate monitor/denied accounts, including instrumentation-off runs."""

    def __init__(self, database):
        self.database = database
        self.monitor = "culprit_replication_monitor"
        self.monitor_password = f"Monitor!{secrets.token_hex(16)}Aa9"
        self.database.commands.passwords.append(self.monitor_password)
        self.channels = [f"culprit_{database.owner}_stopped", f"culprit_{database.owner}_refused"]
        self.mariadb = False
        self.modern_mysql = False

    def __enter__(self):
        try:
            self.database.__enter__()
            self.database.seed()
            self.prepare()
            return self
        except BaseException:
            self.database.close()
            raise

    def __exit__(self, *exception):
        self.database.close()

    def prepare(self):
        database = self.database
        if not database.owns_container():
            raise RuntimeError("the owned database disappeared before channel setup")
        _, version = database.sql("SELECT VERSION();", tuples=True)
        self.mariadb = "mariadb" in version.lower()
        number = re.match(r"(?:5\.5\.5-)?(\d+)\.(\d+)\.(\d+)", version.strip())
        if number is None:
            raise RuntimeError("the owned replication fixture returned an unknown server version")
        self.modern_mysql = not self.mariadb and tuple(map(int, number.groups())) >= (8, 0, 23)
        privilege = "REPLICA MONITOR" if self.mariadb else "REPLICATION CLIENT"
        database.sql(f"CREATE USER '{self.monitor}'@'%' IDENTIFIED BY '{self.monitor_password}'; "
                     f"GRANT {privilege} ON *.* TO '{self.monitor}'@'%'; "
                     f"GRANT SELECT ON {database.fixture.database}.smoke_rows TO '{self.monitor}'@'%';")
        # MariaDB requires distinct native source endpoints for distinct channels.
        # Both refused ports remain inside this disposable container's loopback namespace.
        for refused_port, channel in enumerate(self.channels, start=1):
            if self.mariadb:
                change = f"CHANGE MASTER '{channel}' TO MASTER_HOST='127.0.0.1', MASTER_PORT={refused_port}, "
                prefix, suffix = "MASTER", ";"
            else:
                prefix = "SOURCE" if self.modern_mysql else "MASTER"
                verb = "CHANGE REPLICATION SOURCE" if self.modern_mysql else "CHANGE MASTER"
                change = f"{verb} TO {prefix}_HOST='127.0.0.1', {prefix}_PORT={refused_port}, "
                suffix = f" FOR CHANNEL '{channel}';"
            database.sql(change + f"{prefix}_USER='culprit_unavailable', {prefix}_PASSWORD='', "
                         f"{prefix}_CONNECT_RETRY=1" + suffix)
        database.sql(self.thread_command("START", self.channels[1]))
        self.wait_for_local_refusal(self.channels[1])
        database.sql(self.thread_command("STOP", self.channels[1]))

    def thread_command(self, action, channel):
        if self.mariadb:
            return f"{action} SLAVE '{channel}' IO_THREAD;"
        verb = "REPLICA" if self.modern_mysql else "SLAVE"
        return f"{action} {verb} IO_THREAD FOR CHANNEL '{channel}';"

    def wait_for_local_refusal(self, channel):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.mariadb:
                statement = f"SHOW SLAVE '{channel}' STATUS;"
            else:
                verb = "REPLICA" if self.modern_mysql else "SLAVE"
                statement = f"SHOW {verb} STATUS FOR CHANNEL '{channel}';"
            _, result = self.database.sql(statement, timeout=min(3, max(0.1, deadline - time.monotonic())))
            rows = result.splitlines()
            if len(rows) >= 2:
                values = dict(zip(rows[0].split("\t"), rows[1].split("\t")))
                if int(values.get("Last_IO_Errno", "0")) != 0:
                    return
            time.sleep(0.2)
        raise TimeoutError("the owned loopback-only channel did not report its native connection refusal")

    def verify(self, selected_tests=None):
        environment = {
            "CULPRIT_MYSQL_REPLICATION_MONITOR_USER": self.monitor,
            "CULPRIT_MYSQL_REPLICATION_MONITOR_PASSWORD": self.monitor_password,
            "CULPRIT_MYSQL_REPLICATION_CHANNELS": ",".join(self.channels),
            "CULPRIT_MYSQL_REPLICATION_PERFORMANCE_SCHEMA":
                "off" if "--performance-schema=OFF" in self.database.options else "on",
        }
        for crate, selection in selected_tests or [("culprit-mysql", "telemetry::native_tests::replication::")]:
            self.database.test(crate, selection, library=crate != "culprit-engine", environment=environment)
