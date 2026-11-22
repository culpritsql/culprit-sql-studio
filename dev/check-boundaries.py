#!/usr/bin/env python3
"""Verify resolved normal/build graphs; test/dev dependency composition is deliberately separate."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCES = {"culprit-mssql", "culprit-postgres", "culprit-mysql", "culprit-yugabyte", "culprit-sqlite"}
STORES = {"culprit-store-adx", "culprit-store-clickhouse"}
AUTHENTICATION = {"culprit-auth-azure", "culprit-auth-aws"}
RECOVERY = {"culprit-blob-azure", "culprit-blob-s3"}
CLOUD = {"culprit-cloud"}
FLEET = STORES | RECOVERY | {"culprit-collector"}
LOCAL_DATABASE_FILES = {"tempfile", "rusqlite"}


def dependencies(package, features=None):
    # None proves the shipped default package. An explicit empty selection proves the adapter's read-only build.
    selection = []
    if features is not None:
        selection = ["--no-default-features"]
        if features:
            selection += ["--features", features]
    result = subprocess.run(
        ["cargo", "tree", "--locked", "--package", package, "--edges", "normal,build",
         "--prefix", "none", "--format", "{p}", *selection], cwd=ROOT, check=True, text=True, capture_output=True)
    return {line.split()[0] for line in result.stdout.splitlines() if line.strip()} - {package}


def default_rules():
    rules = {package: FLEET for package in ["culprit-mcp", "culprit-app"]}
    rules.update({package: FLEET | CLOUD | AUTHENTICATION | {"culprit-engine"} for package in SOURCES})
    rules.update({package: SOURCES | CLOUD | AUTHENTICATION | {"culprit-engine"} for package in STORES | RECOVERY})
    rules.update({package: FLEET | CLOUD | SOURCES | {"culprit-engine"} for package in AUTHENTICATION})
    rules.update({package: FLEET | SOURCES | AUTHENTICATION | CLOUD | {"culprit-engine"}
                  for package in ["culprit-auth", "culprit-read", "culprit-core", "culprit-checks"]})
    rules["culprit-collector"] = STORES | RECOVERY | CLOUD | SOURCES | AUTHENTICATION | {"culprit-engine"}
    rules["culprit-cloud"] = FLEET | SOURCES | AUTHENTICATION | {"culprit-engine"}
    rules["culprit-evidence"] = FLEET | SOURCES | AUTHENTICATION | CLOUD | LOCAL_DATABASE_FILES | {"culprit-auth", "culprit-engine"}
    return rules


def engine_compositions():
    read_only = {"culprit-collector"} | SOURCES | AUTHENTICATION | LOCAL_DATABASE_FILES
    return [
        ("store-clickhouse,recovery-s3", {"culprit-store-adx", "culprit-blob-azure"}),
        ("store-adx,recovery-azure", {"culprit-store-clickhouse", "culprit-blob-s3"}),
        ("warehouse-clickhouse,evidence-s3", read_only | {"culprit-store-adx", "culprit-blob-azure"}),
        ("warehouse-adx,evidence-azure", read_only | {"culprit-store-clickhouse", "culprit-blob-s3"}),
    ]


def read_adapter_rules():
    forbidden = {"culprit-collector", "culprit-engine"} | SOURCES | AUTHENTICATION | CLOUD | LOCAL_DATABASE_FILES
    return {package: forbidden | ((STORES | RECOVERY) - {package}) for package in STORES | RECOVERY}


def verify(package, features, forbidden):
    leaked = dependencies(package, features) & forbidden
    selection = "default" if features is None else features or "read-only adapter"
    if leaked:
        raise RuntimeError(f"{package} [{selection}] includes forbidden build dependencies: {', '.join(sorted(leaked))}")
    print(f"{package} [{selection}]: normal/build dependency boundary verified")


def main():
    for package, forbidden in sorted(default_rules().items()):
        verify(package, None, forbidden)
    for features, forbidden in engine_compositions():
        verify("culprit-engine", features, forbidden)
    for package, forbidden in sorted(read_adapter_rules().items()):
        verify(package, "", forbidden)


if __name__ == "__main__":
    main()
