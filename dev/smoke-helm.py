#!/usr/bin/env python3
"""Validate the culprit-collector Helm chart, its safety gates, and rendered collector contracts."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
CHART = ROOT / "deploy/helm/culprit-collector"


def helm(*args, check=True):
    return subprocess.run(["helm", *args], capture_output=True, text=True, check=check, timeout=60)


def extract_configmap_files(manifest_yaml):
    """Extract ConfigMap data entries (connections.toml and collector.json) without requiring PyYAML."""
    # Render only templates/configmap.yaml with -s and parse via python or helm JSON conversion?
    # Even simpler: helm template --show-only templates/configmap.yaml outputs one YAML document;
    # parse the literal block scalars under `data:` deterministically.
    lines = manifest_yaml.splitlines()
    in_data = False
    current_key = None
    buf = {}
    for line in lines:
        if line == "data:":
            in_data = True
            continue
        if not in_data:
            continue
        if line and not line.startswith(" "):
            break
        if line.startswith("  ") and not line.startswith("    ") and line.endswith(": |"):
            current_key = line.strip()[:-3]
            buf[current_key] = []
        elif current_key is not None:
            if line.startswith("    "):
                buf[current_key].append(line[4:])
            elif line == "":
                buf[current_key].append("")
    return {k: "\n".join(v).rstrip() + "\n" for k, v in buf.items()}


def assert_template_fails(values, expected_substring):
    with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
        json.dump(values, handle)
        handle.flush()
        result = helm("template", "culprit-pilot", str(CHART), "-f", handle.name, check=False)
    combined = (result.stdout + "\n" + result.stderr).lower()
    if result.returncode == 0 or expected_substring.lower() not in combined:
        raise AssertionError(
            f"Expected helm template to fail containing {expected_substring!r}, got rc={result.returncode}:\n{result.stdout}\n{result.stderr}"
        )


def validate_rendered_config(binary, values):
    with tempfile.TemporaryDirectory(prefix="culprit-helm-smoke-") as temp_dir:
        temp = Path(temp_dir)
        values_file = temp / "values.json"
        values_file.write_text(json.dumps(values))
        full = helm("template", "culprit-pilot", str(CHART), "--namespace", "db-monitoring", "-f", str(values_file))
        cm = helm(
            "template", "culprit-pilot", str(CHART), "--namespace", "db-monitoring",
            "--show-only", "templates/configmap.yaml", "-f", str(values_file),
        )
        data = extract_configmap_files(cm.stdout)
        if set(data) != {"connections.toml", "collector.json"}:
            raise AssertionError(f"Unexpected ConfigMap keys: {sorted(data)}")
        workspace = temp / "workspace"
        workspace.mkdir(mode=0o700)
        (workspace / "connections.toml").write_text(data["connections.toml"])
        collector_cfg = json.loads(data["collector.json"])
        collector_cfg["workspace"] = str(workspace)
        collector_cfg["state"] = str(temp / "state")
        cfg_path = temp / "collector.json"
        cfg_path.write_text(json.dumps(collector_cfg))
        subprocess.run(
            [str(binary), "collect", "service", "validate", "--config", str(cfg_path)],
            check=True, capture_output=True, text=True, timeout=30,
        )
        return full.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/culprit")
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise FileNotFoundError(f"Culprit CLI binary not found at {binary}")

    helm("lint", str(CHART))

    # Safety gates: single replica max, explicit quiescence/delivery-only, no inline secrets, required WI IDs.
    assert_template_fails({"replicaCount": 2, "activation": {"sourceQuiescent": True}}, "maximum")
    assert_template_fails({"replicaCount": 1}, "sourcequiescent=true or activation.deliveryonly=true")
    assert_template_fails(
        {"replicaCount": 1, "activation": {"sourceQuiescent": True, "deliveryOnly": True}},
        "mutually exclusive",
    )
    assert_template_fails(
        {"connections": {"bad": {"engine": "postgres", "host": "db.internal", "password": "secret"}}},
        "must not contain inline secrets",
    )
    assert_template_fails({"azureWorkloadIdentity": {"enabled": True}}, "tenantid")

    # Positive 1: Azure AKS Workload Identity + Azure Blob + ADX + digest-pinned image + init Job.
    digest = "sha256:" + ("a" * 64)
    azure_manifest = validate_rendered_config(binary, {
        "replicaCount": 1,
        "activation": {"sourceQuiescent": True, "deliveryOnly": False, "initNamespaceJob": {"enabled": True}},
        "image": {"repository": "ghcr.io/culpritsql/culprit-collector", "digest": digest},
        "serviceAccount": {"create": False, "name": "culprit-sa"},
        "azureWorkloadIdentity": {
            "enabled": True,
            "tenantId": "d8353d2a-b153-4d17-8827-902c51f72357",
            "clientId": "2a82e7db-d5f3-4f59-8dd5-3da21f70bf5c",
        },
        "connections": {
            "pilot": {
                "engine": "mssql",
                "environment": "staging",
                "host": "never-contact.database.windows.net",
                "port": 1433,
                "database": "pilot_db",
                "auth": "entra",
                "options": {"encrypt": "True"},
            }
        },
        "collector": {
            "pipeline": "database-telemetry",
            "shard": "pilot",
            "inventory": {
                "targets": [{
                    "id": "pilot-db",
                    "connection": "pilot",
                    "subject": {"kind": "database"},
                    "shard": "pilot",
                    "admission_group": "pilot-pool",
                }]
            },
            "sourceCredentials": {"pilot": "source"},
            "recovery": {
                "provider": "azure-blob",
                "options": {"account": "nevercontactsa", "container": "culprit-native", "prefix": "database-telemetry/pilot"},
                "credential": "recovery",
            },
            "target": {
                "kind": "adx",
                "query": {"endpoint": "https://never-contact.westeurope.kusto.windows.net", "database": "culprit"},
                "credential": "analytics",
                "delivery": {"credential": "ingestion"},
            },
        },
        "persistence": {"storageClassName": "managed-csi", "size": "10Gi"},
    })
    assert f"ghcr.io/culpritsql/culprit-collector@{digest}" in azure_manifest
    assert 'azure.workload.identity/use: "true"' in azure_manifest
    assert "audience: api://AzureADTokenExchange" in azure_manifest
    assert "kind: Job" in azure_manifest

    # Positive 2: AWS S3 + ClickHouse + delivery-only mode.
    ch_manifest = validate_rendered_config(binary, {
        "replicaCount": 1,
        "activation": {"sourceQuiescent": False, "deliveryOnly": True},
        "connections": {
            "orders": {
                "engine": "postgres",
                "environment": "production",
                "host": "orders.internal",
                "port": 5432,
                "database": "orders",
                "user": "culprit_reader",
                "auth": "aws-iam",
            }
        },
        "collector": {
            "pipeline": "database-telemetry",
            "shard": "orders",
            "inventory": {
                "targets": [{
                    "id": "orders-db",
                    "connection": "orders",
                    "subject": {"kind": "database"},
                    "shard": "orders",
                    "admission_group": "orders-primary",
                }]
            },
            "credentials": {
                "source": {"kind": "aws", "profile": None},
                "recovery": {"kind": "aws", "profile": None},
                "analytics": {"kind": "password-environment", "username": "culprit_writer", "variable": "CLICKHOUSE_PASSWORD"},
            },
            "sourceCredentials": {"orders": "source"},
            "recovery": {
                "provider": "s3",
                "options": {
                    "bucket": "culprit-native-archive",
                    "region": "eu-west-1",
                    "prefix": "database-telemetry/orders",
                    "expected_owner": "123456789012",
                },
                "credential": "recovery",
            },
            "target": {
                "kind": "clickhouse",
                "query": {"endpoint": "https://clickhouse.internal:8443", "database": "culprit"},
                "credential": "analytics",
            },
        },
        "secretEnvFrom": [{"secretRef": {"name": "culprit-clickhouse-credentials"}}],
    })
    assert "- --delivery-only" in ch_manifest
    assert "name: culprit-clickhouse-credentials" in ch_manifest

    with tempfile.TemporaryDirectory(prefix="culprit-helm-pkg-") as pkg_dir:
        helm("package", str(CHART), "--destination", pkg_dir)
        charts = list(Path(pkg_dir).glob("culprit-collector-*.tgz"))
        if len(charts) != 1:
            raise AssertionError(f"Expected 1 packaged chart archive, got {charts}")

    print("Helm chart passed lint, safety gates, Azure/ADX + S3/ClickHouse contract validation, and OCI packaging")


if __name__ == "__main__":
    main()
