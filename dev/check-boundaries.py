#!/usr/bin/env python3
"""Verify resolved normal/build graphs; test/dev dependency composition is deliberately separate."""
import argparse
import ast
import hashlib
import json
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCES = {"culprit-mssql", "culprit-postgres", "culprit-mysql", "culprit-yugabyte", "culprit-sqlite"}
STORES = {"culprit-store-adx", "culprit-store-clickhouse"}
AUTHENTICATION = {"culprit-auth-azure", "culprit-auth-aws"}
RECOVERY = {"culprit-blob-azure", "culprit-blob-s3"}
CLOUD = {"culprit-cloud"}
FLEET = STORES | RECOVERY | {"culprit-collector"}
LOCAL_DATABASE_FILES = {"tempfile", "rusqlite"}
WASM_TARGET = "wasm32-unknown-unknown"
WASM_STACK_BYTES = 32 * 1024 * 1024
PORTABLE_OWNERS = {"culprit-plans": {"culprit-core"}, "culprit-wasm": {"culprit-core", "culprit-plans"}}
DATABASE_DRIVERS = {"tiberius", "tokio-postgres", "postgres-protocol", "postgres-types", "mysql_async",
                    "mysql_common", "sqlx", "sqlx-core", "sqlx-mysql", "sqlx-postgres", "sqlx-sqlite",
                    "rusqlite", "libsqlite3-sys"}
NATIVE_IO = {"tokio", "tokio-util", "reqwest", "hyper", "object_store", "azure_core", "aws-config",
             "aws-sdk-s3", "ssh2", "openssh", "libssh2-sys"}
PORTABLE_FORBIDDEN = (FLEET | SOURCES | AUTHENTICATION | CLOUD | LOCAL_DATABASE_FILES | DATABASE_DRIVERS |
                      NATIVE_IO | {"culprit-auth", "culprit-read", "culprit-evidence", "culprit-engine", "culprit-checks"})
WASM_MODEL_ROOTS = {"Plan", "PlanDiff", "Deadlock", "Engine", "CompareContext", "ExportFile", "ExportFormat",
                    "ExportOptions", "Error"}


def tree_output(package, features, output_format, target=None):
    # None proves the shipped default package. An explicit empty selection proves the adapter's read-only build.
    selection = []
    if features is not None:
        selection = ["--no-default-features"]
        if features:
            selection += ["--features", features]
    if target is not None:
        selection += ["--target", target]
    result = subprocess.run(
        ["cargo", "tree", "--locked", "--package", package, "--edges", "normal,build",
         "--prefix", "none", "--format", output_format, *selection], cwd=ROOT, check=True, text=True, capture_output=True)
    return result.stdout.splitlines()


def dependencies(package, features=None, target=None):
    return {line.split()[0] for line in tree_output(package, features, "{p}", target) if line.strip()} - {package}


def feature_names(enabled):
    return {feature.strip() for feature in enabled.split(",") if feature.strip()}


def verify_read_features(package, features=None):
    for line in tree_output(package, features, "{p}|{f}"):
        identity, _, enabled = line.partition("|")
        if not identity.strip():
            continue
        adapter = identity.split()[0]
        if adapter in STORES | RECOVERY and {"default", "write"} & feature_names(enabled):
            raise RuntimeError(f"{package} enables ingestion features on {adapter}: {enabled}")
    print(f"{package}: warehouse and recovery adapters have read-only features")


def default_rules():
    rules = {"culprit-mcp": FLEET, "culprit-app": {"culprit-collector"}}
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


def verify(package, features, forbidden, *, target=None, allowed_workspace=None):
    actual = dependencies(package, features) if target is None else dependencies(package, features, target)
    leaked = actual & forbidden
    if allowed_workspace is not None:
        leaked |= {name for name in actual if name.startswith("culprit-") and name not in allowed_workspace}
    selection = "default" if features is None else features or "read-only adapter"
    if target is not None:
        selection += f", {target}"
    if leaked:
        raise RuntimeError(f"{package} [{selection}] includes forbidden build dependencies: {', '.join(sorted(leaked))}")
    print(f"{package} [{selection}]: normal/build dependency boundary verified")


def verify_english_wasm(features, target=None):
    for line in tree_output("culprit-wasm", features, "{p}|{f}", target):
        identity, _, enabled = line.partition("|")
        if identity.split() and identity.split()[0] in {"culprit-core", "culprit-plans", "culprit-wasm"}:
            if "locales" in feature_names(enabled):
                raise RuntimeError("standalone culprit-wasm enables non-English catalogs")


def verify_wasm_build_selection(root=ROOT):
    module = ast.parse((root / "dev/build-wasm.py").read_text())
    calls = [node for node in ast.walk(module) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "run" and node.args
             and isinstance(node.args[0], ast.Constant) and node.args[0].value == "cargo"]
    expected = ["cargo", "rustc", "--locked", "-p", "culprit-wasm", "--lib", "--no-default-features",
                "--target", WASM_TARGET, "--release", "--", "-C"]
    if len(calls) != 1:
        raise RuntimeError("standalone Wasm build must have one selected final-crate Cargo command")
    arguments = calls[0].args
    prefix = [node.value if isinstance(node, ast.Constant) else None for node in arguments[:-1]]
    if prefix != expected or len(arguments) != len(expected) + 1:
        raise RuntimeError("standalone Wasm build changed its package/target/features or final-crate linker boundary")
    stack = arguments[-1]
    if not (isinstance(stack, ast.JoinedStr) and len(stack.values) == 2
            and isinstance(stack.values[0], ast.Constant) and stack.values[0].value == "link-arg=-zstack-size="
            and isinstance(stack.values[1], ast.FormattedValue)
            and isinstance(stack.values[1].value, ast.Name)
            and stack.values[1].value.id == "PACKAGED_WASM_STACK_BYTES"):
        raise RuntimeError("standalone Wasm stack policy must apply only to the selected final crate")


def verify_portable_graphs():
    for package, allowed in PORTABLE_OWNERS.items():
        verify(package, None, PORTABLE_FORBIDDEN, allowed_workspace=allowed)
        verify(package, "", PORTABLE_FORBIDDEN, target=WASM_TARGET, allowed_workspace=allowed)
    verify_english_wasm(None)
    verify_english_wasm("", WASM_TARGET)
    verify_wasm_build_selection()


def wasm_package_files(package):
    output = package / "pkg"
    required = {"index.js", "index.d.ts", "build-report.json", "THIRD-PARTY-LICENSES.txt", "native/culprit_wasm.js",
                "native/culprit_wasm.d.ts", "native/culprit_wasm_bg.wasm"}
    allowed = required | {"native/culprit_wasm_bg.wasm.d.ts"}
    files = {}
    if output.is_symlink() or not output.is_dir():
        raise RuntimeError("Wasm output must be an owned package directory")
    for path in output.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("Wasm distribution cannot include symlinked files")
        if path.is_file():
            relative = path.relative_to(output).as_posix()
            if relative not in allowed and not re.fullmatch(r"models/[A-Za-z_][A-Za-z_0-9]*\.d\.ts", relative):
                raise RuntimeError(f"unexpected Wasm distribution artifact: {relative}")
            files[relative] = path
    if not required <= files.keys():
        raise RuntimeError("Wasm distribution is missing required executable/declaration/report artifacts")
    return files


def verify_wasm_report(root, files):
    report = json.loads(files["build-report.json"].read_text())
    if (report.get("target") != WASM_TARGET or report.get("profile") != "release"
            or report.get("native_or_database_work") is not False
            or report.get("stack_policy", {}).get("linear_stack_bytes") != WASM_STACK_BYTES):
        raise RuntimeError("Wasm report changed target/profile/source-free or bounded stack policy")
    lock = root / "Cargo.lock"
    if report.get("cargo_lock_sha256") != hashlib.sha256(lock.read_bytes()).hexdigest():
        raise RuntimeError("Wasm artifacts do not match the selected lockfile")
    versions = sorted({row["version"] for row in tomllib.loads(lock.read_text())["package"]
                       if row["name"] == "wasm-bindgen"})
    tools = report.get("tools", {})
    if len(versions) != 1 or tools.get("locked_bindgen_versions") != versions or tools.get("bindgen_cli_version") != versions[0]:
        raise RuntimeError("Wasm artifacts require the exact locked wasm-bindgen CLI version")
    executable = {name for name in files if name.endswith((".js", ".wasm"))}
    if set(report.get("artifacts", {})) != executable:
        raise RuntimeError("Wasm artifact report must cover every executable file exactly")
    for name in executable:
        body = files[name].read_bytes()
        if (report["artifacts"][name].get("bytes") != len(body)
                or report["artifacts"][name].get("sha256") != hashlib.sha256(body).hexdigest()):
            raise RuntimeError(f"Wasm executable/report digest mismatch: {name}")
    models = report.get("models", [])
    names = [row["name"] for row in models]
    if (not WASM_MODEL_ROOTS <= set(names) or len(names) != len(set(names))
            or {f"models/{name}.d.ts" for name in names} != {name for name in files if name.startswith("models/")}):
        raise RuntimeError("Wasm model declarations must match the closed generated declaration manifest")
    for row in models:
        source = root / "app/src/gen" / f"{row['name']}.ts"
        if (not source.is_file() or row.get("source_sha256") != hashlib.sha256(source.read_bytes()).hexdigest()
                or row.get("package_sha256") != hashlib.sha256(files[f"models/{row['name']}.d.ts"].read_bytes()).hexdigest()):
            raise RuntimeError("Wasm declaration/source digest mismatch")


def verify_wasm_artifacts(root=ROOT):
    package = root / "packages/culprit-wasm"
    manifest = json.loads((package / "package.json").read_text())
    exports = {".": {"types": "./pkg/index.d.ts", "import": "./pkg/index.js"}}
    if (manifest.get("type") != "module" or manifest.get("private") is not True
            or manifest.get("files") != ["pkg"] or manifest.get("exports") != exports
            or any(manifest.get(name) for name in ["dependencies", "optionalDependencies", "peerDependencies", "devDependencies"])):
        raise RuntimeError("Wasm package must expose only its standalone typed ESM entry without runtime packages")
    files = wasm_package_files(package)
    notices = root / "target/licenses/THIRD-PARTY-LICENSES.txt"
    if (not notices.is_file() or notices.stat().st_size == 0
            or notices.read_bytes() != files["THIRD-PARTY-LICENSES.txt"].read_bytes()):
        raise RuntimeError("Wasm distribution must retain the exact shared third-party notices")
    verify_wasm_report(root, files)
    print("culprit-wasm: closed standalone package, generated models and executable digests verified")


def verify_plan_viewer_artifacts(root=ROOT):
    package = root / "packages/culprit-plan-viewer"
    manifest = json.loads((package / "package.json").read_text())
    exports = {
        ".": {"types": "./pkg/index.d.ts", "import": "./pkg/index.js"},
        "./styles.css": "./pkg/styles.css",
    }
    if (manifest.get("type") != "module" or manifest.get("private") is not True
            or manifest.get("files") != ["pkg"] or manifest.get("exports") != exports
            or set(manifest.get("dependencies", {})) != {"lucide-react"}
            or set(manifest.get("peerDependencies", {})) != {"react"}
            or any(manifest.get(name) for name in ["optionalDependencies", "devDependencies"])):
        raise RuntimeError("plan-viewer package manifest changed its closed React/lucide dependency boundary")
    output = package / "pkg"
    if output.is_symlink() or not output.is_dir():
        raise RuntimeError("plan-viewer output must be an owned package directory")
    files = {}
    for path in output.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("plan-viewer distribution cannot include symlinked files")
        if path.is_file():
            files[path.relative_to(output).as_posix()] = path
    required = {"index.js", "index.d.ts", "styles.css", "build-report.json", "THIRD-PARTY-LICENSES.txt"}
    if not required <= files.keys():
        raise RuntimeError("plan-viewer distribution is missing required artifacts")
    notices = root / "target/licenses/THIRD-PARTY-LICENSES.txt"
    if (not notices.is_file() or notices.stat().st_size == 0
            or notices.read_bytes() != files["THIRD-PARTY-LICENSES.txt"].read_bytes()):
        raise RuntimeError("plan-viewer distribution must retain the exact shared third-party notices")
    report_raw = files["build-report.json"].read_text()
    if "/Users/" in report_raw or "/home/" in report_raw:
        raise RuntimeError("plan-viewer build-report.json contains host path metadata")
    report = json.loads(report_raw)
    if report.get("target") != "es2022-react-esm" or report.get("native_or_database_work") is not False:
        raise RuntimeError("plan-viewer report changed target or source-free boundary")
    shipped = {name for name in files if name.endswith((".js", ".css"))}
    if set(report.get("artifacts", {})) != shipped:
        raise RuntimeError("plan-viewer artifact report must cover every .js and .css file exactly")
    for name in shipped:
        body = files[name].read_bytes()
        if (report["artifacts"][name].get("bytes") != len(body)
                or report["artifacts"][name].get("sha256") != hashlib.sha256(body).hexdigest()):
            raise RuntimeError(f"plan-viewer artifact digest mismatch: {name}")
    models = report.get("models", [])
    names = [row["name"] for row in models]
    if (len(names) != len(set(names))
            or {f"models/{name}.d.ts" for name in names} != {name for name in files if name.startswith("models/")}):
        raise RuntimeError("plan-viewer model declarations must match the closed generated declaration manifest")
    for row in models:
        source = root / "app/src/gen" / f"{row['name']}.ts"
        if (not source.is_file() or row.get("source_sha256") != hashlib.sha256(source.read_bytes()).hexdigest()
                or row.get("package_sha256") != hashlib.sha256(files[f"models/{row['name']}.d.ts"].read_bytes()).hexdigest()):
            raise RuntimeError("plan-viewer declaration/source digest mismatch")
    print("culprit-plan-viewer: closed standalone package, generated models and artifact digests verified")


def main():
    for package, forbidden in sorted(default_rules().items()):
        verify(package, None, forbidden)
    for features, forbidden in engine_compositions():
        verify("culprit-engine", features, forbidden)
        if features.startswith("warehouse-"):
            verify_read_features("culprit-engine", features)
    for package, forbidden in sorted(read_adapter_rules().items()):
        verify(package, "", forbidden)
        verify_read_features(package, "")
    verify_read_features("culprit-app")
    verify_portable_graphs()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--portable", action="store_true", help="Check only pure Plans/Wasm normal/build graphs and standalone selection")
    mode.add_argument("--wasm-artifacts", action="store_true", help="Verify the actual built standalone browser distribution")
    mode.add_argument("--plan-viewer-artifacts", action="store_true", help="Verify the built @culpritsql/plan-viewer React distribution")
    options = parser.parse_args()
    if options.wasm_artifacts:
        verify_wasm_artifacts()
    elif options.plan_viewer_artifacts:
        verify_plan_viewer_artifacts()
    elif options.portable:
        verify_portable_graphs()
    else:
        main()
