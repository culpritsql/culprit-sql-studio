#!/usr/bin/env python3
"""Stage the public @culpritsql/culprit-wasm repository from the monorepo source of truth."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import tomllib

ROOT = Path(__file__).resolve().parents[2]
DESCRIPTION = (
    "Portable WebAssembly execution plan parser, comparator, deadlock parser, and exporter "
    "for SQL Server, PostgreSQL, MySQL, MariaDB, YugabyteDB, and SQLite."
)
REPOSITORY_URL = "git+https://github.com/culpritsql/culprit-wasm.git"
HOMEPAGE_URL = "https://culpritsql.com"
BUGS_URL = "https://github.com/culpritsql/culprit-wasm/issues"
KEYWORDS = [
    "sql",
    "execution-plan",
    "query-plan",
    "explain",
    "showplan",
    "sqlplan",
    "deadlock",
    "wasm",
    "webassembly",
    "postgres",
    "postgresql",
    "mssql",
    "sql-server",
    "mysql",
    "mariadb",
    "yugabytedb",
    "sqlite",
]
REQUIRED_PKG = {
    "index.js",
    "index.d.ts",
    "build-report.json",
    "THIRD-PARTY-LICENSES.txt",
    "native/culprit_wasm.js",
    "native/culprit_wasm.d.ts",
    "native/culprit_wasm_bg.wasm",
}
ALLOWED_PKG = REQUIRED_PKG | {"native/culprit_wasm_bg.wasm.d.ts"}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_public_manifest(source_manifest: dict[str, object], workspace_version: str) -> dict[str, object]:
    if source_manifest.get("name") != "@culpritsql/culprit-wasm":
        raise RuntimeError(f"unexpected package name: {source_manifest.get('name')}")
    if source_manifest.get("version") != workspace_version:
        raise RuntimeError(
            f"packages/culprit-wasm version {source_manifest.get('version')} "
            f"does not match Cargo.toml version {workspace_version}"
        )
    return {
        "name": source_manifest["name"],
        "version": workspace_version,
        "description": DESCRIPTION,
        "license": source_manifest["license"],
        "type": source_manifest["type"],
        "sideEffects": source_manifest["sideEffects"],
        "files": source_manifest["files"],
        "exports": source_manifest["exports"],
        "repository": {
            "type": "git",
            "url": REPOSITORY_URL,
        },
        "homepage": HOMEPAGE_URL,
        "bugs": {
            "url": BUGS_URL,
        },
        "publishConfig": {
            "access": "public",
            "provenance": True,
        },
        "keywords": KEYWORDS,
    }


def sanitize_build_report(report: dict[str, object]) -> dict[str, object]:
    tools = report.get("tools")
    if not isinstance(tools, dict):
        raise RuntimeError("build-report.json is missing tools metadata")
    sanitized = dict(report)
    sanitized["tools"] = {
        "locked_bindgen_versions": tools["locked_bindgen_versions"],
        "bindgen_cli_version": tools["bindgen_cli_version"],
    }
    encoded = json.dumps(sanitized)
    if "/Users/" in encoded or "/home/" in encoded or "bindgen_executable" in encoded:
        raise RuntimeError("sanitized build-report.json still contains host path metadata")
    return sanitized


def copy_verified_pkg(package_dir: Path, destination_pkg: Path) -> None:
    source_pkg = package_dir / "pkg"
    if source_pkg.is_symlink() or not source_pkg.is_dir():
        raise RuntimeError("packages/culprit-wasm/pkg is missing; build the Wasm package first")
    discovered: dict[str, Path] = {}
    for entry in sorted(source_pkg.rglob("*")):
        if entry.is_symlink():
            raise RuntimeError(f"refusing symlinked Wasm artifact: {entry}")
        if entry.is_file():
            rel = entry.relative_to(source_pkg).as_posix()
            if rel not in ALLOWED_PKG and not re.fullmatch(r"models/[A-Za-z_][A-Za-z_0-9]*\.d\.ts", rel):
                raise RuntimeError(f"unexpected Wasm artifact in pkg/: {rel}")
            discovered[rel] = entry
    if not REQUIRED_PKG <= discovered.keys():
        missing = sorted(REQUIRED_PKG - discovered.keys())
        raise RuntimeError(f"pkg/ is missing required artifacts: {', '.join(missing)}")

    report = json.loads(discovered["build-report.json"].read_text())
    artifacts = report.get("artifacts", {})
    executables = {name for name in discovered if name.endswith((".js", ".wasm"))}
    if set(artifacts) != executables:
        raise RuntimeError("build-report.json executable inventory does not match pkg/")
    for name in sorted(executables):
        raw = discovered[name].read_bytes()
        if artifacts[name].get("bytes") != len(raw) or artifacts[name].get("sha256") != sha256_bytes(raw):
            raise RuntimeError(f"executable digest mismatch while staging: {name}")

    for rel, src in sorted(discovered.items()):
        target = destination_pkg / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if rel == "build-report.json":
            cleaned = sanitize_build_report(report)
            target.write_text(json.dumps(cleaned, indent=2) + "\n")
        else:
            shutil.copyfile(src, target)


def stage_repository(destination: Path, *, root: Path = ROOT, include_pkg: bool = True, include_workflows: bool = True) -> dict[str, object]:
    package_dir = root / "packages/culprit-wasm"
    template_dir = root / ".github/wasm-repo"
    workspace_version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    source_manifest = json.loads((package_dir / "package.json").read_text())
    public_manifest = build_public_manifest(source_manifest, workspace_version)

    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)

    (destination / "package.json").write_text(json.dumps(public_manifest, indent=2) + "\n")
    (destination / ".gitignore").write_text("node_modules/\n*.tgz\n")
    shutil.copyfile(package_dir / "LICENSE", destination / "LICENSE")
    shutil.copyfile(template_dir / "README.md", destination / "README.md")
    shutil.copyfile(template_dir / ".gitattributes", destination / ".gitattributes")

    for rel in ("src/index.ts", "test/contract.ts", "tsconfig.json", "tsconfig.contract.json"):
        target = destination / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(package_dir / rel, target)

    smoke_target = destination / "test/smoke.mjs"
    shutil.copyfile(template_dir / "smoke.mjs", smoke_target)
    smoke_target.chmod(0o755)

    if include_workflows:
        workflows_dir = destination / ".github/workflows"
        workflows_dir.mkdir(parents=True, exist_ok=True)
        for workflow_name in ("ci.yml", "publish.yml"):
            shutil.copyfile(template_dir / "workflows" / workflow_name, workflows_dir / workflow_name)

    if include_pkg:
        copy_verified_pkg(package_dir, destination / "pkg")

    return {"destination": str(destination), "version": workspace_version, "include_pkg": include_pkg}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path, help="Directory to write the staged standalone repository")
    parser.add_argument("--seed-only", action="store_true", help="Omit pkg/ when creating the initial repository seed")
    parser.add_argument("--omit-workflows", action="store_true", help="Omit .github/workflows when syncing via deploy key")
    args = parser.parse_args()
    summary = stage_repository(
        args.destination,
        include_pkg=not args.seed_only,
        include_workflows=not args.omit_workflows,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
