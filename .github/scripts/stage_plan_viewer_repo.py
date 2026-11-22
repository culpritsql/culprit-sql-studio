#!/usr/bin/env python3
"""Stage the public @culpritsql/plan-viewer repository (culpritsql/culprit-plan-viewer) from the monorepo."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil

ROOT = Path(__file__).resolve().parents[2]
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
DESCRIPTION = (
    "Interactive React execution plan diagram, structural plan diff, and deadlock cycle graph components "
    "for SQL Server, PostgreSQL, MySQL, MariaDB, YugabyteDB, and SQLite."
)
REPOSITORY_URL = "git+https://github.com/culpritsql/culprit-plan-viewer.git"
HOMEPAGE_URL = "https://culpritsql.com"
BUGS_URL = "https://github.com/culpritsql/culprit-plan-viewer/issues"
KEYWORDS = [
    "react",
    "sql",
    "execution-plan",
    "query-plan",
    "explain",
    "explain-analyze",
    "showplan",
    "sqlplan",
    "deadlock",
    "plan-viewer",
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
    "styles.css",
    "PlanViewer.js",
    "PlanViewer.d.ts",
    "PlanDiffViewer.js",
    "PlanDiffViewer.d.ts",
    "DeadlockViewer.js",
    "DeadlockViewer.d.ts",
    "build-report.json",
    "THIRD-PARTY-LICENSES.txt",
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_public_manifest(source_manifest: dict[str, object]) -> dict[str, object]:
    if source_manifest.get("name") != "@culpritsql/plan-viewer":
        raise RuntimeError(f"unexpected package name: {source_manifest.get('name')}")
    version = source_manifest.get("version")
    if not isinstance(version, str) or not SEMVER_RE.fullmatch(version):
        raise RuntimeError(f"invalid semver in packages/culprit-plan-viewer/package.json: {version!r}")
    return {
        "name": source_manifest["name"],
        "version": version,
        "description": DESCRIPTION,
        "license": source_manifest["license"],
        "type": source_manifest["type"],
        "sideEffects": source_manifest["sideEffects"],
        "files": source_manifest["files"],
        "exports": source_manifest["exports"],
        "dependencies": source_manifest["dependencies"],
        "peerDependencies": source_manifest["peerDependencies"],
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
        "typescript_version": tools["typescript_version"],
        "lucide_react_version": tools["lucide_react_version"],
    }
    encoded = json.dumps(sanitized)
    if "/Users/" in encoded or "/home/" in encoded:
        raise RuntimeError("sanitized build-report.json still contains host path metadata")
    return sanitized


def copy_verified_pkg(package_dir: Path, destination_pkg: Path) -> None:
    source_pkg = package_dir / "pkg"
    if source_pkg.is_symlink() or not source_pkg.is_dir():
        raise RuntimeError("packages/culprit-plan-viewer/pkg is missing; run python3 dev/build-plan-viewer.py first")
    discovered: dict[str, Path] = {}
    for entry in sorted(source_pkg.rglob("*")):
        if entry.is_symlink():
            raise RuntimeError(f"refusing symlinked plan-viewer artifact: {entry}")
        if entry.is_file():
            rel = entry.relative_to(source_pkg).as_posix()
            is_top_module = bool(re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*\.(?:js|d\.ts)", rel))
            is_model = bool(re.fullmatch(r"models/[A-Za-z_][A-Za-z_0-9]*\.d\.ts", rel))
            if rel not in REQUIRED_PKG and not is_top_module and not is_model:
                raise RuntimeError(f"unexpected artifact in packages/culprit-plan-viewer/pkg/: {rel}")
            discovered[rel] = entry

    if not REQUIRED_PKG <= discovered.keys():
        missing = sorted(REQUIRED_PKG - discovered.keys())
        raise RuntimeError(f"pkg/ is missing required artifacts: {', '.join(missing)}")

    report = json.loads(discovered["build-report.json"].read_text())
    artifacts = report.get("artifacts", {})
    shipped = {name for name in discovered if name.endswith((".js", ".css"))}
    if set(artifacts) != shipped:
        raise RuntimeError("build-report.json artifact inventory does not match pkg/")
    for name in sorted(shipped):
        raw = discovered[name].read_bytes()
        if artifacts[name].get("bytes") != len(raw) or artifacts[name].get("sha256") != sha256_bytes(raw):
            raise RuntimeError(f"artifact digest mismatch while staging: {name}")

    for rel, src in sorted(discovered.items()):
        target = destination_pkg / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if rel == "build-report.json":
            cleaned = sanitize_build_report(report)
            target.write_text(json.dumps(cleaned, indent=2) + "\n")
        else:
            shutil.copyfile(src, target)


def meaningful_digests(repo_dir: Path, *, pkg_only: bool = False) -> dict[str, str]:
    """Compute SHA-256 digests of meaningful files, ignoring .git/, .github/, and build-report.json metadata."""
    base = repo_dir / "pkg" if pkg_only else repo_dir
    if not base.is_dir():
        return {}
    digests: dict[str, str] = {}
    for path in sorted(base.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(repo_dir).as_posix()
        if rel.startswith((".git/", ".github/")) or rel == "pkg/build-report.json":
            continue
        digests[rel] = sha256_bytes(path.read_bytes())
    return digests


def has_meaningful_changes(staged_dir: Path, target_dir: Path) -> bool:
    return meaningful_digests(staged_dir) != meaningful_digests(target_dir)


def has_pkg_changes(staged_dir: Path, previous_release_dir: Path) -> bool:
    return meaningful_digests(staged_dir, pkg_only=True) != meaningful_digests(previous_release_dir, pkg_only=True)


def stage_repository(
    destination: Path,
    *,
    root: Path = ROOT,
    include_pkg: bool = True,
    include_workflows: bool = True,
) -> dict[str, object]:
    package_dir = root / "packages/culprit-plan-viewer"
    template_dir = root / ".github/plan-viewer-repo"
    source_manifest = json.loads((package_dir / "package.json").read_text())
    public_manifest = build_public_manifest(source_manifest)
    version = str(public_manifest["version"])

    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)

    (destination / "package.json").write_text(json.dumps(public_manifest, indent=2) + "\n")
    (destination / ".gitignore").write_text("node_modules/\n*.tgz\n")
    shutil.copyfile(package_dir / "LICENSE", destination / "LICENSE")
    shutil.copyfile(template_dir / "README.md", destination / "README.md")
    shutil.copyfile(template_dir / ".gitattributes", destination / ".gitattributes")

    assets_src = template_dir / "assets"
    if assets_src.is_dir():
        shutil.copytree(assets_src, destination / "assets")

    src_out = destination / "src"
    src_out.mkdir(parents=True, exist_ok=True)
    for src_file in sorted((package_dir / "src").glob("*")):
        if src_file.is_file():
            shutil.copyfile(src_file, src_out / src_file.name)

    for rel in ("test/contract.tsx", "tsconfig.contract.json"):
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

    return {"destination": str(destination), "version": version, "include_pkg": include_pkg}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path, help="Directory for staged repo (or left side of comparison)")
    parser.add_argument("--seed-only", action="store_true", help="Omit pkg/ when creating the initial repository seed")
    parser.add_argument("--omit-workflows", action="store_true", help="Omit .github/workflows when syncing via token")
    parser.add_argument("--compare-sync", type=Path, help="Compare staged dir against target checkout (ignores build-report.json)")
    parser.add_argument("--compare-pkg", type=Path, help="Compare staged pkg/ against previous release checkout")
    args = parser.parse_args()
    if args.compare_sync is not None:
        print("true" if has_meaningful_changes(args.destination, args.compare_sync) else "false")
        return
    if args.compare_pkg is not None:
        print("true" if has_pkg_changes(args.destination, args.compare_pkg) else "false")
        return
    summary = stage_repository(
        args.destination,
        include_pkg=not args.seed_only,
        include_workflows=not args.omit_workflows,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
