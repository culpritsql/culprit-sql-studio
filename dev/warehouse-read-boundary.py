#!/usr/bin/env python3
"""Check the workspace-only normal dependency boundary of warehouse read features.

Cargo compilation and `cargo tree --edges normal` remain the authoritative resolved dependency checks.
This small manifest check catches accidental feature/dependency regressions without building or resolving crates.
It deliberately ignores dev dependencies and never reads credentials or opens a source/warehouse.
"""
from __future__ import annotations
import argparse
import copy
from pathlib import Path
import tomllib
import unittest

FORBIDDEN = frozenset({"culprit-collector", "culprit-mssql", "culprit-postgres", "culprit-yugabyte", "culprit-mysql", "culprit-sqlite", "culprit-auth-aws", "culprit-auth-azure", "rusqlite", "tempfile"})
ADAPTERS = ("culprit-store-adx", "culprit-store-clickhouse", "culprit-blob-azure", "culprit-blob-s3")
READ_FEATURES = (
    ("evidence",),
    ("warehouse-adx", "evidence-azure"),
    ("warehouse-adx", "evidence-s3"),
    ("warehouse-clickhouse", "evidence-azure"),
    ("warehouse-clickhouse", "evidence-s3"),
    ("warehouse-adx", "warehouse-clickhouse", "evidence-azure", "evidence-s3"),
)
class Workspace:
    def __init__(self, root: Path, overlay: Path | None = None):
        self.root = root
        self.overlay = overlay
        self.workspace = self.load(Path("Cargo.toml"))["workspace"]["dependencies"]
        self.packages = {}
        paths = set(root.glob("crates/**/Cargo.toml"))
        if overlay:
            paths.update(root / p.relative_to(overlay) for p in overlay.glob("crates/**/Cargo.toml"))
        for path in sorted(paths):
            rel = path.relative_to(root)
            manifest = self.load(rel)
            self.packages[manifest["package"]["name"]] = manifest
    def load(self, path):
        selected = self.overlay / path if self.overlay and (self.overlay / path).exists() else self.root / path
        return tomllib.loads(selected.read_text())
    def dependencies(self, package):
        manifest = self.packages[package]
        values = dict(manifest.get("dependencies", {}))
        for target in manifest.get("target", {}).values():
            values.update(target.get("dependencies", {}))
        merged = {}
        for alias, spec in values.items():
            spec = {"version": spec} if isinstance(spec, str) else dict(spec)
            if spec.get("workspace"):
                parent = self.workspace[alias]
                parent = {"version": parent} if isinstance(parent, str) else dict(parent)
                features = parent.get("features", []) + spec.get("features", [])
                parent.update(spec)
                parent["features"] = features
                spec = parent
            merged[alias] = spec
        return merged
    def closure(self, selected):
        states = {"culprit-engine": set(selected)}
        changed = True
        while changed:
            changed = False
            for package, enabled in list(states.items()):
                if package not in self.packages:
                    continue
                manifest = self.packages[package]
                dependencies = self.dependencies(package)
                active = {alias for alias, spec in dependencies.items() if not spec.get("optional", False)}
                requested = {}
                frontier = list(enabled)
                visited = set()
                while frontier:
                    feature = frontier.pop()
                    if feature in visited:
                        continue
                    visited.add(feature)
                    for value in manifest.get("features", {}).get(feature, []):
                        if value.startswith("dep:"):
                            active.add(value[4:])
                        elif "/" in value:
                            alias, child = value.split("/", 1)
                            weak = alias.endswith("?")
                            alias = alias.removesuffix("?")
                            if not weak:
                                active.add(alias)
                            requested.setdefault(alias, set()).add(child)
                        else:
                            if value in dependencies:
                                active.add(value)
                            frontier.append(value)
                    if feature in dependencies and feature not in manifest.get("features", {}):
                        active.add(feature)
                for alias in active:
                    spec = dependencies[alias]
                    name = spec.get("package", alias)
                    wanted = set(spec.get("features", [])) | requested.get(alias, set())
                    if spec.get("default-features", True):
                        wanted.add("default")
                    before = set(states.get(name, set()))
                    if name not in states or not wanted <= before:
                        states.setdefault(name, set()).update(wanted)
                        changed = True
        return states
    def assert_reader(self, features):
        states = self.closure(features)
        unexpected = set(states) & FORBIDDEN
        writer_features = {name for name in ADAPTERS if "write" in states.get(name, set()) or "default" in states.get(name, set())}
        if unexpected or writer_features:
            raise AssertionError(f"{features}: forbidden packages {sorted(unexpected)}, writer features {sorted(writer_features)}")
        assert "culprit-evidence" in states
        assert "culprit-read" in states
        return states

class BoundaryControls(unittest.TestCase):
    workspace: Workspace
    def test_all_actual_reader_compositions_exclude_ingestion_sources_and_builtin_auth(self):
        for selected in READ_FEATURES:
            with self.subTest(features=selected):
                self.workspace.assert_reader(selected)
    def test_real_writer_compositions_keep_the_existing_collector_and_adapters(self):
        for selected, adapter, recovery in [(("store-adx", "recovery-azure"), "culprit-store-adx", "culprit-blob-azure"), (("store-clickhouse", "recovery-s3"), "culprit-store-clickhouse", "culprit-blob-s3")]:
            states = self.workspace.closure(selected)
            self.assertIn("culprit-collector", states)
            self.assertIn("write", states[adapter])
            self.assertIn("write", states[recovery])
    def test_an_accidental_normal_collector_dependency_is_detected(self):
        altered = copy.deepcopy(self.workspace)
        altered.packages["culprit-evidence"]["dependencies"]["culprit-collector"] = {"workspace": True}
        with self.assertRaises(AssertionError):
            altered.assert_reader(("evidence",))
    def test_an_accidental_read_adapter_default_is_detected(self):
        altered = copy.deepcopy(self.workspace)
        altered.packages["culprit-engine"]["dependencies"]["culprit-store-adx"]["default-features"] = True
        with self.assertRaises(AssertionError):
            altered.assert_reader(("warehouse-adx", "evidence-azure"))
    def test_dev_collector_usage_cannot_widen_the_normal_graph(self):
        altered = copy.deepcopy(self.workspace)
        altered.packages["culprit-evidence"].setdefault("dev-dependencies", {})["culprit-collector"] = {"workspace": True}
        self.assertEqual(altered.closure(("evidence",)), self.workspace.closure(("evidence",)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--overlay", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    workspace = Workspace(args.root.resolve(), args.overlay.resolve() if args.overlay else None)
    if args.self_test:
        BoundaryControls.workspace = workspace
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(BoundaryControls))
        raise SystemExit(not result.wasSuccessful())
    for selected in READ_FEATURES:
        states = workspace.assert_reader(selected)
        print(f"{','.join(selected)}: {len(set(states) & set(workspace.packages))} workspace crates; no Collector/source-provider/builtin-auth/write feature")
