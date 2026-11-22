import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from stage_wasm_repo import has_meaningful_changes, has_pkg_changes, stage_repository


class StageWasmRepoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "monorepo"
        self.out = Path(self.temp.name) / "staged"
        package = self.root / "packages/culprit-wasm"
        template = self.root / ".github/wasm-repo"
        (package / "src").mkdir(parents=True)
        (package / "test").mkdir(parents=True)
        (package / "pkg/native").mkdir(parents=True)
        (package / "pkg/models").mkdir(parents=True)
        (template / "workflows").mkdir(parents=True)

        # Desktop Cargo.toml can advance independently without forcing npm bumps.
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.6.2"\n')
        (package / "package.json").write_text(
            json.dumps(
                {
                    "name": "@culpritsql/plans",
                    "private": True,
                    "version": "0.5.6",
                    "type": "module",
                    "sideEffects": False,
                    "files": ["pkg"],
                    "exports": {".": {"types": "./pkg/index.d.ts", "import": "./pkg/index.js"}},
                    "license": "MIT",
                }
            )
        )
        (package / "LICENSE").write_text("MIT License\n")
        (package / "src/index.ts").write_text("export {};\n")
        (package / "test/contract.ts").write_text("export {};\n")
        (package / "tsconfig.json").write_text("{}\n")
        (package / "tsconfig.contract.json").write_text("{}\n")

        (template / "README.md").write_text("# @culpritsql/plans\n")
        (template / ".gitattributes").write_text("pkg/** linguist-generated=true\n")
        (template / "smoke.mjs").write_text("#!/usr/bin/env node\n")
        (template / "workflows/ci.yml").write_text("name: CI\n")
        (template / "workflows/publish.yml").write_text("name: Publish\n")

        files = {
            "index.js": b"export function init() {}\n",
            "index.d.ts": b"export declare function init(): Promise<void>;\n",
            "THIRD-PARTY-LICENSES.txt": b"Third-party notices\n",
            "native/culprit_wasm.js": b"export default function init() {}\n",
            "native/culprit_wasm.d.ts": b"export default function init(): Promise<void>;\n",
            "native/culprit_wasm_bg.wasm": b"\x00asm\x01\x00\x00\x00",
            "models/Plan.d.ts": b"export type Plan = unknown;\n",
        }
        for rel, content in files.items():
            (package / "pkg" / rel).write_bytes(content)

        report = {
            "target": "wasm32-unknown-unknown",
            "profile": "release",
            "native_or_database_work": False,
            "stack_policy": {"linear_stack_bytes": 33554432},
            "tools": {
                "missing": [],
                "installed_targets": ["aarch64-apple-darwin", "wasm32-unknown-unknown"],
                "locked_bindgen_versions": ["0.2.129"],
                "bindgen_cli_version": "0.2.129",
                "bindgen_executable": "/Users/developer/.cargo/bin/wasm-bindgen",
            },
            "models": [
                {
                    "name": "Plan",
                    "source_sha256": hashlib.sha256(files["models/Plan.d.ts"]).hexdigest(),
                    "package_sha256": hashlib.sha256(files["models/Plan.d.ts"]).hexdigest(),
                }
            ],
            "artifacts": {
                rel: {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
                for rel, content in files.items()
                if rel.endswith((".js", ".wasm"))
            },
        }
        (package / "pkg/build-report.json").write_text(json.dumps(report))

    def test_full_stage_uses_independent_pkg_version_and_scrubs_host_paths(self) -> None:
        stage_repository(self.out, root=self.root, include_pkg=True, include_workflows=False)
        manifest = json.loads((self.out / "package.json").read_text())
        self.assertNotIn("private", manifest)
        self.assertEqual(manifest["name"], "@culpritsql/plans")
        self.assertEqual(manifest["version"], "0.5.6")
        self.assertEqual(manifest["publishConfig"], {"access": "public", "provenance": True})
        self.assertFalse((self.out / ".github").exists())

        report_text = (self.out / "pkg/build-report.json").read_text()
        self.assertNotIn("/Users/", report_text)
        self.assertNotIn("bindgen_executable", report_text)

    def test_change_detection_ignores_build_report_metadata_churn(self) -> None:
        stage_repository(self.out, root=self.root, include_pkg=True, include_workflows=False)
        target = Path(self.temp.name) / "target"
        shutil.copytree(self.out, target)
        (target / ".github/workflows").mkdir(parents=True)
        (target / ".github/workflows/ci.yml").write_text("name: CI\n")
        (target / "pkg/build-report.json").write_text('{"cargo_lock_sha256": "different"}\n')
        self.assertFalse(has_meaningful_changes(self.out, target))
        self.assertFalse(has_pkg_changes(self.out, target))

        (target / "pkg/models/Plan.d.ts").write_text("export type Plan = { id: number };\n")
        self.assertTrue(has_meaningful_changes(self.out, target))
        self.assertTrue(has_pkg_changes(self.out, target))

    def test_invalid_semver_or_tampered_binary_is_refused(self) -> None:
        pkg_json = self.root / "packages/culprit-wasm/package.json"
        original = json.loads(pkg_json.read_text())
        pkg_json.write_text(json.dumps({**original, "version": "not-semver"}))
        with self.assertRaisesRegex(RuntimeError, "invalid semver"):
            stage_repository(self.out, root=self.root)

        pkg_json.write_text(json.dumps(original))
        wasm = self.root / "packages/culprit-wasm/pkg/native/culprit_wasm_bg.wasm"
        wasm.write_bytes(wasm.read_bytes() + b"\x00")
        with self.assertRaisesRegex(RuntimeError, "executable digest mismatch"):
            stage_repository(self.out, root=self.root)


if __name__ == "__main__":
    unittest.main()
