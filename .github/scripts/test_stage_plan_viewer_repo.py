import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from stage_plan_viewer_repo import has_meaningful_changes, has_pkg_changes, resolve_next_version, stage_repository


class StagePlanViewerRepoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "monorepo"
        self.out = Path(self.temp.name) / "staged"
        package = self.root / "packages/culprit-plan-viewer"
        template = self.root / ".github/plan-viewer-repo"
        (package / "src").mkdir(parents=True)
        (package / "test").mkdir(parents=True)
        (package / "pkg/models").mkdir(parents=True)
        (template / "workflows").mkdir(parents=True)
        (template / "assets").mkdir(parents=True)

        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.6.2"\n')
        (package / "package.json").write_text(
            json.dumps(
                {
                    "name": "@culpritsql/plan-viewer",
                    "private": True,
                    "version": "0.5.6",
                    "type": "module",
                    "sideEffects": ["*.css", "./pkg/styles.css"],
                    "files": ["pkg"],
                    "exports": {
                        ".": {"types": "./pkg/index.d.ts", "import": "./pkg/index.js"},
                        "./styles.css": "./pkg/styles.css",
                    },
                    "dependencies": {"lucide-react": "1.68.0"},
                    "peerDependencies": {"react": "^18.0.0 || ^19.0.0"},
                    "license": "MIT",
                }
            )
        )
        (package / "LICENSE").write_text("MIT License\n")
        (package / "src/index.ts").write_text("export {};\n")
        (package / "src/styles.css").write_text(".culprit-pv {}\n")
        (package / "test/contract.tsx").write_text("export {};\n")
        (package / "tsconfig.contract.json").write_text("{}\n")

        (template / "README.md").write_text("# @culpritsql/plan-viewer\n")
        (template / ".gitattributes").write_text("pkg/** linguist-generated=true\n")
        (template / "smoke.mjs").write_text("#!/usr/bin/env node\n")
        (template / "assets/hero-dark.png").write_bytes(b"\x89PNG\r\n\x1a\n")
        (template / "workflows/ci.yml").write_text("name: CI\n")
        (template / "workflows/publish.yml").write_text("name: Publish\n")

        files = {
            "index.js": b"export {};\n",
            "index.d.ts": b"export {};\n",
            "styles.css": b".culprit-pv {}\n",
            "PlanViewer.js": b"export function PlanViewer() { return null; }\n",
            "PlanViewer.d.ts": b"export declare function PlanViewer(): null;\n",
            "PlanDiffViewer.js": b"export function PlanDiffViewer() { return null; }\n",
            "PlanDiffViewer.d.ts": b"export declare function PlanDiffViewer(): null;\n",
            "DeadlockViewer.js": b"export function DeadlockViewer() { return null; }\n",
            "DeadlockViewer.d.ts": b"export declare function DeadlockViewer(): null;\n",
            "THIRD-PARTY-LICENSES.txt": b"Third-party notices\n",
            "models/Plan.d.ts": b"export type Plan = unknown;\n",
        }
        for rel, content in files.items():
            (package / "pkg" / rel).write_bytes(content)

        report = {
            "target": "es2022-react-esm",
            "native_or_database_work": False,
            "tools": {
                "typescript_version": "5.9.3",
                "lucide_react_version": "1.68.0",
                "host_note": "/Users/developer/ignored",
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
                if rel.endswith((".js", ".css"))
            },
        }
        (package / "pkg/build-report.json").write_text(json.dumps(report))

    def test_full_stage_uses_independent_pkg_version_and_copies_assets(self) -> None:
        stage_repository(self.out, root=self.root, include_pkg=True, include_workflows=False)
        manifest = json.loads((self.out / "package.json").read_text())
        self.assertNotIn("private", manifest)
        self.assertEqual(manifest["name"], "@culpritsql/plan-viewer")
        self.assertEqual(manifest["version"], "0.5.6")
        self.assertTrue((self.out / "assets/hero-dark.png").is_file())
        self.assertFalse((self.out / ".github").exists())

        report_text = (self.out / "pkg/build-report.json").read_text()
        self.assertNotIn("/Users/", report_text)

    def test_change_detection_ignores_build_report_metadata_churn(self) -> None:
        stage_repository(self.out, root=self.root, include_pkg=True, include_workflows=False)
        target = Path(self.temp.name) / "target"
        shutil.copytree(self.out, target)
        (target / "pkg/build-report.json").write_text('{"tools": {"typescript_version": "6.0.0"}}\n')
        self.assertFalse(has_meaningful_changes(self.out, target))
        self.assertFalse(has_pkg_changes(self.out, target))

        (target / "pkg/styles.css").write_text(".culprit-pv { color: red; }\n")
        self.assertTrue(has_meaningful_changes(self.out, target))
        self.assertTrue(has_pkg_changes(self.out, target))

    def test_invalid_semver_or_tampered_artifact_is_refused(self) -> None:
        pkg_json = self.root / "packages/culprit-plan-viewer/package.json"
        original = json.loads(pkg_json.read_text())
        pkg_json.write_text(json.dumps({**original, "version": "bad"}))
        with self.assertRaisesRegex(RuntimeError, "invalid semver"):
            stage_repository(self.out, root=self.root)

        pkg_json.write_text(json.dumps(original))
        css = self.root / "packages/culprit-plan-viewer/pkg/styles.css"
        css.write_bytes(css.read_bytes() + b"\n")
        with self.assertRaisesRegex(RuntimeError, "artifact digest mismatch"):
            stage_repository(self.out, root=self.root)

    def test_monotonic_version_resolution_across_patches_and_minor_trains(self) -> None:
        self.assertEqual(resolve_next_version("0.5.6", [], pkg_changed=True), ("0.5.6", True))
        self.assertEqual(resolve_next_version("0.5.6", ["v0.5.6", "v0.5.8"], pkg_changed=False), ("0.5.8", False))
        self.assertEqual(resolve_next_version("0.5.6", ["v0.5.6", "v0.5.8"], pkg_changed=True), ("0.5.9", True))
        self.assertEqual(resolve_next_version("0.5.7", ["v0.5.6", "v0.5.8"], pkg_changed=True), ("0.5.9", True))
        self.assertEqual(resolve_next_version("0.6.0", ["v0.5.6", "v0.5.8"], pkg_changed=True), ("0.6.0", True))


if __name__ == "__main__":
    unittest.main()
