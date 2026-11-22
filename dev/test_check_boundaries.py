"""Exercise actual graph selectors/refusals without invoking Cargo or accepting a mocked graph as product proof."""
import importlib.util
import hashlib
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("check_boundaries", Path(__file__).with_name("check-boundaries.py"))
BOUNDARIES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BOUNDARIES)


class DependencyBoundaryTests(unittest.TestCase):
    def test_resolved_queries_separate_defaults_explicit_features_and_read_only_adapters(self):
        reply = subprocess.CompletedProcess([], 0, stdout="culprit-engine v0.5.6\nculprit-read v0.5.6\n")
        with patch.object(BOUNDARIES.subprocess, "run", return_value=reply) as run:
            self.assertEqual(BOUNDARIES.dependencies("culprit-engine"), {"culprit-read"})
            default = run.call_args.args[0]
            self.assertNotIn("--no-default-features", default)
            BOUNDARIES.dependencies("culprit-engine", "warehouse-adx,evidence-azure")
            selected = run.call_args.args[0]
            self.assertIn("--no-default-features", selected)
            self.assertEqual(selected[selected.index("--features") + 1], "warehouse-adx,evidence-azure")
            BOUNDARIES.dependencies("culprit-store-adx", "")
            adapter = run.call_args.args[0]
            self.assertIn("--no-default-features", adapter)
            self.assertNotIn("--features", adapter)
            for arguments in [default, selected, adapter]:
                self.assertEqual(arguments[arguments.index("--edges") + 1], "normal,build")
                self.assertIn("--locked", arguments)
                self.assertTrue(run.call_args.kwargs["check"])

    def test_neutral_owner_forbids_source_destination_and_credential_discovery_dependencies(self):
        forbidden = BOUNDARIES.default_rules()["culprit-evidence"]
        self.assertTrue((BOUNDARIES.FLEET | BOUNDARIES.SOURCES | BOUNDARIES.AUTHENTICATION |
                         BOUNDARIES.CLOUD | {"culprit-auth", "culprit-engine", "tempfile", "rusqlite"}) <= forbidden)

    def test_desktop_can_read_archives_while_ingestion_and_mcp_dependencies_stay_separate(self):
        rules = BOUNDARIES.default_rules()
        self.assertIn("culprit-collector", rules["culprit-app"])
        self.assertFalse((BOUNDARIES.STORES | BOUNDARIES.RECOVERY) & rules["culprit-app"])
        self.assertTrue(BOUNDARIES.FLEET <= rules["culprit-mcp"])

    def test_read_graph_refuses_write_features_even_without_a_collector_package(self):
        for package in BOUNDARIES.STORES | BOUNDARIES.RECOVERY:
            for feature in ["write", "default", "read,write", "read, write\n"]:
                reply = subprocess.CompletedProcess([], 0, stdout=f"culprit-app v0.5.6|\n{package} v0.5.6|{feature}\n")
                with self.subTest(package=package, feature=feature), patch.object(BOUNDARIES.subprocess, "run", return_value=reply):
                    with self.assertRaisesRegex(RuntimeError, package):
                        BOUNDARIES.verify_read_features("culprit-app")

    def test_read_feature_query_uses_the_actual_normal_and_build_graph(self):
        reply = subprocess.CompletedProcess([], 0, stdout="culprit-app v0.5.6|\nculprit-store-adx v0.5.6|\n")
        with patch.object(BOUNDARIES.subprocess, "run", return_value=reply) as run, redirect_stdout(StringIO()):
            BOUNDARIES.verify_read_features("culprit-app")
            arguments = run.call_args.args[0]
            self.assertEqual(arguments[arguments.index("--edges") + 1], "normal,build")
            self.assertEqual(arguments[arguments.index("--format") + 1], "{p}|{f}")
            self.assertNotIn("--no-default-features", arguments)

    def test_existing_writer_selections_are_distinct_from_the_two_reader_compositions(self):
        selections = dict(BOUNDARIES.engine_compositions())
        self.assertNotIn("culprit-collector", selections["store-adx,recovery-azure"])
        self.assertNotIn("culprit-collector", selections["store-clickhouse,recovery-s3"])
        for features in ["warehouse-adx,evidence-azure", "warehouse-clickhouse,evidence-s3"]:
            self.assertIn("culprit-collector", selections[features])
            self.assertTrue(BOUNDARIES.SOURCES | BOUNDARIES.AUTHENTICATION <= selections[features])

    def test_collector_and_builtin_auth_leaks_in_reader_compositions_are_rejected(self):
        for features, forbidden in BOUNDARIES.engine_compositions()[2:]:
            for leak in ["culprit-collector", "culprit-postgres", "culprit-auth-aws", "culprit-auth-azure", "tempfile", "rusqlite"]:
                with self.subTest(features=features, leak=leak), patch.object(BOUNDARIES, "dependencies", return_value={leak}):
                    with self.assertRaisesRegex(RuntimeError, leak):
                        BOUNDARIES.verify("culprit-engine", features, forbidden)

    def test_unrelated_store_and_recovery_dependencies_are_rejected_in_each_reader_composition(self):
        for features, leaks in [("warehouse-adx,evidence-azure", ["culprit-store-clickhouse", "culprit-blob-s3"]),
                                ("warehouse-clickhouse,evidence-s3", ["culprit-store-adx", "culprit-blob-azure"])]:
            forbidden = dict(BOUNDARIES.engine_compositions())[features]
            for leak in leaks:
                with self.subTest(features=features, leak=leak), patch.object(BOUNDARIES, "dependencies", return_value={leak}):
                    with self.assertRaisesRegex(RuntimeError, leak):
                        BOUNDARIES.verify("culprit-engine", features, forbidden)

    def test_every_read_only_adapter_excludes_collector_and_other_adapters(self):
        for package, forbidden in BOUNDARIES.read_adapter_rules().items():
            self.assertNotIn(package, forbidden)
            self.assertTrue((BOUNDARIES.STORES | BOUNDARIES.RECOVERY) - {package} <= forbidden)
            with self.subTest(package=package), patch.object(BOUNDARIES, "dependencies", return_value={"culprit-collector"}):
                with self.assertRaisesRegex(RuntimeError, "culprit-collector"):
                    BOUNDARIES.verify(package, "", forbidden)

    def test_main_keeps_default_package_and_read_only_adapter_proofs_separate(self):
        calls = []
        def graph(package, features=None, target=None):
            calls.append((package, features))
            return {"culprit-collector"} if package in BOUNDARIES.STORES | BOUNDARIES.RECOVERY and features is None else set()
        with patch.object(BOUNDARIES, "dependencies", side_effect=graph), \
                patch.object(BOUNDARIES, "verify_read_features") as read_features, \
                patch.object(BOUNDARIES, "verify_english_wasm") as english, \
                patch.object(BOUNDARIES, "verify_wasm_build_selection") as build_selection, redirect_stdout(StringIO()):
            BOUNDARIES.main()
        read_features.assert_any_call("culprit-app")
        for package in BOUNDARIES.STORES | BOUNDARIES.RECOVERY:
            self.assertIn((package, None), calls)
            self.assertIn((package, ""), calls)
        self.assertIn(("culprit-evidence", None), calls)
        for features, _ in BOUNDARIES.engine_compositions():
            self.assertIn(("culprit-engine", features), calls)
        english.assert_any_call(None)
        english.assert_any_call("", BOUNDARIES.WASM_TARGET)
        build_selection.assert_called_once()

    def test_portable_target_queries_keep_normal_build_edges_and_one_package(self):
        reply = subprocess.CompletedProcess([], 0, stdout="culprit-wasm v0.5.6\nculprit-plans v0.5.6\n")
        with patch.object(BOUNDARIES.subprocess, "run", return_value=reply) as run:
            BOUNDARIES.dependencies("culprit-wasm", "", BOUNDARIES.WASM_TARGET)
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("--package") + 1], "culprit-wasm")
        self.assertEqual(command[command.index("--target") + 1], BOUNDARIES.WASM_TARGET)
        self.assertEqual(command[command.index("--edges") + 1], "normal,build")
        self.assertIn("--no-default-features", command)
        self.assertNotIn("--workspace", command)
        self.assertNotIn("--all-features", command)

    def test_portable_owners_refuse_native_drivers_and_unknown_workspace_dependencies(self):
        for package, allowed in BOUNDARIES.PORTABLE_OWNERS.items():
            for leak in ["tiberius", "tokio-postgres", "libsqlite3-sys", "culprit-read", "culprit-engine",
                         "culprit-collector", "culprit-auth", "culprit-cloud", "culprit-new-native-adapter"]:
                with self.subTest(package=package, leak=leak), patch.object(BOUNDARIES, "dependencies", return_value={leak}):
                    with self.assertRaisesRegex(RuntimeError, leak):
                        BOUNDARIES.verify(package, "", BOUNDARIES.PORTABLE_FORBIDDEN,
                                          target=BOUNDARIES.WASM_TARGET, allowed_workspace=allowed)

    def test_standalone_wasm_refuses_catalog_feature_unification(self):
        for package in ["culprit-core", "culprit-plans", "culprit-wasm"]:
            for enabled in ["default,locales", "default,locales\n", "default, locales \n"]:
                reply = [f"culprit-wasm v0.5.6|\n", f"{package} v0.5.6|{enabled}"]
                with self.subTest(package=package, enabled=enabled), patch.object(BOUNDARIES, "tree_output", return_value=reply):
                    with self.assertRaisesRegex(RuntimeError, "non-English"):
                        BOUNDARIES.verify_english_wasm("", BOUNDARIES.WASM_TARGET)

    def test_actual_builder_selects_only_the_final_wasm_crate_and_refuses_workspace_drift(self):
        BOUNDARIES.verify_wasm_build_selection()
        actual = (BOUNDARIES.ROOT / "dev/build-wasm.py").read_text()
        for previous, replacement in [('"culprit-wasm"', '"culprit-engine"'),
                                      ('"--lib"', '"--workspace"'),
                                      ('"--no-default-features"', '"--all-features"')]:
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "dev").mkdir()
                (root / "dev/build-wasm.py").write_text(actual.replace(previous, replacement, 1))
                with self.assertRaisesRegex(RuntimeError, "package/target/features"):
                    BOUNDARIES.verify_wasm_build_selection(root)


class WasmDistributionBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.package = self.root / "packages/culprit-wasm"
        self.output = self.package / "pkg"
        (self.output / "native").mkdir(parents=True)
        (self.output / "models").mkdir()
        (self.root / "app/src/gen").mkdir(parents=True)
        (self.root / "Cargo.lock").write_text('[[package]]\nname = "wasm-bindgen"\nversion = "0.2.129"\n')
        manifest = {"private": True, "type": "module", "files": ["pkg"],
                    "exports": {".": {"types": "./pkg/index.d.ts", "import": "./pkg/index.js"}}}
        (self.package / "package.json").write_text(json.dumps(manifest))
        files = {"index.js": b"export function parsePlan() {}\n", "index.d.ts": b"export {}\n",
                 "THIRD-PARTY-LICENSES.txt": b"Controlled shared third-party notices\n",
                 "native/culprit_wasm.js": b"export default function initialize() {}\n",
                 "native/culprit_wasm.d.ts": b"export {}\n", "native/culprit_wasm_bg.wasm": b"\x00asm\x01\x00\x00\x00"}
        for name, body in files.items():
            (self.output / name).write_bytes(body)
        notices = self.root / "target/licenses/THIRD-PARTY-LICENSES.txt"
        notices.parent.mkdir(parents=True)
        notices.write_bytes(files["THIRD-PARTY-LICENSES.txt"])
        self.report = {"target": BOUNDARIES.WASM_TARGET, "profile": "release", "native_or_database_work": False,
                       "stack_policy": {"linear_stack_bytes": BOUNDARIES.WASM_STACK_BYTES},
                       "cargo_lock_sha256": self.digest(self.root / "Cargo.lock"),
                       "tools": {"locked_bindgen_versions": ["0.2.129"], "bindgen_cli_version": "0.2.129"},
                       "artifacts": {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                                     for name, body in files.items() if name.endswith((".js", ".wasm"))}, "models": []}
        for name in sorted(BOUNDARIES.WASM_MODEL_ROOTS):
            source = self.root / "app/src/gen" / f"{name}.ts"
            source.write_text(f"export type {name} = unknown;\n")
            target = self.output / "models" / f"{name}.d.ts"
            target.write_bytes(source.read_bytes())
            self.report["models"].append({"name": name, "source_sha256": self.digest(source),
                                           "package_sha256": self.digest(target)})
        self.save_report()

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def save_report(self):
        (self.output / "build-report.json").write_text(json.dumps(self.report))

    def test_closed_distribution_hashes_and_generated_models_are_accepted(self):
        with redirect_stdout(StringIO()):
            BOUNDARIES.verify_wasm_artifacts(self.root)

    def test_missing_or_changed_shared_notices_cannot_be_distributed(self):
        notices = self.output / "THIRD-PARTY-LICENSES.txt"
        notices.unlink()
        with self.assertRaisesRegex(RuntimeError, "missing required"):
            BOUNDARIES.verify_wasm_artifacts(self.root)
        notices.write_text("different notices")
        with self.assertRaisesRegex(RuntimeError, "exact shared third-party notices"):
            BOUNDARIES.verify_wasm_artifacts(self.root)

    def test_unexpected_runtime_payload_and_external_package_exports_are_refused(self):
        (self.output / "credentials.json").write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "unexpected Wasm distribution artifact"):
            BOUNDARIES.verify_wasm_artifacts(self.root)
        (self.output / "credentials.json").unlink()
        manifest = self.package / "package.json"
        body = json.loads(manifest.read_text())
        body["exports"]["./native"] = "./pkg/native/culprit_wasm.js"
        manifest.write_text(json.dumps(body))
        with self.assertRaisesRegex(RuntimeError, "standalone typed ESM entry"):
            BOUNDARIES.verify_wasm_artifacts(self.root)

    def test_changed_executable_model_and_runtime_policy_are_independently_refused(self):
        module = self.output / "native/culprit_wasm_bg.wasm"
        original = module.read_bytes()
        module.write_bytes(original + b"changed")
        with self.assertRaisesRegex(RuntimeError, "digest mismatch"):
            BOUNDARIES.verify_wasm_artifacts(self.root)
        module.write_bytes(original)
        source = self.root / "app/src/gen/Plan.ts"
        source.write_text("changed model declaration")
        with self.assertRaisesRegex(RuntimeError, "declaration/source digest mismatch"):
            BOUNDARIES.verify_wasm_artifacts(self.root)
        self.report["stack_policy"]["linear_stack_bytes"] = 1
        self.save_report()
        with self.assertRaisesRegex(RuntimeError, "bounded stack policy"):
            BOUNDARIES.verify_wasm_artifacts(self.root)


if __name__ == "__main__":
    unittest.main()
