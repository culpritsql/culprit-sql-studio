"""Exercise actual graph selectors/refusals without invoking Cargo or accepting a mocked graph as product proof."""
import importlib.util
import subprocess
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
        def graph(package, features=None):
            calls.append((package, features))
            return {"culprit-collector"} if package in BOUNDARIES.STORES | BOUNDARIES.RECOVERY and features is None else set()
        with patch.object(BOUNDARIES, "dependencies", side_effect=graph), redirect_stdout(StringIO()):
            BOUNDARIES.main()
        for package in BOUNDARIES.STORES | BOUNDARIES.RECOVERY:
            self.assertIn((package, None), calls)
            self.assertIn((package, ""), calls)
        self.assertIn(("culprit-evidence", None), calls)
        for features, _ in BOUNDARIES.engine_compositions():
            self.assertIn(("culprit-engine", features), calls)


if __name__ == "__main__":
    unittest.main()
