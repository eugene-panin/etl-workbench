from __future__ import annotations

import importlib.util
from importlib.machinery import SourceFileLoader
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
LOADER = SourceFileLoader("etl_workbench_launcher", str(ROOT / "bin" / "etl-workbench"))
SPEC = importlib.util.spec_from_loader("etl_workbench_launcher", LOADER)
assert SPEC is not None and SPEC.loader is not None
launcher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = launcher
SPEC.loader.exec_module(launcher)


class BundleManifestTests(unittest.TestCase):
    def test_multiple_sources_get_stable_independent_git_connections(self) -> None:
        payload = {
            "version": 1,
            "sources": [
                {
                    "name": "learning-platform",
                    "repository": "git@github.com:example/learning-platform.git",
                    "ref": "main",
                    "subdir": "airflow/dags",
                },
                {
                    "name": "beavers-data",
                    "repository": "git@github.com:example/beavers-data-pipelines.git",
                    "ref": "release-2026-07",
                    "subdir": "dags",
                },
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "bundles.json"
            manifest.write_text(json.dumps(payload))
            bundles = launcher.bundle_manifest(manifest)

        self.assertEqual([bundle.git_connection for bundle in bundles], ["workbench_git_learning_platform", "workbench_git_beavers_data"])
        configuration = json.loads(launcher.bundle_config(bundles))
        self.assertEqual([item["name"] for item in configuration], ["learning-platform", "beavers-data"])
        self.assertEqual(configuration[0]["kwargs"], {
            "tracking_ref": "main",
            "subdir": "airflow/dags",
            "git_conn_id": "workbench_git_learning_platform",
        })
        self.assertEqual(
            launcher.runtime_connection_line(bundles[1], None),
            'AIRFLOW_CONN_WORKBENCH_GIT_BEAVERS_DATA={"conn_type":"git","host":"git@github.com:example/beavers-data-pipelines.git"}',
        )

    def test_manifest_rejects_repeated_source_names(self) -> None:
        payload = {
            "version": 1,
            "sources": [
                {"name": "product", "repository": "https://example.test/a.git"},
                {"name": "product", "repository": "https://example.test/b.git"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "bundles.json"
            manifest.write_text(json.dumps(payload))
            with self.assertRaisesRegex(SystemExit, "repeated"):
                launcher.bundle_manifest(manifest)

    def test_manifest_rejects_connection_id_collisions(self) -> None:
        payload = {
            "version": 1,
            "sources": [
                {"name": "learning-platform", "repository": "https://example.test/a.git"},
                {"name": "learning_platform", "repository": "https://example.test/b.git"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "bundles.json"
            manifest.write_text(json.dumps(payload))
            with self.assertRaisesRegex(SystemExit, "distinct Git connection IDs"):
                launcher.bundle_manifest(manifest)


class LangfuseEnvironmentTests(unittest.TestCase):
    def test_generated_environment_is_private_complete_and_stable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "langfuse.env"
            first = launcher.ensure_langfuse_environment(path)
            first_content = first.read_text()
            second = launcher.ensure_langfuse_environment(path)

            values = dict(
                line.split("=", 1)
                for line in first_content.splitlines()
                if line.strip()
            )
            self.assertEqual(second.read_text(), first_content)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            self.assertEqual(values["DATABASE_URL"].rsplit("/", 1)[-1], "langfuse")
            self.assertEqual(len(values["ENCRYPTION_KEY"]), 64)
            self.assertTrue(
                values["LANGFUSE_INIT_PROJECT_PUBLIC_KEY"].startswith("pk-lf-")
            )
            self.assertTrue(
                values["LANGFUSE_INIT_PROJECT_SECRET_KEY"].startswith("sk-lf-")
            )
            self.assertNotIn("replace", first_content)

    def test_existing_environment_keeps_secrets_and_gains_required_settings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "langfuse.env"
            path.write_text(
                "ENCRYPTION_KEY=existing-secret\n"
                "LANGFUSE_INIT_PROJECT_SECRET_KEY=sk-lf-existing\n"
            )

            launcher.ensure_langfuse_environment(path)
            values = dict(
                line.split("=", 1)
                for line in path.read_text().splitlines()
                if line.strip()
            )

            self.assertEqual(values["ENCRYPTION_KEY"], "existing-secret")
            self.assertEqual(
                values["LANGFUSE_INIT_PROJECT_SECRET_KEY"], "sk-lf-existing"
            )
            self.assertEqual(
                values["LANGFUSE_MIGRATION_V4_NATIVE_OTEL_BEHAVIOUR"], "direct"
            )


if __name__ == "__main__":
    unittest.main()
