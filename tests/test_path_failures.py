"""Filesystem failures must not turn invalid snapshot input into a CLI crash."""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from foia_bias.corpus import local_path


class SnapshotPathFailureTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = {
            "corpus": {"mode": "snapshot", "manifest_path": "manifest.json",
                       "fixture_mode": True, "batch_size": 2},
            "cohort": {"start_date": "1993-01-20", "end_date": "2025-01-20",
                       "include_incomplete_administrations": False},
        }
        records = b'{"synthetic":true}\n'
        (self.root / "records.jsonl").write_bytes(records)
        self.manifest = {
            "schema_version": 1, "snapshot_id": "path-failure-fixture",
            "producer": "synthetic-fixture", "fixture_mode": True,
            "records": {"path": "records.jsonl", "count": 1,
                        "sha256": hashlib.sha256(records).hexdigest()},
        }

    def run_cli(self):
        (self.root / "config.json").write_text(json.dumps(self.config))
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))
        return subprocess.run(
            [sys.executable, "-S", "main.py", "run", "--config",
             str(self.root / "config.json")],
            cwd=Path(__file__).resolve().parents[1],
            env={"PATH": os.defpath}, capture_output=True, text=True, timeout=10,
        )

    def test_library_normalizes_symlink_loop_as_invalid_path(self):
        (self.root / "loop").symlink_to("loop")
        with self.assertRaisesRegex(ValueError, "snapshot path cannot be resolved"):
            local_path(self.root, "loop")

    def test_cli_rejects_manifest_symlink_loop_without_traceback(self):
        (self.root / "loop").symlink_to("loop")
        self.config["corpus"]["manifest_path"] = "loop"
        result = self.run_cli()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn("Invalid corpus snapshot", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_rejects_record_symlink_cycle_without_traceback(self):
        (self.root / "first").symlink_to("second")
        (self.root / "second").symlink_to("first")
        self.manifest["records"]["path"] = "first"
        result = self.run_cli()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn("Invalid corpus snapshot", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_symlink_to_regular_file_inside_root_still_verifies(self):
        (self.root / "alias.jsonl").symlink_to("records.jsonl")
        self.manifest["records"]["path"] = "alias.jsonl"
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["record_count"], 1)
        self.assertEqual(result.stderr, "")
