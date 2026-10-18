"""Default CLI must be safe without source or model credentials."""
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import main

ROOT = Path(__file__).resolve().parents[1]


def fixture_config():
    return {
        "corpus": {
            "mode": "snapshot",
            "manifest_path": "fixtures/synthetic-manifest.json",
            "fixture_mode": True,
            "batch_size": 100,
        },
        "cohort": {
            "start_date": "1993-01-20",
            "end_date": "2025-01-20",
            "include_incomplete_administrations": False,
        },
    }


class OfflineCliTests(unittest.TestCase):
    def test_help_does_not_require_pipeline_or_credentials(self):
        result = subprocess.run(
            [sys.executable, "main.py", "--help"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("read-only Aggregator corpus", result.stdout)

    def test_inspect_validates_fixture_without_loading_collectors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(fixture_config()), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main.main(["inspect", "--config", str(path)]), 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result["mode"], "snapshot")
            self.assertTrue(result["fixture_mode"])
            self.assertIn("snapshot-not-verified", result["status"])
            self.assertNotIn("foia_bias.pipeline", sys.modules)

    def test_run_fails_closed_before_collector_or_output_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(fixture_config()), encoding="utf-8")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                self.assertEqual(main.main(["run", "--config", str(path)]), 2)
            self.assertIn("No snapshot was read", errors.getvalue())
            self.assertNotIn("foia_bias.pipeline", sys.modules)
            self.assertFalse((Path(directory) / "data").exists())

    def test_legacy_source_config_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.json"
            path.write_text(json.dumps({"sources": {"muckrock": {"enabled": True}}}),
                            encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main.main(["run", "--config", str(path)]), 2)


if __name__ == "__main__":
    unittest.main()
