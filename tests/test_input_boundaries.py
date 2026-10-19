"""Regression coverage for untrusted local config and snapshot JSON inputs."""
import contextlib
import hashlib
import io
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import main
from foia_bias.corpus import strict_json, verify_snapshot
from foia_bias.utils.corpus_config import load_config, validate_corpus_config

ROOT = Path(__file__).resolve().parents[1]


def valid_config():
    return {
        "corpus": {"mode": "snapshot", "manifest_path": "manifest.json",
                   "fixture_mode": True, "batch_size": 2},
        "cohort": {"start_date": "1993-01-20", "end_date": "2025-01-20",
                   "include_incomplete_administrations": False},
    }


class InputBoundaryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / "config.json"

    def write(self, text):
        self.path.write_bytes(text.encode("utf-8"))
        return self.path

    def test_decimal_exponent_overflow_is_not_valid_json_data(self):
        for token in ("1e309", "-1e309", "9.99e9999", "-9.99e9999"):
            with self.subTest(token=token):
                with self.assertRaisesRegex(ValueError, "non-finite"):
                    strict_json('{"nested": [0, {"number":' + token + '}]}')

    def test_finite_numeric_values_are_preserved(self):
        for token in ("1.5", "1e308", "-1e308", "0.0", "-0.0", "1e-300"):
            self.assertTrue(math.isfinite(strict_json(token)))
        self.assertEqual(strict_json('12345678901234567890'), 12345678901234567890)

    def test_explicit_nonfinite_constants_still_fail(self):
        for token in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaises(ValueError):
                strict_json(token)

    def test_nested_duplicate_keys_still_fail(self):
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            strict_json('{"outer": {"fixture_mode":false,"fixture_mode":true}}')

    def test_excessive_nesting_is_a_validation_error(self):
        with self.assertRaisesRegex(ValueError, "nesting"):
            strict_json("[" * 5000 + "0" + "]" * 5000)

    def test_json_nesting_limit_and_escaped_string_delimiters(self):
        from foia_bias.corpus import MAX_JSON_DEPTH
        value = "[" * MAX_JSON_DEPTH + "0" + "]" * MAX_JSON_DEPTH
        self.assertIsInstance(strict_json(value), list)
        with self.assertRaisesRegex(ValueError, "nesting"):
            strict_json("[" + value + "]")
        data = {"text": ('[{' * 1000) + '\\"' + ('}]' * 1000)}
        self.assertEqual(strict_json(json.dumps(data)), data)
        with self.assertRaises(ValueError):
            strict_json('"unterminated')

    def test_config_limit_uses_bytes_including_comments_or_whitespace(self):
        text = json.dumps(valid_config())
        with self.assertRaisesRegex(ValueError, "configuration exceeds"):
            load_config(self.write(text + " " * (64 * 1024)))

    def test_exact_configuration_byte_limit_is_accepted(self):
        text = json.dumps(valid_config())
        self.write(text + " " * (64 * 1024 - len(text.encode())))
        self.assertEqual(load_config(self.path), valid_config())

    def test_configuration_reader_is_bounded(self):
        from foia_bias.utils import corpus_config
        raw = io.BytesIO(json.dumps(valid_config()).encode())
        reads = []

        class Reader(io.BytesIO):
            def read(self, size=-1):
                reads.append(size)
                self.assert_bounded(size)
                return super().read(size)

            def assert_bounded(self, size):
                if not 0 < size <= 64 * 1024 + 1:
                    raise AssertionError("configuration performed an unbounded read")

        self.write("{}")
        with patch.object(Path, "open", return_value=Reader(raw.getvalue())):
            self.assertEqual(corpus_config.load_config(self.path), valid_config())
        self.assertEqual(reads, [64 * 1024 + 1])

    def test_only_canonical_historical_date_strings_are_allowed(self):
        for key, value in (("start_date", "19930120"), ("end_date", "20250120"),
                           ("start_date", "1993-W03-3"), ("end_date", 20250120)):
            with self.subTest(key=key, value=value):
                config = valid_config()
                config["cohort"][key] = value
                with self.assertRaisesRegex(ValueError, "ISO dates"):
                    validate_corpus_config(config)

    def test_unknown_formats_are_rejected_before_reading(self):
        path = self.root / "config.txt"
        path.write_text("canary configuration text", encoding="utf-8")
        with patch.object(Path, "open", side_effect=AssertionError("should not read")):
            with self.assertRaisesRegex(ValueError, "Unsupported config format"):
                load_config(path)

    def test_invalid_utf8_config_is_a_validation_error(self):
        self.path.write_bytes(b"\xff")
        with self.assertRaises(ValueError):
            load_config(self.path)

    def test_verifier_rejects_bad_config_before_opening_snapshot(self):
        config = valid_config()
        config["corpus"]["batch_size"] = 0
        with patch.object(Path, "resolve", side_effect=AssertionError("should not resolve")):
            with self.assertRaisesRegex(ValueError, "batch_size"):
                verify_snapshot(config, self.path)

    def test_overflow_record_is_rejected_even_with_matching_hash_and_census(self):
        config = valid_config()
        self.write(json.dumps(config))
        raw = b'{"synthetic":true,"value":1e9999}\n'
        (self.root / "records.jsonl").write_bytes(raw)
        manifest = {"schema_version": 1, "snapshot_id": "synthetic-numeric-fixture",
                    "producer": "synthetic-fixture", "fixture_mode": True,
                    "records": {"path": "records.jsonl", "count": 1,
                                "sha256": hashlib.sha256(raw).hexdigest()}}
        (self.root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "non-finite"):
            verify_snapshot(config, self.path)
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            self.assertEqual(main.main(["run", "--config", str(self.path)]), 2)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("Invalid corpus snapshot", errors.getvalue())

    def test_real_cli_rejects_deep_config_without_traceback_or_success(self):
        self.write("[" * 5000 + "0" + "]" * 5000)
        result = subprocess.run(
            [sys.executable, "-S", "main.py", "inspect", "--config", str(self.path)],
            cwd=ROOT, env={"PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True, text=True, timeout=10, check=False,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("Traceback", result.stderr)
        self.assertIn("Invalid corpus configuration", result.stderr)

    def test_real_cli_rejects_oversized_config_without_success(self):
        self.write(json.dumps(valid_config()) + " " * (64 * 1024))
        result = subprocess.run(
            [sys.executable, "-S", "main.py", "inspect", "--config", str(self.path)],
            cwd=ROOT, env={"PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True, text=True, timeout=10, check=False,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn("configuration exceeds", result.stderr)


if __name__ == "__main__":
    unittest.main()
