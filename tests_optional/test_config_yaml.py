"""Optional PyYAML contract. Install requirements-yaml.txt before this suite.

Kept separate from the dependency-free JSON suite; missing PyYAML is an error,
not a silently skipped passing test. No input is sent to a network service.
"""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

import main
from foia_bias.utils.corpus_config import load_config


VALID = '''corpus:
  mode: snapshot
  manifest_path: fixtures/manifest.json
  fixture_mode: true
  batch_size: 100
cohort:
  start_date: "1993-01-20"
  end_date: "2025-01-20"
  include_incomplete_administrations: false
'''


class YamlConfigurationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "config.yaml"

    def load(self, text):
        self.path.write_text(text, encoding="utf-8")
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            return load_config(self.path)

    def test_optional_runtime_matches_pin(self):
        self.assertEqual(yaml.__version__, "6.0.3")

    def test_valid_yaml_retains_identical_config_values(self):
        actual = self.load(VALID)
        self.assertEqual(actual["corpus"]["batch_size"], 100)
        self.assertTrue(actual["corpus"]["fixture_mode"])
        self.assertFalse(actual["cohort"]["include_incomplete_administrations"])
        self.assertEqual(actual["cohort"]["start_date"], "1993-01-20")

    def test_json_yaml_subset_matches(self):
        config = self.load(VALID)
        self.assertEqual(self.load(json.dumps(config)), config)

    def test_duplicate_top_level_key_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate YAML key"):
            self.load("corpus: {mode: crawler}\n" + VALID)

    def test_duplicate_nested_keys_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate YAML key"):
            self.load(VALID.replace("  fixture_mode: true", "  fixture_mode: false\n  fixture_mode: true"))

    def test_duplicate_escaped_key_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate YAML key"):
            self.load(VALID.replace("  mode: snapshot", '  "m\\u006fde": crawler\n  mode: snapshot'))

    def test_anchor_and_alias_are_rejected(self):
        for text in (VALID.replace("corpus:", "corpus: &corpus"),
                     VALID + "extra: *missing\n",
                     "corpus: &loop {nested: *loop}\n"):
            with self.subTest(text=text):
                with self.assertRaisesRegex(ValueError, "anchors and aliases"):
                    self.load(text)

    def test_merge_keys_are_rejected_without_silent_override(self):
        with self.assertRaisesRegex(ValueError, "merge keys unsupported"):
            self.load(VALID.replace("corpus:", "corpus:\n  <<: {mode: crawler}"))

    def test_non_string_and_complex_mapping_keys_are_rejected(self):
        for key in ("true", "100", "null", "[a, b]"):
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "mapping keys must be strings"):
                    self.load(VALID + key + ": anything\n")

    def test_explicit_object_tags_cannot_execute(self):
        with patch("os.system", side_effect=AssertionError("must not execute")):
            with self.assertRaises(ValueError):
                self.load('!!python/object/apply:os.system ["echo forbidden"]')

    def test_oversized_yaml_is_rejected_before_parser(self):
        with patch("yaml.parse", side_effect=AssertionError("oversized input reached parser")):
            with self.assertRaisesRegex(ValueError, "configuration exceeds"):
                self.load(VALID + "#" + "a" * (64 * 1024))

    def test_deep_yaml_is_rejected_before_construction(self):
        with self.assertRaisesRegex(ValueError, "nesting"):
            self.load("x: " + "[" * 40 + "0" + "]" * 40)

    def test_multiple_documents_cannot_override_settings(self):
        with self.assertRaisesRegex(ValueError, "Malformed YAML"):
            self.load(VALID + "---\n" + VALID)

    def test_native_yaml_dates_are_not_silently_coerced(self):
        with self.assertRaisesRegex(ValueError, "ISO dates"):
            self.load(VALID.replace('"1993-01-20"', '1993-01-20'))

    def test_yaml_parse_errors_do_not_echo_source_values(self):
        canary = "synthetic-parser-canary-not-a-credential"
        self.path.write_text("corpus: [" + canary, encoding="utf-8")
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            self.assertEqual(main.main(["inspect", "--config", str(self.path)]), 2)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(canary, errors.getvalue())
        self.assertNotIn("Traceback", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
