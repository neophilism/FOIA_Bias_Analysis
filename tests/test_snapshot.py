"""Snapshot integrity and offline/fixture safety acceptance tests."""
import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main
from foia_bias.corpus import verify_snapshot
from foia_bias.utils.corpus_config import validate_corpus_config


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = {"corpus": {"mode": "snapshot", "manifest_path": "manifest.json",
                                  "fixture_mode": True, "batch_size": 2},
                       "cohort": {"start_date": "1993-01-20", "end_date": "2025-01-20",
                                  "include_incomplete_administrations": False}}
        self.rows = [{"id": str(i), "synthetic": True, "source_family": source}
                     for i, source in enumerate(["agency-log", "reading-room", "muckrock", "annual", "other"])]
        self.write_rows()
        self.config_path = self.root / "config.json"
        self.config_path.write_text(json.dumps(self.config))

    def write_rows(self):
        data = b"".join((json.dumps(row) + "\n").encode() for row in self.rows)
        (self.root / "records.jsonl").write_bytes(data)
        self.manifest = {"schema_version": 1, "snapshot_id": "synthetic-multisource-v1",
                         "producer": "synthetic-fixture", "fixture_mode": True,
                         "records": {"path": "records.jsonl", "count": len(self.rows),
                                     "sha256": hashlib.sha256(data).hexdigest()}}
        self.write_manifest()

    def write_manifest(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def verify(self):
        return verify_snapshot(validate_corpus_config(self.config), self.config_path)

    def test_complete_census_across_batches_is_read_only(self):
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        result = self.verify()
        self.assertEqual(result["record_count"], 5)
        self.assertEqual(result["batch_count"], 3)
        self.assertEqual(result["status"], "fixture-verified")
        self.assertFalse(result["research_eligible"])
        self.assertFalse(result["analysis_performed"])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_batch_size_changes_progress_not_census(self):
        first = self.verify()
        self.config["corpus"]["batch_size"] = 1
        second = self.verify()
        self.assertEqual(first["records_sha256"], second["records_sha256"])
        self.assertEqual(second["record_count"], 5)
        self.assertEqual(second["batch_count"], 5)

    def test_checksum_mismatch(self):
        self.manifest["records"]["sha256"] = "0" * 64
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.verify()

    def test_census_mismatch(self):
        self.manifest["records"]["count"] = 4
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "census mismatch"):
            self.verify()

    def test_fixture_cannot_be_live(self):
        self.config["corpus"]["fixture_mode"] = False
        with self.assertRaisesRegex(ValueError, "fixture mode mismatch"):
            self.verify()

    def test_synthetic_rows_cannot_be_retagged_live(self):
        self.config["corpus"]["fixture_mode"] = False
        self.manifest.update(fixture_mode=False, producer="FOIA_Aggregator")
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "synthetic record"):
            self.verify()

    def test_live_rows_cannot_be_retagged_fixture(self):
        self.rows[0]["synthetic"] = False
        self.write_rows()
        with self.assertRaisesRegex(ValueError, "synthetic record"):
            self.verify()

    def test_empty_snapshot_has_no_batches(self):
        self.rows = []
        self.write_rows()
        result = self.verify()
        self.assertEqual((result["record_count"], result["batch_count"]), (0, 0))

    def test_malformed_manifests_fail_early(self):
        for field, value in [("schema_version", True), ("schema_version", 2),
                             ("fixture_mode", 1), ("snapshot_id", ""),
                             ("producer", "muckrock")]:
            with self.subTest(field=field, value=value):
                original = self.manifest[field]
                self.manifest[field] = value
                self.write_manifest()
                with self.assertRaises(ValueError):
                    self.verify()
                self.manifest[field] = original

    def test_bad_record_metadata(self):
        for field, value in [("count", True), ("count", -1), ("sha256", "bad"),
                             ("path", "../records.jsonl"), ("path", "/tmp/records.jsonl"),
                             ("path", "https://example.test/records.jsonl")]:
            with self.subTest(field=field, value=value):
                original = self.manifest["records"][field]
                self.manifest["records"][field] = value
                self.write_manifest()
                with self.assertRaises(ValueError):
                    self.verify()
                self.manifest["records"][field] = original

    def test_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "records.jsonl"
            external.write_text("{}\n")
            (self.root / "escape.jsonl").symlink_to(external)
            self.manifest["records"]["path"] = "escape.jsonl"
            self.write_manifest()
            with self.assertRaisesRegex(ValueError, "escapes"):
                self.verify()

    def test_invalid_record_json_and_markers(self):
        for data in [b"\n", b"[]\n", b"{}\n", b'{"synthetic":1}\n',
                     b'{"synthetic":true,"synthetic":false}\n',
                     b'{"synthetic":true,"value":NaN}\n', b"\xff\n"]:
            with self.subTest(data=data):
                (self.root / "records.jsonl").write_bytes(data)
                with self.assertRaises(ValueError):
                    self.verify()

    def test_oversized_record_and_manifest(self):
        with patch("foia_bias.corpus.MAX_RECORD_BYTES", 8):
            with self.assertRaisesRegex(ValueError, "record exceeds"):
                self.verify()
        with patch("foia_bias.corpus.MAX_MANIFEST_BYTES", 8):
            with self.assertRaisesRegex(ValueError, "manifest exceeds"):
                self.verify()

    def test_unknown_settings_are_not_silently_ignored(self):
        for section, key in [("corpus", "max_records"), ("cohort", "max_year")]:
            self.config[section][key] = 1
            with self.assertRaisesRegex(ValueError, "Unsupported"):
                self.verify()
            del self.config[section][key]

    def test_cli_reads_full_fixture_without_network(self):
        output = io.StringIO()
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            with contextlib.redirect_stdout(output):
                code = main.main(["run", "--config", str(self.config_path)])
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["record_count"], 5)
        self.assertTrue(result["fixture_mode"])

    def test_cli_reports_corruption_without_success_output(self):
        (self.root / "records.jsonl").write_text("broken")
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            self.assertEqual(main.main(["run", "--config", str(self.config_path)]), 2)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("Invalid corpus snapshot", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
