"""Read-only local snapshot boundary; never collects sources or labels documents.

Version 1 is a provisional Bias interchange contract, not a claim that a live
Aggregator export already implements it. All files are resolved beneath the
configuration directory. Validation scans the whole census in bounded batches.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path, PurePosixPath

MAX_MANIFEST_BYTES = 1024 * 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 64


def local_path(root: Path, value: str) -> Path:
    if (not isinstance(value, str) or not value.strip() or "\\" in value
            or ":" in value or "\x00" in value
            or PurePosixPath(value).is_absolute()
            or ".." in PurePosixPath(value).parts):
        raise ValueError("snapshot paths must be relative local paths")
    root = root.resolve(strict=True)
    candidate = (root / value).resolve(strict=True)
    if not candidate.is_relative_to(root) or not candidate.is_file():
        raise ValueError("snapshot path escapes configuration directory or is not a file")
    return candidate


def strict_json(text):
    if not isinstance(text, str):
        raise ValueError("JSON input must be UTF-8 text")
    # Bound nesting before constructing objects; ignore delimiters in strings.
    # This is independent of the interpreter's recursion limit. json.loads still
    # performs all grammar validation; this scan never accepts syntax itself.
    depth = 0
    in_string = escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
        elif character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise ValueError("JSON nesting exceeds parser boundary")
        elif character in "]}":
            depth -= 1

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def bad_constant(value):
        raise ValueError("non-finite JSON number")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("non-finite JSON number")
        return number

    try:
        return json.loads(text, object_pairs_hook=pairs,
                          parse_constant=bad_constant, parse_float=finite_float)
    except RecursionError as exc:
        raise ValueError("JSON nesting exceeds parser boundary") from exc


def verify_snapshot(config, config_path):
    """Verify all bytes and rows without retaining a corpus or writing any file."""
    # Direct library callers must satisfy the same boundary as the CLI.
    from foia_bias.utils.corpus_config import validate_corpus_config
    validate_corpus_config(config)
    root = Path(config_path).resolve(strict=True).parent
    corpus = config["corpus"]
    manifest_path = local_path(root, corpus["manifest_path"])
    with manifest_path.open("rb") as stream:
        raw = stream.read(MAX_MANIFEST_BYTES + 1)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("manifest exceeds 1 MiB boundary")
    manifest = strict_json(raw.decode("utf-8"))
    required = {"schema_version", "snapshot_id", "producer", "fixture_mode", "records"}
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("manifest fields must match snapshot contract v1")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise ValueError("unsupported manifest schema_version")
    if not isinstance(manifest["snapshot_id"], str) or not manifest["snapshot_id"].strip():
        raise ValueError("snapshot_id is required")
    fixture = manifest["fixture_mode"]
    if type(fixture) is not bool or fixture != corpus["fixture_mode"]:
        raise ValueError("fixture mode mismatch")
    expected_producer = "synthetic-fixture" if fixture else "FOIA_Aggregator"
    if manifest["producer"] != expected_producer:
        raise ValueError("producer does not match fixture/live mode")
    records = manifest["records"]
    if not isinstance(records, dict) or set(records) != {"path", "sha256", "count"}:
        raise ValueError("records must contain path, sha256 and count")
    if not isinstance(records["sha256"], str) or not re.fullmatch("[0-9a-f]{64}", records["sha256"]):
        raise ValueError("records.sha256 must be lowercase SHA-256")
    if type(records["count"]) is not int or records["count"] < 0:
        raise ValueError("records.count must be a nonnegative integer")
    path = local_path(root, records["path"])
    digest = hashlib.sha256()
    count = size = batches = 0
    with path.open("rb") as stream:
        while True:
            line = stream.readline(MAX_RECORD_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_RECORD_BYTES:
                raise ValueError("record exceeds 16 MiB boundary")
            digest.update(line)
            size += len(line)
            row = strict_json(line.decode("utf-8"))
            if not isinstance(row, dict) or type(row.get("synthetic")) is not bool:
                raise ValueError("each record must be an object with a synthetic boolean")
            if row["synthetic"] != fixture:
                raise ValueError("synthetic record cannot cross fixture/live boundary")
            count += 1
            if count % corpus["batch_size"] == 0:
                batches += 1
    if count % corpus["batch_size"]:
        batches += 1
    if digest.hexdigest() != records["sha256"]:
        raise ValueError("snapshot record SHA-256 mismatch")
    if count != records["count"]:
        raise ValueError("snapshot record census mismatch")
    return {
        "status": "fixture-verified" if fixture else "snapshot-verified",
        "snapshot_id": manifest["snapshot_id"], "fixture_mode": fixture,
        "producer": manifest["producer"], "schema_version": 1,
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "records_sha256": digest.hexdigest(), "record_count": count,
        "record_bytes": size, "batch_count": batches,
        "analysis_performed": False, "research_eligible": False,
    }
