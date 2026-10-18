# FBA-01 read-only snapshot baseline

The default CLI reads local Aggregator-style snapshots. It cannot invoke source
collectors, contact model APIs, mutate the archive or fall back to crawling.
`inspect` validates configuration only; `run` validates all snapshot rows and emits
an integrity/census summary. Neither command labels documents or runs analysis.

## Reproducible environment

The tested baseline is CPython 3.12.14 on Linux, standard library only. The default
JSON configuration, help, fixture and 27 offline unittest cases run in a fresh
virtual environment with no third-party packages. `.python-version` records that
interpreter; `requirements.txt` deliberately has no install requirements. Optional
YAML configuration uses the exact PyYAML 6.0.3 pin in `requirements-yaml.txt`.
`requirements-legacy.txt` preserves earlier heavyweight dependencies for migration;
those collectors/model/analysis modules are outside this tested baseline.

## Configuration

`corpus-config.json` is the default. Only `corpus` and `cohort` are allowed.
Corpus fields: `mode: snapshot`, relative `manifest_path`, boolean `fixture_mode`,
integer `batch_size` in 1..10000. Cohort fields: `start_date: 1993-01-20`,
`end_date: 2025-01-20`, `include_incomplete_administrations: false`. These are the
approved completed-administration dates; the exclusive upper bound does not limit
upstream archival collection. Baseline verification counts every snapshot row and
does not yet apply a cohort filter. Unknown knobs, including population caps and
legacy source settings, are rejected rather than ignored.

## Provisional local snapshot contract v1

This is a Bias-side fixture/export adapter boundary. Compatibility with a deployed
Aggregator export is **not verified** and is acceptance work in FBA-04.

```json
{
  "schema_version": 1,
  "snapshot_id": "synthetic-multisource-v1",
  "producer": "synthetic-fixture",
  "fixture_mode": true,
  "records": {
    "path": "fixtures/records.jsonl",
    "sha256": "64 lowercase hexadecimal characters",
    "count": 5
  }
}
```

Both manifest and record paths resolve beneath the configuration directory,
including symlink targets. Absolute paths, traversal, URL/drive syntax, backslashes
and NUL bytes are rejected. The manifest is limited to 1 MiB. JSONL records are
limited to 16 MiB each and streamed one at a time. Each row must be an object with
a boolean `synthetic` matching both configuration and manifest fixture mode. A
fixture uses producer `synthetic-fixture`; a nonfixture uses `FOIA_Aggregator`.
Producer strings are assertions, not authenticated identities. SHA-256 checks
integrity against the supplied manifest, not authenticity against an independent
trusted release. Operators must pin an authorized upstream manifest separately.
Duplicate JSON keys, nonfinite numbers, blank/malformed records and count/hash
disagreement fail closed. No partial success summary is emitted on error.

The manifest counts the **entire census**. Batch size controls progress accounting,
never selection. The five-row multisource fixture verifies three batches at size
two. Fixture summaries always retain `fixture_mode: true`; both fixture and
nonfixture summaries report `analysis_performed: false`, `research_eligible: false`.
No real-corpus ingestion, provenance ledger or research acceptance is claimed.

## Verification and remaining acceptance

The offline suite covers import/help without keys, no-network fixture execution,
full census, batch invariance, malformed config/JSON/manifest/rows, synthetic/live
crossing, hashes/counts, bounded reads, symlink escape and read-only input checks.
The preexisting GitHub checks validate source/registry/status integrity and compile
Python but do not execute unittest. FBA-01 remains incomplete until substantive
Python test CI is configured through an authorized workflow route and verified
for the current PR head. Do not infer runtime test acceptance from compile checks.
