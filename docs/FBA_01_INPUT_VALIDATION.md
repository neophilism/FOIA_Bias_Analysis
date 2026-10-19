# FBA-01 configuration and JSON validation follow-up

## Scope

This change strengthens the existing read-only baseline on `boardroom/FBA-01`.
It does not add source collectors, model requests, labels, analytical cohorts,
archive writes or a live Aggregator integration. The existing fixture/live marker,
manifest checksum, full-census accounting and explicit unsupported-setting checks
remain mandatory. PR #30 and the full FBA-01 work package remain incomplete until
substantive Python test CI is authorized and verified on its actual head.

## Source review and fixes

At baseline commit `f067d6a8592710639e5aee2cae06946232d1c8d0`, JSON's
`parse_constant` hook rejected literal Infinity/NaN but decimal exponents could
still overflow to infinity. A valid matching manifest hash and count did not
prevent a row containing `1e9999` from being reported verified. The shared parser
now checks converted floats and rejects non-finite results in every nested value.
An explicit 64-container JSON nesting limit is enforced before object construction;
JSON grammar is still validated by the standard parser. Delimiters within quoted
and escaped strings do not consume this depth budget.

Configuration loading now reads at most 64 KiB plus one sentinel byte, rejects
oversized input before parsing, and requires canonical `YYYY-MM-DD` date strings.
The approved dates remain 1993-01-20 and 2025-01-20. Direct `verify_snapshot` callers
receive configuration validation before path resolution or record access.

Optional YAML uses a private SafeLoader subclass, not a global parser mutation.
Duplicate or non-string keys, merge keys, anchors/aliases and nesting beyond 32
containers are rejected. No ambiguous last-value-wins override or recursive alias
graph is accepted. YAML parser diagnostics are reduced to a fixed error without
printing source lines. Keep dates quoted, as in `corpus-config.yaml`.

## Verification

Core, dependency-free command:

```sh
python -S -m unittest discover -s tests -v
```

Optional YAML acceptance requires the existing `requirements-yaml.txt` pin:

```sh
python -m pip install -r requirements-yaml.txt
python -m unittest discover -s tests_optional -v
```

The optional suite errors when its dependency is missing; it does not silently
skip tests. JSON/help and the core suite never need PyYAML. The optional suite is
separate so a no-third-party-package baseline can be exercised honestly.

Local results on CPython 3.13.5: 43 core tests passed (27 inherited plus 16 new),
and 15 optional tests passed with PyYAML 6.0.3. All subprocesses used a cleared
environment without service credentials. Baseline core tests passed before edits;
the new regression file produced 13 failure reports against the original source
before implementation (including subtest reports). These local results are not
validation on the project's pinned CPython 3.12.14 and are not hosted CI results.

The tests cover exponent overflow with matching snapshot hashes, no-success CLI
errors, canonical dates, byte bounds, parser depth, duplicate keys, YAML ambiguity,
unsafe YAML tags and parser-message redaction. No live data or model was used.

## Remaining gate

The existing three source/registry/status checks do not execute unittest.
A prior Boardroom write to `.github/workflows/corpus-tests.yml` was rejected with
`INPUT_REJECTED: Protected path requires owner authorization`; private owner action
`2bbb49c4-3e10-4bfe-a710-1776180840a0` records that boundary. Do not work around it by
modifying unrelated scripts to smuggle new CI behavior through a permitted path.
The authorized workflow should use the pinned interpreter, run both test suites
(with optional YAML installed for that suite), retain existing checks, and report
actual current-head results. No workflow permission, merge, deployment or research
acceptance is asserted here.
