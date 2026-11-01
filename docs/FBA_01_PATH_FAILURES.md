# FBA-01 snapshot path failure handling

On the pinned CPython 3.12.14 runtime, a manifest or record symlink cycle
raised RuntimeError from Path.resolve and escaped the CLI's validation boundary.
The command crashed with a traceback and exit code 1 instead of its documented
invalid-snapshot result (exit code 2, no success output).

local_path now converts filesystem resolution failures into ValueError with a
fixed diagnostic. Existing containment and regular-file checks still run after
resolution. Symlinks to regular files within the configuration root remain valid;
this change does not grant external paths access or claim race-proof filesystem
isolation. Snapshot inputs must remain immutable while verification runs.

Four regression tests cover direct library calls, manifest self-cycles, record
multi-link cycles, and valid contained aliases. Before the fix, two CLI tests
failed and the library test errored; the contained alias test passed. After the
fix, all 47 core tests passed on CPython 3.12.14 using a cleared environment and
python3 -S -m unittest discover -s tests -v. No service credentials or network
were used. The optional YAML suite was not rerun because PyYAML is absent in this
runtime. Hosted product CI remains a separate, unverified acceptance gate.
