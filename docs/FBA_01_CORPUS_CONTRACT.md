# FBA-01 corpus configuration contract (implementation checkpoint)

The approved default analysis path consumes a frozen, read-only FOIA_Aggregator corpus snapshot, rather than starting MuckRock, agency-log or reading-room crawlers. Snapshot IDs, record hashes, text provenance and full record counts must be validated before a live consumer can be enabled. There is no live Aggregator endpoint in this repository; no production ingestion is claimed.

Approved completed-administration cohort: 1993-01-20 through 2025-01-20 (exclusive end for the ongoing second Trump administration). This cutoff limits analysis, not upstream archival collection.

A batch-size setting limits processing per transaction only. It must not cap the total records selected from a manifest. Fixtures must be explicitly tagged and must never be reported as real results.

Legacy `python main.py run` invokes source collectors. It is deprecated for default analysis; do not run that command against production sources until a dedicated corpus-consumer CLI replaces it. This file records the gap for FBA-01 and does not certify closure.
