# FOIA Bias Analysis — development and independent-account handoff plan

Plan version 3 · 2026-10-11 · Repository: `neophilism/FOIA_Bias_Analysis`

Start here, then read [HANDOFF_STATUS.md](HANDOFF_STATUS.md). The machine-readable work register is [registry/prs.json](../registry/prs.json); its work IDs match section 5 below. This document specifies the analysis layer over the full FOIA_Aggregator corpus and the migration required from the legacy implementation. It does not certify that the integration already operates.

## 1. Authority, recovery and historical fidelity

The October 11 owner instruction authorizes completing this missing plan, reconciling it with implementation and the structured PR registry, and updating [Engine Room audit #71](https://github.com/neophilism/Engine-Room/issues/71). It does not authorize a new research population, paid data processing, live deployment, or a claim of scientific results.

Evidence classes used throughout:

| Class | Meaning and limit |
|---|---|
| U | Direct current owner instruction: the October 11 plan-completion request recorded above. |
| R | Verified repository text, code or GitHub PR evidence. Confirms what is documented or implemented, not that every choice was approved. |
| A | October 10 audit's recovered requirements. An authoritative remediation source for this task, but a secondary historical account. |
| C | Prior-context recovery on October 11, summarized with original message dates. Binding continuation constraints recorded here, not a verbatim transcript certificate. |
| E | Engineering reconstruction written for this plan: milestone boundaries, schemas, tests and operational design. No claim these were original approved PR titles. |

No original numbered development plan or complete original conversation transcript was recovered. Preserve this limitation. All `FBA-01`–`FBA-16` work packages are **newly enumerated completion/remediation work**, implementing inherited scope; they are not retroactive names for GitHub PRs 1–21. New future amendments require a dated source and separate lineage. This plan is sufficient to begin independent repository work; historical and operational certification remain separate gates.

### October 11 owner correction — authoritative corpus architecture

At approximately 18:33 America/New_York on October 11, 2026, the owner corrected the initial plan: Bias analysis is based on the massive database FOIA_Aggregator will compile. The first reconstructed plan gave historical MuckRock code too much weight and incorrectly made separate source acquisition central to future milestones. **This version supersedes that architectural framing.** This is a direct current requirement (U), not a recovered historical transcript.

**Authoritative flow:** FOIA_Aggregator collects and maintains its archive across all source classes → a versioned read-only corpus snapshot/export supplies records, provenance, full extracted text and log relationships → FOIA_Bias_Analysis labels the eligible corpus and performs statistical analysis. MuckRock is one source within that shared database, alongside agency reading rooms, disclosure libraries, official logs and other included releases.

**Responsibilities:** the Aggregator owns discovery/crawling, source downloads, raw preservation, record/file identity, cross-source archive deduplication, extraction/OCR and archive completeness. Bias owns corpus consumption, analytical inclusion rules, temporal/actor mapping, full-text automated labeling, analytical checkpoints, diagnostics and reproducible results. Acquisition/extraction gaps return to the Aggregator with its record IDs. A parallel crawler/raw archive/OCR program is not a Bias milestone. Historical MuckRock code/files/labels are migration evidence.

### Source and requirement ledger

Repository baseline inspected: [`fa9484b27d6a0af5e3805b4729f600701d6070b0`](https://github.com/neophilism/FOIA_Bias_Analysis/tree/fa9484b27d6a0af5e3805b4729f600701d6070b0). References below are relative to that baseline unless explicitly called current.

| ID | Preserved requirement or decision | Evidence / classification | Implemented or remaining |
|---|---|---|---|
| BR-01 | Study possible partisan differences in disclosed FOIA materials using wrongdoing and favorability hypotheses, treating D/R symmetrically. Policy disagreement alone is not wrongdoing. | R: [README](https://github.com/neophilism/FOIA_Bias_Analysis/blob/fa9484b27d6a0af5e3805b4729f600701d6070b0/README.md), `llm/prompts.py`, [PR #1](https://github.com/neophilism/FOIA_Bias_Analysis/pull/1); A: #71 | Scaffold present; FBA-09, 11–14 provide valid inputs and analysis. |
| BR-02 | Cover Clinton, Bush, Obama, Trump's first term and Biden: 1993 through the January 2025 transition. Exclude the ongoing, incomplete second Trump term from the completed-administration comparison so the time periods are comparable; this analytical cutoff does not restrict the Aggregator from collecting newer records. | U: BR-12 clarification; A: #71; R: `processing/admin_mapping.py`, [PR #5](https://github.com/neophilism/FOIA_Bias_Analysis/pull/5); C: 2025-11-19 request for earlier-than-2010 coverage | Configuration still starts at 2010 and ends analytical years at 2024; FBA-01, 08, 14. |
| BR-03 | Retrieve completed MuckRock requests with files without political-keyword retrieval filtering; acquire actual released files through communications/files and extract their text. Metadata alone is insufficient. | R: [PR #13](https://github.com/neophilism/FOIA_Bias_Analysis/pull/13), `data_sources/muckrock_client.py`; C: 2025-11-18/19 messages | Historical source requirement; under BR-11 the Aggregator FC-04 owns collection. Bias consumes these releases through the same contract as all other archive sources. |
| BR-04 | Fully automated all-document LLM assessment; no human annotation campaign or sampled substitute for processing all eligible documents. | C: 2025-11-18 16:06 UTC; A: full-text labeling in #71 | Current keyword/NER rejection and text truncation conflict; FBA-09, 10, 13. Automated QA sampling may test quality but cannot replace corpus processing. |
| BR-05 | Preserve local raw files/text caches; avoid duplicate downloads; interrupted work resumes from durable progress. | R: [PR #14](https://github.com/neophilism/FOIA_Bias_Analysis/pull/14), [#15](https://github.com/neophilism/FOIA_Bias_Analysis/pull/15), [#16–21 history](https://github.com/neophilism/FOIA_Bias_Analysis/pulls?q=is%3Apr); C: 2025-11-18/19 | Preserve legacy artifacts during ID/hash reconciliation. Future checkpoints track analytical consumption of Aggregator snapshots; archive preservation stays upstream. |
| BR-06 | Use historical congressional membership and presidents/vice presidents, not a tiny static actor list presented as complete. | R: [PR #10](https://github.com/neophilism/FOIA_Bias_Analysis/pull/10), [#12](https://github.com/neophilism/FOIA_Bias_Analysis/pull/12), `politics_filter.py`; C: 2025-11-18 | FBA-08 must preserve term/party changes and flag fallback coverage. |
| BR-07 | Keep agency logs, reading rooms and FOIA.gov annual data as distinct source classes; analyze release content separately from request descriptions and aggregate statistics. | R: README, `data_sources/*`, `pipeline.py`; A: #71 | Consume these record types through the shared Aggregator corpus, with FBA-02/06/11 separation for valid inference; no separate Bias source-collection program. |
| BR-08 | Maintain wrongdoing/favorability regressions, agency/year effects and robustness checks; quantify uncertainty and limitations. | R: `analysis/*`, `config.yaml`; A: #71 | FBA-11–14; configuration is not proof every option is implemented. |
| BR-09 | FOIA Aggregator broad-corpus coverage precedes substantial analytical advancement and broad statistical claims. Planning, deterministic repairs and fixtures may proceed. | R: pinned Aggregator plan below; C: owner message 2026-10-10 14:01 UTC | External gate EXT-CORPUS, not satisfied by this document or Aggregator's plan existence. |
| BR-10 | Preserve provenance, lawful source access and privacy; focus on government conduct without unnecessarily republishing private individuals' details. | A: #71; C: 2025-11-19 privacy discussion | FBA-02, 05, 09, 15–16. Concrete safeguards are E. |
| BR-11 | The Aggregator database is the authoritative Bias corpus across all included source classes. MuckRock is one source within it. | U: owner correction, October 11, 2026, approximately 18:33 America/New_York | Governs all FBA units: read-only corpus ingestion and corpus-wide analysis replace a separate acquisition pipeline. |
| BR-12 | Exclude the incomplete ongoing second Trump term to preserve comparable time periods; continue archiving newer records upstream. | U: owner clarification, October 11, 2026, approximately 18:34 America/New_York | FBA-08/11/12/13/14: completed equal-duration terms, normalized pooled comparisons and explicit ongoing-term exclusions. |

**Exact upstream evidence:** [FOIA_Aggregator plan at `62fe82950963dcc0dde2003f094047d65bf79a9d`](https://github.com/neophilism/FOIA_Aggregator/blob/62fe82950963dcc0dde2003f094047d65bf79a9d/docs/DEVELOPMENT_PLAN.md), especially FC-03, FC-08, FC-13, FC-14 and downstream gating. This pins the inspected contract, not a deployed API/version or completed milestone. Require an actual snapshot/export manifest with producer commit, schema version, snapshot ID, record/text hashes, source/date coverage, failures and FC-14 review evidence. Enumerate the entire eligible archive; a search-result slice or MuckRock-only export is insufficient. Bias gets read-only access/export, never archive write credentials. No implemented endpoint/current export schema is assumed.

**Unresolved historical detail:** exact treatment of date-only January 20 records, original sampling/estimand definitions, and exact historical model choices are not transcript-certified. Preserve the scope above. E default: use exact event timestamps when available with administration boundaries; flag date-only transition-day records as ambiguous and report both include/exclude sensitivity instead of assigning a party by guess. Do not borrow Send_FOIA_Log_Requests' campaign years or Aggregator's B2 quota as requirements for this repository.

## 2. Product specification and completion boundary

Analyze the eligible records in the Aggregator's massive, provenance-preserving database. The Aggregator compiles the source corpus; Bias produces a reproducible analytical view, automated labels and statistical reports. Include every archive source class under documented historical date, jurisdiction and record-type rules. Source-specific results are optional diagnostic strata, not the default population. The approved date/jurisdiction cohort must never be replaced by selection on suspected political content.

Maintain a census of all imported records: eligible, excluded with a stated reason, waiting for upstream text, failed, labeled or budget-blocked. Every successfully extracted eligible document's full text is assessed, including material after 20,000 characters. Metadata-only, remote-only, unsupported and inaccessible records remain visible in coverage accounting and cannot become neutral labels. Report upstream acquisition/extraction failures with stable Aggregator record IDs.

The two hypothesis families are (a) opposition exposure / incumbent suppression of material alleging wrongdoing and (b) opposition disadvantage / incumbent advantage in favorability. They are hypotheses, not predetermined findings. A disclosed-documents-only corpus measures associations among **observed disclosures**. It cannot by itself identify unreleased content, prove suppression, estimate release probability for all requests, or establish causation. Models requiring a request-level denominator must use linked logs with outcome/missingness evidence; otherwise label that estimand unavailable. State/local records from any archive source must retain jurisdiction and be separated from federal executive-administration inference.

Bias artifacts: pinned upstream corpus manifest and record/full-text references; automated document/chunk labels keyed to Aggregator IDs/hashes; request/log joins; versioned analytical datasets; diagnostics and coverage/exclusion tables; executable report command and machine-readable analysis status. The Aggregator retains authoritative raw binaries/extracted text; Bias may keep bounded run text caches without duplicating the full raw archive. No dashboard, mail sender, automatic political publication, cloud database purchase or new service is required by this recovered scope. Optional database upload in config is an extension, not existing functionality.


### Comparable time periods — October 11 owner clarification

The owner clarified at approximately 18:34 America/New_York that the second Trump term is excluded because it is **ongoing and incomplete**; the goal is comparable time periods. The Aggregator can continue collecting records from 2025 onward. Those records remain in the archive but are outside this completed-period analytical cohort, with an explicit exclusion reason.

Compare complete four-year presidential terms as the primary equal-duration windows: Clinton first/second, Bush first/second, Obama first/second, Trump first and Biden. If results are pooled by administration, account for the different number of completed terms and use exposure-normalized measures; do not compare eight-year raw totals against four-year raw totals as if time exposure were equal. Apply consistent date definitions and observation/availability rules across windows, report unequal source coverage and disclosure lag, and provide sensitivity checks. A completed term is not proof that every FOIA record for it is available. Date-only transition-day ambiguity remains flagged under BR-02 rather than silently leaking ongoing-term records into the historical cohort.

Any later analysis of the second Trump term must be a separately specified comparable-window analysis or a future update after the relevant term is complete. It cannot be silently mixed into the completed-term comparison.

## 3. Legacy implementation and migration inventory

This section records legacy code, not the target acquisition architecture. Existing source collectors are not prerequisites to finish before corpus integration. Retire direct source ingestion from the default analysis workflow; reuse labeling/statistical helpers after their acceptance gaps are addressed. Source acquisition/extraction repairs belong in FOIA_Aggregator.

Current legacy stack: Python >=3.11, Typer CLI, Requests, Pandas/Parquet, pdfplumber/Tesseract, spaCy, OpenAI client wrapper and statsmodels. `requirements.txt` and `pyproject.toml` contain lower bounds, not a reproducible lock. No test directory, application CI workflow or deployment manifest exists at the baseline. No production service was verified. Historical PR bodies generally report `compileall`, which checks syntax only.

| Current code/evidence | Limit requiring completion work |
|---|---|
| `main.py` has run, four ingest commands and two analyze commands. | Importing `politics_filter.py` loads a roster and may access the network even for CLI help; establish offline/import-safe startup in FBA-01. |
| `config.yaml` / `config.json` start MuckRock at 2010, cap at 10,000 requests and analyze 2010–2024. | Legacy direct-source defaults must be replaced by corpus input configuration, explicit analytical dates and bounded processing batches that preserve the full-corpus denominator. |
| MuckRock uses completed + has-files queries and communications/files fallback. | Historical acquisition completeness is unproved. Aggregator FC-04 owns this repair; Bias consumes its export/status evidence rather than completing a parallel MuckRock crawler. |
| `process_muckrock()` writes a page checkpoint before final `save_records()`. | Labels live in memory until run end; resumed `save_records()` overwrites a source Parquet file. Crash/resume can lose data. Reusing a page offset while changing `updated_after` can skip records; stopping mid-page can skip the rest. |
| Filename/file-ID/size-based reuse exists. | No durable cross-source checksum manifest; unsafe upstream filenames and same-name collisions need bounds and isolation. Preserve existing cache during migration. |
| `combine_texts()` routes every file to PDF extraction; extractor sends PDF path directly to Tesseract. | Non-PDFs, scans and mixed PDFs need tested MIME-aware extraction and page rasterization. Configured text cache is not a proven durable implementation. |
| `label_text()` substitutes neutral labels after keyword/NER rejection; NER sees 5,000 characters and classifier sees 20,000. | Does not satisfy all-document full-text assessment. Embedding classifier exists but is not trained/wired by the pipeline. |
| `llm/client.py` uses a configured model string and SDK call. | Provider/model compatibility, strict schema/range validation, retry/concurrency settings and budget handling are not verified. Do not run paid calls to establish documentation completeness. |
| `get_admin_for_date()` expands each administration's interval for transition flags. | Overlapping intervals select the first administration and can misassign a transition record; separate exact assignment from sensitivity flags. |
| Log date selection uses substring matches including `date`; annual data gets neutral labels. | A submitted date can be mistaken for a decision date; metadata must not masquerade as document observations. |
| Analysis picks dominant party via a set/count, fixes wrongdoing threshold at 0.5 and fits logit/OLS. | Ties can be unstable; configured thresholds and clustered SE are not implemented end-to-end. Empty/singular/separated models lack honest diagnostic handling. |

### Historical PR evidence (not completion claims)

At inspection: 21 PR records; **20 merged**, **#18 closed unmerged**, **0 open**. All records remain in `registry/prs.json` with actual IDs, commits and timestamps.

| PRs | Evidenced change | Completion work that uses it |
|---|---|---|
| #1, #2 | Pipeline scaffold and log-row parsing | FBA-01, 02, 06, 11, 12 |
| #3, #4, #5 | HTTP-client/auth compatibility, troubleshooting, tokenless access and cutoff | FBA-01, 04, 08 |
| #6, #7, #8 | Inline guidance, progress logging, local sample logs | FBA-01, 06, 15 |
| #9, #11, #13, #14, #15 | Legacy MuckRock retrieval/files, persistence and download reuse | Migration reference for FBA-03/05; no corpus-consumer completion claim or future crawler task. |
| #10, #12 (also #11, #14) | Congressional/actor sources and fallbacks | FBA-08 |
| #16, #17, #19, #20, #21 | Page/cursor checkpoint revisions | FBA-03, 10 |
| #18 | Closed without merge; similar proposed checkpoint change | Historical evidence only; never count as merged. |

These historical changes do not satisfy the broader completion acceptance criteria below. Their numbers are stored as `priorEvidencePrs`, not completion `prs`. Existing implementation is real; its existence is not evidence that a new remediation unit is finished.

## 4. Target architecture and data contracts (E)

Reuse Python labeling/analysis helpers and provide a corpus-consumer CLI. Target stages: **open Aggregator snapshot → stream records/full text → validate/select cohort → label → assemble analytical tables → analyze**. Discovery, crawling, source downloads, archive deduplication and extraction/OCR stay upstream. Use a transactional analysis/job ledger and versioned analytical exports; choose analytical storage against measured corpus scale, rather than requiring a second local copy of the massive archive.

### Read-only Aggregator input contract (E specification; not a certified implemented interface)

FC-13 supplies repeatable partitioned/streaming exports or a paginated read-only snapshot. Transport can be files, an authenticated API or read-only replica according to actual upstream implementation. No endpoint or shared-write permission is invented. FC-14 supplies coverage acceptance; contract fixtures and migration preparation may proceed until those gates are met.

| Entity | Required fields/invariants |
|---|---|
| Snapshot manifest | Stable snapshot/export ID, producer commit, schema/version, UTC observation, partition/record counts and hashes, source/agency/date coverage and unresolved failures. Freeze membership for reproducible runs; never consume changing pages as one snapshot. |
| Catalog record | Stable Aggregator record ID/revision, original SHA-256, all source/provenance IDs/URLs, agency/component/jurisdiction, record type, request/release links and separate typed dates/precision. |
| Full text | Upstream record/revision/file hash, text hash/revision, extractor/OCR version/method/language, page offsets, full-text location/stream and extraction coverage status. Search snippets alone are insufficient. |
| Context | Namespaced request/log row IDs, component/period, outcome/disposition, raw provenance and evidenced release links. Annual totals/request descriptions remain contextual. |
| Archive state | Preserved vs remote-only; extracted/partial/unsupported/failed/waiting; access/rights and failure reason. Missing text cannot be a successful assessment. |
| Analysis run/job | Snapshot ID, cohort/config/code/roster/model/prompt versions, immutable record/text key, partition/cursor and durable label state, usage/budget/retries, counts and incomplete/complete status. |
| Label/result | Aggregator IDs/revisions/hashes, full-coverage chunk map, actor/time evidence, scores/uncertainty, provider provenance, analytical specification, exclusions and diagnostics. |

**All-source contract acceptance:** fixtures include official reading-room releases, MuckRock releases, another included release collection, request logs, annual statistics, duplicate provenance, changed redactions, unknown dates and remote-only/failed-text records. Eligible releases share one input/labeling path. Preserve all source relationships and declare analytical deduplication/weight rules. Source-stratified robustness analyses accompany combined-corpus results.

**Massive-corpus processing:** stream/partition input, maintain stable snapshot cursors and bounded memory/queues, and avoid a single in-memory dataframe/list of all archive records. Use repeatable multi-pass/partitioned analytical assembly where models require it. Benchmark a documented large workload under fixed memory/concurrency limits before selecting analytical storage. A per-run budget or batch ceiling throttles progress, never defines the research population; unfinished partitions stay in the denominator. Do not require re-downloading every raw binary.

Commit labels/job outcomes before cursor acknowledgment. Changed text/model hashes invalidate affected label caches. New snapshots enumerate additions/revisions/tombstones and reuse unchanged hashes; statistical reports never mix moving snapshots. Reconcile legacy files/labels to Aggregator identities in an explicit migration. Unmatched legacy files become upstream import/review candidates, not an automatically merged independent research corpus. Archive outages pause the consumer; there is no automatic fallback to direct MuckRock/agency crawling.

Maintain source privacy/access constraints, bounded full-text retrieval, and prompt-injection resistance. Keep model and read-only corpus secrets in the process environment. Authoritative raw files and OCR artifacts remain with the Aggregator.

## 5. Executable completion milestones

Each ID is one reviewable work package; split implementation across PRs if necessary and map **every required PR** explicitly. Prerequisites below form the registry dependency graph. New test filenames are deliverables, not claims that those tests already exist. Use Python's standard `unittest` as the initial test runner. Each unit requires passing relevant tests plus `python -m compileall -q main.py foia_bias` and `python scripts/validate_plan.py`.

### FBA-01 — Reproducible corpus-consumer baseline and configuration

**Prerequisites:** this plan and current PR review. **Deliver:** Lock a tested Python environment, offline/import-safe CLI, corpus snapshot/export input config, historical cohort dates, fixture mode and nonempty tests/CI. Make Aggregator input the default analysis mode; deprecate source collection in default config/help. Record unsupported settings. **Acceptance:** fresh setup/help/fixtures require no source/model keys; default analysis cannot invoke MuckRock/agency collectors; invalid snapshot config fails early; sample rows cannot be live data; batch caps limit progress, not the corpus census. **Evidence:** Legacy #1/#3–8 are foundation evidence.

### FBA-02 — Aggregator record identity and analytical provenance ledger

**Prerequisites:** FBA-01. **Deliver:** Implement section 4 contracts, schema migrations, stable Aggregator record/revision/text keys and analytical run provenance. Preserve source families, record types, date precision and empty/remote-only/failed states. **Acceptance:** all-source fixture round-trip retains IDs/hashes/nulls/provenance; duplicate associations survive; log rows, annual totals and releases remain distinct; revised text yields auditable label invalidation. **Evidence:** Legacy #1/#2 are related schema work, not a finished corpus adapter.

### FBA-03 — Lossless corpus-consumer and labeling checkpoints

**Prerequisites:** FBA-02. **Deliver:** Checkpoint fixed-snapshot partition/cursor and durable job/label outcomes with transactions/leases and corrupt-state recovery. Reconcile legacy cached files/checkpoints/labels without resuming a new research run from source-specific page offsets. **Acceptance:** kill during corpus import/text retrieval/label commit/mid-batch cap; resume retains outputs and exactly matches expected snapshot membership; changed snapshot/config cannot reuse an incompatible cursor; no duplicate paid work or overwritten historical partitions. **Evidence:** #15–17/#19–21 preserve resumption intent, not the target consumer implementation.

### FBA-04 — Full Aggregator corpus snapshot ingestion

**Prerequisites:** FBA-02, FBA-03. **Deliver:** Build the read-only streaming/paginated consumer against the actual FC-13 export. Enumerate all eligible shared-archive records across source families and validate manifest counts/partitions/hashes. Persist an outcome for every record. Missing upstream contract blocks live consumption, rather than triggering a new crawler. **Acceptance:** multi-source multi-partition fixture matches the entire manifest; official/MuckRock/other eligible releases enter the same queue; limited search results cannot certify complete import; missing/duplicate/truncated pages and changed snapshot tokens fail visibly; batches resume without a permanent 10,000-request population cap; later live acceptance pins producer/schema/snapshot hashes. **Evidence:** No historical MuckRock PR establishes this consumer; this replaces the initial separate acquisition milestone.

### FBA-05 — Shared-corpus deduplication and legacy artifact reconciliation

**Prerequisites:** FBA-02, FBA-04. **Deliver:** Use Aggregator IDs/hashes and duplicate/revision associations for an explicit analytical dedup/weight policy. Keep bounded text caches. Reconcile legacy artifacts with upstream identities and queue unmatched items for upstream review. Archive identity/raw preservation stay upstream. **Acceptance:** repeat import is idempotent; identical releases across source families do not get accidental extra statistical weight; every provenance association survives; different redactions/revisions remain distinguishable; legacy artifacts survive without becoming a separate default archive. **Evidence:** #14/#15 are historical migration references only.

### FBA-06 — Shared-corpus request logs and annual context integration

**Prerequisites:** FBA-02, FBA-05. **Deliver:** Join Aggregator request/log/annual-context records to releases with evidenced IDs, component/period and date mappings. Preserve census/jurisdiction. Missing data types are upstream contract gaps, not dedicated Bias scraper work. **Acceptance:** submitted dates cannot fill decision dates; log descriptions/annual totals cannot enter release-content regression; partial/unknown joins remain explicit; record-type and source/agency/date counts reconcile to the snapshot. **Evidence:** #1/#2/#8 are related legacy scaffolding.

### FBA-07 — Aggregator full-text and extraction completeness validation

**Prerequisites:** FBA-05. **Deliver:** Consume Aggregator text/OCR with record/file/text hashes, revisions, page offsets and extraction status. Validate full-text coverage and return precise missing/partial/unsupported repair items upstream. Reuse text instead of rebuilding Bias OCR or raw-file ingestion. **Acceptance:** digital/scanned/mixed/legacy-format fixture outputs preserve late-page evidence; partial OCR/remote-only metadata cannot be neutral/success labels; valid text is reused without fetching originals again; bad record/hash association fails; gap reports identify upstream record IDs. **Evidence:** Legacy extraction PRs are migration evidence, not acceptance of shared-text integration.

### FBA-08 — Administration, temporal scope and historical actor mapping

**Prerequisites:** FBA-02, FBA-06. **Deliver:** exact administration intervals plus independent transition flags and complete four-year comparison windows, explicit request/decision/release date policy, complete sourced congressional terms/party changes and executive actors with cached version hashes. **Acceptance:** fixtures around every 2001/2009/2017/2021/2025 boundary, 1993 lower bound, date-only ambiguity, unknown dates, party-switching and mixed/unknown actors; transition flag cannot change incumbent assignment; ongoing second-term records stay archived upstream but are excluded with a reason from completed-period comparisons; window boundaries and source-availability rules are applied consistently. Roster fallback is degraded and cannot certify full coverage. **Evidence:** #5, #10–12, #14.

### FBA-09 — Automated all-document, full-text labeling

**Prerequisites:** FBA-07, FBA-08. **Deliver:** every successfully extracted eligible Aggregator cohort document goes through the same LLM assessment regardless of source family; token-aware full-coverage chunks and documented aggregation; strict schema/range/evidence validation, model/SDK capability check and provider-independent boundary where useful. Prefilters may prioritize work, never silently produce neutral final labels or discard documents. **Acceptance:** fixtures with decisive content after 20k characters and without early political keywords are processed; chunk map covers all text; malformed JSON, out-of-range scores, conflicting parties, model refusal and prompt injection yield explicit states. Deterministic fake provider tests prove orchestration only; live model accuracy remains separately unverified. **Evidence:** #1 and BR-03/04; existing classifier does not satisfy this unit.

### FBA-10 — Resumable labeling, usage controls and full-corpus accounting

**Prerequisites:** FBA-03, FBA-09. **Deliver:** durable jobs keyed by text/prompt/model hash, configured bounded concurrency/retries, per-run usage ceilings, billing ambiguity state, pause/resume and skipped-reason census. **Acceptance:** concurrent retries, crash after provider response and changed-model rerun cannot silently double-bill; no-key/budget cap pauses labeling while retaining snapshot references and analysis state; processed + queued + failed + waiting-for-upstream-text + budget-blocked counts reconcile to the complete eligible shared-corpus census; bounded-memory streaming handles the growing archive. Actual budgets/providers require authorized inputs; this plan provides none. **Evidence:** #16–21 concern ingestion only, not completion of this unit.

### FBA-11 — Analytical cohorts and deterministic derived variables

**Prerequisites:** FBA-06, FBA-08, FBA-10. **Deliver:** versioned cohort builder over the full shared archive snapshot, document/request/release unit distinctions, deterministic mixed-party/tie handling, configurable thresholds, jurisdiction separation, duplicate-cluster policy and exclusion/missingness report. **Acceptance:** golden synthetic fixtures reproduce exact rows and outcomes under shuffled input; January 2025 scope is not silently removed by max-year=2024; request description, annual data, partial text and unassessed records never become complete labels; thresholds actually affect output. **Evidence:** #1, #2.

### FBA-12 — Regressions, diagnostics and honest estimands

**Prerequisites:** FBA-11. **Deliver:** specified wrongdoing logit and favorability models over comparable completed periods, term-duration/exposure normalization for pooled administration results, agency/year fixed effects and documented cluster covariance, incumbent-aligned favorability interpretation, confidence intervals, deterministic exports and unsuitable-data diagnostics. Distinguish disclosure-composition models from request-outcome models. **Acceptance:** synthetic null and known-signal data; party-label swap tests; empty, missing, separated, singular and too-few-cluster cases return diagnostics rather than false successful estimates; changing each regression option changes its recorded specification; repeat run reproduces outputs within documented numerical tolerance. No causal/suppression conclusion from observed releases alone. **Evidence:** #1; README's clustered-SE statement exceeds current implementation.

### FBA-13 — Automated annotation quality and robustness

**Prerequisites:** FBA-09, FBA-12. **Deliver:** synthetic labeled fixtures, automated repeat/model agreement and party-swap symmetry checks, full-text versus truncation comparison, prefilter-loss audit, sensitivity to dates/transitions/thresholds/duplicates/missingness/source/agency, equal-duration term windows, normalized pooled administrations and source coverage/disclosure lag. No mandatory human annotation or manual corpus labeling. Predefine release thresholds and report failures rather than choosing thresholds after seeing partisan results. **Acceptance:** deliberately asymmetric/broken labeler is detected; repeated automated QA and all-corpus accounting are reproducible; report unresolved measurement validity and observational limits. QA samples do not substitute for all-document processing.

### FBA-14 — Full shared-corpus coverage and research eligibility review

**Prerequisites:** FBA-06, FBA-11, FBA-13; **EXT-CORPUS**. **Deliver:** Review actual FC-13/FC-14 evidence and analytical snapshot census by source family, agency, year, administration, jurisdiction, type and disclosure category. Reconcile imported/labeled/excluded/waiting/blocked counts to the shared database and report known gaps. **Acceptance:** pin producer/schema/snapshot and coverage audit; no eligible source class silently omitted because legacy code only handled MuckRock; every record gets an inclusion/exclusion/state reason; combined-corpus results are primary and source-strata robustness is explicit; broad claims stay blocked until coverage evidence exists. **Evidence:** A MuckRock-only result or nominal Aggregator readiness checkbox is insufficient.

### FBA-15 — Scalable analytical operations, recovery and release packaging

**Prerequisites:** FBA-03, FBA-05, FBA-10, FBA-12. **Deliver:** Package bounded-memory corpus consumption, labeling/model jobs and analytical assembly with read-only archive access, durable analysis state, budgets/backlog monitoring and isolated restore. Benchmark selected storage/model path at documented scale; archive crawling/backups stay upstream. **Acceptance:** restore analysis ledger/labels/results and reconnect to the same snapshot with no lost provenance or duplicate billing; missing archive pauses with no direct-crawl fallback; large fixture stays in declared memory/concurrency limits; rollback preserves newer accepted results. **Evidence:** No new raw archive, archive write credentials or paid hosting is inferred.

### FBA-16 — Reproducible release and independent handoff evidence

**Prerequisites:** FBA-01–15; authorized bounded external checks where applicable. **Deliver:** release manifest, test log, analytical report and limitations, current PR mapping, source/corpus/model versions, operating instructions, credential owner/access checklist without secrets and next-ready work. **Acceptance:** fresh account follows section 7 and reproduces fixture outputs; authorized real run retains every source/label/exclusion lineage; audit distinguishes merged, tested, run, deployed (if applicable), research-eligible and historically verified. Never replace remaining gates with an unconditional certificate.

## 6. Registry semantics and change control

`registry/prs.json` schema v1 is canonical. `roadmapVersion: 3` retains 16 work IDs and applies the dated owner corpus-architecture correction. Titles/tasks now specify shared-corpus consumption; no duplicate acquisition program or guessed historical PRs are introduced. Each has `origin: addition` because the numbered plan is newly reconstructed; that does **not** mean the underlying product requirements are new. `scopeBasis`, `requirementIds` (including direct current BR-11), `priorEvidencePrs`, `dependsOn`, `externalPrerequisites` and `planSection` preserve the distinction. `corpusAuthority` records the Aggregator repository, read-only interface and collection/analysis responsibility boundary. `prs` holds only actual completion PRs whose combined acceptance covers the unit. Empty completion mappings intentionally do not count old partial work as finished.

`planVerified: true` means this **current enumerated plan** has been checked for source/registry consistency under the current owner instruction. It does not mean original transcript fidelity, scientific validation, green runtime CI, operational readiness or user approval of reconstructed details as historical facts. `historicalPlanVerified: false` and `handoffCertified: false` make that explicit. Denominator = 16 units, not 21 observed PRs. Preserve #18 as closed-unmerged. Do not renumber work IDs to match GitHub PR numbers.

The former `registry/roadmap.json` is a redirect only; do not edit a second set of milestones or manually update Engine Room dashboard data. Engine Room reads this source registry and live GitHub PR state. A schema/parse check verifies compatibility, not the running dashboard's refresh. After implementation PRs, attach real completion numbers and refresh observed PR metadata; keep documented snapshots dated and never fabricate future merge times. Add scope via dated amendments and a version bump.

## 7. Independent-account execution and operations

1. Clone the repository using your own authorized GitHub connection. Read this plan, handoff status, canonical registry, README, dependency files, config and code. Inspect current `main`, all open PRs/branches, checks and upstream changes; the baseline SHA above may be superseded. No `AGENTS.md` was present at inspection; obey one if later added. Claim an unowned ready unit and avoid overlapping existing work.
2. Run current offline checks from repository root:

   ```bash
   python --version
   python scripts/validate_plan.py
   python -m compileall -q main.py foia_bias
   ```

   These are the only no-install baseline checks established by this documentation change. Full dependency installation/runtime tests have **not** been certified. Python 3.11+ is declared; use a virtual environment and `python -m pip install -r requirements.txt` when dependencies are available. Current CLI import can contact the roster source; do not claim `--help` is offline until FBA-01 passes. `python -m spacy download en_core_web_sm` and the Tesseract executable are external prerequisites, not installed by `requirements.txt` alone. FBA-07 adds/verifies the PDF renderer. Do not interpret successful imports/compilation as OCR/LLM/research acceptance.
3. **First next work: FBA-01**, establishing the Aggregator corpus consumer as the default, then FBA-02/FBA-03 and FBA-04 contract integration. Use `python -m unittest discover -s tests -v` only after FBA-01 creates a nonempty suite. Existing direct `ingest muckrock`/source-specific analysis commands are legacy migration tools, not the corrected target workflow. The corpus-consumer command and real FC-13 export must be implemented and documented before claiming live integration. Prototype with multi-source snapshots; do not finish an independent MuckRock collection adventure first.
4. Keep Python/source changes in small PRs with ID, prerequisite commit(s), tests, data migration effect and rollback. Chain passing, conflict-free PRs under the owner's standing workflow. Pause only affected work for failed checks, collisions, missing credentials, consequential product decisions or external gates; finish independent work. A documentation merge alone is not permission to begin a large corpus/model run.
5. Inputs for live analysis: authorized read-only Aggregator snapshot/export access, verified producer/schema/manifest/hash, complete historical roster, authorized model credentials/budget and durable analysis job/label/result storage. A MuckRock API token or independent crawler is not a production prerequisite for the corrected Bias architecture. Never copy secrets between accounts or commit them; record secure access ownership/location.
6. Back up analysis ledger/labels/configs/result manifests; pin a reproducible upstream snapshot and release, migrate a copy, compare counts/hashes and run fixture checks before authorized live acceptance. On failure stop jobs and restore analysis state without mutating the archive. Aggregator raw-store backups remain an upstream duty. Hosting is not implied; if selected, verify durable state and deployed revision independently.
7. Monitor imported/eligible/excluded/text-ready/upstream-text-waiting/labeled/failed/budget-blocked counts against the whole snapshot, per-source/agency/time coverage, durable analysis cursor, throughput, roster quality, model usage and diagnostics. Missing archive access or source text is explicit blocked state, never a trigger for a parallel crawler. Omit keys and unnecessary private text from logs.
8. Update HANDOFF_STATUS with exact evidence UTC timestamp, current base/release/PR state, tests actually run, upstream export version, blockers and next-ready ID. To resolve historical fidelity, recover source text or record owner ratification as a **new dated baseline**, never retroactively claim an original transcript was found.

## 8. Completion states

- **Documentation complete:** this plan, source distinctions, registry and independent starting instructions are consistent.
- **Implementation accepted:** each unit has explicit merged PR evidence and passing acceptance checks.
- **Research eligible:** EXT-CORPUS, automated full-text labeling/quality, analytical diagnostics and authorized real-data execution are evidenced.
- **Operationally reproducible:** durable runtime, migration, restore and live-source/provider checks have been exercised.
- **Unconditional handoff certified:** historical fidelity or clearly ratified new baseline, all applicable technical/operational evidence, and independent account reproduction are reviewed. This status is currently **false**.

