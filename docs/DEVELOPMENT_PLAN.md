# FOIA Bias Analysis — development and independent-account handoff plan

Plan version 2 · 2026-10-11 · Repository: `neophilism/FOIA_Bias_Analysis`

Start here, then read [HANDOFF_STATUS.md](HANDOFF_STATUS.md). The machine-readable work register is [registry/prs.json](../registry/prs.json); its work IDs match section 5 below. This document specifies completion work against the existing implementation, not a replacement project or a certificate that it works in production.

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

### Source and requirement ledger

Repository baseline inspected: [`fa9484b27d6a0af5e3805b4729f600701d6070b0`](https://github.com/neophilism/FOIA_Bias_Analysis/tree/fa9484b27d6a0af5e3805b4729f600701d6070b0). References below are relative to that baseline unless explicitly called current.

| ID | Preserved requirement or decision | Evidence / classification | Implemented or remaining |
|---|---|---|---|
| BR-01 | Study possible partisan differences in disclosed FOIA materials using wrongdoing and favorability hypotheses, treating D/R symmetrically. Policy disagreement alone is not wrongdoing. | R: [README](https://github.com/neophilism/FOIA_Bias_Analysis/blob/fa9484b27d6a0af5e3805b4729f600701d6070b0/README.md), `llm/prompts.py`, [PR #1](https://github.com/neophilism/FOIA_Bias_Analysis/pull/1); A: #71 | Scaffold present; FBA-09, 11–14 provide valid inputs and analysis. |
| BR-02 | Cover Clinton, Bush, Obama, Trump's first term and Biden: 1993 through the January 2025 transition. Do not silently extend to the second Trump administration. | A: #71; R: `processing/admin_mapping.py`, [PR #5](https://github.com/neophilism/FOIA_Bias_Analysis/pull/5); C: 2025-11-19 request for earlier-than-2010 coverage | Configuration still starts at 2010 and ends analytical years at 2024; FBA-01, 08, 14. |
| BR-03 | Retrieve completed MuckRock requests with files without political-keyword retrieval filtering; acquire actual released files through communications/files and extract their text. Metadata alone is insufficient. | R: [PR #13](https://github.com/neophilism/FOIA_Bias_Analysis/pull/13), `data_sources/muckrock_client.py`; C: 2025-11-18/19 messages | Existing path is partial; FBA-03–07. |
| BR-04 | Fully automated all-document LLM assessment; no human annotation campaign or sampled substitute for processing all eligible documents. | C: 2025-11-18 16:06 UTC; A: full-text labeling in #71 | Current keyword/NER rejection and text truncation conflict; FBA-09, 10, 13. Automated QA sampling may test quality but cannot replace corpus processing. |
| BR-05 | Preserve local raw files/text caches; avoid duplicate downloads; interrupted work resumes from durable progress. | R: [PR #14](https://github.com/neophilism/FOIA_Bias_Analysis/pull/14), [#15](https://github.com/neophilism/FOIA_Bias_Analysis/pull/15), [#16–21 history](https://github.com/neophilism/FOIA_Bias_Analysis/pulls?q=is%3Apr); C: 2025-11-18/19 | Keep downloaded artifacts and legacy checkpoints; FBA-03, 05, 10, 15 harden them. |
| BR-06 | Use historical congressional membership and presidents/vice presidents, not a tiny static actor list presented as complete. | R: [PR #10](https://github.com/neophilism/FOIA_Bias_Analysis/pull/10), [#12](https://github.com/neophilism/FOIA_Bias_Analysis/pull/12), `politics_filter.py`; C: 2025-11-18 | FBA-08 must preserve term/party changes and flag fallback coverage. |
| BR-07 | Keep agency logs, reading rooms and FOIA.gov annual data as distinct source classes; analyze release content separately from request descriptions and aggregate statistics. | R: README, `data_sources/*`, `pipeline.py`; A: #71 | Source scaffolds exist; FBA-02, 06, 11. Separation is E clarification needed for valid inference. |
| BR-08 | Maintain wrongdoing/favorability regressions, agency/year effects and robustness checks; quantify uncertainty and limitations. | R: `analysis/*`, `config.yaml`; A: #71 | FBA-11–14; configuration is not proof every option is implemented. |
| BR-09 | FOIA Aggregator broad-corpus coverage precedes substantial analytical advancement and broad statistical claims. Planning, deterministic repairs and fixtures may proceed. | R: pinned Aggregator plan below; C: owner message 2026-10-10 14:01 UTC | External gate EXT-CORPUS, not satisfied by this document or Aggregator's plan existence. |
| BR-10 | Preserve provenance, lawful source access and privacy; focus on government conduct without unnecessarily republishing private individuals' details. | A: #71; C: 2025-11-19 privacy discussion | FBA-02, 05, 09, 15–16. Concrete safeguards are E. |

**Exact upstream evidence:** [FOIA_Aggregator plan at `62fe82950963dcc0dde2003f094047d65bf79a9d`](https://github.com/neophilism/FOIA_Aggregator/blob/62fe82950963dcc0dde2003f094047d65bf79a9d/docs/DEVELOPMENT_PLAN.md), especially FC-03, FC-08, FC-13, FC-14 and downstream gating. This pins the inspected contract, not a deployed API/version or completed milestone. Require an actual export manifest with producer commit, schema version, timestamp, hashes, source/date coverage, failures and FC-14 review evidence. No shared database write access or particular API endpoint is assumed.

**Unresolved historical detail:** exact treatment of date-only January 20 records, original sampling/estimand definitions, and exact historical model choices are not transcript-certified. Preserve the scope above. E default: use exact event timestamps when available with administration boundaries; flag date-only transition-day records as ambiguous and report both include/exclude sensitivity instead of assigning a party by guess. Do not borrow Send_FOIA_Log_Requests' campaign years or Aggregator's B2 quota as requirements for this repository.

## 2. Product specification and completion boundary

Produce a reproducible, source-linked corpus of eligible released documents, automated labels and transparent statistical reports. Maintain separate agency request-log and annual-statistics datasets for context and possible denominator analysis. Acquisition has no partisan keyword preselection. Every eligible acquired document reaches a recorded extraction/labeling state; every successfully extracted document's full text is assessed, including text beyond the first 20,000 characters. A failed, inaccessible, unsupported or budget-blocked document is never reported as neutral or successfully assessed.

The two hypothesis families are (a) opposition exposure / incumbent suppression of material alleging wrongdoing and (b) opposition disadvantage / incumbent advantage in favorability. They are hypotheses, not predetermined findings. A disclosed-documents-only corpus measures associations among **observed disclosures**. It cannot by itself identify unreleased content, prove suppression, estimate release probability for all requests, or establish causation. Models requiring a request-level denominator must use linked logs with outcome/missingness evidence; otherwise label that estimand unavailable. State/local MuckRock records must retain jurisdiction and be separated from federal executive-administration inference.

Artifacts: immutable source/file manifest; bounded raw file storage and extracted page text; automated document/chunk labels with provenance; request/log linkage; versioned analytical dataset; diagnostics and coverage/exclusion tables; executable report command and machine-readable run status. No dashboard, mail sender, automatic political publication, cloud database purchase or new service is required by this recovered scope. Optional database upload in config is an extension, not existing functionality.

## 3. Existing implementation and verified gaps

Current stack: Python >=3.11, Typer CLI, Requests, Pandas/Parquet, pdfplumber/Tesseract, spaCy, OpenAI client wrapper and statsmodels. `requirements.txt` and `pyproject.toml` contain lower bounds, not a reproducible lock. No test directory, application CI workflow or deployment manifest exists at the baseline. No production service was verified. Historical PR bodies generally report `compileall`, which checks syntax only.

| Current code/evidence | Limit requiring completion work |
|---|---|
| `main.py` has run, four ingest commands and two analyze commands. | Importing `politics_filter.py` loads a roster and may access the network even for CLI help; establish offline/import-safe startup in FBA-01. |
| `config.yaml` / `config.json` start MuckRock at 2010, cap at 10,000 requests and analyze 2010–2024. | These are current defaults, not complete 1993–January 2025 coverage. Caps must report incomplete progress. |
| MuckRock uses completed + has-files queries and communications/files fallback. | Embedded files can short-circuit communications enumeration; failed downloads are skipped. Full attachment accounting is unproved. |
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
| #9, #11, #13, #14, #15 | MuckRock retrieval/files, persistence and download reuse | FBA-03–05, 07 |
| #10, #12 (also #11, #14) | Congressional/actor sources and fallbacks | FBA-08 |
| #16, #17, #19, #20, #21 | Page/cursor checkpoint revisions | FBA-03, 10 |
| #18 | Closed without merge; similar proposed checkpoint change | Historical evidence only; never count as merged. |

These historical changes do not satisfy the broader completion acceptance criteria below. Their numbers are stored as `priorEvidencePrs`, not completion `prs`. Existing implementation is real; its existence is not evidence that a new remediation unit is finished.

## 4. Target architecture and data contracts (E)

Keep the current Python package and CLI. Separate discover → acquire → extract → label → assemble → analyze stages so discovery can finish without LLM credentials. Use immutable files and a single-writer transactional local ledger (SQLite is the initial engineering choice), with Parquet as versioned analytical export. Adopt versioned migrations; migrate, never discard, legacy `data/muckrock/state.json`, cached files and existing labels. Store provider keys only in the process secret environment.

| Entity | Required fields/invariants |
|---|---|
| Source/run | Source ID, jurisdiction, query and config hash, code revision, fetched time, roster version, date-window semantics, next stable cursor/page + intra-page offset, error counts, byte/token budget, incomplete/complete status. |
| Request/log row | Namespaced request ID, component and parent agency, submitted/decision/release dates independently typed, date evidence and precision, disposition, source URL/row ID, raw hash, requester reference kept private when unnecessary. |
| File/release | Source file + communication + request associations; URL, retrieval/access status, MIME, original hash, bytes, relative safe path, release/revision relation, acquisition time. Different redactions remain distinct. |
| Extraction | File hash, extractor/version/language, page or segment offsets, full text hash, OCR/text/mixed method, success/partial/unsupported/failed status, bounded error. No content silently lost. |
| Label | File and text hashes, full coverage chunk offsets, prompt/schema/model versions, configured and actual provider identifier, target party with temporal evidence, scores and uncertainty, structured rationale/evidence spans, status, retry count, usage/cost. Never equate an allegation with a proven act. |
| Analytical snapshot | Input manifest hashes, source/cohort rules, dates, missingness/exclusions, dedupe clusters, label version, estimand/model specification, diagnostics, coefficients/intervals, seed and output hashes. |

Commit durable stage outputs before acknowledging progress. Atomic filesystem rename alone does not transact labels, downloads and cursors. Use idempotent keys and a recovery reconciliation journal; prevent a second writer. Bound response size, redirects, extraction CPU/memory/time and decompression; reject path escape and private-network targets for externally supplied URLs. Treat document text as data, never tool instructions. Do not expose raw private details in logs or upload them to an external model without a recorded source/privacy decision.

## 5. Executable completion milestones

Each ID is one reviewable work package; split implementation across PRs if necessary and map **every required PR** explicitly. Prerequisites below form the registry dependency graph. New test filenames are deliverables, not claims that those tests already exist. Use Python's standard `unittest` as the initial test runner. Each unit requires passing relevant tests plus `python -m compileall -q main.py foia_bias` and `python scripts/validate_plan.py`.

### FBA-01 — Reproducible offline baseline and configuration

**Prerequisites:** this plan; existing main/PR review. **Deliver:** lock a tested Python/dependency environment, make CLI imports/`--help` offline, validate YAML/JSON equivalence and every effective setting; fixture-only mode with no outbound network or billable API. Record unsupported options instead of accepting inert settings. Create runtime test/CI entry points. **Acceptance:** fresh environment executes help and local fixture tests without secrets/network; malformed config fails before work; default sample rows cannot be labeled live corpus; full-scope dates and configurable bounded batches are explicit. No live all-source `run` as a smoke test. **Evidence:** prior PRs #1, #3–8.

### FBA-02 — Provenance ledger and stage data contracts

**Prerequisites:** FBA-01. **Deliver:** versioned contracts in section 4, migration/export tools and transactional ledger. Preserve request/file/communication linkage, source jurisdiction, failed items and old Parquet input. **Acceptance:** round-trip fixture preserves IDs, dates, nulls and hashes; legacy migration produces reconciled counts and backup; a log row, annual total and release file remain different record kinds. **Evidence:** #1, #2.

### FBA-03 — Lossless checkpoints and restart migration

**Prerequisites:** FBA-02. **Deliver:** durable labels/download ledger before cursor advancement, stable query snapshot with page/intra-page continuation, explicit changed-query rescan preserving cache, corrupt-state recovery and single-writer exclusion. Preserve the intent of #19 (no needless restart), without unsafe reuse of a cursor for a different query. **Acceptance:** kill after download, label save, page save and mid-page cap; resume yields exactly the fixture's expected ID set and prior labels, no duplicate paid work or overwritten historical Parquet; filter change and invalid checkpoint produce explicit recoverable states. **Evidence:** #15–17, #19–21; #18 unmerged is not a prerequisite.

### FBA-04 — Complete MuckRock request and attachment acquisition

**Prerequisites:** FBA-02, FBA-03. **Deliver:** inspect current official API behavior, then enumerate requests/communications/files through all pages, merge embedded and endpoint attachments, track release versus requester correspondence evidence, retry failed files durably and preserve tokenless access. **Acceptance:** multi-page nested fixture with duplicate, failed, empty, partial-embedded and missing-date records matches expected acquisition set; requests have no partisan search query; downloaded bytes actually exist; 401/403 stop or quarantine, 429 respects retry policy, cap reports unfinished. A later bounded lawful live probe must record endpoint/date and counts separately from fixtures. **Evidence:** #3, #5, #9, #11–15.

### FBA-05 — Safe storage and identity-preserving deduplication

**Prerequisites:** FBA-02, FBA-04. **Deliver:** atomic bounded downloads, namespaced content hashes and provenance associations, cache migration and configurable disk reserve. **Acceptance:** repeated acquisition is a no-op; interrupted/zero-byte/same-size-corrupt files are detected; same-name and missing-ID attachments cannot collide; `../` paths, unsafe redirects and over-limit responses fail safely; revised/redacted versions and cross-source associations survive. Do not delete originals just to achieve dedupe. **Evidence:** #14, #15.

### FBA-06 — Agency logs, reading rooms and contextual annual data

**Prerequisites:** FBA-02, FBA-05. **Deliver:** verified source inventory, typed log column mappings, CSV/XLSX and explicitly supported formats, reading-room pagination/provenance, metadata-only FOIA.gov import; consume a versioned Aggregator export through a fixture-defined contract. **Acceptance:** fixture-only agency logs never contact example domains; submitted date does not fill decision date; all adapter pages accounted; PDF/legacy unsupported cases enter a queue; annual statistics and request narratives cannot enter the release-content regression. Live source coverage remains unknown until measured. **Evidence:** #1, #2, #8.

### FBA-07 — Full-text extraction and OCR

**Prerequisites:** FBA-05. **Deliver:** MIME-aware PDF/text/HTML extraction, bounded image rasterization + OCR for scans/mixed PDFs, page text cache, provenance and unsupported/error states; additional formats via tested adapters rather than treating every file as PDF. **Acceptance:** digital, scanned, mixed, non-English, malformed and oversized fixtures; searchable evidence late in a long file survives; page count/text coverage and OCR failures are reported; restart reuses valid extraction without dropping pages. **Evidence:** #1, #9, #13–14.

### FBA-08 — Administration, temporal scope and historical actor mapping

**Prerequisites:** FBA-02, FBA-06. **Deliver:** exact administration intervals plus independent transition flags, explicit request/decision/release date policy, complete sourced congressional terms/party changes and executive actors with cached version hashes. **Acceptance:** fixtures around every 2001/2009/2017/2021/2025 boundary, 1993 lower bound, date-only ambiguity, unknown dates, party-switching and mixed/unknown actors; transition flag cannot change incumbent assignment. Roster fallback is degraded and cannot certify full coverage. **Evidence:** #5, #10–12, #14.

### FBA-09 — Automated all-document, full-text labeling

**Prerequisites:** FBA-07, FBA-08. **Deliver:** every successfully extracted eligible document goes through LLM assessment; token-aware full-coverage chunks and documented aggregation; strict schema/range/evidence validation, model/SDK capability check and provider-independent boundary where useful. Prefilters may prioritize work, never silently produce neutral final labels or discard documents. **Acceptance:** fixtures with decisive content after 20k characters and without early political keywords are processed; chunk map covers all text; malformed JSON, out-of-range scores, conflicting parties, model refusal and prompt injection yield explicit states. Deterministic fake provider tests prove orchestration only; live model accuracy remains separately unverified. **Evidence:** #1 and BR-03/04; existing classifier does not satisfy this unit.

### FBA-10 — Resumable labeling, usage controls and full-corpus accounting

**Prerequisites:** FBA-03, FBA-09. **Deliver:** durable jobs keyed by text/prompt/model hash, configured bounded concurrency/retries, per-run usage ceilings, billing ambiguity state, pause/resume and skipped-reason census. **Acceptance:** concurrent retries, crash after provider response and changed-model rerun cannot silently double-bill; no-key/budget cap pauses labeling while retaining acquired data; processed + queued + failed + blocked counts reconcile to eligible corpus. Actual budgets/providers require authorized inputs; this plan provides none. **Evidence:** #16–21 concern ingestion only, not completion of this unit.

### FBA-11 — Analytical cohorts and deterministic derived variables

**Prerequisites:** FBA-06, FBA-08, FBA-10. **Deliver:** versioned cohort builder, document/request/release unit distinctions, deterministic mixed-party/tie handling, configurable thresholds, jurisdiction separation, duplicate-cluster policy and exclusion/missingness report. **Acceptance:** golden synthetic fixtures reproduce exact rows and outcomes under shuffled input; January 2025 scope is not silently removed by max-year=2024; request description, annual data, partial text and unassessed records never become complete labels; thresholds actually affect output. **Evidence:** #1, #2.

### FBA-12 — Regressions, diagnostics and honest estimands

**Prerequisites:** FBA-11. **Deliver:** specified wrongdoing logit and favorability models, agency/year fixed effects and documented cluster covariance, incumbent-aligned favorability interpretation, confidence intervals, deterministic exports and unsuitable-data diagnostics. Distinguish disclosure-composition models from request-outcome models. **Acceptance:** synthetic null and known-signal data; party-label swap tests; empty, missing, separated, singular and too-few-cluster cases return diagnostics rather than false successful estimates; changing each regression option changes its recorded specification; repeat run reproduces outputs within documented numerical tolerance. No causal/suppression conclusion from observed releases alone. **Evidence:** #1; README's clustered-SE statement exceeds current implementation.

### FBA-13 — Automated annotation quality and robustness

**Prerequisites:** FBA-09, FBA-12. **Deliver:** synthetic labeled fixtures, automated repeat/model agreement and party-swap symmetry checks, full-text versus truncation comparison, prefilter-loss audit, sensitivity to dates/transitions/thresholds/duplicates/missingness/source/agency. No mandatory human annotation or manual corpus labeling. Predefine release thresholds and report failures rather than choosing thresholds after seeing partisan results. **Acceptance:** deliberately asymmetric/broken labeler is detected; repeated automated QA and all-corpus accounting are reproducible; report unresolved measurement validity and observational limits. QA samples do not substitute for all-document processing.

### FBA-14 — Upstream coverage and research eligibility review

**Prerequisites:** FBA-06, FBA-11, FBA-13; **EXT-CORPUS**. **Deliver:** consume verified Aggregator FC-13/FC-14 evidence, document known/missing coverage by administration, agency, jurisdiction, year, file type and disclosure category; reconcile any independently acquired MuckRock files to the manifest. **Acceptance:** pin producer commit/schema/export hashes and corpus audit; unknown cells remain unknown; demonstrate 1993–January 2025 inclusion where sources exist and explicit gaps elsewhere; representative acquisition/label counts reconcile. Without upstream evidence, fixture/design work may continue but broad substantive research execution/publication remains blocked. A fixed document count is not representative coverage.

### FBA-15 — Durable operations, recovery and release packaging

**Prerequisites:** FBA-03, FBA-05, FBA-10, FBA-12. **Deliver:** reproducible batch runtime, durable paths, source/rate/disk/cost monitoring, secrets handling, backups and isolated restore, versioned migrations, operator stop controls and incident runbook. No Render/Neon requirement is invented; any hosting decision must record existing resources, access and actual durability. **Acceptance:** restore ledger + files + label hashes to an isolated target and resume with no loss; restart under low disk/denied source/no model key produces clear blocked status; rollback to previous code/export without downgrading data destructively. Record measured restore duration and real environment separately from fixture acceptance.

### FBA-16 — Reproducible release and independent handoff evidence

**Prerequisites:** FBA-01–15; authorized bounded external checks where applicable. **Deliver:** release manifest, test log, analytical report and limitations, current PR mapping, source/corpus/model versions, operating instructions, credential owner/access checklist without secrets and next-ready work. **Acceptance:** fresh account follows section 7 and reproduces fixture outputs; authorized real run retains every source/label/exclusion lineage; audit distinguishes merged, tested, run, deployed (if applicable), research-eligible and historically verified. Never replace remaining gates with an unconditional certificate.

## 6. Registry semantics and change control

`registry/prs.json` schema v1 is canonical. `roadmapVersion: 2` introduces 16 explicit E completion units. Each has `origin: addition` because the numbered plan is newly reconstructed; that does **not** mean the underlying product requirements are new. `scopeBasis`, `requirementIds`, `priorEvidencePrs`, `dependsOn`, `externalPrerequisites` and `planSection` preserve the distinction. `prs` holds only actual completion PRs whose combined acceptance covers the unit. Empty completion mappings intentionally do not count old partial work as finished.

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
3. **First next work: FBA-01**, then FBA-02/FBA-03 before restarting significant ingestion. Use `python -m unittest discover -s tests -v` only after FBA-01 creates a nonempty suite; zero discovered tests is not a pass. Subsequent existing CLI commands include `python main.py ingest muckrock --config config.yaml` and `python main.py analyze wrongdoing --config config.yaml --source muckrock`; these are real commands but **not safe default smoke tests**: ingestion can contact sources and invoke paid labeling. There is currently no verified `--dry-run` flag. Add fixture mode before using these commands for CI.
4. Keep Python/source changes in small PRs with ID, prerequisite commit(s), tests, data migration effect and rollback. Chain passing, conflict-free PRs under the owner's standing workflow. Pause only affected work for failed checks, collisions, missing credentials, consequential product decisions or external gates; finish independent work. A documentation merge alone is not permission to begin a large corpus/model run.
5. Inputs for later live runs: verified source policy/endpoints; optional `MUCKROCK_API_TOKEN`; authorized `OPENAI_API_KEY` plus valid model/usage budget; complete roster cache; writable persistent raw/text/ledger paths; actual corpus/export evidence. `FOIA_DB_URI` is currently an optional unimplemented path, not a required secret. Never copy secrets between accounts or commit them. Record credential location/owner and access steps, not values.
6. Before a release, back up raw files, ledger, labels, configs and manifests. Stop the sole writer, pin release commit/environment, migrate a copy, compare counts/hashes, run fixture checks, then perform only authorized bounded live checks. On failure stop jobs and restore verified state to a new location; avoid resetting cursor alone or overwriting a more complete dataset. No web deployment is implied. If hosted later, verify deployed revision, durable volume and job liveness independently.
7. Monitor discovered/downloaded/extracted/labeled/failed/blocked counts; newest durable checkpoint; source errors/429; missing roster coverage; storage headroom; model cost and unresolved outcomes; analysis exclusions/diagnostics. Logs must omit keys and unnecessary private text. A process running or a model response returning HTTP success is not completion.
8. Update HANDOFF_STATUS with exact evidence UTC timestamp, current base/release/PR state, tests actually run, upstream export version, blockers and next-ready ID. To resolve historical fidelity, recover source text or record owner ratification as a **new dated baseline**, never retroactively claim an original transcript was found.

## 8. Completion states

- **Documentation complete:** this plan, source distinctions, registry and independent starting instructions are consistent.
- **Implementation accepted:** each unit has explicit merged PR evidence and passing acceptance checks.
- **Research eligible:** EXT-CORPUS, automated full-text labeling/quality, analytical diagnostics and authorized real-data execution are evidenced.
- **Operationally reproducible:** durable runtime, migration, restore and live-source/provider checks have been exercised.
- **Unconditional handoff certified:** historical fidelity or clearly ratified new baseline, all applicable technical/operational evidence, and independent account reproduction are reviewed. This status is currently **false**.
