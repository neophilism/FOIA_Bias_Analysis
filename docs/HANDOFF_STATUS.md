# FOIA Bias Analysis — handoff status

Documentation baseline: **2026-10-11**, corrected plan version **3**. Source/PR verification: **2026-10-11T20:39:09.617Z**. This is a dated evidence ledger, not a live service-status feed.

Start with [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) and [registry/prs.json](../registry/prs.json). Run `python scripts/validate_plan.py` from repository root. Work IDs, dependencies, source requirements and related historical PRs are in the registry; completion mappings must remain separate from historical partial implementation.


Latest correction verification: **2026-10-11T23:06:08.067Z**. Current pre-correction main is `52db8f9a0f661f73dbc058dbb503d45cc386613b`; 22 PR records now observed, 21 merged, #18 unmerged, 0 open. Older inventory below is explicitly the historical pre-documentation baseline.

## Owner correction incorporated

The October 11 owner correction makes FOIA_Aggregator's full database the authoritative Bias input. Aggregator owns collection/raw storage/archive deduplication/extraction; Bias owns read-only corpus consumption, all-document full-text labeling and statistics. MuckRock is one source within the archive. FBA-01–07/14/15 have been rewritten accordingly, with stable 16-unit denominator and explicit BR-11 source lineage.

BR-12 explains the historical cutoff: the ongoing second Trump term is incomplete, so it is excluded from the completed-term comparison to keep time periods comparable. The Aggregator can retain newer records. Primary comparison windows are completed four-year terms; pooled administrations use exposure normalization and coverage/lag checks.

The application still contains legacy direct-source code; this documentation correction does not claim a working corpus consumer. The next coding account must implement the shared-corpus input path, not complete an independent MuckRock crawler first. Live consumption depends on the actual FC-13 export/full-text contract and FC-14 coverage evidence.

## Verified starting point

- Inspected main: [`fa9484b27d6a0af5e3805b4729f600701d6070b0`](https://github.com/neophilism/FOIA_Bias_Analysis/tree/fa9484b27d6a0af5e3805b4729f600701d6070b0), before this documentation change. Do not assume this remains HEAD. The final documentation merge is linked from [audit #71](https://github.com/neophilism/Engine-Room/issues/71); use current GitHub for subsequent changes.
- Historical PRs: 21 observed, 20 merged, #18 closed unmerged, 0 open at baseline. Existing source files and all PR metadata were inspected; registry `priorEvidencePrs` links them to relevant remediation, while empty `prs` means those broader completion units are unaccepted.
- Python CLI/ingestion/extraction/labeling/analysis scaffold exists. Baseline contained no `tests/`, runtime CI workflows, deployment manifest or `AGENTS.md`. GitHub Actions API returned zero workflow runs. The new plan validator is a documentation check, not an application test suite.
- No Render, Neon, object store, production data, live MuckRock/API/model call, full dependency installation or deployed revision was verified in this task. No existing infrastructure is asserted absent; its status is **unverified**.

## Documentation completed and validation scope

Comprehensive source-labeled plan; BR-01–12 requirement lineage; FBA-01–16 executable completion units aligned to shared-corpus analysis; dependencies, acceptance tests, current-code limitations, operations and recovery; canonical structured registry plus legacy redirect; README entry point and reproducible standard-library validator.

Checks for this documentation change:

```bash
python scripts/validate_plan.py
python -m compileall -q main.py foia_bias
```

The publication check also loads the registry with the real Engine Room `parseStructuredPrRegistry` consumer, pinned to Engine Room commit `5491b7a75bbd7f4d82b4575283ad3929310ce50b`. It checks the 16-unit denominator, historical PR state and absence of fabricated completion mappings; it does not test the running dashboard. **PASS at 2026-10-11T20:42:30Z:** plan/registry validation and the Engine Room parser check; Python source compilation also passed. These are documentation/syntax results only.

Concrete source probe: existing `get_admin_for_date('2021-01-21', 6)` returns Trump/R with transition=true; `transition_months=0` returns Biden/D. This confirms the FBA-08 assignment defect. The probe required no network/model call. Other findings in the plan are identified as code inspection, not executed end-to-end failures.

Correction validation: the version 3 plan/registry validator and actual Engine Room parser pass; milestone IDs/counts, dependencies, BR-11/BR-12 mappings and archive/analysis ownership are consistent. This validates documentation, not a running corpus adapter.

Runtime baseline: local Python 3.12.14 compiled source. Requests, spaCy, statsmodels, OpenAI, Typer and PyArrow were unavailable in the inspection environment; runtime setup and full CLI/ingestion/OCR/LLM/regression acceptance were not run. Do not convert compile success or the absence of CI into a passing runtime test claim.

## Remaining certification requirements

| Dimension | Current state | Required evidence |
|---|---|---|
| Standalone specification | Complete current reconstruction | Preserve plan/registry consistency on later edits. |
| Original history | Partial repository/audit/context recovery; not transcript-certified | Recover original texts or ratify a new dated baseline without claiming retroactive recovery. |
| Implementation acceptance | Partial scaffold and historical PRs; new completion units unaccepted | FBA-01–16 acceptance tests and exact completion PR/commit links. |
| Runtime/CI | Syntax checked; no application suite/CI verified | FBA-01 reproducible dependencies, offline fixtures and nonempty runtime tests/CI. |
| Upstream corpus | Plan contract pinned; completion/data contract not verified | Aggregator export with actual producer commit/schema/hash and FC-13/FC-14 evidence. |
| Scientific validity | Not established | Full-text all-document labels, automated quality/robustness, valid estimands/cohorts and diagnostics; limits on disclosure-only inference. |
| Operations | Unverified | Authorized source/model check, durable persistence, backup/restore, runtime/release evidence; hosting only if selected. |
| Independent account | Instructions present; independent reproduction not performed | New account executes setup, fixtures, recovery and applicable integration checks with its own authorized access. |

Exact upstream planning reference: [FOIA_Aggregator at `62fe82950963dcc0dde2003f094047d65bf79a9d`](https://github.com/neophilism/FOIA_Aggregator/blob/62fe82950963dcc0dde2003f094047d65bf79a9d/docs/DEVELOPMENT_PLAN.md). That SHA pins a document; it is not an upstream runtime version proven compatible with this application.

**Next ready unit: FBA-01 — corpus-consumer baseline**, then FBA-02/FBA-03/FBA-04 shared-corpus integration. Refresh open PRs and source changes first. Continue authorized fixture/configuration repair while the corpus gate is unresolved; do not launch substantive all-source/model processing from this handoff. Required future secrets/inputs are listed in plan section 7; secret values do not belong here. Unconditional handoff certification remains **NOT CERTIFIED**.

