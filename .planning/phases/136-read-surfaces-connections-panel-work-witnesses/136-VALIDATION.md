---
phase: 136
slug: read-surfaces-connections-panel-work-witnesses
status: planned
nyquist_compliant: true
wave_0_complete: false  # Wave-0 artifacts are owned by plans 136-02 (shared honesty gate), 136-03 (gate-1 decision record + owner label file), 136-05 (preservation harness + pinned expectations) and 136-01 (budget caps)
created: 2026-07-31
revised: 2026-08-02  # re-scoped: 19 plans / 57 tasks / 9 waves (revision 1)
---

# Phase 136 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `136-RESEARCH.md` § Validation Architecture. Per-task rows are
> populated by `gsd-planner` once PLAN.md files exist.

**Dominant risk framing (from research).** This phase's dominant risk is *not*
"does the code compile" — the existing 394 discovery tests already cover the data
layer heavily. The dominant risks are (a) an unverified rebuild silently losing or
corrupting 268K claims / 297K evidence rows, and (b) a masking leak reaching a
surface or a public projection. Ordinary unit tests catch neither. This contract is
therefore weighted toward offline verification gates and render-smoke, not unit count.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.2 (repo-wide, no per-module override) |
| **Config file** | `pyproject.toml` → `[tool.pytest.ini_options]` (deliberately no default-exclude `addopts`) |
| **Quick run command** | `python -m pytest tests/test_discovery_service.py tests/test_discovery_band_labels.py -x` |
| **Full suite command** | `python -m pytest tests/ -k discovery -x` |
| **Estimated runtime** | Quick: **8.3 s** (70 tests). Full discovery suite: **109 s** (394 passed, 3 skipped, 5654 deselected). Both measured 2026-07-31 on this machine. |
| **Required env** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` — a bare run pops desktop GUI dialogs (see `feedback_full_suite_testing_windows`) |
| **Never** | `-n auto` — exhausts RAM loading Tantivy per worker |
| **Render-smoke convention** | `tests/render_smoke/test_<surface>_render_smoke.py`, modeled on the existing `test_help_methods_render_smoke.py` (Phase 135-02) |
| **Offline harnesses to extend** | `scripts/verify_discovery_sidecar.py` · `scripts/cert01_frame.py` · `scripts/verify_cert01_grading.py` · `scripts/bench_discovery.py` · `scripts/check_atlas_masking.py` |

---

## Sampling Rate

- **After every task commit:** `python -m pytest tests/test_discovery_service.py tests/test_discovery_band_labels.py -x` (~8 s)
- **After every plan wave:** `python -m pytest tests/ -k discovery -x` (~109 s)
- **Before `/gsd:verify-work`:** full discovery suite green **and** every offline gate below exited 0
- **Max feedback latency:** **9 s**

---

## Per-Criterion Verification Map

Derived from the 8 ROADMAP success criteria. Per-task rows (`Task ID | Plan | Wave | …`)
are appended by the planner; this table is the contract those rows must satisfy.

| SC | Requirement | Verification mechanism | Type | New or extend | Gate |
|----|-------------|------------------------|------|---------------|------|
| 1 | Rebuild preservation (D-02b) | `scripts/verify_rebuild_preservation.py <old> <new> --expected <pinned.json>` — per-table streamed content hash over `works`, `discovery_claim`, `discovery_evidence`, `witness_units`, `witness_unit_members`, `discovery_routing_audit`, allowlisting only the new coverage/novelty columns + the one authorized `tier_a` registry row | offline gate | **NEW** (sibling of `verify_discovery_sidecar.py`) | 2 |
| 1 | Expected-hash pinning (F-04 fix) | `136-REBUILD-PRESERVATION-EXPECTED.json` generated from the **currently-live** asset **before** the rebuild; hashes reuse `cert01_frame.py::population_hash()`/`cluster_map_hash()`. Never read expectations from the candidate's own manifest. | artifact | **NEW** | 1→2 |
| 1 | CERT-01 card binding (D-02c) | For every graded card, assert `claim_id`, `display_evidence_id`, `span_start/end`, `snapshot_hash` byte-identical old vs new. Hard fail. `verify_cert01_grading.py` check 10 is **never weakened** — publish a separate compatibility attestation instead. | offline gate | **NEW** check in the new script | 2 |
| 1 | D-02a lockstep (6 sites) | Fixtures proving **both** branches: PASS (authorized `ci_low=0.9084` + `measurement_status='measured_pass'` → `is_default_eligible` flips True for `tier_a`) and FAIL (`ci_low` < `STRICT_FLOOR`, status outside vocab, or any non-NULL `precision` → rejected). Includes a test pinning the dict-literal `**r`-override semantics in the `band_precision` INSERT. | unit | extend `test_discovery_schema.py`, `test_discovery_build.py` | 1 |
| 1 | Masking (DATA-05) | `check_atlas_masking.py --scan-sqlite / --scan-asset / --scan-repo --strict` over: rebuilt private asset, public projection, rendered HTML of all 5 surfaces, JSON payloads, copy/clipboard paths, error paths. `MASKING_SCAN_PATTERNS_FILE` unset must fail closed. | offline gate | extend (script exists) | 2, 4–6 |
| 1 | VIS-02 positive control | `tests/test_vis02_positive_control.py` — seed a throwaway copy of the public projection with one row carrying restricted (M-source) origin, scan with the **real** pattern file, assert **nonzero exit**. The built-in `--self-test` proves the matcher is encoding-robust but does **not** prove a real leak would be caught. | integration | **NEW** | 2 |
| 1 | VIS-01 closed graph | FK closure, no unreachable works, routing-audit rows, counts, aggregates, sort behaviour and auxiliary tables all projected and verified — not claim rows alone. Public/private row-count reconciliation recorded in a new dated frame doc. | offline gate | extend `verify_discovery_sidecar.py` | 2 |
| 2 | Panel display rules (D-13a/b/d/e/f/g/h/i) | Pure-function tests over fabricated claim-row fixtures: collapse by `canonical_work_id`; identical-span group extraction; short-passage bucketing; three-bucket assignment. | unit | extend | 4 |
| 2 | D-13g routing bug | Regression test for the exact mockup symptom: a `human_confirmed` row with `routing_status='review_only'` must render (with a low-coverage note), not be filtered out in SQL before `is_default_eligible` runs. | unit | **NEW** | 4 |
| 2 | D-13 envelope (F-14) | `{status, items, total}` — assert timeout/overload/absent-sidecar are distinguishable from a genuine zero, and that a genuine zero hides the entry control while an outage shows a visible retry state. | unit + render-smoke | **NEW** | 4 |
| 3 | Offset renderer (D-12) | Byte-for-byte fixture with `&`/`<`/`>` **before** the target span, proving slice-then-escape is correct where `highlight_text` (escape-then-substitute) would corrupt offsets. Plus per-side fail-closed on `snapshot_hash` drift: identification + tier still render, span withheld. Plus the b-side structural-absence copy (permanent note, *not* a drift error). | unit | **NEW** | 6 |
| 4 | `/work/{id}` pagination | Sort stability across pages (page 2's first row never duplicates/skips page 1's last) with the tie-break extended to the new display fields. Unit counts asserted against the corrected F-11 figures (heaviest work 4,796 manuscripts / 4,637 units — **not** 13,038 claim rows). | unit | extend | 4 |
| 5 | Findings-page perf (F-10) | Re-run the representative novelty/tier/coverage ordering via `bench_discovery.py::bench_findings_page()` after D-10a's materialized `band_rank`/`coverage_ppm` + indexes land. **Known failing baseline: 3.41–3.55 s vs the 1.5 s cap** — strongest kind of perf test, it already caught something real. | perf | extend `bench_discovery.py` | 5 |
| 5 | Catalogue honesty (D-18/D-21) | Assert catalogued FJMS titles and computed neutral titles are never rendered under a shared "identified as" wording; "copy of" / "quotes" / "witness of" absent from display. | render-smoke | **NEW** | 5 |
| 6 | Novelty fail-closed (D-23a) | One forced-failure test per named path — source unavailable, unnormalizable identifier, truncated snapshot, stale cache, model abstention — each asserting `indeterminate`, never silently `not_found`. | unit | **NEW** | 6 |
| 6 | One result per claim | New verifier check: every claim with ≥2 evidence rows agrees on the novelty tri-state. (665 of 29,054 multi-evidence claims currently disagree on the legacy boolean.) | offline gate | extend `verify_discovery_sidecar.py` | 2/6 |
| 6 | LLM contract (D-23c) | Assert committed constants for prompt hash, model+version (`gemini-3.6-flash`, `reasoning:{effort:"low"}`) and normalized-input hash appear literally in the request payload, so a silent model downgrade fails CI rather than surfacing in a cost report. Structured abstention → `indeterminate` via a test double. | unit | **NEW** | 6 |
| 7 | No precision percentage anywhere | Render-smoke over EN **and** HE rendered text of `/help` methods + all 4 new surfaces: no `\d+(\.\d+)?%` and no `\[\s*0\.\d+\s*,\s*0\.\d+\s*\]`. Extends the existing no-"certified" gate technique. | render-smoke | extend + **NEW** | 6 |
| 7 | Review badge suppressed (D-13f) | Assert "Expert-reviewed" / "נבדק בידי מומחה" never appears on any new surface until the 121-row provenance task closes separately. | render-smoke | **NEW** | 4 |
| 8 | Flag-off / absent cleanliness | Per surface: (a) flag OFF → absent, zero errors; (b) flag ON + sidecar absent → same; (c) flag ON + query timeout → visible temporary-unavailable + retry, **not** a silent empty list. Copy the proven `atlas_preview_available()` early-return pattern; (c) is the one genuinely new test class. | render-smoke | **NEW** | 4–6 |
| 8 | Budgets versioned | New findings-page row in `docs/specs/discovery-budgets.md` §1.2 **and** a new build-time rebuild-preservation budget section. No unversioned number anywhere in code. | doc gate | extend | 1, 2 |

*Status legend for planner-appended task rows: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Nothing below exists yet; each blocks the criterion it serves.

- [ ] `scripts/verify_rebuild_preservation.py` — the exact allowlisted old/new diff (SC 1)
- [ ] `136-REBUILD-PRESERVATION-EXPECTED.json` — pinned from the **currently-live** asset **before** the rebuild begins (SC 1; ordering is load-bearing)
- [ ] `tests/test_vis02_positive_control.py` — seeded-leak control, distinct from `--self-test` (SC 1)
- [ ] `tests/render_smoke/test_panel_render_smoke.py` — no discovery *UI* render-smoke exists yet (SC 2/3/8)
- [ ] `tests/render_smoke/test_work_page_render_smoke.py` (SC 4/8)
- [ ] `tests/render_smoke/test_findings_page_render_smoke.py` (SC 5/8)
- [ ] `bench_discovery.py::bench_findings_page()` probe (SC 5)
- [ ] `docs/specs/discovery-budgets.md` — findings-page cap (none exists today) + build-time rebuild-preservation budget (SC 8)
- [ ] A pinned, hash-recorded novelty-verdict-cache handoff artifact — the LLM-gate scripts have **no committed consumer** (see the gitignored-path risk below) (SC 6)

---

## Manual-Only Verifications

| Behavior | Requirement | Why manual | Test instructions |
|----------|-------------|------------|-------------------|
| Three disclosure levels render as three visually distinct, correctly-nested containers | PANEL-01/02 | Headless pytest misses async NiceGUI render (`feedback_nicegui_render_smoke_gap`); flex/height and nesting bugs are visual | Load a browse page for each of the **7 mockup manuscripts** (the standing regression set per D-05) and confirm bucket nesting, collapse state and counts |
| On-demand per-work expansion fires without a stale `page_client` | PANEL-02 | `run.io_bound` loses NiceGUI context and `ensure_future` empties the slot stack (`reference_io_bound_safe_storage_trap`) | Click the expansion on a page with ≥2 works; confirm rows load and no `RuntimeError` in the log. Bind `page_client` at render time |
| RTL / bidi rendering of match-framing strings and the U+05BE maqaf | I18N, D-21 | Bidi correctness is visual, not assertable from a string | Switch UI to Hebrew, inspect each surface's rows and section headers |
| Mobile layout of all 4 surfaces | — | `/atlas` came in ~68% mobile (`project_atlas_mobile_majority`); phone-first is the house rule | Phone-width check per surface, including the nested expansions and filter controls |
| Interaction-created strings masking scan | DATA-05 | Browser-DOM capture is the **only** surface that sees strings created by interaction. STATE.md records this capture was **SKIPPED for the atlas** because Playwright wasn't installed locally | Install Playwright and run the DOM capture, **or** explicitly record the skip as a known gap — do not silently drop it a second time |
| Owner approval before the paired asset-first deploy | SC 1, D-02/gate 3 | Explicit owner gate by decision, not a test | Present the verification report + attestation; deploy DBs **before** code |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or a Wave 0 dependency
- [ ] Sampling continuity: no 3 consecutive tasks without an automated verify
- [ ] Wave 0 covers all MISSING references above
- [ ] No watch-mode flags
- [ ] Feedback latency < 9 s (quick run measured 8.3 s)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending

---

## Carried risks from research (planner must account for these)

1. **`scripts/discovery_identified_gate.py` and `scripts/title_gate_llm.py` are NOT in the repo.** They live in the **gitignored** `same_work_spike/probe/scripts/` tree (`git check-ignore` → `.gitignore:212: same_work_spike/`; `git ls-files` returns nothing). CONTEXT.md cites them as if committed. Gate 6's novelty wiring cannot depend on an uncommitted script — it needs either a committed consumer or a pinned, hash-recorded handoff artifact.
2. **`_CatalogFacetWorker` is the wrong model.** It is a PyQt6 `QThread` in `genizah_app.py` (desktop). The correct web model for the findings page's counts is `/catalog-browse`'s own `_fetch_results_blocking()` via `await run.io_bound(...)`, already in `web/pages/catalog_browse.py`.
3. **`web/discovery.py` is no longer caller-free** — `web/main.py:715-719` already calls three of its functions for the `/help` methods section. Cosmetic; the four PANEL/WORK read paths *are* still unconsumed.
4. **Line numbers in `web/main.py` drifted ~16–20 lines** (nav now 1769-1789, routes 2102-2923). Use `grep -n "^@ui.page"` at execution time; trust neither CONTEXT.md's nor research's numbers.
5. **`coverage_ppm` is already computed and then discarded** (`_attach_coverage` → `spec["coverage"]`, never reaches `_mk_evidence`'s return dict). Persisting it is a small, low-risk change — not new metric design.
6. **Rebuild-preservation runtime is unmeasured.** No committed doc records a full-table scan wall-clock at this row count. Measure it at gate 2 and version the number; do not guess a cap first.

---

## Per-Task Verification Rows (repopulated by the planner, 2026-08-02 - RE-SCOPE, revision 1)

**Supersedes the 31-plan / 26-wave table.** The re-scoped phase is **19 plans / 57 tasks / 9 waves**.
PANEL-03, WORK-01 and WORK-02 moved to Phase 136.1, and `w_start`/`w_end` plus the Sefaria versemap
resolution were trimmed out of the rebuild - so the rows for the archived plans 136-08, 136-09, 136-17
and 136-22 through 136-26 no longer apply here.

**Revision 1 (plan-checker round 1):** every wave-1 owner decision is consolidated into ONE sitting in
136-03, which now carries four tasks (evidence &rarr; decision checkpoint &rarr; label checkpoint &rarr;
record); 136-04 lost its checkpoint, became autonomous and moved to wave 2. The phase now has exactly
**two** blocking owner checkpoints, both listed below.

| Task ID | Plan | Wave | SC | Type | Automated verify | Status |
|---------|------|------|----|------|------------------|--------|
| 136-01-T1 | 136-01 | 1 | 1,7,8 | doc gate | `python -c "import io; t=io.open('.planning/REQUIREMENTS.md',encoding='utf-8').read();...` | &#9744; |
| 136-01-T2 | 136-01 | 1 | 1,7,8 | doc gate | `python -c "import io; t=io.open('docs/specs/discovery-band-labels-v1.md',encoding='ut...` | &#9744; |
| 136-01-T3 | 136-01 | 1 | 1,7,8 | doc gate | `python -c "import io; t=io.open('docs/specs/discovery-sidecar-schema-v1.md',encoding=...` | &#9744; |
| 136-02-T1 | 136-02 | 1 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-02-T2 | 136-02 | 1 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-02-T3 | 136-02 | 1 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-03-T1 | 136-03 | 1 | 2,5,6 | evidence + consolidated owner gate | `python scripts/discovery_gate1_evidence.py --help && python -c "import io; t=io.open(...` | &#9744; |
| 136-03-T2 | 136-03 | 1 | 2,5,6 | evidence + consolidated owner gate (checkpoint) | `echo "CHECKPOINT: owner replies to A (five decisions), B (spend), C (eval size) and D...` | &#9744; |
| 136-03-T3 | 136-03 | 1 | 2,5,6 | evidence + consolidated owner gate (checkpoint) | `echo "CHECKPOINT: owner verdicts recorded per case; unanswered cases explicitly liste...` | &#9744; |
| 136-03-T4 | 136-03 | 1 | 2,5,6 | evidence + consolidated owner gate | `python -c "import io,json; t=io.open('.planning/phases/136-read-surfaces-connections-...` | &#9744; |
| 136-04-T1 | 136-04 | 2 | 6 | unit + funnel run | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_n...` | &#9744; |
| 136-04-T2 | 136-04 | 2 | 6 | unit + funnel run | `python scripts/discovery_novelty_funnel.py --help && GITHUB_ACTIONS=true QT_QPA_PLATF...` | &#9744; |
| 136-04-T3 | 136-04 | 2 | 6 | unit + funnel run | `python -c "import io; r=io.open('.planning/phases/136-read-surfaces-connections-panel...` | &#9744; |
| 136-05-T1 | 136-05 | 1 | 1 | offline gate | `python scripts/verify_rebuild_preservation.py --help && python -c "import io; s=io.op...` | &#9744; |
| 136-05-T2 | 136-05 | 1 | 1 | offline gate | `python -c "import json,io; d=json.load(io.open('.planning/phases/136-read-surfaces-co...` | &#9744; |
| 136-05-T3 | 136-05 | 1 | 1 | offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_rebuild_pre...` | &#9744; |
| 136-06-T1 | 136-06 | 2 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_b...` | &#9744; |
| 136-06-T2 | 136-06 | 2 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "verify_disc...` | &#9744; |
| 136-06-T3 | 136-06 | 2 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_s...` | &#9744; |
| 136-07-T1 | 136-07 | 2 | 2,5 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_m...` | &#9744; |
| 136-07-T2 | 136-07 | 2 | 2,5 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_g...` | &#9744; |
| 136-07-T3 | 136-07 | 2 | 2,5 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_m...` | &#9744; |
| 136-08-T1 | 136-08 | 2 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_v...` | &#9744; |
| 136-08-T2 | 136-08 | 2 | 1 | unit + offline gate | `python scripts/project_discovery_public.py --help && python -c "import io; s=io.open(...` | &#9744; |
| 136-08-T3 | 136-08 | 2 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_vis01_proje...` | &#9744; |
| 136-09-T1 | 136-09 | 2 | 1,5 | offline gate + unit | `python scripts/curate_work_domains.py --help && GITHUB_ACTIONS=true QT_QPA_PLATFORM=o...` | &#9744; |
| 136-09-T2 | 136-09 | 2 | 1,5 | offline gate + unit | `python scripts/curate_work_domains.py --validate discovery_data/work_domains-v1.json ...` | &#9744; |
| 136-09-T3 | 136-09 | 2 | 1,5 | offline gate + unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_work_domain...` | &#9744; |
| 136-10-T1 | 136-10 | 3 | 2,5 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_d...` | &#9744; |
| 136-10-T2 | 136-10 | 3 | 2,5 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "translation...` | &#9744; |
| 136-10-T3 | 136-10 | 3 | 2,5 | unit | `python -c "import io,re; s=io.open('web/static/common.css',encoding='utf-8').read(); ...` | &#9744; |
| 136-11-T1 | 136-11 | 3 | 1,8 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_b...` | &#9744; |
| 136-11-T2 | 136-11 | 3 | 1,8 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_b...` | &#9744; |
| 136-11-T3 | 136-11 | 3 | 1,8 | unit + perf | `python scripts/bench_discovery.py --help && python -c "import io; s=io.open('scripts/...` | &#9744; |
| 136-12-T1 | 136-12 | 4 | 1,6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_b...` | &#9744; |
| 136-12-T2 | 136-12 | 4 | 1,6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_b...` | &#9744; |
| 136-12-T3 | 136-12 | 4 | 1,6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_s...` | &#9744; |
| 136-13-T1 | 136-13 | 5 | 1,8 | offline gate + deploy | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel...` | &#9744; |
| 136-13-T2 | 136-13 | 5 | 1,8 | offline gate + deploy (checkpoint) | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel...` | &#9744; |
| 136-13-T3 | 136-13 | 5 | 1,8 | offline gate + deploy | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel...` | &#9744; |
| 136-14-T1 | 136-14 | 6 | 2,5,8 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_s...` | &#9744; |
| 136-14-T2 | 136-14 | 6 | 2,5,8 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_s...` | &#9744; |
| 136-14-T3 | 136-14 | 6 | 2,5,8 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_f...` | &#9744; |
| 136-15-T1 | 136-15 | 7 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_p...` | &#9744; |
| 136-15-T2 | 136-15 | 7 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_p...` | &#9744; |
| 136-15-T3 | 136-15 | 7 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_p...` | &#9744; |
| 136-16-T1 | 136-16 | 7 | 5,8 | unit + render | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_findings_pa...` | &#9744; |
| 136-16-T2 | 136-16 | 7 | 5,8 | unit + render | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_findings_pa...` | &#9744; |
| 136-16-T3 | 136-16 | 7 | 5,8 | unit + render | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_findings_pa...` | &#9744; |
| 136-17-T1 | 136-17 | 8 | 2,8 | render-smoke + deploy | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "browse_enri...` | &#9744; |
| 136-17-T2 | 136-17 | 8 | 2,8 | render-smoke + deploy | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "discovery_p...` | &#9744; |
| 136-17-T3 | 136-17 | 8 | 2,8 | render-smoke + deploy | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-18-T1 | 136-18 | 8 | 5,6,8 | render-smoke + perf + deploy | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_findings_pa...` | &#9744; |
| 136-18-T2 | 136-18 | 8 | 5,6,8 | render-smoke + perf + deploy | `python -c "import io; s=io.open('scripts/bench_discovery.py',encoding='utf-8').read()...` | &#9744; |
| 136-18-T3 | 136-18 | 8 | 5,6,8 | render-smoke + perf + deploy | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-19-T1 | 136-19 | 9 | 8 | masking + docs | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/tes...` | &#9744; |
| 136-19-T2 | 136-19 | 9 | 8 | masking + docs | `PYTHONUTF8=1 python scripts/check_docs.py && python -c "import io; t=io.open('.planni...` | &#9744; |

### Wave-0 ownership

| Wave-0 artifact | Owning plan | Wave |
|---|---|---|
| `tests/render_smoke/discovery_honesty_gate.py` (the ONE shared no-numbers gate, imported by every later surface suite) | 136-02 | 1 |
| `scripts/discovery_gate1_evidence.py` + `136-GATE1-DECISIONS.md` (the constants three later plans cite) | 136-03 | 1 |
| `discovery_data/novelty_hardcase_labels-v1.json` (OWNER-supplied ground truth; `label_provenance` enforced downstream) | 136-03 | 1 |
| `scripts/verify_rebuild_preservation.py` | 136-05 | 1 |
| `136-REBUILD-PRESERVATION-EXPECTED.json` (pinned from the LIVE asset, BEFORE the rebuild) | 136-05 | 1 |
| `tests/test_vis01_projection.py` (leak + structural-absence controls) | 136-08 | 2 |
| Pinned novelty-verdict-cache handoff artifact | 136-04 | 2 |
| `bench_discovery.py::bench_findings_page()` | 136-11 | 3 |
| `docs/specs/discovery-budgets.md` findings cap + build-time budget | 136-01 (caps) / 136-11, 136-13, 136-18 (measured) | 1 / 3, 5, 8 |
| `tests/render_smoke/test_panel_render_smoke.py` | 136-17 | 8 |
| `tests/render_smoke/test_findings_render_smoke.py` | 136-18 | 8 |
| `tests/render_smoke/test_discovery_masking_sweep.py` | 136-19 | 9 |

### Positive controls - the assertions that must be proven able to fail

| Control | Plan | What it seeds |
|---|---|---|
| No-numbers gate (methods page) | 136-02 | A precision figure plus an interval; a stored vocabulary key; a bare percentage without the matched-letter qualifier; a NEGATED prohibited word |
| Self-labelling guard | 136-04 | A label entry with no owner `label_provenance` (must be excluded from grading); a label file with ZERO owner entries (the harness must FAIL, not report a vacuous score); an edited label file failing its content hash |
| Novelty masking | 136-04 | An adversarial-input table asserting the restricted name never appears for unknown / `None` / malformed provenance codes |
| Rebuild preservation | 136-05 | In-stratum `matched_letters` drift, a deleted evidence row, an added claim, a changed `works` title, an unauthorized `band_precision` change, a repointed graded card, and a candidate self-attesting its own frame |
| D-02a both branches | 136-06 | An unauthorized `ci_low`, an out-of-vocabulary status, any non-NULL `tier_a` precision, `measured_pass` on an undeclared band |
| Second-implementation guard | 136-07 | A locally-defined band-set predicate in a scratch module |
| VIS-01 leak + structural absence | 136-08 | A restricted marker in a projected title, an orphaned FK, a copied total, a ruleless table, and an unset masking pattern file |
| Curation vocabulary + needs-ruling | 136-09 | An out-of-tree domain leaf, a non-canonical key, a duplicate assignment, and a `needs-ruling` row carrying a guessed leaf |
| Novelty ingestion | 136-12 | An unmasked provenance label, a disagreeing per-claim novelty fixture, a substituted verdict cache, a `kept_tie` row with a NULL `demoted_work_id` |
| Wrong-axis guard | 136-14 | A findings query path calling the manuscript-domain accessor |
| Panel model honesty | 136-15 | A precision figure in a row field, a stored vocabulary key in a chip, a row whose bucket disagrees with the shared rule |
| Panel render honesty | 136-17 | A precision figure in a rendered row, a stored vocabulary key in a chip, a review badge |
| Findings render honesty | 136-18 | "New discovery" plus a precision figure; an out-of-vocabulary domain plus a header mislabelled as the manuscript's domain; a rendered row whose bucket disagrees with the shared rule |
| Cross-surface masking | 136-19 | A restricted value seeded into a rendered row, a JSON payload, a copy/export output and an exception message - one per path class |

### Differing-case assertions (PANEL-02, revision 1)

PANEL-02's clause *"shows each side's own relation type when they differ"* is unamended by this phase,
so presence-of-field is not sufficient evidence. Two plans carry a fixture where the two sides'
relation kinds genuinely DIFFER, plus a same-relation companion:

| Plan | Assertion |
|---|---|
| 136-14 | `relations_differ` is true and both `claim_type` values are present and distinct on a differing fixture; false on a same-relation fixture |
| 136-17 | TWO distinct relation chips render with the correct label on each; exactly ONE renders when the kinds agree; neither chip carries a stored vocabulary key or a kind-keyed colour |

### Blocking owner checkpoints

| Plan | Wave | Tasks | Decision |
|---|---|---|---|
| 136-03 | 1 | T2 (`checkpoint:decision`) + T3 (`checkpoint:human-action`) | **ONE sitting, four groups:** (A) the five gate-1 decisions - D-13e bucket count, D-16 relation filter on the findings page, the D-13c threshold, the D-13b tie-break, the D-13d granularity separation rule; (B) the novelty funnel authorization (~$27, pinned model); (C) the hard-case evaluation-set size; (D) the `needs-ruling` domain posture. T3 then collects the owner's ground-truth verdict per hard case. |
| 136-13 | 5 | T2 (`checkpoint:decision`) | Approve the ONE authorized production redeploy on the gate evidence, flag OFF |

Waves are hard barriers in this project, so a checkpoint anywhere in a wave blocks the next wave
regardless of dependency edges. That is why owner latency is concentrated into one wave-1 sitting
rather than spread across two plans: the fix for barrier semantics is fewer round-trips, not more waves.

### What is no longer verified here (moved to Phase 136.1)

PANEL-03's offset renderer and evidence view, the `/work/{id}` service extension and page, and the
`/catalog-browse` integration. Their validation rows live with the six archived plans at
`superseded-2026-08-02/`. The licence-gate and M-source-locus positive controls move with them.

*Status legend: &#9744; pending &middot; &#9989; green &middot; &#10060; red &middot; &#9888; flaky*
