---
phase: 136
slug: read-surfaces-connections-panel-work-witnesses
status: planned
nyquist_compliant: true
wave_0_complete: false  # Wave-0 artifacts are owned by plans 136-03 (preservation harness + pinned expectations) and 136-01 (budgets)
created: 2026-07-31
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

## Per-Task Verification Rows (populated by the planner, 2026-08-02)

31 plans / 93 tasks / 26 waves. Every task carries an `<automated>` verify command;
four are blocking owner checkpoints whose verify is the recorded reply.

| Task ID | Plan | Wave | SC | Type | Automated verify | Status |
|---------|------|------|----|------|------------------|--------|
| 136-01-T1 | 136-01 | 1 | 7,8 | doc gate | `python -c "import io,re,sys; t=io.open('.planning/REQUIREMENTS.md',encoding='utf-8').read(); ...` | ⬜ |
| 136-01-T2 | 136-01 | 1 | 7,8 | doc gate | `python -c "import io; t=io.open('docs/specs/discovery-band-labels-v1.md',encoding='utf-8').re...` | ⬜ |
| 136-01-T3 | 136-01 | 1 | 7,8 | doc gate | `python -c "import io,re; t=io.open('docs/specs/discovery-budgets.md',encoding='utf-8').read()...` | ⬜ |
| 136-02-T1 | 136-02 | 1 | 1 | doc gate + unit | `python -c "import io; t=io.open('docs/specs/discovery-sidecar-schema-v1.md',encoding='utf-8')...` | ⬜ |
| 136-02-T2 | 136-02 | 1 | 1 | doc gate + unit | `python -c "import io; t=io.open('docs/specs/discovery-deploy.md',encoding='utf-8').read(); [t...` | ⬜ |
| 136-02-T3 | 136-02 | 1 | 1 | doc gate + unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_schema_am...` | ⬜ |
| 136-03-T1 | 136-03 | 1 | 1 | offline gate | `python scripts/verify_rebuild_preservation.py --help && python -c "import ast,io; src=io.open...` | ⬜ |
| 136-03-T2 | 136-03 | 1 | 1 | offline gate | `python -c "import json,io; d=json.load(io.open('.planning/phases/136-read-surfaces-connection...` | ⬜ |
| 136-03-T3 | 136-03 | 1 | 1 | offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_rebuild_preservatio...` | ⬜ |
| 136-04-T1 | 136-04 | 1 | 2,5 | offline report + decision | `python scripts/discovery_gate1_evidence.py --out .planning/phases/136-read-surfaces-connectio...` | ⬜ |
| 136-04-T2 | 136-04 | 1 | 2,5 | offline report + decision (checkpoint) | `echo "CHECKPOINT: Owner reply recorded in the plan summary, quoting the chosen option id for ...` | ⬜ |
| 136-04-T3 | 136-04 | 1 | 2,5 | offline report + decision | `python -c "import json,io,hashlib; p='discovery_data/granularity_aliases.json'; b=io.open(p,'...` | ⬜ |
| 136-05-T1 | 136-05 | 2 | 1,7 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-05-T2 | 136-05 | 2 | 1,7 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "verify_discovery or...` | ⬜ |
| 136-05-T3 | 136-05 | 2 | 1,7 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-06-T1 | 136-06 | 3 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-06-T2 | 136-06 | 3 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-06-T3 | 136-06 | 3 | 1 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-07-T1 | 136-07 | 4 | 1,5 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_main_pool...` | ⬜ |
| 136-07-T2 | 136-07 | 4 | 1,5 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-07-T3 | 136-07 | 4 | 1,5 | unit + perf | `python scripts/bench_discovery.py --help && python -c "import io; s=io.open('scripts/bench_di...` | ⬜ |
| 136-08-T1 | 136-08 | 5 | 1,3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-08-T2 | 136-08 | 5 | 1,3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-08-T3 | 136-08 | 5 | 1,3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_work_offs...` | ⬜ |
| 136-09-T1 | 136-09 | 6 | 1,3 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_workref.p...` | ⬜ |
| 136-09-T2 | 136-09 | 6 | 1,3 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-09-T3 | 136-09 | 6 | 1,3 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_workref.p...` | ⬜ |
| 136-10-T1 | 136-10 | 7 | 1,5 | offline gate + unit | `python -c "import json,io,hashlib; p='discovery_data/work_domains.build.json'; b=io.open(p,'r...` | ⬜ |
| 136-10-T2 | 136-10 | 7 | 1,5 | offline gate + unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-10-T3 | 136-10 | 7 | 1,5 | offline gate + unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_work_doma...` | ⬜ |
| 136-11-T1 | 136-11 | 8 | 6 | unit + decision | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_novelty_c...` | ⬜ |
| 136-11-T2 | 136-11 | 8 | 6 | unit + decision (checkpoint) | `echo "CHECKPOINT: Owner reply recorded in the plan summary, quoting the chosen option id for ...` | ⬜ |
| 136-11-T3 | 136-11 | 8 | 6 | unit + decision | `python -c "import json,io,hashlib; p='discovery_data/novelty_verdicts.build.json'; b=io.open(...` | ⬜ |
| 136-12-T1 | 136-12 | 9 | 6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-12-T2 | 136-12 | 9 | 6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "verify_discovery" -q` | ⬜ |
| 136-12-T3 | 136-12 | 9 | 6 | unit + offline gate | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_novelty_i...` | ⬜ |
| 136-13-T1 | 136-13 | 10 | 1 | offline gate + integration | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_build.py ...` | ⬜ |
| 136-13-T2 | 136-13 | 10 | 1 | offline gate + integration | `python scripts/project_discovery_public.py --help && GITHUB_ACTIONS=true QT_QPA_PLATFORM=offs...` | ⬜ |
| 136-13-T3 | 136-13 | 10 | 1 | offline gate + integration | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_vis02_positive_cont...` | ⬜ |
| 136-14-T1 | 136-14 | 11 | 1 | offline gate | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-14-T2 | 136-14 | 11 | 1 | offline gate | `python -c "import io,re; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work...` | ⬜ |
| 136-14-T3 | 136-14 | 11 | 1 | offline gate | `python -c "import io; a=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-15-T1 | 136-15 | 12 | 1,8 | deploy + perf (checkpoint) | `echo "CHECKPOINT: Owner reply recorded in the plan summary, quoting the chosen option id for ...` | ⬜ |
| 136-15-T2 | 136-15 | 12 | 1,8 | deploy + perf | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-15-T3 | 136-15 | 12 | 1,8 | deploy + perf | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-16-T1 | 136-16 | 13 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-16-T2 | 136-16 | 13 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-16-T3 | 136-16 | 13 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-17-T1 | 136-17 | 13 | 3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_render.py -q` | ⬜ |
| 136-17-T2 | 136-17 | 13 | 3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_render.py...` | ⬜ |
| 136-17-T3 | 136-17 | 13 | 3 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_render.py...` | ⬜ |
| 136-18-T1 | 136-18 | 14 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_panel_mod...` | ⬜ |
| 136-18-T2 | 136-18 | 14 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_panel_mod...` | ⬜ |
| 136-18-T3 | 136-18 | 14 | 2 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_panel_mod...` | ⬜ |
| 136-19-T1 | 136-19 | 15 | 2,8 | unit + render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "browse and not desk...` | ⬜ |
| 136-19-T2 | 136-19 | 15 | 2,8 | unit + render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "browse_enrichment o...` | ⬜ |
| 136-19-T3 | 136-19 | 15 | 2,8 | unit + render-smoke | `python -c "import io; c=io.open('web/static/common.css',encoding='utf-8').read(); import re; ...` | ⬜ |
| 136-20-T1 | 136-20 | 16 | 2 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "discovery_rows or p...` | ⬜ |
| 136-20-T2 | 136-20 | 16 | 2 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "discovery_panel or ...` | ⬜ |
| 136-20-T3 | 136-20 | 16 | 2 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "translations or i18...` | ⬜ |
| 136-21-T1 | 136-21 | 17 | 2,7,8 | render-smoke + masking | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_panel_...` | ⬜ |
| 136-21-T2 | 136-21 | 17 | 2,7,8 | render-smoke + masking | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_panel_...` | ⬜ |
| 136-21-T3 | 136-21 | 17 | 2,7,8 | render-smoke + masking | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_panel_...` | ⬜ |
| 136-22-T1 | 136-22 | 17 | 3 | offline report + decision | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-22-T2 | 136-22 | 17 | 3 | offline report + decision (checkpoint) | `echo "CHECKPOINT: Owner reply recorded in the plan summary, quoting the chosen option id for ...` | ⬜ |
| 136-22-T3 | 136-22 | 17 | 3 | offline report + decision | `python -c "import io; t=io.open('.planning/phases/136-read-surfaces-connections-panel-work-wi...` | ⬜ |
| 136-23-T1 | 136-23 | 18 | 3 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "evidence" -q && pyt...` | ⬜ |
| 136-23-T2 | 136-23 | 18 | 3 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "browse and not desk...` | ⬜ |
| 136-23-T3 | 136-23 | 18 | 3 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_eviden...` | ⬜ |
| 136-24-T1 | 136-24 | 19 | 4 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-24-T2 | 136-24 | 19 | 4 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-24-T3 | 136-24 | 19 | 4 | unit | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_service.p...` | ⬜ |
| 136-25-T1 | 136-25 | 20 | 4,8 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "work_page or routes...` | ⬜ |
| 136-25-T2 | 136-25 | 20 | 4,8 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "work_page" -q && GI...` | ⬜ |
| 136-25-T3 | 136-25 | 20 | 4,8 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_work_p...` | ⬜ |
| 136-26-T1 | 136-26 | 21 | 5 | unit + render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_titles.py -q` | ⬜ |
| 136-26-T2 | 136-26 | 21 | 5 | unit + render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "catalog_browse" -q ...` | ⬜ |
| 136-26-T3 | 136-26 | 21 | 5 | unit + render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_catalo...` | ⬜ |
| 136-27-T1 | 136-27 | 22 | 5 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_findings_...` | ⬜ |
| 136-27-T2 | 136-27 | 22 | 5 | unit + perf | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_findings_...` | ⬜ |
| 136-27-T3 | 136-27 | 22 | 5 | unit + perf | `python -c "import io; s=io.open('scripts/bench_discovery.py',encoding='utf-8').read(); assert...` | ⬜ |
| 136-28-T1 | 136-28 | 23 | 5,8 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "findings or routes ...` | ⬜ |
| 136-28-T2 | 136-28 | 23 | 5,8 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "findings" -q && pyt...` | ⬜ |
| 136-28-T3 | 136-28 | 23 | 5,8 | render-smoke | `python -c "import io; c=io.open('web/static/common.css',encoding='utf-8').read(); i=c.find('f...` | ⬜ |
| 136-29-T1 | 136-29 | 24 | 5,6 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "findings" -q` | ⬜ |
| 136-29-T2 | 136-29 | 24 | 5,6 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -k "novelty or findings...` | ⬜ |
| 136-29-T3 | 136-29 | 24 | 5,6 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_findin...` | ⬜ |
| 136-30-T1 | 136-30 | 25 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_help_m...` | ⬜ |
| 136-30-T2 | 136-30 | 25 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_help_m...` | ⬜ |
| 136-30-T3 | 136-30 | 25 | 7 | render-smoke | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/ -q` | ⬜ |
| 136-31-T1 | 136-31 | 26 | 6,8 | integration + masking | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/ -q -k "pan...` | ⬜ |
| 136-31-T2 | 136-31 | 26 | 6,8 | integration + masking | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_discovery_masking_s...` | ⬜ |
| 136-31-T3 | 136-31 | 26 | 6,8 | integration + masking | `python scripts/check_atlas_masking.py --scan-repo --strict && python -c "import io; t=io.open...` | ⬜ |

### Wave-0 ownership

| Wave-0 artifact | Owning plan | Wave |
|---|---|---|
| `scripts/verify_rebuild_preservation.py` | 136-03 | 1 |
| `136-REBUILD-PRESERVATION-EXPECTED.json` (pinned from the LIVE asset, BEFORE the rebuild) | 136-03 | 1 |
| `tests/test_vis02_positive_control.py` | 136-13 | 10 |
| `tests/render_smoke/test_panel_render_smoke.py` | 136-21 | 17 |
| `tests/render_smoke/test_work_page_render_smoke.py` | 136-25 | 20 |
| `tests/render_smoke/test_findings_page_render_smoke.py` | 136-29 | 24 |
| `bench_discovery.py::bench_findings_page()` | 136-07 | 4 |
| `docs/specs/discovery-budgets.md` findings cap + build-time budget | 136-01 (caps) / 136-14 (measured) | 1 / 11 |
| Pinned novelty-verdict-cache handoff artifact | 136-11 | 8 |
| `tests/render_smoke/test_evidence_view_render_smoke.py` (added by the planner) | 136-23 | 18 |
| `tests/test_discovery_masking_surfaces.py` (added by the planner) | 136-31 | 26 |

### Positive controls — the assertions that must be proven able to fail

| Control | Plan | What it seeds |
|---|---|---|
| Rebuild preservation | 136-03 | In-stratum `matched_letters` drift, a deleted claim, a non-NULL tier-A precision, a changed card binding, a candidate self-attesting its own hashes |
| D-02a both branches | 136-05 | An unauthorized `ci_low`, an out-of-vocabulary status, any non-NULL precision |
| Licence gate | 136-09, 136-17, 136-23 | A misspelled/padded permissive flag, an absent work, a seeded non-permissive work rendered as permissive |
| M-source locus | 136-09, 136-31 | A locus string on a restricted row |
| VIS-02 leak | 136-13 | A restricted row inserted into a copy of the public projection; plus an unset pattern file |
| Panel honesty | 136-18, 136-21 | A precision figure, an interval, a review badge, a NEGATED prohibited word, a stored vocabulary key, a bare page percentage, a removed caveat, a same-phrase-elsewhere scope case |
| Catalogue separation | 136-26 | A catalogued row and a computed row under one shared heading; a prohibited word in the computed heading |
| Findings honesty | 136-29 | A precision figure plus the prohibited stronger novelty wording; an out-of-vocabulary domain plus a header mislabelled as the manuscript's domain; a locally-computed bucket disagreeing with the shared rule |
| No-numbers gate | 136-30 | A precision figure and an interval on the methods page and one row per surface; a bare percentage without the matched-letter qualifier |
| Cross-surface masking | 136-31 | A restricted value seeded into a row, a JSON payload, a copy string and an error message |

### Blocking owner checkpoints

| Plan | Wave | Decision |
|---|---|---|
| 136-04 | 1 | D-13e bucket count, D-16 relation filter on the findings page, the D-13c threshold, the D-13b tie-break, the D-13d granularity rule + alias ratification |
| 136-11 | 8 | Authorize the novelty funnel run, its pinned model configuration and the evaluation-set size |
| 136-15 | 12 | Approve the one authorized production redeploy on the gate evidence |
| 136-22 | 17 | The evidence view's render source and the b-side form |

*Status legend: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
