# Phase 136: Read Surfaces — Connections Panel & Work→Witnesses - Research

**Researched:** 2026-07-31
**Domain:** Verification/validation mechanism design for an already-decided read-surface + rebuild phase (NiceGUI web, SQLite sidecar, offline Python bake scripts)
**Confidence:** HIGH on code-state verification and validation mechanisms (everything below was checked against HEAD, not asserted from memory); MEDIUM on the three open technical unknowns (mechanisms are recommended, not yet built/measured); LOW on nothing load-bearing — where evidence was thin it is flagged explicitly.

## Summary

CONTEXT.md is unusually complete: 25 locked decisions, a dispositioned Codex adversarial-review table, and measured real-asset counts. This research does **not** revisit any of that. It does four things instead: (1) specifies concrete, mechanical verification for each of the 8 ROADMAP success criteria — the thing CONTEXT.md deliberately left to research/planning; (2) re-verifies every file:line claim in CONTEXT.md against HEAD (2026-07-31, one day after CONTEXT.md was written) and reports drift; (3) proposes mechanisms for the three narrow technical unknowns CONTEXT.md defers to execution gate 1; (4) collects phase-specific pitfalls.

**Headline code-state finding:** CONTEXT.md's claim that `web/discovery.py` has "zero callers" is now **stale** — since Phase 135-02 (2026-07-24), `web/main.py`'s `/help` route imports and calls three of its functions (`get_all_band_precision`, `get_band_claim_counts`, `discovery_methods_noindex`) for the BAND-05 methods section. The four PANEL-01/02/03 + WORK-01 read paths this phase actually needs (`get_claims_for_page`, `get_pages_related_to_page`, `get_evidence`, `get_work_witnesses`) remain genuinely callerless outside tests — so the substance of CONTEXT.md's claim ("the read spine is built, tested, and unused for the surfaces this phase ships") still holds, but the literal "zero callers" is no longer true of the module as a whole. Every other file:line citation checked (12 of them, across `shared/discovery_service.py`, `scripts/build_discovery_sidecar.py`, `scripts/verify_discovery_sidecar.py`, `scripts/cert01_frame.py`, `scripts/verify_cert01_grading.py`, `web/pages/browse.py`, `web/discovery_assets.py`) matched HEAD either exactly or within a small (~15-20 line) uniform drift in `web/main.py` from unrelated intervening edits — see Code-State Verification below.

**Second finding, not previously flagged anywhere:** the two scripts CONTEXT.md names as "the pre-built NOVEL-01 funnel and title gate to rewire" — `scripts/discovery_identified_gate.py` and `scripts/title_gate_llm.py` — do **not** live in the committed `scripts/` tree. They live at `same_work_spike/probe/scripts/discovery_identified_gate.py` and `same_work_spike/probe/scripts/title_gate_llm.py`, inside the **gitignored** research tree (confirmed via `git check-ignore` and `git ls-files`). This is consistent with the project's established handoff pattern (`docs/specs/discovery-coordination.md` §3: only owner-ratified, hash-pinned artifacts cross from the research tree into the committed build) — but it means "rewire the LLM gate" is not an in-place edit to a tracked file; it is new work to (a) either port the relevant logic into a new committed `scripts/` module, or (b) run the existing gitignored scripts as a pre-build step and hand `build_discovery_sidecar.py` a new pinned, hash-recorded verdict-cache JSON input (exactly the `--canonical-merges`/`v2_canonical_merges.build.json` pattern already used for the D-17 canonical merges). Recommendation: (b) — it is the established, already-proven pattern, requires no new masking-scanned code path for the LLM call itself, and matches NOVEL-02's "verdict cache is a build-time artifact and never ships" requirement.

**Primary recommendation:** treat this phase's verification burden as three layers that already have working models in the repo — extend, don't invent: (1) the offline verifier pattern (`scripts/verify_discovery_sidecar.py`, `scripts/cert01_frame.py`) for the rebuild-preservation gate and the D-02a lockstep; (2) the `atlas_preview_available()` / `check_atlas_masking.py --self-test` pattern for flag-off cleanliness and the masking positive control; (3) the `bench_discovery.py` harness for PERF-01. All three already exist, are tested, and just need new call sites and new assertions — none needs a new mechanism designed from scratch.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|---|---|---|---|
| Panel render (browse-page "Computed identifications") | Frontend Server (SSR) | API/Backend | NiceGUI pages execute server-side Python that both renders HTML/DOM ops and calls the service layer directly (no separate HTTP hop) — `web/pages/browse_enrichment.py` |
| `/work/{id}` page render + pagination/sort/filter | Frontend Server (SSR) | API/Backend | New `web/pages/work.py`; calls `DiscoveryService` in-process |
| Findings page render + pagination/filter | Frontend Server (SSR) | API/Backend | New page module; gated exactly like `/atlas` |
| `/catalog-browse` computed-identification integration | Frontend Server (SSR) | API/Backend | Extends `web/pages/catalog_browse.py`, which already resolves shelfmarks off-loop via `run.io_bound` |
| `DiscoveryService` reads (claims/witnesses/evidence/counts) | API/Backend | Database/Storage | `shared/discovery_service.py` — the one async chokepoint, already built |
| Offset-based evidence rendering (PANEL-03) | Frontend Server (SSR) | — | Pure server-side string slicing before HTML escaping; no client-side logic needed |
| `discovery.db` sidecar (read-only, versioned) | Database/Storage | — | SQLite file outside `web/static/`, loaded once at startup |
| Offline rebuild (`build_discovery_sidecar.py`) | Database/Storage (producer side) | — | Doesn't fit the live-request model; it is the ETL that produces the Database/Storage artifact |
| Novelty computation (heuristic funnel + LLM gate) | Database/Storage (producer side) | — | Build-time only; verdict cache never ships (NOVEL-02) |
| Public/private projection (VIS-01) | Database/Storage (producer side) | — | Deterministic projection at bake time, per `v9-PUBLICATION-STRATEGY.md` §3 |
| Masking scan (DATA-05/VIS-02) | Database/Storage (producer side) | CDN/Static (rendered-HTML surface) | Runs over the asset AND over rendered output/JSON/copy paths |
| Feature-flag + sidecar-readiness gating | API/Backend | Frontend Server (SSR) | `web/discovery_assets.py::discovery_available()` — one predicate, read by both the page-route gate and the nav-link gate |

**Why this matters here:** the biggest risk this phase carries is architectural mis-assignment in the OTHER direction from the usual trap — putting novelty/visibility computation in the frontend-server tier (e.g. deriving `assertion_visibility` at render time from `works.source_corpus`) instead of the Database/Storage producer tier, which is exactly the mistake D-22/F-05 already caught and fixed by decision. The map above is a checklist for the plan-checker: every novelty/visibility/coverage computation must land in the offline bake, never in a page-render function.

## Code-State Verification (HEAD vs. CONTEXT.md, checked 2026-07-31)

CONTEXT.md's file:line claims were made 2026-07-30. HEAD at research time is `f7f7a2205a9b8c6ea1c0d8f3490bd405f3c07b80` (2026-07-31 07:49:47+03:00), one day later. Every citation the phase's canonical_refs and decisions rely on was re-checked directly.

| Claim in CONTEXT.md | Status at HEAD | Evidence |
|---|---|---|
| `web/discovery.py` — "zero callers" | **STALE.** `web/main.py:715-719` imports and calls `discovery_methods_noindex`, `get_all_band_precision`, `get_band_claim_counts` from `web/discovery.py` for the `/help` BAND-05 methods section (landed in Phase 135-02, 2026-07-24 — six days before CONTEXT.md was written, so this was already true when CONTEXT.md was drafted). | `grep` for `from web.discovery import` in `web/main.py` |
| PANEL-01/02/03 + WORK-01 read paths (`get_claims_for_page`, `get_pages_related_to_page`, `get_evidence`, `get_work_witnesses`) — no caller | **Still true.** Only `web/discovery.py` itself and test files reference these four functions. This phase is genuinely their first UI consumer. | `grep -rn` across `web/`, `shared/`, `scripts/`, `tests/` |
| `shared/discovery_service.py:209-226` — the CTE exposing only unit/page/work/claim ids, family, band, band_rank | **Exact match.** `_WORK_WITNESSES_RANKED_CTE_SQL` at lines 209-226, no shelfmark/library/coverage/novelty/total. | Read in full |
| `shared/discovery_service.py:797-842` — the paginated result, no total/shelfmark/library/coverage/novelty | **Exact match** (paginated query at 797, returned dict list ending 844). | Read in full |
| `scripts/build_discovery_sidecar.py:3865-3890` `_validate_precision_spec`, enforced `4019-4027` | **Exact match**, function starts line 3867, the per-band precision-equality check is at 4019-4027 verbatim. | Read in full |
| `scripts/build_discovery_sidecar.py:4419-4427` `band_precision` INSERT | **Exact match.** | Read in full |
| `scripts/build_discovery_sidecar.py:532-568` coverage functions | **Exact match** (`norm_stream_letter_count` 532-552, `compute_page_coverage` 555-568). | Read in full |
| `scripts/build_discovery_sidecar.py:2555-2570` / `2689-2713` largest-span selection | **Exact match** (`_largest_track1_span` 2555-2570, `_ingest_tier_a` 2689-2714). | Read in full |
| `scripts/verify_discovery_sidecar.py:553-560` M4 tier_a-precision-NULL check | **Exact match.** | Read in full |
| `scripts/verify_discovery_sidecar.py:319-332` claim/work `source_corpus` equality | **Exact match** (`check_source_corpus_consistency`). | Read in full |
| `scripts/cert01_frame.py:293-306` hash recipes | **Exact match** (`population_hash` 293-298, `cluster_map_hash` 301-306). | Read in full |
| `scripts/verify_cert01_grading.py:206-212` `db_content_hash` pin | **Exact match** (`check_10_input_hash_pinning`). | Read in full |
| `web/pages/browse.py:1577-1601` `highlight_text` | **Exact match**, zero drift. Confirms F-09: escapes-then-substitutes, cannot consume offsets. | Read in full |
| `web/main.py:1753-1772` nav list | **Drifted ~16 lines** — nav_items literal now at 1769-1789. Same content/order, same `atlas_preview_available()` gating pattern. Not material. | `grep -n` |
| `web/main.py:2086-2907` `@ui.page` routes | **Drifted ~16-20 lines** — routes now span 2102-2923. Same set, same order, same pattern (`/atlas` at 2599, not 2086-ish). Not material, but **use `grep -n "^@ui.page"` at plan time, don't trust either number.** | `grep -n "^@ui.page"` |
| `scripts/discovery_identified_gate.py` / `scripts/title_gate_llm.py` referenced as if in `scripts/` | **Materially wrong path.** Both live at `same_work_spike/probe/scripts/*.py`, gitignored, untracked (`git check-ignore` confirms `.gitignore:212: same_work_spike/`; `git ls-files` returns nothing). See Summary above and Pitfalls below. | `Glob` + `git check-ignore -v` + `git ls-files` |
| Canonical_refs: "`/catalog-browse` facet worker (`_CatalogFacetWorker`, off-UI-thread counts) is the model for the findings page's counts" | **Wrong tier.** `_CatalogFacetWorker` is a PyQt6 `QThread` subclass in `genizah_app.py` (the **desktop** app) — architecturally inapplicable to NiceGUI (no QThread in an asyncio/NiceGUI process). The web `/catalog-browse` page's own actual pattern — `_fetch_results_blocking()` run via `await run.io_bound(...)` inside an async `fetch_results()` — is the correct model to copy for the findings page's counts, and it already exists in the same file. | Read `web/pages/catalog_browse.py:280-314`; `grep` confirmed `_CatalogFacetWorker` only in `genizah_app.py` |
| `_attach_coverage` / `spec["coverage"]` (not cited by number in CONTEXT.md, but relevant to D-08a) | Confirms a **ready-to-use mechanism**: `compute_page_coverage(matched_letters, page_norm_letters)` is already computed at ingestion for every `track1_direct` witness row (`scripts/build_discovery_sidecar.py:2641-2657`), used only transiently for Lever-1 routing, then **discarded** — `_mk_evidence`'s returned dict (1316-1346) has no `coverage` key, so nothing currently persists it. Persisting `coverage_ppm` is therefore a small, well-understood change: multiply the already-computed `spec['coverage']` by 1e6, round, add a column + INSERT param. No new metric needs to be invented. | Read in full |

**Net assessment:** CONTEXT.md's code-state claims are close to perfectly accurate — the only two corrections that matter for planning are the `web/discovery.py` caller-count staleness (cosmetic — doesn't change what this phase must build) and the gitignored-location of the novelty-funnel scripts (real — changes HOW gate 6's novelty wiring must be planned, see Pitfalls). Everything else is either exact or drifted by a small, uniform, immaterial line-number offset from unrelated intervening commits.

## Validation Architecture

> This phase's dominant risk is NOT "does the code compile" — the existing 154+ tests across `test_discovery_service.py` / `test_discovery_band_labels.py` / `test_discovery_build.py` already cover the data layer heavily. The dominant risk is an *unverified rebuild* silently losing or corrupting 268K claims / 297K evidence rows, or a masking leak reaching a public surface. Ordinary unit tests do not catch either. This section is deliberately mechanical: for each ROADMAP success criterion, a concrete verification step, in most cases extending an existing script rather than inventing one.

### Test Framework

| Property | Value |
|---|---|
| Framework | pytest 9.0.2 (repo-wide; no per-module override) |
| Config | `pyproject.toml` `[tool.pytest.ini_options]` — deliberately no default-exclude `addopts` (per its own comment) |
| Quick run command | `python -m pytest tests/test_discovery_service.py tests/test_discovery_band_labels.py -x` |
| Full discovery-suite command | `python -m pytest tests/ -k discovery -x` (14 existing `test_discovery_*` files + `test_no_back_edges_discovery.py`) |
| Render-smoke location/convention | `tests/render_smoke/test_<surface>_render_smoke.py` — established by `tests/render_smoke/test_help_methods_render_smoke.py` (Phase 135-02, 6 tests: EN+HE render, anchor presence, runtime population, placeholder-safety, noindex three-state transition, no-"certified" gate, HE RTL) |
| Offline verification harness | `scripts/verify_discovery_sidecar.py` (release-contract + enum + F4 + M4 + v2-no-mixed-enum-state checks), `scripts/cert01_frame.py` (hash recipes), `scripts/verify_cert01_grading.py` (12-check pre-registration validator), `scripts/bench_discovery.py` (PERF-01 latency/RSS probe), `scripts/check_atlas_masking.py` (masking scan + `--self-test`) |

### Success Criterion 1 — the one authorized rebuild lands, flag-OFF, with a rebuild-preservation gate

This is the highest-risk, least-precedented item (F-04, D-02b). No existing script performs an exact old/new allowlisted diff — it must be written, but as a **sibling of `scripts/verify_discovery_sidecar.py`**, not a new paradigm.

**Recommended mechanism — `scripts/verify_rebuild_preservation.py <old_db> <new_db> --expected <pinned.json>`:**

1. **Pin expectations BEFORE the rebuild runs, from the OLD (currently-live) asset — never from the new build's own manifest** (this is the direct fix for F-04's flaw: "the deploy verifier compares the candidate against the candidate manifest, not against the certified asset"). Concretely: before touching the v2 asset, run a one-time snapshot script against `discovery-v1-33499c5b….db` that writes `136-REBUILD-PRESERVATION-EXPECTED.json` containing: `population_hash`, `cluster_map_hash` (both recomputed via `scripts/cert01_frame.py`'s existing `population_hash()`/`cluster_map_hash()` functions — reuse, don't reimplement), plus a **per-table row-count and per-table full-row content hash** for `works`, `discovery_claim`, `discovery_evidence`, `witness_units`, `witness_unit_members`, `discovery_routing_audit`. Commit this file alongside the phase's plan artifacts (masking-clean by construction — it is hashes and counts only, exactly like `discovery-frames.md`).
2. **Per-table full-row hash, streamed, not held in memory.** For each of the 6 tables: `SELECT * FROM <table> ORDER BY <deterministic key>` (PK for `works`/`witness_units`; `(page_id, work_id)` for `discovery_claim`; `evidence_id` for `discovery_evidence`; `(unit_id, sys_id)` for `witness_unit_members`; `id` for `discovery_routing_audit`), project OUT the explicitly-allowlisted new/changed columns (`coverage_ppm`, the new coverage-validity column, the new novelty tri-state column(s), and — for `band_precision` only — the `tier_a` row's `measurement_status`/`ci_low`), and feed a running `hashlib.sha256().update(...)` over each row's remaining column values joined with a fixed delimiter. This is an O(n) sequential scan per table — at 297K evidence rows this is the same order of magnitude as `verify_discovery_sidecar.py`'s existing full-table enum/consistency scans, which already run inside the 135-06/135-07 build+verify pipeline without a reported timeout; budget it explicitly (see below) rather than assuming it is free.
3. **Compare:** old-hash == new-hash per table (after the allowlisted exclusion) is a **hard fail** if unequal for any of the 6 fully-preserved tables. For `band_precision`, assert every row identical **except** the one authorized `tier_a` row (`measurement_status`/`ci_low` may differ; `precision` must NOT differ — it was and remains NULL).
4. **Recompute `population_hash`/`cluster_map_hash` on the NEW asset** and assert equality against the value pinned in step 1's JSON (not against anything in the new build's own manifest).
5. **Bind every CERT-01 graded card to the same work/claim/display-evidence/span/snapshot.** For each of the 280 graded `(page_id, work_id)` cards recorded in the CERT-01 deck manifest (`same_work_spike/probe/review/cert01_deck_verdicts.json` or the deck's own manifest file — confirm exact path at execution time; it is gitignored per the coordination doc), look up the SAME claim key in the NEW asset and assert `claim_id`, `display_evidence_id`, `span_start`/`span_end`, `snapshot_hash` are byte-identical to what the OLD asset (and by extension the graded deck) recorded. Any mismatch invalidates CERT-01 silently if not caught here — this is a hard fail, not a warning.
6. **Never echo raw cell values in a mismatch report** — name only the offending row's primary key (masking discipline; defense-in-depth even though the sidecar is already build-time-masked).

**Practical runtime — genuinely unmeasured; do not fabricate a number.** No committed doc records a wall-clock time for a full-table scan at this row count. Budget this as a **new, versioned, build-time** entry in `docs/specs/discovery-budgets.md` (distinct from PERF-01's request-time caps — this is an offline/CI-time budget) once gate 2 measures it for the first time; do not guess a cap before that measurement exists.

**D-02a lockstep — enumerated with exact evidence, not just named:**

| # | Site | What must change |
|---|---|---|
| 1 | `docs/specs/discovery-sidecar-schema-v1.md` §1.6 | New dated amendment section permitting the `tier_a` `scope='band'` row to carry `measurement_status='measured_pass'` + `ci_low=0.9084` while `precision`/`ci_high` stay NULL |
| 2 | `scripts/build_discovery_sidecar.py::_frozen_real_band_precision_rows` (tier_a dict, ~lines 1747-1756) | Add `"ci_low": 0.9084` and a **new** `"measurement_status": "measured_pass"` key. Verified: the DB-insert list comprehension at ~4417-4427 (`[{"measurement_status": None, **r} for r in bp_rows]`) uses Python dict-literal semantics where a later `**r`-supplied key wins over the earlier literal — so adding `measurement_status` to the frozen row is suficient to make it reach the DB; **write a fixture test proving this**, the mechanism is non-obvious from reading the insert alone. |
| 3 | `_validate_precision_spec` (~3867-4039) | **Currently checks ONLY the `precision` field** per band row (confirmed by reading 4019-4027) — it does NOT currently cross-check `ci_low`/`measurement_status`. Must be widened to assert the `tier_a` row's `ci_low`/`measurement_status` match the frozen (D-02a-amended) row exactly, or a caller-supplied `--precision-spec` could smuggle an unauthorized value through undetected. |
| 4 | `scripts/verify_discovery_sidecar.py` M4 (~553-560) | Currently asserts `tier_a` `precision` must be NULL (unaffected by D-02a — precision stays NULL). Must gain a **new** assertion that `tier_a`'s `ci_low`/`measurement_status` equal exactly the authorized values — otherwise a future build could write an arbitrary `ci_low` into this slot and the release verifier would never catch it. |
| 5 | `shared/discovery_band_labels.py::is_default_eligible` (lines 306-311) | **No change needed** — already reads exactly `measurement_status == 'measured_pass' and ci_low is not None and ci_low >= STRICT_FLOOR` (0.85). 0.9084 ≥ 0.85, so `tier_a` becomes default-eligible the moment sites 1-4 land. This is the READER the write-side lockstep must satisfy. |
| 6 | Schema/build tests (`tests/test_discovery_schema.py`, `tests/test_discovery_build.py`) | New fixtures proving BOTH branches: (a) PASS — a spec with the authorized `ci_low`/`measurement_status` builds clean and `is_default_eligible` flips True for tier_a; (b) FAIL — a spec with `ci_low` below `STRICT_FLOOR`, or `measurement_status` outside the closed vocab, or a `precision` value (never allowed), is REJECTED by the widened validator from #3. |

### Success Criterion 1 (masking half) — DATA-05/VIS-02

**Reuse, don't invent:** `scripts/check_atlas_masking.py` already ships `--scan-repo`, `--scan-sqlite <db>`, `--scan-asset <path>`, `--strict`, and `--self-test`. Run all of `--scan-sqlite`/`--scan-asset`/`--scan-repo --strict` over: (a) the rebuilt private asset, (b) the new PUBLIC projection asset, (c) rendered HTML captured from all 4 new/changed surfaces (panel, evidence view, `/work/{id}`, findings page, `/catalog-browse`) via the same `capture_atlas_html.py`-style browser-DOM capture the Phase 133 atlas used (note: that capture is the ONLY surface that sees interaction-created strings — STATE.md records it was SKIPPED for the atlas because Playwright wasn't installed locally; **do not repeat that gap here** — install Playwright or explicitly document the skip as a known gap, don't silently drop it), (d) JSON payloads from any new endpoint, (e) copy/clipboard paths (PANEL-03's evidence text, novelty provenance strings), (f) error/exception paths.

**`--self-test` is necessary but NOT sufficient for VIS-02's specific claim.** `_run_self_test()` (confirmed at `scripts/check_atlas_masking.py:1292-1322`) builds its OWN throwaway matcher from a synthetic non-restricted token (`ZZZ_SELFTEST_TOKEN_ZZZ`) and proves the matching ALGORITHM is encoding-robust (literal/URL/HTML-entity/JS-escape/UTF-16). It does **not** exercise the real `.masking_patterns` file, and it says nothing about whether a restricted row that leaked into the public projection would actually be caught. **VIS-02's positive control is a genuinely new, phase-specific test:** build a throwaway copy of the public projection DB, `INSERT` one row copied verbatim from the private asset that legitimately carries restricted (`msource`) origin, run `--scan-sqlite`/`--scan-asset --strict` against that seeded copy with the REAL `MASKING_SCAN_PATTERNS_FILE`, and assert **nonzero exit**. Record this as a new test (e.g. `tests/test_vis02_positive_control.py`) — without it, "the scan is live" and "the scan would catch a real leak" are two different claims and only the weaker one is currently provable.

**Known blind spot to carry forward, not re-derive:** the masking scan covers `git index/HEAD`, never history (already true, already accepted at the milestone level per `v9-PUBLICATION-STRATEGY.md` §9 — not this phase's problem to solve, just don't be surprised a green `--scan-repo` says nothing about history).

### Success Criterion 2/3 — the panel (three disclosure levels) + evidence view

**What can be proven headlessly:** every data-shape assertion — `serialize_banded_claim` never omits its band fields (already tested), the D-13a/b/d/e/f/g/h/i display rules as PURE functions over fabricated claim-row fixtures (group-by-`canonical_work_id`, same-span-group extraction, threshold-based bucketing, `is_default_eligible`'s human_confirmed-vs-routing interaction), the offset-slicing renderer's escape-then-wrap correctness (byte-for-byte against a fixture with `&`/`<`/`>` inside the highlighted span, proving the F-09 bug this design avoids), the fail-closed-on-snapshot-drift path (mutate `snapshot_hash`, assert the span is withheld but identification/tier still render), and the D-13g routing-vs-human_confirmed interaction bug fix (a `human_confirmed` row with `routing_status='review_only'` must now render with a low-coverage note, per D-13g — write the regression test for the EXACT symptom found in the mockup, Moss. V,374 P22 vs P23, using synthetic fixture data at the same shape).

**What genuinely cannot be proven headlessly (house lesson: `feedback_nicegui_render_smoke_gap` / `reference_nicegui_flex_height_css`):** whether the panel's three collapsed/expanded sections actually render as three visually distinct, correctly-nested NiceGUI containers; whether the on-demand `get_work_witnesses` expansion actually fires on click without a stale `page_client` (see the `reference_io_bound_safe_storage_trap` memory — any lazy-loaded section MUST bind `page_client` at render time, exactly as `BrowsePageRefs.page_client` already does); RTL/bidi rendering of the match-framing strings. These need render-smoke tests (`tests/render_smoke/test_panel_render_smoke.py`, modeled on `test_help_methods_render_smoke.py`) PLUS a live/human smoke pass — the regression set is the 7 real manuscripts already fixed in the mockup (D-05: "the standing regression set for any future panel change"), not a fresh arbitrary sample.

### Success Criterion 4 — `/work/{id}`

Extend `scripts/bench_discovery.py` (already the harness — see PERF-01 below) with `get_work_witnesses`-shaped queries carrying the NEW display fields (D-17a: shelfmark/library/coverage/novelty + a count query using identical grouped predicates). Correctness tests: server-side sort stability across pages (page 2's first row must never duplicate/skip page 1's last row — a classic pagination-with-ties bug, already partially guarded by the existing `page_id`/`claim_id` tie-breakers in `_project_work_witnesses`/the SQL projection — extend the SAME tie-break to the new display fields). Unit projection counts: assert against the CORRECTED figures from Codex F-11 (heaviest work 4,796 distinct manuscripts / 4,637 witness units, NOT 13,038 claim rows; medians 9 claims / 5 manuscripts / 4 units) rather than re-deriving them.

### Success Criterion 5 — `/catalog-browse` integration + findings page

**Findings-page query-shape verification is F-10's fix, and it is ALREADY MEASURED as failing (3.41-3.55s vs 1.5s cap).** The verification here is: after D-10a's materialized `band_rank`/`coverage_ppm` + new indexes land, re-run the SAME representative novelty/tier/coverage-ordering query via `bench_discovery.py` (extend it with a `bench_findings_page()` probe) and assert it now sits under the cap with margin — this is a regression test with a KNOWN prior failing baseline, which is the strongest kind of performance test to have (it already caught something real once).

**Catalogue-integration correctness:** `/catalog-browse`'s shelfmark resolution happens only AFTER a page fetch (`web/pages/catalog_browse.py:262-309/376-438`, confirmed unchanged at HEAD) — so the mapping between computed identifications and catalogued works is inherently incomplete. The verification obligation here is honesty, not completeness: a test asserting the two vocabularies (catalogued FJMS titles vs. computed neutral titles) are NEVER rendered under a shared "identified as" wording (per D-18/D-21's match-framing prohibition on "copy of"/"quotes"/"witness of").

### Success Criterion 6 — novelty tri-state, fail-closed

**Verify the fail-closed default is real, not asserted.** For each of the 5 named failure paths in D-23a (source unavailable, identifier won't normalize, incomplete snapshot, stale cache, model abstention), write a test that FORCES that specific failure (mock the source lookup raising, feed an unnormalizable identifier, truncate the source snapshot, expire the cache, force an LLM abstention response) and asserts the resulting value is `indeterminate` — never silently `not_found`. This is the exact shape of bug the boolean predecessor had (F-06: 665 claims with internally-disagreeing `is_new`).

**Verify one-result-per-claim.** A new verifier check (extend `scripts/verify_discovery_sidecar.py`'s checklist, not a separate script): for every claim with ≥2 evidence rows, assert all evidence rows agree on the novelty tri-state value — the exact regression this phase must fix (665 of 29,054 multi-evidence claims currently disagree on the legacy boolean).

**Verify the LLM gate's reproducible contract (D-23c) mechanically, not by inspection.** Pin: prompt hash (hash the actual prompt string used, assert it matches a committed constant), model+version string (`gemini-3.6-flash`, `reasoning:{effort:"low"}` — assert the request payload literally contains this, so a silent model downgrade is caught at CI time, not discovered in a cost report), normalized-input hash (same idea), and a structured-abstention path (force the LLM client to return an abstention-shaped response in a test double, assert it maps to `indeterminate`).

### Success Criterion 7 — no precision percentage reachable

**This is a grep-able, CI-enforceable invariant, and the repo already has the pattern.** `tests/render_smoke/test_help_methods_render_smoke.py` already includes "the no-'certified' gate (EN + HE, scoped to the section)". Extend the SAME technique: a render-smoke assertion over the rewritten `/help` methods section (D-06a) that NO digit-percent pattern (`r'\d+(\.\d+)?%'` or similar) and no confidence-interval bracket pattern (`r'\[\s*0\.\d+\s*,\s*0\.\d+\s*\]'`) appears anywhere in the rendered EN/HE text, PLUS the same assertion extended to every new surface's rendered output (panel, evidence view, work page, findings page). This is cheap, mechanical, and catches the exact class of regression BAND-03/05's amendment exists to prevent.

**Review-badge suppression (D-13f) is a one-line grep-able invariant too:** assert "Expert-reviewed" / "נבדק בידי מומחה" never appears in ANY new surface's rendered output until the 121-row provenance task is separately closed — a render-smoke test, not a manual check.

### Success Criterion 8 — flag-off/absent cleanliness + budgets + masking

**The `atlas_preview_available()` pattern is the exact, already-proven model — name it, copy it, do not redesign it.** `discovery_available()` already exists and already gates PANEL-01 through the enrichment path implicitly (any new panel code must check it before rendering, exactly as the `/atlas` route's early-return pattern at `web/main.py:2599-2650` does: gate check → early return with a "temporarily unavailable" card → only THEN delegate to the real page module). Verification: a render-smoke test per new surface asserting (a) flag OFF → surface absent/hidden, zero errors; (b) flag ON + sidecar absent → same; (c) flag ON + sidecar present but a query times out → the D-13/F-14 envelope (`{status, items, total}`) surfaces a visible "temporarily unavailable, retry" state, not a silent empty list masquerading as zero results. (c) is the one truly NEW test class this phase needs beyond the existing atlas-proven pattern, because it requires the envelope change (D-13) the atlas's simpler binary-ready/absent model never needed.

**PERF-01 budget entries:** extend `docs/specs/discovery-budgets.md` §1.2 (already has Work/Leads page caps — 200 rows/500KB/1.5s p95/5s timeout) with a **new** findings-page row in the SAME versioned-artifact discipline, plus the build-time rebuild-preservation budget from Success Criterion 1. Do not add an unversioned number anywhere in code.

### Wave 0 Gaps

- [ ] `tests/render_smoke/test_panel_render_smoke.py` — none exists yet for any discovery UI surface (confirmed: only `test_help_methods_render_smoke.py` exists, covering `/help`, not the panel/work/findings pages)
- [ ] `tests/render_smoke/test_work_page_render_smoke.py`
- [ ] `tests/render_smoke/test_findings_page_render_smoke.py`
- [ ] `scripts/verify_rebuild_preservation.py` — does not exist; new script per the mechanism above
- [ ] `136-REBUILD-PRESERVATION-EXPECTED.json` (or equivalent pinned-before-rebuild snapshot) — does not exist; must be generated from the CURRENTLY-LIVE asset before the rebuild starts
- [ ] `tests/test_vis02_positive_control.py` — does not exist; distinct from the existing `--self-test`
- [ ] `docs/specs/discovery-budgets.md` — needs the new findings-page entry (already flagged by D-10a) and a new build-time rebuild-preservation budget section
- [ ] A pinned, hash-recorded novelty-verdict-cache handoff artifact (see Pitfalls) — does not exist yet; the gitignored LLM-gate scripts have no committed consumer

## The Three Narrow Technical Unknowns

### 1. D-13d granularity separation (the ⚠ KNOWN FLAW)

**The problem restated precisely:** two claims on the identical span need to be told apart — (a) *same work at two granularities* (e.g. `רש"י על התורה` w000171 vs `רש"י על בראשית` w001281, span 0-962, DIFFERENT `canonical_work_id`s) which must COLLAPSE like a duplicate, vs. (b) *genuinely different works quoting/containing the same passage* (Tur Orach Chaim + Yalkut Shimoni on byte-identical offsets) which correctly stays in the D-13d "generic shared text" bucket. This is explicitly a **display-time** test (no data fix in scope) — CONTEXT.md is right that the underlying reference-granularity fix is a v2.1/gen-2 concern.

**Signals genuinely available at display time, with failure modes:**

| Candidate mechanism | What it uses | Failure mode |
|---|---|---|
| **Title-substring containment** (e.g. does one work's `neutral_title` appear as a normalized substring of the other's, or vice versa?) | `works.neutral_title` (already shipped, human-reviewed) | Cheap, no new data needed. Fails on any pair where the broader/narrower relationship isn't lexically visible in the title (e.g. two titles that don't substring-relate but ARE the same work at different granularity — this is likely the MINORITY case given how neutral titles are curated, but it is not exhaustive; also risks a false COLLAPSE for two textually similar but genuinely unrelated titles — needs a length/token-overlap threshold, not bare substring, to avoid false positives) |
| **`canonical_work_id` cross-reference via the SAME merge seed already used for D-13a** (the twin/canonical-merge list at `discovery_data/v2_canonical_merges.build.json`-equivalent) extended with a NEW "granularity-alias" relation distinct from "same-work merge" | The existing canonical-merge infrastructure (already proven, already has an owner-reviewed census) | Requires the owner to positively identify granularity-alias PAIRS (like the Rashi example) during the SAME census/review pass that already produces canonical merges — this is real curation work, not a code trick, but it reuses an already-budgeted-for owner review step rather than inventing a new one |
| **Work-hierarchy metadata (author + genre + a new explicit `parent_work_id`/`broader_work_id` field)** | Would require a NEW schema column | Most robust long-term, but is exactly the "reference-granularity stage" CONTEXT.md explicitly defers to v2.1/gen-2 (D-01) — recommending this for 136 would re-open a locked scope decision. **Do not recommend this for 136.** |
| **Author-token overlap only** (both works share the same reviewed `author` field, e.g. both attributed to "Rashi") | `works.author` (625/1,270 filled) | Weak alone — many genuinely different works share an author (e.g. two different Rashi commentaries on two different, non-overlapping books are NOT a granularity pair) — this signal alone would produce false collapses. Only usable as a NECESSARY, not sufficient, pre-filter alongside title containment. |

**Recommendation:** combine the first two — a normalized title-containment/token-overlap heuristic (cheap, computable NOW from already-shipped `neutral_title` data, no schema change) as the AUTOMATIC display-time signal, cross-checked against a small owner-reviewed allowlist of known granularity-alias pairs collected during execution gate 1 (the SAME kind of small, hash-pinned, owner-ratified artifact the D-13a canonical merges already are — `discovery_data/granularity_aliases.json`, format mirroring `v2_canonical_merges.build.json`). This keeps the "no data fix" constraint (it's a display-time lookup table, not a rebuild of `canonical_work_id`), gives the automatic heuristic a correctness backstop for the cases it can't get right lexically, and re-uses the exact artifact-pinning mechanism the phase already uses elsewhere (traceable, hash-recorded, small). **This is the one open unknown where "gather counts and bring options to gate 1" (per Claude's Discretion in CONTEXT.md) is the right disposition — do not lock a specific threshold value in planning; lock the MECHANISM (title-containment heuristic + owner-reviewed allowlist backstop) and defer the threshold to gate 1 with real counts, exactly as CONTEXT.md already says for D-13c.**

### 2. The offset-based span renderer (D-12)

**Confirmed:** `web/pages/browse.py::highlight_text` (1577-1601, zero drift from CONTEXT.md's citation) escapes the FULL text first (`html_module.escape(text)`), THEN does case-insensitive term substitution on the ALREADY-ESCAPED string. Reusing it for stored offsets would corrupt them the instant the raw text contains any of `&<>"'` before the target span (escaping `&` to `&amp;` shifts every subsequent character's index by 4).

**Safe mechanism (D-12's own prescription, confirmed implementable with what exists):**
```python
def render_offset_highlight(raw_text: str, start: int, end: int) -> str:
    """Slice RAW text at stored offsets BEFORE escaping (never after)."""
    before, middle, after = raw_text[:start], raw_text[start:end], raw_text[end:]
    return (
        html_module.escape(before)
        + f'<span class="discovery-match">{html_module.escape(middle)}</span>'
        + html_module.escape(after)
    )
```
This is a NEW, small, pure function — it does not touch `highlight_text` (which stays as-is for search-term highlighting, an unrelated concern) and needs its own home (e.g. a new `shared/discovery_render.py` or inline in the new evidence-view page module — either is fine; it is pure and easily unit-tested against a fixture string containing `&`/`<` before the target span).

**Snapshot-hash drift, fail-closed, per side independently:** `discovery_evidence.snapshot_hash` (a-side) and `snapshot_hash_b` (b-side, shared_text only) are both already stored (confirmed in the frozen schema, §1.3/§9 OQ3). At render time: re-hash the CURRENT page text the same way the build did (`hashlib.sha256(text.encode('utf-8')).hexdigest()`, matching `PageTextIndex.get`'s exact recipe) and compare. On mismatch: render the identification, band, and coverage as normal, but replace the offset-highlight block with an explicit "the passage location could not be verified against the current text" note — never withhold the whole row, never render a stale/wrong span. This must be checked **independently per side** for a `shared_text` claim (a-side drift and b-side drift are unrelated events).

**The b-side-has-no-offsets case (confirmed, schema §1.3: `b_start`/`b_end` are nullable/absent by design for `shared_text` rows):** when the CURRENTLY VIEWED page is the b-side of a relation, there is no stored span to highlight at all — this is not a drift failure, it's a structural absence. Render the relation (work/manuscript names, tier, the "Pages matching this page in other manuscripts" framing) with an explicit, permanent (not a drift-error) note: "the passage location on this side was not recorded." This is the FROZEN behavior per D-12/schema §1.3 — do not treat it as a bug to fix or as equivalent to a drift failure; the copy should read differently (a permanent structural fact, not a "please retry" transient state).

### 3. `coverage_ppm`

**Confirmed, all four sub-claims verified against HEAD:**
- The metric IS matched Hebrew base letters ÷ normalized page stream: `compute_page_coverage(matched_letters, page_norm_letters) = min(1.0, matched_letters/page_norm_letters)` (`scripts/build_discovery_sidecar.py:555-568`), where `page_norm_letters = norm_stream_letter_count(page_text)` strips nikud/cantillation/all combining marks/brackets/digits/Latin and keeps ONLY Hebrew base letters U+05D0-U+05EA after NFC + final-letter folding (532-552).
- Only the largest span is stored for `tier_a` (`_largest_track1_span`, 2555-2570, tie-break `(end-start)` DESC then `start`/`end` ASC) even when `track1_matches.spans_json` carries multiple disjoint spans — confirmed 9,549 shipped direct rows have >1 span per the frame doc's own count.
- All 42,776/40,995 shipped propagated evidence rows have NULL `matched_letters` (confirmed structurally: `_ingest_propagated_witness`/`_ingest_e1_rows`-adjacent code paths for the propagated family never populate `matched_letters` from a coverage-denominator source — the propagated pipeline has no `page_norm_letters` computation wired to it at all).
- **The mechanism to persist `coverage_ppm` is already 90% built and simply needs its output kept instead of discarded.** `_attach_coverage(spec, page_index, page_id, matched_letters)` (2641-2657) ALREADY computes `spec["coverage"]` for every `track1_direct` witness row at ingestion, via the EXACT `PageTextIndex.norm_letters(page_id)` cached lookup (2533-2548) that reads `pages.text` from the research DB, reduces it to `norm_stream_letter_count`, and NEVER exposes/persists the raw text (masking-safe by construction, per the existing docstring's own claim). `apply_lever1_coverage` (814-848) reads `spec["coverage"]` for the ship/demote routing decision — but `_mk_evidence`'s returned dict shape (1316-1346) has NO `coverage` key, so nothing currently carries it through to the DB INSERT.

**Recommended storage mechanism:** add a `coverage_ppm INTEGER` column (fixed-point: `round(spec['coverage'] * 1_000_000)`, so it ranges 0-1,000,000 given the existing `min(1.0, ...)` clamp) plus a `coverage_validity TEXT CHECK (coverage_validity IN ('valid','no_denominator'))` column (or reuse the existing pattern of a NULL sentinel — `coverage_validity='no_denominator'` when `page_norm_letters` was 0/missing, matching `compute_page_coverage`'s own "0.0 on missing denominator" fallback so a genuinely-zero-Hebrew-content page is distinguishable from "never computed") to `discovery_evidence`, populated ONLY for `evidence_source='track1_direct'` (D-08a: direct-family only) by threading `spec["coverage"]` through `_mk_evidence`'s return dict and the corresponding column list in the DB INSERT. Index it (`ix_discovery_evidence_coverage_ppm` or fold into the D-10a findings-ordering composite index) since the findings page needs to sort/filter by it (D-10a). The page-length/`page_norm_letters` input comes from the ALREADY-EXISTING `PageTextIndex.norm_letters()` — no new page-length column or new data source is needed; it is computed transiently at build time exactly as it is today, just no longer thrown away afterward.

## Common Pitfalls

### Pitfall 1: Deploying code before the database (asset-first ordering)
**What goes wrong:** if the new web code (which expects the new `coverage_ppm`/novelty/VIS-01 columns and the D-13 envelope shape) reaches production before the rebuilt sidecar does, every discovery read either 500s or silently degrades depending on how defensively the new code reads optional columns.
**Why it happens:** the deploy runbook (`docs/specs/discovery-deploy.md`) is explicit about asset-first (`scp` the DB, verify, THEN swap the manifest, THEN restart) — but that discipline is about the DB-swap step itself; it says nothing about the RELATIONSHIP between the code deploy and the DB deploy for a phase that changes BOTH the schema (new columns) and the code (new UI reading them).
**How to avoid:** treat this rebuild+redeploy exactly like the 135-08 precedent (DB swap while flag stays OFF, restart, smoke via `bench_discovery.py`, THEN and only then land the UI code in a LATER deploy once the new asset is confirmed live) — never bundle a schema-dependent code change into the same deploy as the schema change itself, even though the flag stays OFF throughout gate 1-3 and the risk is theoretically contained.
**Warning signs:** any plan task that says "add the coverage_ppm column AND wire the panel to read it" in the same deploy step.

### Pitfall 2: Weakening `verify_cert01_grading.py` check 10 instead of publishing a compatibility attestation
**What goes wrong:** check 10 (`check_10_input_hash_pinning`, confirmed at lines 206-212) pins the ORIGINAL asset's `db_content_hash` — by design, EVERY legitimately rebuilt byte-stream fails it. The tempting "fix" is to update the pinned hash or loosen the check so the rebuild passes.
**Why it happens:** a naive reading of "the rebuild must pass the CERT-01 gate" leads directly here.
**How to avoid:** per D-02c, the pre-registration (`cert01_prereg.json`, `verify_cert01_grading.py`) stays byte-for-byte immutable, forever, as the audit trail of what was actually graded. Publish a SEPARATE, new document (a "compatibility attestation") recording: the original hash, the new hash, the rebuild-preservation diff result (Success Criterion 1 above), and the recomputed population/cluster hashes. Never touch `verify_cert01_grading.py`'s pinned values.
**Warning signs:** any diff touching `verify_cert01_grading.py`'s hash constants or its check-10 assertion logic.

### Pitfall 3: The naming/nav collision with `/discoveries`
**What goes wrong:** `web/pages/discoveries.py` (route `/discoveries`, nav label "Community") is the PRE-EXISTING, unrelated Supabase-backed community discoveries/questions feature — nothing to do with the v9 discovery module. A findings page or its nav entry accidentally named "Discoveries" collides semantically and possibly literally (route/label reuse).
**Why it happens:** the word "discovery"/"discoveries" is heavily overloaded in this codebase (the whole v9 module is called "Discovery"; the pre-existing community page is ALSO called "Discoveries").
**How to avoid:** D-19 already settles this — the new page needs its own name and route, NOT `/discoveries` and NOT the label "Discoveries". This is locked; just don't accidentally violate it by pattern-matching against the wrong existing page when copying the `/atlas` gating model.
**Warning signs:** grep for `/discoveries` before adding any new route; confirm the new route is genuinely distinct.

### Pitfall 4: Treating the novelty-funnel scripts as in-tree, editable files
**What goes wrong:** planning a task like "edit `scripts/discovery_identified_gate.py` to add tri-state support" will fail immediately — that file does not exist at that path. The real files are gitignored, dev-box-only, under `same_work_spike/probe/scripts/`.
**Why it happens:** CONTEXT.md's canonical_refs cites them with a `scripts/` prefix that reads as if they're in the committed tree (see Code-State Verification above).
**How to avoid:** plan the novelty work as (a) modify/extend the gitignored scripts locally (dev-box only, never committed) to emit a tri-state verdict cache instead of a boolean, (b) hash-pin the resulting cache file and stage a COPY at `discovery_data/<name>.build.json` (mirroring `v2_canonical_merges.build.json`'s exact precedent), (c) add a new `--novelty-verdicts <path>` (or similarly named) CLI flag to `build_discovery_sidecar.py` that ingests the pinned cache and records its SHA-256 in `meta`, exactly like `canonical_merges_sha256`/`composition_dates_sha256`/`seftja_dates_sha256` already do. The LLM-gate script itself never needs to enter the committed tree; only its OUTPUT artifact does, hash-pinned.
**Warning signs:** any plan task with a file-edit target under `scripts/discovery_identified_gate.py` or `scripts/title_gate_llm.py` literally — those paths don't exist; the real paths are under `same_work_spike/probe/scripts/`.

### Pitfall 5: The catalogue-never-evidence rule, in both directions
**What goes wrong (forward direction, well-known):** using an FJMS/PGP/NLI catalogue entry as acceptance evidence for a computed identification's correctness.
**What goes wrong (reverse direction, specific to NOVEL-01/D-25, easy to miss under time pressure):** treating ABSENCE from the finding-aid check as evidence a claim IS correct ("nobody else found this, so it must be a real discovery"). D-23a/D-24 are explicit: novelty is orthogonal to correctness in BOTH directions — a novel claim can still be wrong, and a catalogued claim can still be right. A findings-page sort/filter/copy that implies "novel = more interesting/more likely correct" violates this even if it never touches the stored band.
**How to avoid:** D-15a already locks novelty as filter/group-only, never sort-by-default — verification for this is covered under Success Criterion 6/7 above (render-smoke assertions that novelty never changes row ORDER, only which rows are shown/grouped).

### Pitfall 6: Restricted-corpus names in ANY committed artifact this phase produces
**What goes wrong:** the M-source/R-source codename rule is easy to violate incidentally in a NEW artifact type this phase introduces (the compatibility attestation, the rebuild-preservation-expected JSON, the granularity-aliases JSON, the novelty-verdict-cache hash-pinned copy) simply because those artifact TYPES are new and haven't been through the masking-scan-by-construction discipline the existing frame docs have.
**How to avoid:** every new committed artifact this phase produces (the compatibility attestation, `136-REBUILD-PRESERVATION-EXPECTED.json`, `discovery_data/granularity_aliases.json`, any novelty-verdict-cache copy staged at `discovery_data/`) must contain ONLY opaque `w000xxx` ids, hashes, and counts/years — exactly the discipline `discovery-frames.md`/`discovery-frames-v2.md` already demonstrate. Run `check_atlas_masking.py --scan-repo` after creating each one, before committing.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | The graded CERT-01 deck's manifest (needed to bind cards to claims for the rebuild-preservation gate) lives at `same_work_spike/probe/review/cert01_deck_verdicts.json` or an adjacent path under the same gitignored tree | Validation Architecture, Success Criterion 1 | If the actual path differs, the card-binding step of the rebuild-preservation gate needs a path correction at execution time — low risk, easily discovered by listing the directory, does not change the mechanism |
| A2 | The rebuild-preservation full-table hash diff completes in a reasonable time (minutes, not hours) at 297K evidence rows, by analogy to the existing verifier's comparable full-table scans | Validation Architecture, Success Criterion 1 | If wrong, gate 2 needs either a sampling strategy or a longer CI budget than assumed — this is explicitly flagged as unmeasured in the text above, not asserted as fact |
| A3 | A title-containment/token-overlap heuristic over `neutral_title` will correctly auto-detect MOST (not all) same-work-different-granularity pairs, with an owner-reviewed allowlist covering the residual | Three Technical Unknowns, #1 | If the heuristic's false-positive/false-negative rate is high, gate 1 may need to lean more heavily on the owner-reviewed allowlist alone (pure lookup table, no heuristic) — this only affects the SIZE of the manual review burden, not the mechanism's soundness |

**Note on `[ASSUMED]` tagging:** every other claim in this document above the Assumptions Log line was verified directly against a tool call (file read, grep, git check-ignore) during this research session and is tagged implicitly HIGH confidence by virtue of that verification — this document does not carry package-installation claims (`[VERIFIED: npm registry]`-style provenance is not applicable; this phase installs no new external packages).

## Open Questions (RESOLVED 2026-08-02 — both closed in planning)

> **Q1 → resolved by plan `136-03` Task 1**, which resolves the verdict-manifest path dynamically at
> execution time (grep-based, `--cards`, raising a loud violation unless explicitly `SKIP`) rather than
> hardcoding a path this research could not confirm. **Q2 → resolved by plans `136-21` Task 3 and
> `136-31` Task 3**, which adopt this section's own recommendation: probe for the tool, and on absence
> skip **loudly** — naming the missing tool and recording the residual risk in the verification report
> — so the Phase 133 atlas's silent skip is not repeated.
>
> ⚠ **Staleness notice (2026-08-02).** This document was written 2026-07-31 and is **stale on the six
> items** the owner settled 2026-08-01/02: the two-bucket main-pool model (the three-level confidence
> scale was retired), tooltip-only band labels, the deleted tier filter, licence-gated reference-text
> rendering, the five added rebuild fields, and the settled findings page. See the **AMENDMENT
> 2026-08-02** block at the top of `136-CONTEXT.md` `<decisions>`, which supersedes. Use this file for
> codebase orientation, integration points and the validation architecture only.

1. **Exact path of the CERT-01 graded-deck verdict manifest for card-binding (Success Criterion 1, step 5).**
   - What we know: the deck lives in the gitignored `same_work_spike/probe/` tree per `docs/specs/discovery-coordination.md`; `verify_cert01_grading.py` references a verdicts file.
   - What's unclear: the exact filename/relative path was not directly confirmed by reading `verify_cert01_grading.py` end-to-end (time-bounded research pass read only the check-10 region).
   - Recommendation: `grep -n "verdicts\|deck_verdicts\|cert01_deck" scripts/verify_cert01_grading.py` at plan/execution time to pin the exact path before writing the rebuild-preservation script's card-binding step.

2. **Whether Playwright (or an equivalent browser-DOM capture tool) is installed on the dev/build box for the masking scan's interaction-created-string coverage.**
   - What we know: STATE.md records this was SKIPPED for the Phase 133 atlas ("the ONLY surface that sees interaction-created catalogue strings... it SKIPPED locally because Playwright is not installed").
   - What's unclear: whether this has been remedied since, and whether it will be available for this phase's 4 new/changed surfaces.
   - Recommendation: check `pip show playwright` / `npm ls playwright` at execution gate 2 before assuming this coverage is achievable; if still absent, document the gap explicitly in the phase's verification report rather than silently omitting it (per Pitfall/lesson: don't repeat the atlas's silent skip).

## Contradictions found in locked decisions

None found. Every locked decision (D-01 through D-25) checked cleanly against current code state and current requirements text. The one area that LOOKS like a live contradiction — D-06 ("no precision percentages anywhere") vs. CERT-02's "R-A shows 0.889"/"tier-A goes public with its measured number" — is already explicitly flagged and dispositioned INSIDE CONTEXT.md itself (the `⟨CONFLICT FLAGGED⟩` sub-bullet on CERT-02, D-06b) as "not amended here, resolution owed at Phase 139" — this is a known, already-surfaced, already-owned deferral, not a new contradiction this research discovered.

## Sources

### Primary (HIGH confidence — direct code/doc reads at HEAD, 2026-07-31)
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-CONTEXT.md` — full read
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-CODEX-REVIEW.md` — full read
- `.planning/ROADMAP.md` §Phase 136 + §137/138/139 (cross-phase relationship)
- `.planning/REQUIREMENTS.md` — full discovery-requirements block, all `⟨AMENDED⟩` sub-bullets
- `.planning/STATE.md` (pages 1-170; Phase 133-136 decision log)
- `.planning/v9-PUBLICATION-STRATEGY.md` — full read
- `docs/specs/discovery-sidecar-schema-v1.md` — full read
- `docs/specs/discovery-band-labels-v1.md` — full read
- `docs/specs/discovery-budgets.md` — full read
- `docs/specs/discovery-deploy.md` — full read
- `docs/specs/discovery-frames.md` + `discovery-frames-v2.md` — full read
- `docs/specs/discovery-coordination.md` — full read
- `shared/discovery_service.py` — full read (1043 lines)
- `web/discovery.py` — full read
- `shared/discovery_band_labels.py` — full read
- `web/discovery_assets.py` — full read
- `web/atlas_assets.py` (partial, header + doctring) + `web/main.py` (nav list, `@ui.page` routes, `/atlas` route body, `/help` precision-rendering region)
- `web/pages/browse.py` (`highlight_text`, 1560-1624)
- `web/pages/browse_enrichment.py` — full read (645 lines)
- `web/pages/catalog_browse.py` (255-314)
- `scripts/build_discovery_sidecar.py` (targeted reads: 520-568, 790-860, 1316-1346, 1690-1780, 2495-2570, 2635-2720, 3855-3910, 4000-4045, 4405-4435)
- `scripts/verify_discovery_sidecar.py` (305-340, 540-570)
- `scripts/cert01_frame.py` (280-315)
- `scripts/verify_cert01_grading.py` (195-225)
- `scripts/check_atlas_masking.py` (self-test region 1292-1385)
- `same_work_spike/probe/scripts/discovery_identified_gate.py` (1-80, header + mechanism)
- `genizah_app.py` (grep for `_CatalogFacetWorker`)
- `.gitignore` (line 212, `same_work_spike/`)
- `git check-ignore -v` / `git ls-files` — direct confirmation of gitignore status
- `.planning/config.json` — `nyquist_validation: true` confirmed

### Secondary (MEDIUM confidence)
- Test-file existence/counts derived via `grep -c "^def test_"` on `tests/test_discovery_service.py` (44), `tests/test_discovery_band_labels.py` (26), `tests/test_discovery_build.py` (84) — counts, not content, verified

### Tertiary (LOW confidence — none used as load-bearing)
- None. Every claim above traces to a direct tool-call read during this session.

## Metadata

**Confidence breakdown:**
- Code-state verification: HIGH — every citation checked directly against HEAD via Read/Grep/Bash, not recalled from training or from CONTEXT.md's own assertions
- Validation Architecture mechanisms: HIGH for the parts that extend existing, already-working scripts (masking, benchmark, offline verifier); MEDIUM for the rebuild-preservation gate specifically, since no such script exists yet and its runtime is genuinely unmeasured
- Three technical unknowns: MEDIUM — mechanisms are reasoned from what data/code already exists, but none has been built or tested yet; explicitly flagged as recommendations for gate 1, not settled answers
- Pitfalls: HIGH — each is either a directly-observed code fact (gitignored scripts, naming collision, coverage-mechanism-exists-but-discarded) or an explicit, already-recorded project policy (asset-first deploy, immutable pre-registration, catalogue-never-evidence)

**Research date:** 2026-07-31
**Valid until:** ~14 days (this phase's own execution will change the exact line numbers cited here within days; the MECHANISMS recommended are stable regardless of line-number drift — re-verify citations with `grep -n`, don't trust the numbers in this document at execution time, per the drift already observed in `web/main.py`)
